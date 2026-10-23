package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Environment variables for the three secrets (D1023). The command's flags override them.
const (
	// EnvEtcdPassword is the password of the etcd user "api".
	EnvEtcdPassword = "MFTIKO_ETCD_PASSWORD"
	// EnvCookieKey is the HMAC-SHA256 key every replica uses to sign the console cookie (D1009).
	EnvCookieKey = "MFTIKO_COOKIE_KEY"
	// EnvPostgresDSN is the Postgres connection string. Run opens a pool and does not wait for it to
	// connect. Readiness does not use it (D1048, modifies D1024).
	EnvPostgresDSN = "MFTIKO_POSTGRES_DSN"
)

// Defaults of the API JSON. A zero or omitted duration selects the default (D1017, D1014, D1027, D1029).
const (
	DefaultRetention        = 7 * 24 * time.Hour
	DefaultUnavailableAfter = 15 * time.Second
	DefaultListen           = ":8080"
	DefaultShutdownTimeout  = 10 * time.Second
)

// keyPrefix is the whole mftiko keyspace. The api role can read all of it, so one mirror per cluster watches
// this prefix and nothing narrower (D1025, PROTOCOL §3.1).
const keyPrefix = state.Root + "/"

// User is one operator. Token is the SHA-256 of their static token; the token itself is not stored (D1009).
type User struct {
	Name  string
	Token [32]byte
}

// Input is everything that is not the two JSON files: secrets, the web directory, TLS, and test knobs.
// A flag overrides the environment variable of the same secret (D1023).
type Input struct {
	EtcdPassword string
	CookieKey    string
	PostgresDSN  string
	WebDir       string // empty: API routes only (D1028)
	TLSCert      string // with TLSKey: HTTPS and HTTP/2; both empty: plaintext HTTP (D1029)
	TLSKey       string
	Mirror       state.MirrorOptions // zero: the Mirror defaults; tests shorten the probe
	Log          zerolog.Logger
	Now          func() time.Time // nil: time.Now
}

// Config is the validated settings Run serves with. The secret fields are credentials; do not log a Config.
type Config struct {
	Topology         state.Topology
	Planes           map[string]state.Plane
	Users            []User
	Repositories     []string
	TDKeys           map[string]sealbox.PublicKey
	Retention        time.Duration
	UnavailableAfter time.Duration
	Listen           string
	ShutdownTimeout  time.Duration
	EtcdPassword     string
	CookieKey        string
	PostgresDSN      string
	WebDir           string
	TLSCert          string
	TLSKey           string
	Mirror           state.MirrorOptions
	Log              zerolog.Logger
	Now              func() time.Time
}

// deployment is the mftiko-etcd-roles input (D1312). Planes names every instance except the controller.
type deployment struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

// configFile is the -config JSON (D1023). Durations are Go duration strings; empty or zero means the default.
type configFile struct {
	Users            []configUser      `json:"users"`
	Repositories     []string          `json:"repositories"`
	TDKeys           map[string]string `json:"td_keys"`
	Retention        string            `json:"retention"`
	UnavailableAfter string            `json:"unavailable_after"`
	Listen           string            `json:"listen"`
	ShutdownTimeout  string            `json:"shutdown_timeout"`
}

type configUser struct {
	Name        string `json:"name"`
	TokenSHA256 string `json:"token_sha256"`
}

// Load reads the deployment JSON and the API JSON, applies in, and checks the whole configuration (D1024).
// It does not dial etcd, Postgres or a registry. deployment and config are each one JSON value; an unknown
// field is an error.
func Load(deployment, config io.Reader, in Input) (Config, error) {
	dep, err := decodeDeployment(deployment)
	if err != nil {
		return Config{}, err
	}
	file, err := decodeConfig(config)
	if err != nil {
		return Config{}, err
	}
	cfg, err := file.config(dep)
	if err != nil {
		return Config{}, err
	}
	cfg.EtcdPassword = in.EtcdPassword
	cfg.CookieKey = in.CookieKey
	cfg.PostgresDSN = in.PostgresDSN
	cfg.WebDir = in.WebDir
	cfg.TLSCert = in.TLSCert
	cfg.TLSKey = in.TLSKey
	cfg.Mirror = in.Mirror
	cfg.Log = in.Log
	cfg.Now = in.Now
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func decodeDeployment(r io.Reader) (deployment, error) {
	var d deployment
	if err := decodeJSON(r, &d); err != nil {
		return deployment{}, fmt.Errorf("read deployment: %w", err)
	}
	return d, nil
}

func decodeConfig(r io.Reader) (configFile, error) {
	var f configFile
	if err := decodeJSON(r, &f); err != nil {
		return configFile{}, fmt.Errorf("read config: %w", err)
	}
	return f, nil
}

func decodeJSON(r io.Reader, out any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("extra data after JSON value")
	}
	return nil
}

func (f configFile) config(dep deployment) (Config, error) {
	retention, err := parseDuration("retention", f.Retention, DefaultRetention)
	if err != nil {
		return Config{}, err
	}
	unavailable, err := parseDuration("unavailable_after", f.UnavailableAfter, DefaultUnavailableAfter)
	if err != nil {
		return Config{}, err
	}
	shutdown, err := parseDuration("shutdown_timeout", f.ShutdownTimeout, DefaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}
	listen, err := parseListen(f.Listen)
	if err != nil {
		return Config{}, err
	}
	users, err := parseUsers(f.Users)
	if err != nil {
		return Config{}, err
	}
	if err := validateRepos(f.Repositories); err != nil {
		return Config{}, err
	}
	keys, err := parseTDKeys(f.TDKeys)
	if err != nil {
		return Config{}, err
	}
	return Config{
		Topology: dep.Topology, Planes: dep.Planes, Users: users, Repositories: append([]string(nil), f.Repositories...),
		TDKeys: keys, Retention: retention, UnavailableAfter: unavailable, Listen: listen, ShutdownTimeout: shutdown,
	}, nil
}

func parseDuration(field, s string, def time.Duration) (time.Duration, error) {
	if s == "" || s == "0" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", field, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s: negative duration %s", field, s)
	}
	if d == 0 {
		return def, nil
	}
	return d, nil
}

func parseListen(s string) (string, error) {
	if s == "" {
		return DefaultListen, nil
	}
	if _, _, err := net.SplitHostPort(s); err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	return s, nil
}

func parseUsers(in []configUser) ([]User, error) {
	users := make([]User, 0, len(in))
	names := map[string]bool{}
	tokens := map[[32]byte]string{}
	for _, u := range in {
		if u.Name == "" {
			return nil, errors.New("user name is empty")
		}
		if names[u.Name] {
			return nil, fmt.Errorf("duplicate user %q", u.Name)
		}
		token, err := parseToken(u.TokenSHA256)
		if err != nil {
			return nil, fmt.Errorf("user %q: %w", u.Name, err)
		}
		if prev, ok := tokens[token]; ok {
			return nil, fmt.Errorf("users %q and %q have the same token hash", prev, u.Name)
		}
		names[u.Name] = true
		tokens[token] = u.Name
		users = append(users, User{Name: u.Name, Token: token})
	}
	return users, nil
}

func parseToken(s string) ([32]byte, error) {
	var out [32]byte
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(out) {
		return out, errors.New("token_sha256 must be 64 hex characters")
	}
	copy(out[:], b)
	return out, nil
}

func validateRepos(repos []string) error {
	seen := map[string]bool{}
	for _, r := range repos {
		if r == "" || strings.ContainsAny(r, " \t\r\n") {
			return fmt.Errorf("invalid repository %q", r)
		}
		if seen[r] {
			return fmt.Errorf("duplicate repository %q", r)
		}
		seen[r] = true
	}
	return nil
}

func parseTDKeys(in map[string]string) (map[string]sealbox.PublicKey, error) {
	out := make(map[string]sealbox.PublicKey, len(in))
	for _, inst := range slices.Sorted(maps.Keys(in)) {
		pub, err := sealbox.ParsePublicKey(in[inst])
		if err != nil {
			return nil, fmt.Errorf("td key %s: %w", inst, err)
		}
		out[inst] = pub
	}
	return out, nil
}

func (c Config) validate() error {
	if _, err := state.AuthTables(c.Topology, c.Planes); err != nil {
		return err
	}
	if err := validateUsers(c.Users); err != nil {
		return err
	}
	if err := validateRepos(c.Repositories); err != nil {
		return err
	}
	if err := validateTDKeys(c.Planes, c.TDKeys); err != nil {
		return err
	}
	if c.Retention <= 0 || c.UnavailableAfter <= 0 || c.ShutdownTimeout <= 0 {
		return errors.New("retention, unavailable_after and shutdown_timeout must be positive")
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if c.Mirror.PageSize < 0 || c.Mirror.MinBackoff < 0 || c.Mirror.MaxBackoff < 0 || c.Mirror.ProbeInterval < 0 {
		return errors.New("negative mirror option")
	}
	if c.EtcdPassword == "" {
		return errors.New("etcd password is required")
	}
	if c.CookieKey == "" {
		return errors.New("cookie key is required")
	}
	if c.PostgresDSN == "" {
		return errors.New("postgres DSN is required")
	}
	if _, err := pgx.ParseConfig(c.PostgresDSN); err != nil {
		// ParseConfig's error quotes the string, which holds the password. Say only that it did not parse.
		return errors.New("postgres DSN is not a valid connection string")
	}
	if err := checkWeb(c.WebDir); err != nil {
		return err
	}
	return checkTLS(c.TLSCert, c.TLSKey)
}

func validateUsers(users []User) error {
	names := map[string]bool{}
	tokens := map[[32]byte]string{}
	for _, u := range users {
		if u.Name == "" {
			return errors.New("user name is empty")
		}
		if names[u.Name] {
			return fmt.Errorf("duplicate user %q", u.Name)
		}
		if prev, ok := tokens[u.Token]; ok {
			return fmt.Errorf("users %q and %q have the same token hash", prev, u.Name)
		}
		names[u.Name] = true
		tokens[u.Token] = u.Name
	}
	return nil
}

func validateTDKeys(planes map[string]state.Plane, keys map[string]sealbox.PublicKey) error {
	want := map[string]bool{}
	for inst, p := range planes {
		if p == state.PlaneTD {
			want[inst] = true
		}
	}
	var extra []string
	for inst := range keys {
		if !want[inst] {
			extra = append(extra, inst)
		}
	}
	if len(extra) > 0 {
		slices.Sort(extra)
		return fmt.Errorf("td key for %s, which is not a td instance", extra[0])
	}
	var missing []string
	for inst := range want {
		if _, ok := keys[inst]; !ok {
			missing = append(missing, inst)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return fmt.Errorf("td instance %s has no public key", missing[0])
	}
	return nil
}

func checkWeb(dir string) error {
	if dir == "" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("-web: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("-web: %s is not a directory", dir)
	}
	return nil
}

func checkTLS(cert, key string) error {
	switch {
	case cert == "" && key == "":
		return nil
	case cert == "" || key == "":
		return errors.New("-tls-cert and -tls-key must both be set")
	}
	if err := regularFile(cert); err != nil {
		return fmt.Errorf("-tls-cert: %w", err)
	}
	if err := regularFile(key); err != nil {
		return fmt.Errorf("-tls-key: %w", err)
	}
	return nil
}

func regularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a file", path)
	}
	return nil
}
