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

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Environment variables for the three secrets (E10-D023). The command's flags override them.
const (
	// EnvEtcdPassword is the password of the etcd user "api".
	EnvEtcdPassword = "MFTIKO_ETCD_PASSWORD"
	// EnvCookieKey is the HMAC-SHA256 key every replica uses to sign the console cookie (E10-D009).
	EnvCookieKey = "MFTIKO_COOKIE_KEY"
	// EnvPostgresDSN is the Postgres connection string. Run opens a pool and does not wait for it to
	// connect. Readiness does not use it (E10-D048, modifies E10-D024).
	EnvPostgresDSN = "MFTIKO_POSTGRES_DSN"
)

// Defaults of the API JSON. A zero or omitted duration selects the default (E10-D017, E10-D014, E10-D027, E10-D029).
const (
	DefaultRetention        = 7 * 24 * time.Hour
	DefaultUnavailableAfter = 15 * time.Second
	DefaultListen           = ":8080"
	DefaultShutdownTimeout  = 10 * time.Second
)

// keyPrefix is the whole mftiko keyspace. The api role can read all of it, so one mirror per cluster watches
// this prefix and nothing narrower (E10-D025, PROTOCOL §3.1).
const keyPrefix = state.Root + "/"

// User is one operator. Token is the SHA-256 of their static token; the token itself is not stored (E10-D009).
type User struct {
	Name  string
	Token [32]byte
}

// Input is everything that is not the two JSON files: secrets, the web directory, TLS, and test knobs.
// A flag overrides the environment variable of the same secret (E10-D023).
type Input struct {
	EtcdPassword string
	CookieKey    string
	PostgresDSN  string
	WebDir       string // empty: API routes only (E10-D028)
	TLSCert      string // with TLSKey: HTTPS and HTTP/2; both empty: plaintext HTTP (E10-D029)
	TLSKey       string
	Mirror       state.MirrorOptions // zero: the Mirror defaults; tests shorten the probe
	Log          zerolog.Logger
	Now          func() time.Time // nil: time.Now
}

// Kind is one account kind the API lists and checks (E03-D001, E10-D034). cmd/mftiko-api fills it from the
// exchange registry. This package does not import exchange.
type Kind struct {
	Name       string
	Label      string
	Trades     []string
	KeyTypes   []string
	Passphrase bool
}

// Supported is true when the kind trades exactly one venue (E06-D014, E10-D011).
func (k Kind) Supported() bool { return len(k.Trades) == 1 }

// Config is the validated settings Run serves with. The secret fields are credentials; do not log a Config.
type Config struct {
	Topology         state.Topology
	Planes           map[string]state.Plane
	Users            []User
	Allow            []string // strategy repository allowlist; "*" alone allows every path on Registry (E10-D119)
	TDKeys           map[string]sealbox.PublicKey
	Kinds            []Kind // not in the JSON; the command sets it (E10-D034). Empty rejects every kind.
	Retention        time.Duration
	UnavailableAfter time.Duration
	Listen           string
	ShutdownTimeout  time.Duration
	Registry         artifact.Registry // zero address: catalog requests report registry_unavailable (E10-D060)
	Catalog          *artifact.Catalog // set when Registry is configured; nil does not dial
	EtcdPassword     string
	CookieKey        string
	PostgresDSN      string
	WebDir           string
	TLSCert          string
	TLSKey           string
	Mirror           state.MirrorOptions
	Log              zerolog.Logger
	Now              func() time.Time
	// ReconcileEvery is the full compare interval. Zero means 30 seconds (E10-D070).
	// It is not a deployment setting and is not read from JSON. Tests set it.
	ReconcileEvery time.Duration
}

// deployment is the mftiko-etcd-roles input (E13-D012). Planes names every instance except the controller.
type deployment struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

// configFile is the -config JSON (E10-D023). Durations are Go duration strings; empty or zero means the default.
type configFile struct {
	Users            []configUser      `json:"users"`
	Allow            []string          `json:"allow"`
	TDKeys           map[string]string `json:"td_keys"`
	Retention        string            `json:"retention"`
	UnavailableAfter string            `json:"unavailable_after"`
	Listen           string            `json:"listen"`
	ShutdownTimeout  string            `json:"shutdown_timeout"`
	Registry         string            `json:"registry"`
	RegistryCreds    string            `json:"registry_creds"`
	PlainHTTP        bool              `json:"plain_http"`
}

type configUser struct {
	Name        string `json:"name"`
	TokenSHA256 string `json:"token_sha256"`
}

// Load reads the deployment JSON and the API JSON, applies in, and checks the whole configuration (E10-D024).
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
	allow, err := normalizeAllow(f.Allow, f.Registry)
	if err != nil {
		return Config{}, err
	}
	keys, err := parseTDKeys(f.TDKeys)
	if err != nil {
		return Config{}, err
	}
	reg, cat, err := openRegistry(f.Registry, f.RegistryCreds, f.PlainHTTP)
	if err != nil {
		return Config{}, err
	}
	return Config{
		Topology: dep.Topology, Planes: dep.Planes, Users: users, Allow: allow,
		TDKeys: keys, Retention: retention, UnavailableAfter: unavailable, Listen: listen, ShutdownTimeout: shutdown,
		Registry: reg, Catalog: cat,
	}, nil
}

// openRegistry loads the catalog client when any registry field is set (E10-D060). An empty address with
// no credentials and plain_http false leaves the client nil: existing configs keep loading. The client
// is not dialled here (E10-D024).
func openRegistry(address, creds string, plain bool) (artifact.Registry, *artifact.Catalog, error) {
	reg := artifact.Registry{Address: address, Credentials: creds, PlainHTTP: plain}
	if address == "" && creds == "" && !plain {
		return reg, nil, nil
	}
	if address == "" {
		return artifact.Registry{}, nil, errors.New("registry address is required")
	}
	cat, err := artifact.OpenCatalog(reg)
	if err != nil {
		return artifact.Registry{}, nil, err
	}
	return reg, cat, nil
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

// repositoryShape rejects an empty repository, whitespace, and empty, "." or ".." segments (E10-D119).
func repositoryShape(repository string) error {
	if repository == "" || strings.ContainsAny(repository, " \t\r\n") {
		return fmt.Errorf("invalid repository %q", repository)
	}
	parts := strings.Split(repository, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return fmt.Errorf("invalid repository %q", repository)
		}
	}
	return nil
}

func lastPathSegment(repository string) string {
	repository = strings.TrimRight(repository, "/")
	if i := strings.LastIndex(repository, "/"); i >= 0 {
		return repository[i+1:]
	}
	return repository
}

// normalizeAllow checks the strategy allowlist (E10-D119). "*" must be the only entry and does not
// require a registry address. Any other entry is a registry host plus an optional path prefix.
// A trailing slash is not part of the prefix. A concrete prefix requires Registry and must be that
// address or a path under it. The returned slice is what requests are matched against.
func normalizeAllow(allow []string, registry string) ([]string, error) {
	stars := 0
	for _, raw := range allow {
		if raw == "*" {
			stars++
		}
	}
	if stars > 0 {
		if stars != 1 || len(allow) != 1 {
			return nil, errors.New("allow * must be the only entry")
		}
		return []string{"*"}, nil
	}
	out := make([]string, 0, len(allow))
	seen := map[string]bool{}
	reg := strings.TrimRight(registry, "/")
	for _, raw := range allow {
		entry := strings.TrimRight(raw, "/")
		if err := repositoryShape(entry); err != nil {
			return nil, err
		}
		if seen[entry] {
			return nil, fmt.Errorf("duplicate allow entry %q", entry)
		}
		seen[entry] = true
		if reg == "" {
			return nil, errors.New("registry address is required")
		}
		if entry != reg && !strings.HasPrefix(entry, reg+"/") {
			return nil, fmt.Errorf("allow entry %q is not on the configured registry", raw)
		}
		out = append(out, entry)
	}
	return out, nil
}

// allowMatches reports whether repository is on the allowlist (E10-D119).
// A prefix matches the repository itself and any path under it. "*" matches every repository.
// Trailing slashes are not part of the comparison.
func allowMatches(allow []string, repository string) bool {
	repository = strings.TrimRight(repository, "/")
	for _, entry := range allow {
		if entry == "*" {
			return true
		}
		prefix := strings.TrimRight(entry, "/")
		if repository == prefix || strings.HasPrefix(repository, prefix+"/") {
			return true
		}
	}
	return false
}

// pathOnRegistry returns the path stored in the session spec: repository without the configured
// registry host (E10-D119). STS pulls that path from its own registry. A host that is not Address
// is not ok.
func pathOnRegistry(address, repository string) (string, bool) {
	address = strings.TrimRight(address, "/")
	repository = strings.TrimRight(repository, "/")
	if address == "" || repository == "" {
		return "", false
	}
	rest, ok := strings.CutPrefix(repository, address+"/")
	if !ok || repositoryShape(rest) != nil {
		return "", false
	}
	return rest, true
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
	if _, err := normalizeAllow(c.Allow, c.Registry.Address); err != nil {
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
