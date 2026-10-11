package natsdev

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const (
	// AccountName is the one data-plane account (D414).
	AccountName = "MFTIKO"
	// SystemName is the system account used by the resolver and server monitoring.
	SystemName = "SYS"
	// OperatorName is the dev operator's name.
	OperatorName = "mftiko-dev"

	// PlaneMD and PlaneFetch are the platform inbox plane tokens (D416).
	// md.fetch is not an etcd plane; "fetch" keeps its inbox off the MD prefix.
	PlaneMD    = "md"
	PlaneFetch = "fetch"

	// DefaultInst is the instance token make dev-up generates.
	DefaultInst = "dev"

	// ContainerOperator and ContainerResolver are paths inside the dev container.
	// The generated server config names these, and compose bind-mounts the host files there.
	ContainerOperator = "/etc/nats/operator.jwt"
	ContainerResolver = "/data/resolver"

	composeHost = "0.0.0.0"
	composePort = 4222
	composeHTTP = 8222
	serverName  = "dev"

	fileOperatorJWT = "operator.jwt"
	fileConfig      = "nats-server.conf"
	fileManifest    = "manifest.json"
	dirResolver     = "resolver"
	dirKeys         = "keys"
	dirCreds        = "creds"

	keyOperator = "operator.nk"
	keySystem   = "sys.nk"
	keyAccount  = "mftiko.nk"
	keySigning  = "mftiko-signing.nk"

	// responseMaxMsgs and responseTTL are nats-server's allow_responses defaults
	// (one reply, two minutes). MD and md.fetch need them because D416 does not
	// grant publish on _INBOX.>. They are not a separate decision.
	responseMaxMsgs = 1
	responseTTL     = 2 * time.Minute
)

// instToken is one NATS subject token and one path segment.
var instToken = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Options selects which instance tokens Generate issues. Nil lists mean DefaultInst.
// An empty list is rejected.
type Options struct {
	MDInsts    []string
	FetchInsts []string
}

// User is one generated user. Paths are relative to the output directory and hold no seeds.
type User struct {
	Inst        string `json:"inst"`
	Name        string `json:"name"`
	PublicKey   string `json:"public_key"`
	Creds       string `json:"creds"`
	InboxPrefix string `json:"inbox_prefix"`
}

// Manifest is the public description of one generated directory.
type Manifest struct {
	Operator       string `json:"operator"`
	System         string `json:"system"`
	Account        string `json:"account"`
	SigningKey     string `json:"signing_key"`
	SigningKeyFile string `json:"signing_key_file"`
	MD             []User `json:"md"`
	Fetch          []User `json:"fetch"`
}

// Layout is a generated directory plus its manifest.
type Layout struct {
	Dir      string
	Manifest Manifest
}

// ServerConfig is the input to FormatServerConfig. Paths are written into the
// config as given, so the compose file and the CI server can differ only there.
type ServerConfig struct {
	OperatorFile  string
	SystemAccount string
	ResolverDir   string
	Host          string
	Port          int
	HTTPPort      int // zero omits http_port
	ServerName    string
}

// InboxPrefix is the platform reply prefix _INBOX_{plane}_{inst} (D416).
// plane and inst are each one token. The result has no trailing dot.
func InboxPrefix(plane, inst string) (string, error) {
	if !tokenOK(plane) || !tokenOK(inst) {
		return "", errors.New("natsdev: inbox prefix needs a plane token and an instance token")
	}
	return "_INBOX_" + plane + "_" + inst, nil
}

// Generate writes operator, account, and user material into dir.
// Keys that are already there are reused, and JWTs and creds are reissued,
// so a later dev-up does not rotate the account identity. A seed file that
// does not parse is an error and is left untouched.
func Generate(dir string, opt Options) error {
	if dir == "" {
		return errors.New("natsdev: empty directory")
	}
	mdInsts, err := normalizeInsts(opt.MDInsts)
	if err != nil {
		return err
	}
	fetchInsts, err := normalizeInsts(opt.FetchInsts)
	if err != nil {
		return err
	}
	// Parents created with the keys directory would otherwise be 0700. The output
	// directory itself only holds public files beside the secret subdirectories.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, dirKeys), 0o700); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(dir, dirKeys), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, dirCreds), 0o700); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(dir, dirCreds), 0o700); err != nil {
		return err
	}
	resolver := filepath.Join(dir, dirResolver)
	if err := os.MkdirAll(resolver, 0o777); err != nil {
		return err
	}
	// 0777, not the umask-clipped result of MkdirAll: the container user must be able to write.
	if err := os.Chmod(resolver, 0o777); err != nil {
		return err
	}

	operatorKP, err := ensureKey(filepath.Join(dir, dirKeys, keyOperator), keyOperatorKind, nkeys.CreateOperator)
	if err != nil {
		return err
	}
	systemKP, err := ensureKey(filepath.Join(dir, dirKeys, keySystem), keyAccountKind, nkeys.CreateAccount)
	if err != nil {
		return err
	}
	accountKP, err := ensureKey(filepath.Join(dir, dirKeys, keyAccount), keyAccountKind, nkeys.CreateAccount)
	if err != nil {
		return err
	}
	signingKP, err := ensureKey(filepath.Join(dir, dirKeys, keySigning), keyAccountKind, nkeys.CreateAccount)
	if err != nil {
		return err
	}

	mdUsers := make([]loadedUser, 0, len(mdInsts))
	for _, inst := range mdInsts {
		u, err := ensureServiceUser(dir, PlaneMD, inst)
		if err != nil {
			return err
		}
		mdUsers = append(mdUsers, u)
	}
	fetchUsers := make([]loadedUser, 0, len(fetchInsts))
	for _, inst := range fetchInsts {
		u, err := ensureServiceUser(dir, PlaneFetch, inst)
		if err != nil {
			return err
		}
		fetchUsers = append(fetchUsers, u)
	}

	opPub, err := operatorKP.PublicKey()
	if err != nil {
		return err
	}
	sysPub, err := systemKP.PublicKey()
	if err != nil {
		return err
	}
	accPub, err := accountKP.PublicKey()
	if err != nil {
		return err
	}
	signPub, err := signingKP.PublicKey()
	if err != nil {
		return err
	}

	opJWT, err := encodeOperator(opPub, operatorKP)
	if err != nil {
		return err
	}
	sysJWT, err := encodeSystem(sysPub, operatorKP)
	if err != nil {
		return err
	}
	accJWT, err := encodeAccount(accPub, signPub, operatorKP)
	if err != nil {
		return err
	}
	if err := writePublic(filepath.Join(dir, fileOperatorJWT), []byte(opJWT)); err != nil {
		return err
	}
	if err := writePublic(filepath.Join(resolver, sysPub+".jwt"), []byte(sysJWT)); err != nil {
		return err
	}
	if err := writePublic(filepath.Join(resolver, accPub+".jwt"), []byte(accJWT)); err != nil {
		return err
	}
	if err := removeOtherJWT(resolver, sysPub+".jwt", accPub+".jwt"); err != nil {
		return err
	}

	manifest := Manifest{
		Operator:       opPub,
		System:         sysPub,
		Account:        accPub,
		SigningKey:     signPub,
		SigningKeyFile: filepath.ToSlash(filepath.Join(dirKeys, keySigning)),
		MD:             make([]User, 0, len(mdUsers)),
		Fetch:          make([]User, 0, len(fetchUsers)),
	}
	keepCreds := map[string]bool{}
	for _, u := range mdUsers {
		entry, err := writeServiceUser(dir, u, accountKP, true)
		if err != nil {
			return err
		}
		manifest.MD = append(manifest.MD, entry)
		keepCreds[filepath.Base(entry.Creds)] = true
	}
	for _, u := range fetchUsers {
		entry, err := writeServiceUser(dir, u, accountKP, false)
		if err != nil {
			return err
		}
		manifest.Fetch = append(manifest.Fetch, entry)
		keepCreds[filepath.Base(entry.Creds)] = true
	}
	if err := removeOther(filepath.Join(dir, dirCreds), keepCreds); err != nil {
		return err
	}

	conf, err := FormatServerConfig(ServerConfig{
		OperatorFile:  ContainerOperator,
		SystemAccount: sysPub,
		ResolverDir:   ContainerResolver,
		Host:          composeHost,
		Port:          composePort,
		HTTPPort:      composeHTTP,
		ServerName:    serverName,
	})
	if err != nil {
		return err
	}
	if err := writePublic(filepath.Join(dir, fileConfig), []byte(conf)); err != nil {
		return err
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return writePublic(filepath.Join(dir, fileManifest), body)
}

// Load reads a directory produced by Generate.
func Load(dir string) (Layout, error) {
	if dir == "" {
		return Layout{}, errors.New("natsdev: empty directory")
	}
	path := filepath.Join(dir, fileManifest)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Layout{}, fmt.Errorf("natsdev: %s is missing; run make dev-up", path)
	}
	if err != nil {
		return Layout{}, fmt.Errorf("natsdev: read manifest: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return Layout{}, fmt.Errorf("natsdev: manifest: %w", err)
	}
	if err := m.validate(); err != nil {
		return Layout{}, err
	}
	return Layout{Dir: dir, Manifest: m}, nil
}

// Summary is the human-readable description Generate prints. It has public keys and
// relative paths, not seeds.
func (m Manifest) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "operator %s\n", m.Operator)
	fmt.Fprintf(&b, "system %s %s\n", SystemName, m.System)
	fmt.Fprintf(&b, "account %s %s\n", AccountName, m.Account)
	fmt.Fprintf(&b, "signing key %s (%s)\n", m.SigningKey, m.SigningKeyFile)
	for _, u := range m.MD {
		fmt.Fprintf(&b, "md %s inbox %s creds %s\n", u.Inst, u.InboxPrefix, u.Creds)
	}
	for _, u := range m.Fetch {
		fmt.Fprintf(&b, "fetch %s inbox %s creds %s\n", u.Inst, u.InboxPrefix, u.Creds)
	}
	fmt.Fprintf(&b, "config %s\n", fileConfig)
	return b.String()
}

// FormatServerConfig renders the dev server config: a full resolver, one operator,
// the system account, and no TLS, cluster, or JetStream. Port -1 asks the OS for a port.
func FormatServerConfig(c ServerConfig) (string, error) {
	if c.OperatorFile == "" || c.SystemAccount == "" || c.ResolverDir == "" || c.Host == "" || c.ServerName == "" {
		return "", errors.New("natsdev: incomplete server config")
	}
	if strings.ContainsAny(c.SystemAccount, " \t\r\n\"'#") {
		return "", errors.New("natsdev: system account is not a single token")
	}
	op, err := quoteConf(c.OperatorFile)
	if err != nil {
		return "", err
	}
	dir, err := quoteConf(c.ResolverDir)
	if err != nil {
		return "", err
	}
	host, err := quoteConf(c.Host)
	if err != nil {
		return "", err
	}
	name, err := quoteConf(c.ServerName)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "host: %s\nport: %d\n", host, c.Port)
	if c.HTTPPort != 0 {
		fmt.Fprintf(&b, "http_port: %d\n", c.HTTPPort)
	}
	fmt.Fprintf(&b, "server_name: %s\n", name)
	fmt.Fprintf(&b, "operator: %s\n", op)
	fmt.Fprintf(&b, "system_account: %s\n", c.SystemAccount)
	fmt.Fprintf(&b, "resolver: {\n  type: full\n  dir: %s\n  allow_delete: false\n  interval: \"2m\"\n}\n", dir)
	return b.String(), nil
}

func (m Manifest) validate() error {
	if !nkeys.IsValidPublicOperatorKey(m.Operator) ||
		!nkeys.IsValidPublicAccountKey(m.System) ||
		!nkeys.IsValidPublicAccountKey(m.Account) ||
		!nkeys.IsValidPublicAccountKey(m.SigningKey) {
		return errors.New("natsdev: manifest public keys are incomplete")
	}
	if err := safeRel(m.SigningKeyFile); err != nil {
		return err
	}
	if len(m.MD) == 0 || len(m.Fetch) == 0 {
		return errors.New("natsdev: manifest needs an MD user and an md.fetch user")
	}
	if err := validateUsers(PlaneMD, m.MD); err != nil {
		return err
	}
	return validateUsers(PlaneFetch, m.Fetch)
}

func validateUsers(plane string, users []User) error {
	seen := map[string]bool{}
	for _, u := range users {
		if !tokenOK(u.Inst) || seen[u.Inst] {
			return fmt.Errorf("natsdev: bad manifest instance %q", u.Inst)
		}
		seen[u.Inst] = true
		if !nkeys.IsValidPublicUserKey(u.PublicKey) {
			return fmt.Errorf("natsdev: manifest user %s is missing a public key", u.Inst)
		}
		want, err := InboxPrefix(plane, u.Inst)
		if err != nil {
			return err
		}
		if u.InboxPrefix != want {
			return fmt.Errorf("natsdev: manifest inbox for %s/%s is %q", plane, u.Inst, u.InboxPrefix)
		}
		if u.Name != plane+"-"+u.Inst {
			return fmt.Errorf("natsdev: manifest name for %s/%s is %q", plane, u.Inst, u.Name)
		}
		if err := safeRel(u.Creds); err != nil {
			return err
		}
		if u.Creds != credsRel(plane, u.Inst) {
			return fmt.Errorf("natsdev: manifest creds for %s/%s is %q", plane, u.Inst, u.Creds)
		}
	}
	return nil
}

func normalizeInsts(in []string) ([]string, error) {
	if in == nil {
		return []string{DefaultInst}, nil
	}
	if len(in) == 0 {
		return nil, errors.New("natsdev: empty instance list")
	}
	seen := map[string]bool{}
	out := make([]string, len(in))
	for i, inst := range in {
		if !tokenOK(inst) {
			return nil, fmt.Errorf("natsdev: instance %q is not a single token", inst)
		}
		if seen[inst] {
			return nil, fmt.Errorf("natsdev: duplicate instance %q", inst)
		}
		seen[inst] = true
		out[i] = inst
	}
	return out, nil
}

func tokenOK(s string) bool {
	return instToken.MatchString(s)
}

type keyKind int

const (
	keyOperatorKind keyKind = iota
	keyAccountKind
	keyUserKind
)

type loadedUser struct {
	inst  string
	plane string
	kp    nkeys.KeyPair
	pub   string
	inbox string
	creds string
	name  string
}

func ensureServiceUser(dir, plane, inst string) (loadedUser, error) {
	inbox, err := InboxPrefix(plane, inst)
	if err != nil {
		return loadedUser{}, err
	}
	name := plane + "-" + inst
	path := filepath.Join(dir, dirKeys, "user-"+name+".nk")
	kp, err := ensureKey(path, keyUserKind, nkeys.CreateUser)
	if err != nil {
		return loadedUser{}, err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return loadedUser{}, err
	}
	return loadedUser{
		inst:  inst,
		plane: plane,
		kp:    kp,
		pub:   pub,
		inbox: inbox,
		creds: credsRel(plane, inst),
		name:  name,
	}, nil
}

func credsRel(plane, inst string) string {
	return filepath.ToSlash(filepath.Join(dirCreds, plane+"-"+inst+".creds"))
}

func writeServiceUser(dir string, u loadedUser, accountKP nkeys.KeyPair, md bool) (User, error) {
	seed, err := u.kp.Seed()
	if err != nil {
		return User{}, err
	}
	var pub, sub []string
	if md {
		pub = []string{"md.f." + u.inst + ".>", "sym"}
		sub = []string{"md.view." + u.inst + ".>", u.inbox + ".>"}
	} else {
		pub = []string{"sym"}
		sub = []string{"md.fetch", u.inbox + ".>"}
	}
	// Signed by the account identity key, so IssuerAccount stays empty (D414).
	token, err := encodeUser(u.pub, u.name, pub, sub, true, "", accountKP)
	if err != nil {
		return User{}, err
	}
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		return User{}, err
	}
	if err := writeSecret(filepath.Join(dir, filepath.FromSlash(u.creds)), creds); err != nil {
		return User{}, err
	}
	return User{
		Inst:        u.inst,
		Name:        u.name,
		PublicKey:   u.pub,
		Creds:       u.creds,
		InboxPrefix: u.inbox,
	}, nil
}

func encodeOperator(pub string, kp nkeys.KeyPair) (string, error) {
	oc := jwt.NewOperatorClaims(pub)
	oc.Name = OperatorName
	token, err := oc.Encode(kp)
	if err != nil {
		return "", fmt.Errorf("natsdev: operator jwt: %w", err)
	}
	return token, nil
}

func encodeSystem(pub string, operatorKP nkeys.KeyPair) (string, error) {
	ac := jwt.NewAccountClaims(pub)
	ac.Name = SystemName
	token, err := ac.Encode(operatorKP)
	if err != nil {
		return "", fmt.Errorf("natsdev: system jwt: %w", err)
	}
	return token, nil
}

func encodeAccount(pub, signingPub string, operatorKP nkeys.KeyPair) (string, error) {
	ac := jwt.NewAccountClaims(pub)
	ac.Name = AccountName
	ac.SigningKeys.Add(signingPub)
	token, err := ac.Encode(operatorKP)
	if err != nil {
		return "", fmt.Errorf("natsdev: account jwt: %w", err)
	}
	return token, nil
}

func encodeUser(pub, name string, allowPub, allowSub []string, resp bool, issuerAccount string, signer nkeys.KeyPair) (string, error) {
	uc := jwt.NewUserClaims(pub)
	uc.Name = name
	uc.Pub.Allow.Add(allowPub...)
	uc.Sub.Allow.Add(allowSub...)
	if resp {
		uc.Resp = &jwt.ResponsePermission{MaxMsgs: responseMaxMsgs, Expires: responseTTL}
	}
	uc.IssuerAccount = issuerAccount
	token, err := uc.Encode(signer)
	if err != nil {
		return "", fmt.Errorf("natsdev: user jwt: %w", err)
	}
	return token, nil
}

func ensureKey(path string, kind keyKind, create func() (nkeys.KeyPair, error)) (nkeys.KeyPair, error) {
	kp, err := readKey(path, kind)
	if err == nil {
		return kp, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	kp, err = create()
	if err != nil {
		return nil, err
	}
	if err := checkKeyKind(kp, kind); err != nil {
		return nil, err
	}
	seed, err := kp.Seed()
	if err != nil {
		return nil, err
	}
	if err := writeSecret(path, seed); err != nil {
		return nil, err
	}
	return kp, nil
}

func readKey(path string, kind keyKind) (nkeys.KeyPair, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	kp, err := nkeys.FromSeed(bytes.TrimSpace(b))
	if err != nil {
		// The nkeys error can echo the bytes. Keep the failure to the file name.
		return nil, fmt.Errorf("natsdev: %s: invalid seed", filepath.Base(path))
	}
	if err := checkKeyKind(kp, kind); err != nil {
		return nil, fmt.Errorf("natsdev: %s: %w", filepath.Base(path), err)
	}
	return kp, nil
}

func checkKeyKind(kp nkeys.KeyPair, kind keyKind) error {
	pub, err := kp.PublicKey()
	if err != nil {
		return err
	}
	switch kind {
	case keyOperatorKind:
		if !nkeys.IsValidPublicOperatorKey(pub) {
			return errors.New("natsdev: not an operator key")
		}
	case keyAccountKind:
		if !nkeys.IsValidPublicAccountKey(pub) {
			return errors.New("natsdev: not an account key")
		}
	case keyUserKind:
		if !nkeys.IsValidPublicUserKey(pub) {
			return errors.New("natsdev: not a user key")
		}
	default:
		return errors.New("natsdev: unknown key kind")
	}
	return nil
}

func writeSecret(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func writePublic(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	return os.Chmod(path, 0o644)
}

func removeOtherJWT(dir string, keep ...string) error {
	allowed := map[string]bool{}
	for _, name := range keep {
		allowed[name] = true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || allowed[name] || !strings.HasSuffix(name, ".jwt") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func removeOther(dir string, keep map[string]bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || keep[e.Name()] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func safeRel(rel string) error {
	if rel == "" || filepath.IsAbs(rel) {
		return fmt.Errorf("natsdev: path %q must be relative", rel)
	}
	clean := filepath.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("natsdev: path %q escapes the output directory", rel)
	}
	return nil
}

func quoteConf(s string) (string, error) {
	if s == "" || strings.ContainsAny(s, "\"\r\n") {
		return "", fmt.Errorf("natsdev: cannot quote config value %q", s)
	}
	return `"` + s + `"`, nil
}
