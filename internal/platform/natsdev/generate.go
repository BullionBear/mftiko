package natsdev

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const (
	// AccountName is the one data-plane account (E04-D014).
	AccountName = "MFTIKO"
	// SystemName is the system account used by the resolver and server monitoring.
	SystemName = "SYS"
	// OperatorName is the dev operator's name.
	OperatorName = "mftiko-dev"

	// PlaneMD and PlaneFetch are the platform inbox plane tokens (E04-D016).
	// md.fetch is not an etcd plane; "fetch" keeps its inbox off the MD prefix.
	// Instances of one plane share that prefix.
	// PlaneTD is the token in an account worker's inbox _INBOX_td_{api_id} (E06-D011).
	// The TD agent has no NATS user and no _INBOX_td grant (E06-D039).
	PlaneMD    = "md"
	PlaneFetch = "fetch"
	PlaneTD    = "td"

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
	// (one reply, two minutes). MD, md.fetch and the TD account worker need them
	// because E04-D016 does not grant publish on _INBOX.>. They are not a separate decision.
	responseMaxMsgs = 1
	responseTTL     = 2 * time.Minute

	// MaxControlLine is the max_control_line written into the generated server
	// config (E08-D027). 256 KiB. The nats-server default is 4096, which a session
	// CONNECT that lists each feed (E08-D018) exceeds at a few dozen feeds. STS
	// rejects a larger session CONNECT argument when it signs the user JWT.
	MaxControlLine = 262144
)

// instToken is one NATS subject token and one path segment.
var instToken = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Options selects which instance tokens Generate issues. Nil lists mean DefaultInst.
// An empty list is rejected. There is no TD agent user (E06-D039).
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
	// TD is empty. A non-empty list is rejected: the TD agent has no NATS user (E06-D039).
	TD []User `json:"td,omitempty"`
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

// InboxPrefix is the platform reply prefix _INBOX_{plane} (E04-D016).
// plane is one token. The result has no trailing dot. Every instance of the
// plane uses it, so one plane credential can subscribe to every same-plane reply.
func InboxPrefix(plane string) (string, error) {
	if !tokenOK(plane) {
		return "", errors.New("natsdev: inbox prefix needs a plane token")
	}
	return "_INBOX_" + plane, nil
}

// TDAccountInbox is the reply prefix _INBOX_td_{api_id} of one TD account worker (E06-D011).
// Each account worker has its own credential, so it has its own prefix, as a session does
// (E09-D006). It does not overlap a session's inbox: a session id is six hex digits. The TD
// agent has no inbox (E06-D039). The result has no trailing dot.
func TDAccountInbox(apiID int64) (string, error) {
	if apiID <= 0 {
		return "", errors.New("natsdev: api_id must be positive")
	}
	return "_INBOX_" + PlaneTD + "_" + strconv.FormatInt(apiID, 10), nil
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
	for _, plane := range []struct {
		users []loadedUser
		out   *[]User
	}{
		{mdUsers, &manifest.MD},
		{fetchUsers, &manifest.Fetch},
	} {
		for _, u := range plane.users {
			entry, err := writeServiceUser(dir, u, accountKP)
			if err != nil {
				return err
			}
			*plane.out = append(*plane.out, entry)
			keepCreds[filepath.Base(entry.Creds)] = true
		}
	}
	if err := removeOther(filepath.Join(dir, dirCreds), keepCreds); err != nil {
		return err
	}
	// A directory generated before E06-D039 still has the TD agent's seed. Drop it so the
	// next dev-up does not leave a user we no longer issue.
	if err := removeTDAgentKeys(dir); err != nil {
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
	for _, u := range m.TD {
		fmt.Fprintf(&b, "td %s inbox %s creds %s\n", u.Inst, u.InboxPrefix, u.Creds)
	}
	fmt.Fprintf(&b, "config %s\n", fileConfig)
	return b.String()
}

// FormatServerConfig renders the dev server config: a full resolver, one operator,
// the system account, max_control_line 262144 (E08-D027), and no TLS, cluster, or
// JetStream. Port -1 asks the OS for a port.
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
	fmt.Fprintf(&b, "max_control_line: %d\n", MaxControlLine)
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
		return errors.New("natsdev: manifest needs an MD user and an md.fetch user; run make dev-up")
	}
	if len(m.TD) != 0 {
		return errors.New("natsdev: TD agent has no NATS user")
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
		want, err := InboxPrefix(plane)
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
	inbox, err := InboxPrefix(plane)
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

// servicePerms are the allow lists of one platform instance user (E04-D015), and whether
// it answers requests. The TD agent has no NATS user (E06-D039). Account workers are
// tdAccountPerms, signed in memory, not here.
func servicePerms(plane, inst, inbox string) (pub, sub []string, resp bool, err error) {
	switch plane {
	case PlaneMD:
		// md.v1.f.{inst}.> is the feed and md.v1.view.{inst}.> is the read (E04-D055).
		// sym.v1.changed.> is the JWT. The conn worker subscribes only its own
		// venue, sym.v1.changed.{venue}.> (E04-D057). The MD agent does not subscribe.
		return []string{"md.v1.f." + inst + ".>", "sym"}, []string{"md.v1.view." + inst + ".>", "sym.v1.changed.>", inbox + ".>"}, true, nil
	case PlaneFetch:
		// md.v1.fetch.> is every venue subject. The process subscribes only to the
		// venues it can reach (E04-D018, E04-D056). The exact subject md.v1.fetch is not
		// in this list. sym.v1.changed.> is the resolver (E04-D022, E04-D057).
		return []string{"sym"}, []string{"md.v1.fetch.>", "sym.v1.changed.>", inbox + ".>"}, true, nil
	}
	return nil, nil, false, fmt.Errorf("natsdev: no permissions for plane %q", plane)
}

// removeTDAgentKeys deletes user-td-*.nk left by a generator that issued a TD agent user (E06-D039).
func removeTDAgentKeys(dir string) error {
	entries, err := os.ReadDir(filepath.Join(dir, dirKeys))
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "user-td-") || !strings.HasSuffix(name, ".nk") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, dirKeys, name)); err != nil {
			return err
		}
	}
	return nil
}

// tdAccountPerms are the allow lists of the TD account worker for apiID (E06-D011): it serves
// td.order and td.view of its own account only and publishes that account's td.evt. The TD
// agent signs a user with these lists at spawn, with the account signing key, and sends it
// in tdctl.InitPayload.nats; nothing writes it to disk. It answers requests, so the user
// carries the response permission.
func tdAccountPerms(apiID int64) (pub, sub []string, err error) {
	inbox, err := TDAccountInbox(apiID)
	if err != nil {
		return nil, nil, err
	}
	id := strconv.FormatInt(apiID, 10)
	return []string{"td.evt." + id, "sym"},
		[]string{"td.order." + id, "td.view." + id, "sym.v1.changed.>", inbox + ".>"}, nil
}

func writeServiceUser(dir string, u loadedUser, accountKP nkeys.KeyPair) (User, error) {
	seed, err := u.kp.Seed()
	if err != nil {
		return User{}, err
	}
	pub, sub, resp, err := servicePerms(u.plane, u.inst, u.inbox)
	if err != nil {
		return User{}, err
	}
	// Signed by the account identity key, so IssuerAccount stays empty (E04-D014).
	token, err := encodeUser(u.pub, u.name, pub, sub, resp, "", accountKP)
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
