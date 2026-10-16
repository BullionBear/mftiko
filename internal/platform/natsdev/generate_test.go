package natsdev

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func TestGenerateReusesKeys(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	first, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	seed := readFile(t, filepath.Join(dir, "keys", "user-md-dev.nk"))
	if err := os.Remove(filepath.Join(dir, "operator.jwt")); err != nil {
		t.Fatal(err)
	}
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	second, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if second.Manifest.Operator != first.Manifest.Operator || second.Manifest.Account != first.Manifest.Account {
		t.Fatalf("identity changed")
	}
	if !bytes.Equal(readFile(t, filepath.Join(dir, "keys", "user-md-dev.nk")), seed) {
		t.Fatal("user seed changed")
	}
	if _, err := jwt.DecodeOperatorClaims(string(readFile(t, filepath.Join(dir, "operator.jwt")))); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateRejectsBadInstancesBeforeWriting(t *testing.T) {
	cases := []Options{
		{MDInsts: []string{"a.b"}},
		{MDInsts: []string{"../x"}},
		{MDInsts: []string{"a/b"}},
		{MDInsts: []string{"*"}},
		{MDInsts: []string{"has space"}},
		{MDInsts: []string{}},
		{MDInsts: []string{"dev", "dev"}},
		{FetchInsts: []string{}},
	}
	for _, opt := range cases {
		dir := t.TempDir()
		if err := Generate(dir, opt); err == nil {
			t.Fatalf("Generate(%+v) succeeded", opt)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("Generate(%+v) wrote %d entries", opt, len(entries))
		}
	}
}

func TestGenerateCorruptSeedDoesNotRotate(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	account := readFile(t, filepath.Join(dir, "keys", "mftiko.nk"))
	if err := os.WriteFile(filepath.Join(dir, "keys", "operator.nk"), []byte("nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Generate(dir, Options{}); err == nil {
		t.Fatal("corrupt operator seed was accepted")
	}
	if !bytes.Equal(readFile(t, filepath.Join(dir, "keys", "mftiko.nk")), account) {
		t.Fatal("account seed changed after a corrupt operator seed")
	}
	if string(readFile(t, filepath.Join(dir, "keys", "operator.nk"))) != "nope" {
		t.Fatal("corrupt operator seed was replaced")
	}
}

func TestGenerateMaterial(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{MDInsts: []string{"box"}, FetchInsts: []string{"box"}}); err != nil {
		t.Fatal(err)
	}
	lay, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if lay.Manifest.Operator == lay.Manifest.Account || lay.Manifest.Account == lay.Manifest.SigningKey {
		t.Fatal("operator, account, and signing key are not distinct")
	}
	assertPerm(t, filepath.Join(dir, "resolver"), 0o777)
	assertPerm(t, filepath.Join(dir, "keys"), 0o700)
	assertPerm(t, filepath.Join(dir, "creds"), 0o700)

	seeds := map[string][]byte{}
	keyEntries, err := os.ReadDir(filepath.Join(dir, "keys"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range keyEntries {
		seeds[e.Name()] = bytes.TrimSpace(readFile(t, filepath.Join(dir, "keys", e.Name())))
		assertPerm(t, filepath.Join(dir, "keys", e.Name()), 0o600)
	}
	publics := []string{
		filepath.Join(dir, "operator.jwt"),
		filepath.Join(dir, "nats-server.conf"),
		filepath.Join(dir, "manifest.json"),
		filepath.Join(dir, "resolver", lay.Manifest.System+".jwt"),
		filepath.Join(dir, "resolver", lay.Manifest.Account+".jwt"),
	}
	for _, path := range publics {
		body := readFile(t, path)
		for name, seed := range seeds {
			if bytes.Contains(body, seed) {
				t.Fatalf("%s contains seed %s", filepath.Base(path), name)
			}
		}
	}

	credEntries, err := os.ReadDir(filepath.Join(dir, "creds"))
	if err != nil {
		t.Fatal(err)
	}
	var credNames []string
	for _, e := range credEntries {
		credNames = append(credNames, e.Name())
		body := readFile(t, filepath.Join(dir, "creds", e.Name()))
		assertPerm(t, filepath.Join(dir, "creds", e.Name()), 0o600)
		var userSeeds int
		for name, seed := range seeds {
			if !bytes.Contains(body, seed) {
				continue
			}
			switch {
			case strings.HasPrefix(name, "user-"):
				userSeeds++
			default:
				t.Fatalf("%s contains non-user seed %s", e.Name(), name)
			}
		}
		if userSeeds != 1 {
			t.Fatalf("%s contains %d user seeds", e.Name(), userSeeds)
		}
	}
	slices.Sort(credNames)
	if !slices.Equal(credNames, []string{"fetch-box.creds", "md-box.creds"}) {
		t.Fatalf("creds %v", credNames)
	}

	conf := string(readFile(t, filepath.Join(dir, "nats-server.conf")))
	if MaxControlLine != 262144 {
		t.Fatalf("MaxControlLine = %d", MaxControlLine)
	}
	for _, want := range []string{"type: full", "allow_delete: false", lay.Manifest.System, ContainerOperator, ContainerResolver, "port: 4222", "http_port: 8222", "max_control_line: 262144"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("config missing %q\n%s", want, conf)
		}
	}
	for _, banned := range []string{"tls", "cluster", "jetstream"} {
		if strings.Contains(conf, banned) {
			t.Fatalf("config contains %q\n%s", banned, conf)
		}
	}

	acc, err := jwt.DecodeAccountClaims(string(readFile(t, filepath.Join(dir, "resolver", lay.Manifest.Account+".jwt"))))
	if err != nil {
		t.Fatal(err)
	}
	if acc.Name != AccountName || !acc.SigningKeys.Contains(lay.Manifest.SigningKey) || len(acc.SigningKeys) != 1 {
		t.Fatalf("account signing keys %+v", acc.SigningKeys)
	}
	if len(acc.Imports) != 0 || len(acc.Exports) != 0 || acc.Limits.IsJSEnabled() {
		t.Fatal("account has imports, exports, or JetStream")
	}

	assertUser := func(u User, pub, sub []string, resp bool) {
		t.Helper()
		token, err := jwt.ParseDecoratedJWT(readFile(t, filepath.Join(dir, filepath.FromSlash(u.Creds))))
		if err != nil {
			t.Fatal(err)
		}
		uc, err := jwt.DecodeUserClaims(token)
		if err != nil {
			t.Fatal(err)
		}
		if uc.Subject != u.PublicKey || uc.Issuer != lay.Manifest.Account || uc.IssuerAccount != "" || uc.Expires != 0 {
			t.Fatalf("user claims issuer %s account %s exp %d", uc.Issuer, uc.IssuerAccount, uc.Expires)
		}
		if !sameSubjects([]string(uc.Pub.Allow), pub...) || !sameSubjects([]string(uc.Sub.Allow), sub...) {
			t.Fatalf("pub %v sub %v", uc.Pub.Allow, uc.Sub.Allow)
		}
		if !resp {
			if uc.Resp != nil {
				t.Fatalf("%s answers no requests, resp %+v", u.Name, uc.Resp)
			}
			return
		}
		if uc.Resp == nil || uc.Resp.MaxMsgs != responseMaxMsgs || uc.Resp.Expires != responseTTL {
			t.Fatalf("resp %+v", uc.Resp)
		}
	}
	if len(lay.Manifest.MD) != 1 || lay.Manifest.MD[0].Inst != "box" {
		t.Fatalf("md users %+v", lay.Manifest.MD)
	}
	md := lay.Manifest.MD[0]
	if md.InboxPrefix != "_INBOX_md" {
		t.Fatal(md.InboxPrefix)
	}
	assertUser(md, []string{"md.v1.f.box.>", "sym"}, []string{"md.v1.view.box.>", "sym.v1.changed.>", "_INBOX_md.>"}, true)
	fetch := lay.Manifest.Fetch[0]
	if fetch.InboxPrefix != "_INBOX_fetch" {
		t.Fatal(fetch.InboxPrefix)
	}
	assertUser(fetch, []string{"sym"}, []string{"md.v1.fetch.>", "sym.v1.changed.>", "_INBOX_fetch.>"}, true)
	if len(lay.Manifest.TD) != 0 {
		t.Fatalf("td users %+v", lay.Manifest.TD)
	}
	if _, _, _, err := servicePerms(PlaneTD, "box", "_INBOX_td"); err == nil {
		t.Fatal("PlaneTD still has permissions")
	}

	if _, err := nkeys.FromSeed(seeds["mftiko-signing.nk"]); err != nil {
		t.Fatal("signing key")
	}
}

func TestInboxPrefixAndConfig(t *testing.T) {
	got, err := InboxPrefix(PlaneMD)
	if err != nil || got != "_INBOX_md" {
		t.Fatalf("%s %v", got, err)
	}
	got, err = InboxPrefix(PlaneFetch)
	if err != nil || got != "_INBOX_fetch" {
		t.Fatalf("%s %v", got, err)
	}
	got, err = InboxPrefix(PlaneTD)
	if err != nil || got != "_INBOX_td" {
		t.Fatalf("%s %v", got, err)
	}
	if _, err := InboxPrefix("not.a.plane"); err == nil {
		t.Fatal("dot in plane was accepted")
	}
	got, err = TDAccountInbox(42)
	if err != nil || got != "_INBOX_td_42" {
		t.Fatalf("%s %v", got, err)
	}
	for _, id := range []int64{0, -1} {
		if _, err := TDAccountInbox(id); err == nil {
			t.Fatalf("api_id %d was accepted", id)
		}
	}
	if _, err := FormatServerConfig(ServerConfig{
		OperatorFile:  "bad'path",
		SystemAccount: "A",
		ResolverDir:   "/data/resolver",
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "dev",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := FormatServerConfig(ServerConfig{
		OperatorFile:  "bad\"path",
		SystemAccount: "SYS",
		ResolverDir:   "/data/resolver",
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "dev",
	}); err == nil {
		t.Fatal("quote in path was accepted")
	}
}

func TestTDAccountPerms(t *testing.T) {
	pub, sub, err := tdAccountPerms(42)
	if err != nil {
		t.Fatal(err)
	}
	if !sameSubjects(pub, "td.evt.42", "sym") {
		t.Fatalf("pub %v", pub)
	}
	if !sameSubjects(sub, "td.order.42", "td.view.42", "sym.v1.changed.>", "_INBOX_td_42.>") {
		t.Fatalf("sub %v", sub)
	}
	if _, _, err := tdAccountPerms(0); err == nil {
		t.Fatal("api_id 0 was accepted")
	}
}

// TestLoadRejectsTDAgentUser: a manifest that still lists a TD agent user is not half-used.
func TestLoadRejectsTDAgentUser(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	lay, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	lay.Manifest.TD = []User{{
		Inst:        "dev",
		Name:        "td-dev",
		PublicKey:   lay.Manifest.MD[0].PublicKey,
		Creds:       "creds/td-dev.creds",
		InboxPrefix: "_INBOX_td",
	}}
	body, err := json.Marshal(lay.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "no NATS user") {
		t.Fatal(err)
	}
}

// TestGenerateDropsTDAgentMaterial removes a TD agent seed and creds left by an older generator.
func TestGenerateDropsTDAgentMaterial(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "creds", "td-dev.creds"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keys", "user-td-dev.nk"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "creds", "td-dev.creds")); !os.IsNotExist(err) {
		t.Fatalf("td creds: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "keys", "user-td-dev.nk")); !os.IsNotExist(err) {
		t.Fatalf("td seed: %v", err)
	}
	lay, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(lay.Manifest.TD) != 0 {
		t.Fatalf("td users %+v", lay.Manifest.TD)
	}
}

// TestCheckRejectsTDAgentCreds fails before dialing when a TD agent creds file is present.
func TestCheckRejectsTDAgentCreds(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "creds", "td-dev.creds"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Check(dir, "nats://127.0.0.1:1")
	if err == nil || !strings.Contains(err.Error(), "no NATS user") {
		t.Fatal(err)
	}
}

// TestCheckRejectsTDEventSubscriber fails when an on-disk user may subscribe to every td.evt,
// even if the file is not named td-*.
func TestCheckRejectsTDEventSubscriber(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	account, err := readKey(filepath.Join(dir, "keys", "mftiko.nk"), keyAccountKind)
	if err != nil {
		t.Fatal(err)
	}
	userKP, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := userKP.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	token, err := encodeUser(pub, "md-sneaky", nil, []string{"td.evt.>"}, false, "", account)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := userKP.Seed()
	if err != nil {
		t.Fatal(err)
	}
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "creds", "sneaky.creds"), creds, 0o600); err != nil {
		t.Fatal(err)
	}
	err = Check(dir, "nats://127.0.0.1:1")
	if err == nil || !strings.Contains(err.Error(), "no NATS user") {
		t.Fatal(err)
	}
}

func TestCheckMissingManifest(t *testing.T) {
	err := Check(t.TempDir(), "nats://127.0.0.1:1")
	if err == nil || !strings.Contains(err.Error(), "make dev-up") {
		t.Fatal(err)
	}
}

func assertPerm(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != want {
		t.Fatalf("%s mode %o, want %o", path, fi.Mode().Perm(), want)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sameSubjects(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	slices.Sort(g)
	slices.Sort(w)
	return slices.Equal(g, w)
}

func TestResponseTTL(t *testing.T) {
	if responseTTL != 2*time.Minute || responseMaxMsgs != 1 {
		t.Fatalf("response permission %d %s", responseMaxMsgs, responseTTL)
	}
}
