package api

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

func tokenHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func mustPub(t *testing.T) string {
	t.Helper()
	k, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k.Public().String()
}

func testInput(t *testing.T) Input {
	t.Helper()
	return Input{
		EtcdPassword: "pw",
		CookieKey:    "cookie-key",
		PostgresDSN:  "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable",
	}
}

func deployDoc(regions map[string][]string, instances, planes map[string]string) string {
	var b strings.Builder
	b.WriteString(`{"regions":{`)
	first := true
	for name, eps := range regions {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(`"` + name + `":{"endpoints":[`)
		for i, ep := range eps {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`"` + ep + `"`)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`},"instances":{`)
	first = true
	for inst, region := range instances {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(`"` + inst + `":"` + region + `"`)
	}
	b.WriteString(`},"planes":{`)
	first = true
	for inst, plane := range planes {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(`"` + inst + `":"` + plane + `"`)
	}
	b.WriteString(`}}`)
	return b.String()
}

func goodDeploy() string {
	return deployDoc(
		map[string][]string{"tw": {"http://127.0.0.1:1"}},
		map[string]string{"controller": "tw", "td-tw-1": "tw"},
		map[string]string{"td-tw-1": "td"},
	)
}

func goodConfig(pub string) string {
	return `{
		"users": [{"name": "yi", "token_sha256": "` + tokenHash("yi-token") + `"}],
		"repositories": ["mftiko/strategies"],
		"td_keys": {"td-tw-1": "` + pub + `"}
	}`
}

func loadWithin(t *testing.T, deploy, conf string, in Input) Config {
	t.Helper()
	type result struct {
		cfg Config
		err error
	}
	ch := make(chan result, 1)
	go func() {
		cfg, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in)
		ch <- result{cfg, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r.cfg
	case <-time.After(2 * time.Second):
		t.Fatal("Load blocked; it must not dial Postgres")
	}
	return Config{}
}

func TestLoadDefaults(t *testing.T) {
	pub := mustPub(t)
	cfg := loadWithin(t, goodDeploy(), goodConfig(pub), testInput(t))
	if cfg.Retention != DefaultRetention || cfg.UnavailableAfter != DefaultUnavailableAfter ||
		cfg.Listen != DefaultListen || cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Fatalf("retention %s unavailable %s listen %s shutdown %s",
			cfg.Retention, cfg.UnavailableAfter, cfg.Listen, cfg.ShutdownTimeout)
	}
	if cfg.Topology.Instances["controller"] != "tw" || cfg.Planes["td-tw-1"] != "td" {
		t.Fatalf("topology %+v planes %+v", cfg.Topology, cfg.Planes)
	}
	sum := sha256.Sum256([]byte("yi-token"))
	if len(cfg.Users) != 1 || cfg.Users[0].Name != "yi" || cfg.Users[0].Token != sum {
		t.Fatalf("users %+v", cfg.Users)
	}
	if len(cfg.Repositories) != 1 || cfg.Repositories[0] != "mftiko/strategies" {
		t.Fatalf("repos %+v", cfg.Repositories)
	}
	want, err := sealbox.ParsePublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TDKeys["td-tw-1"] != want {
		t.Fatal("td key was not kept")
	}
	if cfg.EtcdPassword != "pw" || cfg.CookieKey != "cookie-key" {
		t.Fatal("secrets were not copied")
	}
}

func TestZeroDurationIsDefault(t *testing.T) {
	pub := mustPub(t)
	conf := `{
		"retention": "0",
		"unavailable_after": "0s",
		"shutdown_timeout": "0",
		"td_keys": {"td-tw-1": "` + pub + `"}
	}`
	cfg := loadWithin(t, goodDeploy(), conf, testInput(t))
	if cfg.Retention != DefaultRetention || cfg.UnavailableAfter != DefaultUnavailableAfter ||
		cfg.ShutdownTimeout != DefaultShutdownTimeout || cfg.Listen != DefaultListen {
		t.Fatalf("%s %s %s %s", cfg.Retention, cfg.UnavailableAfter, cfg.ShutdownTimeout, cfg.Listen)
	}
}

func TestExplicitDurationsAndListen(t *testing.T) {
	pub := mustPub(t)
	conf := `{
		"retention": "1h",
		"unavailable_after": "1s",
		"shutdown_timeout": "1s",
		"listen": "127.0.0.1:9",
		"td_keys": {"td-tw-1": "` + pub + `"}
	}`
	cfg := loadWithin(t, goodDeploy(), conf, testInput(t))
	if cfg.Retention != time.Hour || cfg.UnavailableAfter != time.Second ||
		cfg.ShutdownTimeout != time.Second || cfg.Listen != "127.0.0.1:9" {
		t.Fatalf("%s %s %s %s", cfg.Retention, cfg.UnavailableAfter, cfg.ShutdownTimeout, cfg.Listen)
	}
}

func TestUppercaseTokenHash(t *testing.T) {
	pub := mustPub(t)
	sum := tokenHash("other")
	conf := `{
		"users": [{"name": "yi", "token_sha256": "` + strings.ToUpper(sum) + `"}],
		"td_keys": {"td-tw-1": "` + pub + `"}
	}`
	cfg := loadWithin(t, goodDeploy(), conf, testInput(t))
	want := sha256.Sum256([]byte("other"))
	if cfg.Users[0].Token != want {
		t.Fatal("uppercase hex was not accepted")
	}
}

func TestMinimumConfig(t *testing.T) {
	deploy := deployDoc(
		map[string][]string{"tw": {"http://127.0.0.1:1"}},
		map[string]string{"controller": "tw"},
		map[string]string{},
	)
	cfg := loadWithin(t, deploy, `{}`, testInput(t))
	if len(cfg.Users) != 0 || len(cfg.Repositories) != 0 || len(cfg.TDKeys) != 0 {
		t.Fatalf("users %d repos %d keys %d", len(cfg.Users), len(cfg.Repositories), len(cfg.TDKeys))
	}
}

func TestLoadRejects(t *testing.T) {
	pub := mustPub(t)
	deploy := goodDeploy()
	conf := goodConfig(pub)
	hash := tokenHash("yi-token")
	cases := []struct {
		name, deploy, conf, want string
	}{
		{"unknown config field", deploy, `{"nope": 1}`, "unknown field"},
		{"unknown deployment field", `{"regions":{"tw":{"endpoints":["http://127.0.0.1:1"]}},"instances":{"controller":"tw"},"planes":{},"extra":1}`, conf, "unknown field"},
		{"not json", deploy, `{`, "read config"},
		{"extra value", deploy, conf + `{}`, "extra data"},
		{"negative retention", deploy, `{"retention": "-1s", "td_keys": {"td-tw-1": "` + pub + `"}}`, "negative duration"},
		{"bad listen", deploy, `{"listen": "8080", "td_keys": {"td-tw-1": "` + pub + `"}}`, "listen:"},
		{"empty user", deploy, `{"users": [{"name": "", "token_sha256": "` + hash + `"}], "td_keys": {"td-tw-1": "` + pub + `"}}`, "user name is empty"},
		{"duplicate user", deploy, `{"users": [
			{"name": "yi", "token_sha256": "` + hash + `"},
			{"name": "yi", "token_sha256": "` + tokenHash("b") + `"}
		], "td_keys": {"td-tw-1": "` + pub + `"}}`, "duplicate user"},
		{"same token hash", deploy, `{"users": [
			{"name": "a", "token_sha256": "` + hash + `"},
			{"name": "b", "token_sha256": "` + strings.ToUpper(hash) + `"}
		], "td_keys": {"td-tw-1": "` + pub + `"}}`, "same token hash"},
		{"short token", deploy, `{"users": [{"name": "yi", "token_sha256": "abcd"}], "td_keys": {"td-tw-1": "` + pub + `"}}`, "64 hex"},
		{"empty repo", deploy, `{"repositories": [""], "td_keys": {"td-tw-1": "` + pub + `"}}`, "invalid repository"},
		{"repo space", deploy, `{"repositories": ["a b"], "td_keys": {"td-tw-1": "` + pub + `"}}`, "invalid repository"},
		{"duplicate repo", deploy, `{"repositories": ["a", "a"], "td_keys": {"td-tw-1": "` + pub + `"}}`, "duplicate repository"},
		{"bad pubkey", deploy, `{"td_keys": {"td-tw-1": "not-a-key"}}`, "td key"},
		{"missing td key", deploy, `{}`, "has no public key"},
		{"td key for other plane", deployDoc(
			map[string][]string{"tw": {"http://127.0.0.1:1"}},
			map[string]string{"controller": "tw", "td-tw-1": "tw", "md-tw-1": "tw"},
			map[string]string{"td-tw-1": "td", "md-tw-1": "md"},
		), `{"td_keys": {"td-tw-1": "` + pub + `", "md-tw-1": "` + pub + `"}}`, "not a td instance"},
		{"no controller", deployDoc(
			map[string][]string{"tw": {"http://127.0.0.1:1"}},
			map[string]string{"md-tw-1": "tw"},
			map[string]string{"md-tw-1": "md"},
		), `{}`, "controller"},
		{"bad plane", deployDoc(
			map[string][]string{"tw": {"http://127.0.0.1:1"}},
			map[string]string{"controller": "tw", "md-tw-1": "tw"},
			map[string]string{"md-tw-1": "nope"},
		), `{}`, "want sts, md or td"},
		{"no endpoints", deployDoc(
			map[string][]string{"tw": {}},
			map[string]string{"controller": "tw"},
			map[string]string{},
		), `{}`, "no endpoints"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(tc.deploy), strings.NewReader(tc.conf), testInput(t))
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q, want substring %q", err, tc.want)
			}
		})
	}
}

func TestPostgresDSN(t *testing.T) {
	pub := mustPub(t)
	in := testInput(t)
	const pass = "secretword"
	in.PostgresDSN = "postgres://mftiko:" + pass + "@127.0.0.1:5432/mftiko?sslmode=not-a-mode"
	_, err := Load(strings.NewReader(goodDeploy()), strings.NewReader(goodConfig(pub)), in)
	if err == nil {
		t.Fatal("accepted an unparsable DSN")
	}
	if err.Error() != "postgres DSN is not a valid connection string" {
		t.Fatal(err)
	}
	if strings.Contains(err.Error(), pass) {
		t.Fatal(err)
	}

	in = testInput(t)
	in.PostgresDSN = ""
	_, err = Load(strings.NewReader(goodDeploy()), strings.NewReader(goodConfig(pub)), in)
	if err == nil || !strings.Contains(err.Error(), "postgres DSN is required") {
		t.Fatal(err)
	}
	in.EtcdPassword = ""
	in.PostgresDSN = testInput(t).PostgresDSN
	_, err = Load(strings.NewReader(goodDeploy()), strings.NewReader(goodConfig(pub)), in)
	if err == nil || !strings.Contains(err.Error(), "etcd password is required") {
		t.Fatal(err)
	}
	in = testInput(t)
	in.CookieKey = ""
	_, err = Load(strings.NewReader(goodDeploy()), strings.NewReader(goodConfig(pub)), in)
	if err == nil || !strings.Contains(err.Error(), "cookie key is required") {
		t.Fatal(err)
	}
}

func TestWebAndTLS(t *testing.T) {
	pub := mustPub(t)
	deploy, conf := goodDeploy(), goodConfig(pub)
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	in := testInput(t)
	in.WebDir = file
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatal(err)
	}
	in.WebDir = filepath.Join(dir, "missing")
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err == nil || !strings.Contains(err.Error(), "-web:") {
		t.Fatal(err)
	}
	in.WebDir = dir
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err != nil {
		t.Fatal(err)
	}

	in = testInput(t)
	in.TLSCert = file
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err == nil || !strings.Contains(err.Error(), "must both be set") {
		t.Fatal(err)
	}
	in.TLSCert, in.TLSKey = file, dir
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err == nil || !strings.Contains(err.Error(), "-tls-key:") {
		t.Fatal(err)
	}
	key := filepath.Join(dir, "key")
	if err := os.WriteFile(key, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	in.TLSKey = key
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err != nil {
		t.Fatal(err)
	}

	in = testInput(t)
	in.Mirror.ProbeInterval = -time.Millisecond
	if _, err := Load(strings.NewReader(deploy), strings.NewReader(conf), in); err == nil || !strings.Contains(err.Error(), "negative mirror option") {
		t.Fatal(err)
	}
}

func TestStrategyRepositoryIDs(t *testing.T) {
	pub := mustPub(t)
	deploy := goodDeploy()
	cases := []struct {
		name, repos, want string
	}{
		{"duplicate id", `["a/x","b/x"]`, `duplicate strategy id "x"`},
		{"duplicate id of a bare name", `["x","y/x"]`, `duplicate strategy id "x"`},
		{"dot segment", `["a/./b"]`, "invalid repository"},
		{"dotdot segment", `["a/../b"]`, "invalid repository"},
		{"empty segment", `["a//b"]`, "invalid repository"},
		{"leading slash", `["/a"]`, "invalid repository"},
		{"trailing slash", `["a/"]`, "invalid repository"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := `{"repositories":` + tc.repos + `,"td_keys":{"td-tw-1":"` + pub + `"}}`
			_, err := Load(strings.NewReader(deploy), strings.NewReader(conf), testInput(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestRegistrySettings(t *testing.T) {
	pub := mustPub(t)
	deploy := goodDeploy()
	keys := `,"td_keys":{"td-tw-1":"` + pub + `"}`
	reject := func(conf, want string) {
		t.Helper()
		_, err := Load(strings.NewReader(deploy), strings.NewReader(conf), testInput(t))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v, want substring %q", err, want)
		}
	}
	reject(`{"plain_http":true`+keys+`}`, "registry address is required")
	reject(`{"registry_creds":"/no/such/config.json"`+keys+`}`, "registry address is required")
	reject(`{"registry":"http://127.0.0.1:5000"`+keys+`}`, "host[:port]")
	reject(`{"registry":"127.0.0.1:5000/repo"`+keys+`}`, "host[:port]")

	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`{`), 0o600); err != nil {
		t.Fatal(err)
	}
	reject(`{"registry":"127.0.0.1:1","registry_creds":"`+bad+`"`+keys+`}`, "credentials")

	missing := t.TempDir() + "/missing.json"
	done := make(chan error, 1)
	go func() {
		conf := `{"registry":"127.0.0.1:1","registry_creds":"` + missing + `"` + keys + `}`
		_, err := Load(strings.NewReader(deploy), strings.NewReader(conf), testInput(t))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "credentials") {
			t.Fatalf("missing credentials file: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Load blocked on a missing credentials file")
	}

	conf := `{"registry":"127.0.0.1:1","plain_http":true` + keys + `}`
	cfg := loadWithin(t, deploy, conf, testInput(t))
	if cfg.Catalog == nil || cfg.Registry.Address != "127.0.0.1:1" || !cfg.Registry.PlainHTTP || cfg.Registry.Credentials != "" {
		t.Fatalf("registry %+v catalog nil=%v", cfg.Registry, cfg.Catalog == nil)
	}

	creds := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(creds, []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	conf = `{"registry":"127.0.0.1:1","registry_creds":"` + creds + `"` + keys + `}`
	cfg = loadWithin(t, deploy, conf, testInput(t))
	if cfg.Catalog == nil || cfg.Registry.Credentials != creds {
		t.Fatalf("creds %q catalog nil=%v", cfg.Registry.Credentials, cfg.Catalog == nil)
	}
}
