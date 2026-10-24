package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

const (
	tokenA = "test-token-a"
	tokenB = "test-token-b"
	tokenC = "test-token-c"
)

func TestLoginAndWhoami(t *testing.T) {
	exp := time.Date(2026, 3, 4, 5, 6, 7, 0, time.FixedZone("CLI+8", 8*3600))
	srv := apitest.Start(apitest.Option{
		Token:     tokenA,
		Name:      "ada",
		Expires:   &exp,
		RequestID: "req-9c1e",
	})
	defer srv.Close()

	home := t.TempDir()
	path := writeConfig(t, home, configYAML(srv.URL(), true, ""))

	var in countReader
	in.r = strings.NewReader(tokenA + "\n")
	got := runAt(context.Background(), home, nil, &in, "login", "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("login exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.out, "ada") {
		t.Fatalf("login stdout %q", got.out)
	}
	wantLocal := exp.In(time.Local).Format(time.RFC3339)
	if !strings.Contains(got.out, wantLocal) {
		t.Fatalf("login stdout %q, want local time %s", got.out, wantLocal)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o", fi.Mode().Perm())
	}
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Contexts["dev"].Token != tokenA {
		t.Fatal("dev token was not stored")
	}
	if f.Contexts["accept"].Token != tokenC {
		t.Fatal("accept token changed")
	}
	assertMeCall(t, srv.Calls(), tokenA)

	// A failed login does not replace the stored token, and does not call POST /auth/login.
	bad := apitest.Start(apitest.Option{Token: tokenA, Name: "ada"})
	defer bad.Close()
	writeConfig(t, home, configYAML(bad.URL(), true, tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(tokenB+"\n"), "login", "--context", "dev")
	if got.code != ExitUnauthenticated {
		t.Fatalf("bad login exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenB, got.out+got.err)
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.err, "unauthenticated (11):") {
		t.Fatalf("stderr %q", got.err)
	}
	f, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Contexts["dev"].Token != tokenA {
		t.Fatal("failed login changed the stored token")
	}
	for _, c := range bad.Calls() {
		if c.Operation == "Login" {
			t.Fatal("login called POST /auth/login")
		}
	}

	// whoami uses the config token. -o json is the API body unchanged.
	plain := apitest.Start(apitest.Option{Token: tokenA, Name: "ada", Expires: &exp, RequestID: "req-9c1e"})
	defer plain.Close()
	writeConfig(t, home, configYAML(plain.URL(), true, tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev", "-o", "json")
	if got.code != ExitOK {
		t.Fatalf("whoami exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !json.Valid([]byte(got.out)) {
		t.Fatalf("stdout is not JSON: %q", got.out)
	}
	if !bytes.Equal([]byte(got.out), plain.LastBody()) {
		t.Fatalf("stdout is not the API body")
	}
	if !strings.Contains(got.out, "2026-03-04T05:06:07+08:00") {
		t.Fatalf("json reformatted time: %s", got.out)
	}
	var me apiv1.Me
	if err := json.Unmarshal([]byte(got.out), &me); err != nil {
		t.Fatal(err)
	}
	if me.Name != "ada" {
		t.Fatalf("name %q", me.Name)
	}

	// MFTIKO_TOKEN wins over the config token and does not rewrite the file.
	over := apitest.Start(apitest.Option{Token: tokenB, Name: "bea"})
	defer over.Close()
	writeConfig(t, home, configYAML(over.URL(), true, tokenA))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got = runAt(context.Background(), home, map[string]string{"MFTIKO_TOKEN": tokenB, "MFTIKO_CONTEXT": "dev"}, strings.NewReader(""), "whoami")
	if got.code != ExitOK {
		t.Fatalf("override exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	noLeak(t, tokenB, got.out+got.err)
	if !strings.Contains(got.out, "bea") {
		t.Fatalf("stdout %q", got.out)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("whoami rewrote the config")
	}
	if over.Calls()[0].Authorization != "Bearer "+tokenB {
		t.Fatal("environment token was not sent")
	}

	// login reads stdin even when MFTIKO_TOKEN is set.
	writeConfig(t, home, configYAML(srv.URL(), true, ""))
	got = runAt(context.Background(), home, map[string]string{"MFTIKO_TOKEN": tokenB}, strings.NewReader(tokenA+"\n"), "--context", "dev", "login")
	if got.code != ExitOK {
		t.Fatalf("login with env exit %d stderr %q", got.code, got.err)
	}
	f, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Contexts["dev"].Token != tokenA {
		t.Fatal("login stored the environment token")
	}
}

func TestWhoamiJSONRedactsToken(t *testing.T) {
	srv := apitest.Start(apitest.Option{Token: tokenA, Name: tokenA})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "-o", "json", "whoami", "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d", got.code)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.out, "***") {
		t.Fatal("token was not redacted")
	}
	if !json.Valid([]byte(got.out)) {
		t.Fatal("redacted json is not valid")
	}
}

func TestVerboseRequestID(t *testing.T) {
	srv := apitest.Start(apitest.Option{Token: tokenA, Name: "ada", RequestID: "req-9c1e"})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	quiet := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if quiet.code != 0 || strings.Contains(quiet.err, "X-Request-Id") {
		t.Fatalf("quiet %+v", quiet)
	}
	loud := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev", "-v")
	if loud.code != 0 || !strings.Contains(loud.err, "X-Request-Id: req-9c1e\n") {
		t.Fatalf("verbose stderr %q", loud.err)
	}
}

func TestProblemExitCodes(t *testing.T) {
	cases := []struct {
		code   apiv1.ErrorCode
		status int
		title  string
		exit   int
	}{
		{apiv1.ErrorCodeValidationFailed, 400, "Unprocessable Entity", ExitValidationFailed},
		{apiv1.ErrorCodeUnauthenticated, 401, "Unauthorized", ExitUnauthenticated},
		{apiv1.ErrorCodeNotFound, 404, "Not Found", ExitNotFound},
		{apiv1.ErrorCodeConflict, 409, "Conflict", ExitConflict},
		{apiv1.ErrorCodeNotTerminal, 409, "Not Terminal", ExitNotTerminal},
		{apiv1.ErrorCodeReferenced, 409, "Referenced", ExitReferenced},
		{apiv1.ErrorCodeAlgoOverlap, 409, "Algo Overlap", ExitAlgoOverlap},
		{apiv1.ErrorCodeRegionUnavailable, 503, "Region Unavailable", ExitRegionUnavailable},
		{apiv1.ErrorCodeStorageUnavailable, 503, "Storage Unavailable", ExitStorageUnavailable},
		{apiv1.ErrorCodeRegistryUnavailable, 503, "Registry Unavailable", ExitRegistryUnavailable},
		{apiv1.ErrorCodeInternal, 500, "Internal", ExitInternal},
		{apiv1.ErrorCodeInvalidArtifact, 502, "Bad Gateway", ExitInvalidArtifact},
	}
	for _, tc := range cases {
		t.Run(string(tc.code), func(t *testing.T) {
			detail := "because " + string(tc.code)
			errs := []apiv1.FieldError{{Path: "/feeds/0/feed", Reason: "unknown"}, {Path: "/name", Reason: "required"}}
			p := &apiv1.Problem{
				Type: "about:blank", Title: tc.title, Status: tc.status, Code: tc.code,
				Detail: &detail, Errors: &errs,
			}
			srv := apitest.Start(apitest.Option{Token: tokenA, Problem: p, RequestID: "req-9c1e"})
			defer srv.Close()
			home := t.TempDir()
			writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
			got := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
			if got.code != tc.exit {
				t.Fatalf("exit %d, want %d, stderr %q", got.code, tc.exit, got.err)
			}
			noLeak(t, tokenA, got.out+got.err)
			want := "mftiko: " + string(tc.code) + " (" + strconv.Itoa(tc.exit) + "): " + tc.title + "\n"
			if tc.code == apiv1.ErrorCodeRegistryUnavailable {
				want += "registry connection problem\n"
			}
			want += detail + "\n/feeds/0/feed: unknown\n/name: required\nX-Request-Id: req-9c1e\n"
			if got.err != want {
				t.Fatalf("stderr\n%s\nwant\n%s", got.err, want)
			}
			if len(srv.Calls()) != 1 || srv.Calls()[0].Operation != "GetMe" {
				t.Fatalf("calls %+v", srv.Calls())
			}
		})
	}
}

func TestExitCodeFollowsBodyNotHTTPStatus(t *testing.T) {
	detail := "still validation"
	p := &apiv1.Problem{
		Type: "about:blank", Title: "Bad Gateway", Status: 500,
		Code: apiv1.ErrorCodeValidationFailed, Detail: &detail,
	}
	srv := apitest.Start(apitest.Option{Problem: p, RequestID: "req-status"})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitValidationFailed {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if !strings.Contains(got.err, "validation_failed (10):") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestEchoedTokenIsRedacted(t *testing.T) {
	srv := apitest.Start(apitest.Option{Token: tokenA, EchoAuthorization: true, RequestID: "req-echo"})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitUnauthenticated {
		t.Fatalf("exit %d", got.code)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.err, "Bearer ***") {
		t.Fatal("detail was not redacted")
	}
}

func TestNetworkAndTimeout(t *testing.T) {
	home := t.TempDir()
	writeConfig(t, home, configYAML("http://127.0.0.1:1/api/v1", true, tokenA))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--timeout", "1s", "whoami", "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("refused exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.HasPrefix(got.err, "mftiko: network (2): ") {
		t.Fatalf("stderr %q", got.err)
	}

	blocked := apitest.Start(apitest.Option{Token: tokenA, Name: "ada", Block: true})
	defer blocked.Close()
	writeConfig(t, home, configYAML(blocked.URL(), true, tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--timeout", "200ms", "whoami", "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("timeout exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.HasPrefix(got.err, "mftiko: network (2): ") {
		t.Fatalf("stderr %q", got.err)
	}
	if len(blocked.Calls()) != 1 {
		t.Fatalf("calls %+v", blocked.Calls())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writeConfig(t, home, configYAML(blocked.URL(), true, tokenA))
	got = runAt(ctx, home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("canceled exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestTLS(t *testing.T) {
	srv := apitest.Start(apitest.Option{TLS: true, Token: tokenA, Name: "ada"})
	defer srv.Close()
	other := apitest.Start(apitest.Option{TLS: true, Token: tokenA, Name: "ada"})
	defer other.Close()
	home := t.TempDir()
	ca := filepath.Join(home, "ca.pem")
	if err := os.WriteFile(ca, srv.CertPEM(), 0o600); err != nil {
		t.Fatal(err)
	}
	wrong := filepath.Join(home, "wrong.pem")
	if err := os.WriteFile(wrong, other.CertPEM(), 0o600); err != nil {
		t.Fatal(err)
	}

	writeConfig(t, home, tlsConfig(srv.URL(), ca, tokenA))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("ca exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.out, "ada") {
		t.Fatalf("stdout %q", got.out)
	}
	call := srv.Calls()[0]
	if call.Path != "/api/v1/auth/me" {
		t.Fatalf("path %s", call.Path)
	}

	writeConfig(t, home, tlsConfig(srv.URL(), "", tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("system roots exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)

	writeConfig(t, home, tlsConfig(srv.URL(), wrong, tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("wrong ca exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)

	missing := filepath.Join(home, "missing.pem")
	var in countReader
	in.r = strings.NewReader(tokenA + "\n")
	writeConfig(t, home, tlsConfig(srv.URL(), missing, ""))
	got = runReader(context.Background(), home, nil, &in, "login", "--context", "dev")
	if got.code != ExitUsage || in.n != 0 {
		t.Fatalf("missing ca exit %d reads %d stderr %q", got.code, in.n, got.err)
	}
	if strings.Contains(got.err, tokenA) || !strings.Contains(got.err, "cannot read ca_file") {
		t.Fatal("missing ca error")
	}
}

func TestPlainHTTPRequiresFlag(t *testing.T) {
	srv := apitest.Start(apitest.Option{Token: tokenA, Name: "ada"})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), false, tokenA))
	var in countReader
	in.r = strings.NewReader(tokenA + "\n")
	got := runReader(context.Background(), home, nil, &in, "login", "--context", "dev")
	if got.code != ExitUsage || in.n != 0 {
		t.Fatalf("exit %d reads %d stderr %q", got.code, in.n, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.err, "insecure_http: true") {
		t.Fatalf("stderr %q", got.err)
	}
	if len(srv.Calls()) != 0 {
		t.Fatalf("calls %+v", srv.Calls())
	}

	// A URL without a trailing slash still requests /api/v1/auth/me.
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if srv.Calls()[0].Path != "/api/v1/auth/me" {
		t.Fatalf("path %s", srv.Calls()[0].Path)
	}
	slash := srv.URL() + "/"
	writeConfig(t, home, configYAML(slash, true, tokenA))
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitOK || srv.Calls()[1].Path != "/api/v1/auth/me" {
		t.Fatalf("slash exit %d path %s stderr %q", got.code, srv.Calls()[len(srv.Calls())-1].Path, got.err)
	}
}

func TestConfigGuards(t *testing.T) {
	home := t.TempDir()
	path := writeConfig(t, home, "nope: "+tokenA+"\n")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	var in countReader
	in.r = strings.NewReader(tokenA + "\n")
	got := runReader(context.Background(), home, map[string]string{"MFTIKO_CONTEXT": "dev"}, &in, "login")
	if got.code != ExitUsage || in.n != 0 {
		t.Fatalf("wide exit %d reads %d", got.code, in.n)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.err, "0600") {
		t.Fatalf("stderr %q", got.err)
	}

	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	got = runAt(context.Background(), home, map[string]string{"MFTIKO_CONTEXT": "dev"}, strings.NewReader(tokenA+"\n"), "login")
	if got.code != ExitUsage {
		t.Fatalf("yaml exit %d", got.code)
	}
	noLeak(t, tokenA, got.out+got.err)
	if !strings.Contains(got.err, "invalid YAML") {
		t.Fatalf("stderr %q", got.err)
	}

	writeConfig(t, home, configYAML("http://127.0.0.1:9/api/v1", true, tokenA))
	if err := os.Chmod(ConfigPath(home), 0o400); err != nil {
		t.Fatal(err)
	}
	f, err := Load(ConfigPath(home))
	if err != nil {
		t.Fatal(err)
	}
	if f.Contexts["dev"].Token != tokenA {
		t.Fatal("0400 was refused")
	}

	dir := t.TempDir()
	target := ConfigPath(dir)
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(target); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("dir: %v", err)
	}
}

func TestSaveForces0600(t *testing.T) {
	old := syscall.Umask(0o077)
	home := t.TempDir()
	path := ConfigPath(home)
	err := Save(path, &File{Contexts: map[string]Context{
		"dev": {URL: "https://example.com/api/v1", Token: tokenA},
	}})
	fi, statErr := os.Stat(path)
	syscall.Umask(old)
	if err != nil {
		t.Fatal(err)
	}
	if statErr != nil {
		t.Fatal(statErr)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o", fi.Mode().Perm())
	}
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Contexts["dev"].Token != tokenA || f.Contexts["dev"].URL != "https://example.com/api/v1" {
		t.Fatal("round trip")
	}
}

func TestUsage(t *testing.T) {
	home := t.TempDir()
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "help")
	if got.code != ExitOK || !strings.Contains(got.out, "mftiko login") && !strings.Contains(got.out, "login") {
		t.Fatalf("help %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "-h")
	if got.code != ExitOK || !strings.Contains(got.out, "usage:") {
		t.Fatalf("-h %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--version")
	if got.code != ExitOK || strings.TrimSpace(got.out) != buildinfo.String() {
		t.Fatalf("version %q", got.out)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "nope")
	if got.code != ExitUsage {
		t.Fatalf("unknown %d", got.code)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(tokenA+"\n"), "login", "extra")
	if got.code != ExitUsage || !strings.Contains(got.err, "login takes no arguments") {
		t.Fatalf("args %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "-token", tokenA, "whoami")
	if got.code != ExitUsage {
		t.Fatalf("flag %d", got.code)
	}
	noLeak(t, tokenA, got.out+got.err)
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "-token="+tokenA, "whoami")
	noLeak(t, tokenA, got.out+got.err)
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "-timeout", tokenA, "whoami")
	if got.code != ExitUsage {
		t.Fatalf("timeout flag %d", got.code)
	}
	noLeak(t, tokenA, got.out+got.err)

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami")
	if got.code != ExitUsage || !strings.Contains(got.err, "set --context or MFTIKO_CONTEXT") {
		t.Fatalf("no context %+v", got)
	}
	got = runAt(context.Background(), home, map[string]string{"MFTIKO_CONTEXT": "accept"}, strings.NewReader(""), "--context", "dev", "whoami")
	if got.code != ExitUsage || !strings.Contains(got.err, "differ") {
		t.Fatalf("differ %+v", got)
	}

	srv := apitest.Start(apitest.Option{Token: tokenA, Name: "ada"})
	defer srv.Close()
	writeConfig(t, home, configYAML(srv.URL(), true, ""))
	var in countReader
	in.r = strings.NewReader("\n")
	got = runReader(context.Background(), home, nil, &in, "login", "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 {
		t.Fatalf("empty token exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}

	pass := "pass-" + tokenA
	writeConfig(t, home, "contexts:\n  dev:\n    url: http://u:"+pass+"@127.0.0.1:9/api/v1\n    insecure_http: true\n")
	in = countReader{r: strings.NewReader(tokenA + "\n")}
	got = runReader(context.Background(), home, nil, &in, "login", "--context", "dev")
	if got.code != ExitUsage || in.n != 0 {
		t.Fatalf("userinfo exit %d reads %d", got.code, in.n)
	}
	noLeak(t, pass, got.out+got.err)
	noLeak(t, tokenA, got.out+got.err)
}

func TestLoginPromptsOnTTY(t *testing.T) {
	srv := apitest.Start(apitest.Option{Token: tokenA, Name: "ada"})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, ""))
	var stderr bytes.Buffer
	app := &App{
		ctx:    context.Background(),
		Stdin:  strings.NewReader(tokenA + "\n"),
		Stdout: &bytes.Buffer{},
		Stderr: &stderr,
		env:    func(string) string { return "" },
		home:   home,
		opt:    options{timeout: 30 * time.Second, context: "dev", contextSet: true},
		tty:    true,
	}
	if code := runLogin(app); code != ExitOK {
		t.Fatalf("exit %d stderr %q", code, stderr.String())
	}
	if !strings.HasPrefix(stderr.String(), "Token: ") {
		t.Fatalf("stderr %q", stderr.String())
	}
	noLeak(t, tokenA, stderr.String())
}

func TestHeaders(t *testing.T) {
	srv := apitest.Start(apitest.Option{Token: tokenA, Name: "ada"})
	defer srv.Close()
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--context", "dev")
	if got.code != ExitOK {
		t.Fatal(got.err)
	}
	call := srv.Calls()[0]
	if call.Method != "GET" || call.Path != "/api/v1/auth/me" {
		t.Fatalf("request %s %s", call.Method, call.Path)
	}
	if call.Authorization != "Bearer "+tokenA {
		t.Fatal("authorization")
	}
	if call.UserAgent != "mftiko-cli/"+buildinfo.Version {
		t.Fatalf("user agent %q", call.UserAgent)
	}
	if call.ContentType != "" || call.Cookie != "" {
		t.Fatalf("content-type %q cookie %q", call.ContentType, call.Cookie)
	}

	api, err := Dial(Endpoint{URL: srv.URL(), Token: tokenA, InsecureHTTP: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	logout := srv.Calls()[1]
	if logout.Operation != "Logout" || logout.Method != "POST" || logout.ContentType != "" {
		t.Fatalf("logout %+v", logout)
	}
	if logout.Authorization != "Bearer "+tokenA {
		t.Fatal("logout authorization")
	}
}

func TestConfirm(t *testing.T) {
	var out bytes.Buffer
	var in countReader
	in.r = strings.NewReader("y\n")
	if err := Confirm(&in, &out, false, false, "delete the session"); !errors.Is(err, ErrYesRequired) || in.n != 0 {
		t.Fatalf("non-tty %v reads %d", err, in.n)
	}
	if err := Confirm(strings.NewReader("n\n"), io.Discard, false, true, "delete"); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := Confirm(strings.NewReader("y\n"), &out, true, true, "delete the session"); err != nil {
		t.Fatal(err)
	}
	if out.String() != "delete the session [y/N] " {
		t.Fatalf("prompt %q", out.String())
	}
	for _, line := range []string{"Y\n", "y", "n\n", "yes\n", "\n"} {
		err := Confirm(strings.NewReader(line), io.Discard, true, false, "delete")
		wantNil := line == "Y\n" || line == "y"
		if wantNil && err != nil || !wantNil && !errors.Is(err, ErrNotConfirmed) {
			t.Fatalf("%q -> %v", line, err)
		}
	}
	app := &App{Stdin: strings.NewReader("y\n"), Stderr: io.Discard, tty: true}
	if err := app.confirm("drop"); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareAndTable(t *testing.T) {
	if _, _, err := prepare(Endpoint{URL: "http://127.0.0.1:9/api/v1/auth/me", InsecureHTTP: true, Timeout: time.Second}); err == nil {
		t.Fatal("operation path accepted")
	}
	raw, _, err := prepare(Endpoint{URL: "http://127.0.0.1:9/api/v1", InsecureHTTP: true, Timeout: time.Second})
	if err != nil || raw != "http://127.0.0.1:9/api/v1/" {
		t.Fatalf("normalized %q %v", raw, err)
	}
	header, rows := meTable(apiv1.Me{Name: "林"})
	var buf strings.Builder
	if err := WriteTable(&buf, header, rows); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "NAME  EXPIRES_AT\n林\n" {
		t.Fatalf("table %q", buf.String())
	}
}

type countReader struct {
	r io.Reader
	n int
}

func (c *countReader) Read(p []byte) (int, error) {
	c.n++
	return c.r.Read(p)
}

type result struct {
	code int
	out  string
	err  string
}

func runAt(ctx context.Context, home string, env map[string]string, stdin io.Reader, args ...string) result {
	return runReader(ctx, home, env, stdin, args...)
}

func runReader(ctx context.Context, home string, env map[string]string, stdin io.Reader, args ...string) result {
	var out, err bytes.Buffer
	if env == nil {
		env = map[string]string{}
	}
	code := Run(ctx, args, stdin, &out, &err, func(k string) string { return env[k] }, home)
	return result{code: code, out: out.String(), err: err.String()}
}

func writeConfig(t *testing.T, home, body string) string {
	t.Helper()
	path := ConfigPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func configYAML(url string, insecure bool, devToken string) string {
	flag := "false"
	if insecure {
		flag = "true"
	}
	tok := ""
	if devToken != "" {
		tok = "    token: " + devToken + "\n"
	}
	return "contexts:\n  dev:\n    url: " + url + "\n    insecure_http: " + flag + "\n" + tok +
		"  accept:\n    url: " + url + "\n    insecure_http: " + flag + "\n    token: " + tokenC + "\n"
}

func tlsConfig(url, ca, devToken string) string {
	tok := ""
	if devToken != "" {
		tok = "    token: " + devToken + "\n"
	}
	caLine := ""
	if ca != "" {
		caLine = "    ca_file: " + ca + "\n"
	}
	return "contexts:\n  dev:\n    url: " + url + "\n" + caLine + tok
}

func assertMeCall(t *testing.T, calls []apitest.Call, token string) {
	t.Helper()
	if len(calls) != 1 {
		t.Fatalf("calls %+v", calls)
	}
	c := calls[0]
	if c.Operation != "GetMe" || c.Method != "GET" || c.Path != "/api/v1/auth/me" {
		t.Fatalf("call %+v", c)
	}
	if c.Authorization != "Bearer "+token || c.ContentType != "" || c.Cookie != "" {
		t.Fatalf("headers %+v", c)
	}
	if c.UserAgent != "mftiko-cli/"+buildinfo.Version {
		t.Fatalf("ua %q", c.UserAgent)
	}
}

func noLeak(t *testing.T, token, text string) {
	t.Helper()
	if token != "" && strings.Contains(text, token) {
		t.Fatal("token appeared in output")
	}
}
