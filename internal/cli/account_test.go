package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
)

func TestAccountCreateHMAC(t *testing.T) {
	srv, home := startAcct(t, apitest.Option{RequestID: "test-secret-a"})
	key := writeCred(t, "test-key-a\n")
	secret := writeCred(t, "test-secret-a\n\n")
	pass := writeCred(t, "test-pass-a\r\n")
	got := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "create",
		"-name", "ada-okx", "-kind", "Okx", "-td-instance", "td1",
		"-key-file", key, "-secret-file", secret, "-passphrase-file", pass, "-v")
	noLeak(t, "test-key-a", got.out+got.err)
	noLeak(t, "test-secret-a", got.out+got.err)
	noLeak(t, "test-pass-a", got.out+got.err)
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if !strings.Contains(got.out, "ada-okx") || !strings.Contains(got.out, "Okx") {
		t.Fatalf("stdout %q", got.out)
	}
	if strings.Contains(got.out, "test-secret") || strings.Contains(got.err, "credentials") {
		t.Fatalf("credential field in output")
	}
	if !strings.Contains(got.err, "X-Request-Id: ***") {
		t.Fatalf("stderr %q", got.err)
	}
	body := createBody(t, srv.Calls())
	if body.Credentials == nil || body.Credentials.KeyType != apiv1.Hmac {
		t.Fatalf("credentials %+v", body.Credentials)
	}
	if body.Credentials.Key != "test-key-a" || body.Credentials.Secret != "test-secret-a\n" {
		t.Fatalf("hmac bytes key %q secret %q", body.Credentials.Key, body.Credentials.Secret)
	}
	if body.Credentials.Passphrase == nil || *body.Credentials.Passphrase != "test-pass-a" {
		t.Fatalf("passphrase %+v", body.Credentials.Passphrase)
	}
	c := opCalls(srv.Calls(), "CreateAccount")[0]
	if c.Authorization != "Bearer "+tokenA || c.ContentType != "application/json" || c.Cookie != "" {
		t.Fatalf("headers %+v", c)
	}
	if !strings.Contains(c.UserAgent, "mftiko-cli/") {
		t.Fatalf("ua %q", c.UserAgent)
	}
}

func TestAccountCreateEd25519Bytes(t *testing.T) {
	srv, home := startAcct(t, apitest.Option{RequestID: "req-pem"})
	pemBytes := testPEM(t)
	fileBytes := append(append([]byte(nil), pemBytes...), '\n')
	secret := writeCred(t, string(fileBytes))
	key := writeCred(t, "test-key-a")
	got := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "-o", "json", "-v", "account", "create",
		"-name", "ada-ed", "-kind", "Ed", "-td-instance", "td1",
		"-key-file", key, "-secret-file", secret)
	for _, line := range strings.Split(string(fileBytes), "\n") {
		if len(line) >= 8 {
			noLeak(t, line, got.out+got.err)
		}
	}
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if !strings.Contains(got.err, "X-Request-Id: req-pem") {
		t.Fatalf("stderr %q", got.err)
	}
	if got.out != string(srv.LastBody()) {
		t.Fatal("json was rewritten")
	}
	body := createBody(t, srv.Calls())
	if body.Credentials == nil || body.Credentials.Secret != string(fileBytes) || body.Credentials.KeyType != apiv1.Ed25519 {
		t.Fatal("pem was not sent unchanged")
	}
	if strings.Contains(got.out, "BEGIN") {
		t.Fatal("pem header in stdout")
	}
}

func TestAccountCreateFromStdin(t *testing.T) {
	srv, home := startAcct(t, apitest.Option{})
	key := writeCred(t, "test-key-a\n")
	pass := writeCred(t, "test-pass-a\n")
	var in countReader
	in.r = strings.NewReader("test-secret-a\n")
	got := runReader(context.Background(), home, nil, &in,
		"--context", "dev", "account", "create",
		"-name", "ada-okx", "-kind", "Okx", "-td-instance", "td1",
		"-key-file", key, "-secret-file", "-", "-passphrase-file", pass)
	noLeak(t, "test-secret-a", got.out+got.err)
	if got.code != ExitOK || in.n == 0 {
		t.Fatalf("exit %d reads %d stderr %q", got.code, in.n, got.err)
	}
	body := createBody(t, srv.Calls())
	if body.Credentials == nil || body.Credentials.Secret != "test-secret-a" {
		t.Fatalf("stdin secret %+v", body.Credentials)
	}
}

func TestAccountCreatePrompts(t *testing.T) {
	srv, home := startAcct(t, apitest.Option{})
	app, out, err := newApp(home, strings.NewReader("test-key-a\ntest-secret-a\ntest-pass-a\n"), options{
		context:    "dev",
		contextSet: true,
		name:       strOpt("ada-okx"),
		kind:       strOpt("Okx"),
		tdInstance: strOpt("td1"),
	}, true)
	code := runAccountCreate(app)
	text := out.String() + err.String()
	noLeak(t, "test-key-a", text)
	noLeak(t, "test-secret-a", text)
	noLeak(t, "test-pass-a", text)
	if code != ExitOK {
		t.Fatalf("exit %d stderr %q", code, err.String())
	}
	if !strings.Contains(err.String(), "Key: ") || !strings.Contains(err.String(), "Secret: ") || !strings.Contains(err.String(), "Passphrase: ") {
		t.Fatalf("prompts %q", err.String())
	}
	body := createBody(t, srv.Calls())
	if body.Credentials == nil || body.Credentials.Key != "test-key-a" || body.Credentials.Secret != "test-secret-a" || body.Credentials.Passphrase == nil || *body.Credentials.Passphrase != "test-pass-a" {
		t.Fatalf("prompted credentials %+v", body.Credentials)
	}
}

func TestAccountCreateEd25519Prompt(t *testing.T) {
	srv, home := startAcct(t, apitest.Option{})
	pemBytes := testPEM(t)
	stdin := "test-key-a\n" + string(pemBytes)
	app, out, err := newApp(home, strings.NewReader(stdin), options{
		context:    "dev",
		contextSet: true,
		name:       strOpt("ada-ed"),
		kind:       strOpt("Ed"),
		tdInstance: strOpt("td1"),
	}, true)
	code := runAccountCreate(app)
	text := out.String() + err.String()
	for _, line := range strings.Split(string(pemBytes), "\n") {
		if len(line) >= 8 {
			noLeak(t, line, text)
		}
	}
	if code != ExitOK {
		t.Fatalf("exit %d stderr %q", code, err.String())
	}
	if strings.Contains(err.String(), "Passphrase:") {
		t.Fatalf("passphrase prompted: %q", err.String())
	}
	body := createBody(t, srv.Calls())
	if body.Credentials == nil || body.Credentials.Secret != string(pemBytes) {
		t.Fatal("prompted pem was changed")
	}
}

func TestAccountCreateRejected(t *testing.T) {
	t.Run("unsupported", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		var in countReader
		in.r = strings.NewReader("")
		got := runReader(context.Background(), home, nil, &in,
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "BinancePM", "-td-instance", "td1",
			"-key-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/kind: is not accepted") {
			t.Fatalf("%+v", got)
		}
		if in.n != 0 || len(opCalls(srv.Calls(), "CreateAccount")) != 0 || len(opCalls(srv.Calls(), "GetInstance")) != 0 {
			t.Fatalf("calls %+v reads %d", srv.Calls(), in.n)
		}
		if strings.Contains(got.err, "cannot read") {
			t.Fatalf("read a credential: %q", got.err)
		}
	})
	t.Run("md plane", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "md1",
			"-key-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/td_instance: is not a td instance") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 0 || len(opCalls(srv.Calls(), "GetInstance")) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("not a name", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "TD1")
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/td_instance: is not an instance name") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "GetInstance")) != 0 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("missing instance", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "td-missing")
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/td_instance: is not in the topology") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 0 || len(opCalls(srv.Calls(), "GetInstance")) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("key type required", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Binance", "-td-instance", "td1",
			"-key-file", filepath.Join(t.TempDir(), "missing"),
			"-secret-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitUsage || !strings.Contains(got.err, "-key-type is required") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 0 || strings.Contains(got.err, "cannot read") {
			t.Fatalf("%+v calls %+v", got, srv.Calls())
		}
	})
	t.Run("key type rejected", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "td1", "-key-type", "ed25519")
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/credentials/key_type: is not accepted") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 0 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("passphrase unused", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Binance", "-td-instance", "td1", "-key-type", "hmac",
			"-passphrase-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/credentials/passphrase: is not used by this account kind") {
			t.Fatalf("%+v", got)
		}
		if strings.Contains(got.err, "cannot read") || len(opCalls(srv.Calls(), "CreateAccount")) != 0 {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("passphrase required", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "td1",
			"-key-file", filepath.Join(t.TempDir(), "missing"),
			"-secret-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/credentials/passphrase: required") {
			t.Fatalf("%+v", got)
		}
		if strings.Contains(got.err, "cannot read") || len(opCalls(srv.Calls(), "CreateAccount")) != 0 {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("two stdins", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		var in countReader
		in.r = strings.NewReader("test-secret-a\n")
		got := runReader(context.Background(), home, nil, &in,
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "td1",
			"-key-file", "-", "-secret-file", "-", "-passphrase-file", filepath.Join(t.TempDir(), "missing"))
		noLeak(t, "test-secret-a", got.out+got.err)
		if got.code != ExitUsage || !strings.Contains(got.err, "at most one credential file can be -") || in.n != 0 {
			t.Fatalf("exit %d reads %d stderr %q", got.code, in.n, got.err)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 0 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("flag value", func(t *testing.T) {
		var in countReader
		in.r = strings.NewReader("")
		got := runReader(context.Background(), t.TempDir(), nil, &in,
			"account", "create", "--secret", "test-secret-a")
		noLeak(t, "test-secret-a", got.out+got.err)
		if got.code != ExitUsage || in.n != 0 {
			t.Fatalf("exit %d reads %d stderr %q", got.code, in.n, got.err)
		}
	})
	t.Run("echo redacted", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{EchoCredential: true, RequestID: "req-echo"})
		key := writeCred(t, "test-key-a\n")
		secret := writeCred(t, "test-secret-a\n")
		pass := writeCred(t, "test-pass-a\n")
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "create",
			"-name", "ada", "-kind", "Okx", "-td-instance", "td1",
			"-key-file", key, "-secret-file", secret, "-passphrase-file", pass)
		noLeak(t, "test-secret-a", got.out+got.err)
		noLeak(t, "test-key-a", got.out+got.err)
		noLeak(t, "test-pass-a", got.out+got.err)
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "***") {
			t.Fatalf("exit %d stderr %q", got.code, got.err)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("name conflict", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{})
		args := func() []string {
			return []string{"--context", "dev", "account", "create",
				"-name", "ada-okx", "-kind", "Okx", "-td-instance", "td1",
				"-key-file", writeCred(t, "test-key-a\n"),
				"-secret-file", writeCred(t, "test-secret-a\n"),
				"-passphrase-file", writeCred(t, "test-pass-a\n")}
		}
		first := runAt(context.Background(), home, nil, strings.NewReader(""), args()...)
		noLeak(t, "test-secret-a", first.out+first.err)
		if first.code != ExitOK {
			t.Fatalf("first %+v", first)
		}
		second := runAt(context.Background(), home, nil, strings.NewReader(""), args()...)
		noLeak(t, "test-secret-a", second.out+second.err)
		if second.code != ExitConflict || !strings.Contains(second.err, "/name: already in use") || !strings.Contains(second.err, "run the command again") {
			t.Fatalf("%+v", second)
		}
		if len(opCalls(srv.Calls(), "CreateAccount")) != 2 {
			t.Fatalf("retried %+v", srv.Calls())
		}
	})
}

func TestAccountUpdate(t *testing.T) {
	t.Run("keep credentials", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "update", "7")
		if got.code != ExitOK || !strings.Contains(got.out, "ada-okx") {
			t.Fatalf("%+v", got)
		}
		calls := opCalls(srv.Calls(), "UpdateAccount")
		if len(calls) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
		var body apiv1.AccountUpdate
		if err := json.Unmarshal(calls[0].Body, &body); err != nil {
			t.Fatal(err)
		}
		if body.Credentials != nil || body.Revision != 1 || bytes.Contains(calls[0].Body, []byte("credentials")) {
			t.Fatalf("body %s", calls[0].Body)
		}
		if len(opCalls(srv.Calls(), "ListAccountKinds")) != 0 {
			t.Fatal("kinds were fetched")
		}
	})
	t.Run("replace credentials", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "update", "7",
			"-key-file", writeCred(t, "test-key-a\n"),
			"-secret-file", writeCred(t, "test-secret-a\n"),
			"-passphrase-file", writeCred(t, "test-pass-a\n"))
		noLeak(t, "test-secret-a", got.out+got.err)
		if got.code != ExitOK {
			t.Fatalf("%+v", got)
		}
		calls := opCalls(srv.Calls(), "UpdateAccount")
		if len(calls) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
		var body apiv1.AccountUpdate
		if err := json.Unmarshal(calls[0].Body, &body); err != nil {
			t.Fatal(err)
		}
		if body.Revision != 1 || body.Credentials == nil || body.Credentials.Secret != "test-secret-a" || body.Credentials.KeyType != apiv1.Hmac {
			t.Fatalf("%+v", body)
		}
	})
	t.Run("partial", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "update", "7",
			"-key-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/credentials/secret: required") || !strings.Contains(got.err, "/credentials/passphrase: required") {
			t.Fatalf("%+v", got)
		}
		if strings.Contains(got.err, "cannot read") || len(opCalls(srv.Calls(), "UpdateAccount")) != 0 {
			t.Fatalf("%+v calls %+v", got, srv.Calls())
		}
	})
	t.Run("key type alone", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "update", "7", "-key-type", "hmac")
		if got.code != ExitUsage || !strings.Contains(got.err, "-key-type was set but no credential file was given") {
			t.Fatalf("%+v", got)
		}
		if len(srv.Calls()) != 0 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("conflict", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{
			Accounts:    []apiv1.Account{seedAccount(7, "ada-okx", "Okx")},
			ConflictCAS: true,
		})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "update", "7",
			"-key-file", writeCred(t, "test-key-a\n"),
			"-secret-file", writeCred(t, "test-secret-a\n"),
			"-passphrase-file", writeCred(t, "test-pass-a\n"))
		noLeak(t, "test-secret-a", got.out+got.err)
		if got.code != ExitConflict || !strings.Contains(got.err, "conflict (13)") || !strings.Contains(got.err, "run the command again") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "UpdateAccount")) != 1 || len(opCalls(srv.Calls(), "GetAccount")) != 1 {
			t.Fatalf("retried %+v", srv.Calls())
		}
	})
	t.Run("unsupported kind", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-pm", "BinancePM")}})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "account", "update", "7",
			"-key-file", filepath.Join(t.TempDir(), "missing"))
		if got.code != ExitValidationFailed || !strings.Contains(got.err, "/kind: is not accepted") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "UpdateAccount")) != 0 || strings.Contains(got.err, "cannot read") {
			t.Fatalf("%+v", got)
		}
	})
}

func TestAccountDelete(t *testing.T) {
	t.Run("yes", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		var in countReader
		in.r = strings.NewReader("y\n")
		got := runReader(context.Background(), home, nil, &in,
			"--context", "dev", "--yes", "account", "delete", "7")
		if got.code != ExitOK || strings.TrimSpace(got.out) != "" || in.n != 0 {
			t.Fatalf("exit %d reads %d out %q err %q", got.code, in.n, got.out, got.err)
		}
		calls := opCalls(srv.Calls(), "DeleteAccount")
		if len(calls) != 1 || !strings.Contains(calls[0].Query, "revision=1") || calls[0].ContentType != "" {
			t.Fatalf("delete %+v", calls)
		}
	})
	t.Run("tty declines even with yes", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		app, _, err := newApp(home, strings.NewReader("n\n"), options{
			context: "dev", contextSet: true, yes: true,
		}, true)
		app.Args = []string{"7"}
		code := runAccountDelete(app)
		if code != ExitUsage || !strings.Contains(err.String(), "not confirmed") {
			t.Fatalf("exit %d stderr %q", code, err.String())
		}
		if len(srv.Calls()) != 0 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("tty confirms", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		app, _, err := newApp(home, strings.NewReader("y\n"), options{
			context: "dev", contextSet: true, yes: true,
		}, true)
		app.Args = []string{"7"}
		code := runAccountDelete(app)
		if code != ExitOK {
			t.Fatalf("exit %d stderr %q", code, err.String())
		}
		if len(opCalls(srv.Calls(), "DeleteAccount")) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("not a terminal", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{seedAccount(7, "ada-okx", "Okx")}})
		var in countReader
		in.r = strings.NewReader("y\n")
		got := runReader(context.Background(), home, nil, &in, "--context", "dev", "account", "delete", "7")
		if got.code != ExitUsage || !strings.Contains(got.err, "not a terminal") || in.n != 0 {
			t.Fatalf("exit %d reads %d stderr %q", got.code, in.n, got.err)
		}
		if len(srv.Calls()) != 0 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("referenced", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{Accounts: []apiv1.Account{
			seedAccount(7, "ada-okx", "Okx", "ab12cd", "cd34ef"),
		}})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "--yes", "account", "delete", "7")
		if got.code != ExitReferenced || !strings.Contains(got.err, "referenced (15)") {
			t.Fatalf("%+v", got)
		}
		if !strings.Contains(got.err, "referenced_by:") || !strings.Contains(got.err, "ab12cd") || !strings.Contains(got.err, "cd34ef") {
			t.Fatalf("stderr %q", got.err)
		}
		if len(opCalls(srv.Calls(), "DeleteAccount")) != 1 {
			t.Fatalf("calls %+v", srv.Calls())
		}
	})
	t.Run("conflict", func(t *testing.T) {
		srv, home := startAcct(t, apitest.Option{
			Accounts:    []apiv1.Account{seedAccount(7, "ada-okx", "Okx")},
			ConflictCAS: true,
		})
		got := runAt(context.Background(), home, nil, strings.NewReader(""),
			"--context", "dev", "--yes", "account", "delete", "7")
		if got.code != ExitConflict || !strings.Contains(got.err, "run the command again") {
			t.Fatalf("%+v", got)
		}
		if len(opCalls(srv.Calls(), "DeleteAccount")) != 1 {
			t.Fatalf("retried %+v", srv.Calls())
		}
	})
}

func TestAccountListAndShow(t *testing.T) {
	accounts := []apiv1.Account{
		seedAccount(1, "one", "Okx"),
		seedAccount(2, "two", "Okx"),
		seedAccount(3, "three", "Okx"),
	}
	fresh := apiv1.Freshness{UnavailableRegions: []string{"tw"}}
	next := apiv1.Freshness{ControllerAbsent: true, UnavailableRegions: []string{"jp"}}
	srv, home := startAcct(t, apitest.Option{
		Accounts: accounts, PageSize: 1, Freshness: fresh, NextFreshness: &next,
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "account", "list")
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
	for _, name := range []string{"one", "two", "three"} {
		if !strings.Contains(got.out, name) {
			t.Fatalf("stdout %q", got.out)
		}
	}
	if len(opCalls(srv.Calls(), "ListAccounts")) != 3 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	if strings.Count(got.err, "unavailable regions") != 1 || strings.Count(got.err, "controller absent") != 1 {
		t.Fatalf("stderr %q", got.err)
	}
	if !strings.Contains(got.err, "jp, tw") {
		t.Fatalf("regions %q", got.err)
	}

	before := len(srv.Calls())
	limited := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "list", "-limit", "2")
	if limited.code != ExitOK || !strings.Contains(limited.out, "one") || !strings.Contains(limited.out, "two") || strings.Contains(limited.out, "three") {
		t.Fatalf("%+v", limited)
	}
	if len(srv.Calls())-before != 2 {
		t.Fatalf("limit calls %d", len(srv.Calls())-before)
	}

	srv, home = startAcct(t, apitest.Option{
		Accounts: accounts, PageSize: 1, Freshness: fresh, NextFreshness: &next,
	})
	js := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "-o", "json", "account", "list")
	if js.code != ExitOK {
		t.Fatalf("%+v", js)
	}
	var merged struct {
		Items []apiv1.Account `json:"items"`
		Next  *string         `json:"next_cursor"`
		Fresh apiv1.Freshness `json:"freshness"`
	}
	if err := json.Unmarshal([]byte(js.out), &merged); err != nil {
		t.Fatal(err)
	}
	if len(merged.Items) != 3 || merged.Next != nil || !merged.Fresh.ControllerAbsent {
		t.Fatalf("merged %+v", merged)
	}
	if strings.Join(merged.Fresh.UnavailableRegions, ",") != "jp,tw" {
		t.Fatalf("regions %+v", merged.Fresh.UnavailableRegions)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(js.out), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["next_cursor"]; ok {
		t.Fatal("merged json has next_cursor")
	}

	page := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "-o", "json", "account", "list", "--cursor=")
	if page.code != ExitOK || page.out != string(srv.LastBody()) || !strings.Contains(page.out, "next_cursor") {
		t.Fatalf("page out %q body %q err %q", page.out, srv.LastBody(), page.err)
	}
	var first apiv1.AccountList
	if err := json.Unmarshal([]byte(page.out), &first); err != nil || first.NextCursor == nil {
		t.Fatalf("first page %v %v", err, first.NextCursor)
	}
	before = len(srv.Calls())
	nextPage := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "-o", "json", "account", "list", "--cursor", *first.NextCursor)
	if nextPage.code != ExitOK || nextPage.out != string(srv.LastBody()) {
		t.Fatalf("next %q body %q err %q", nextPage.out, srv.LastBody(), nextPage.err)
	}
	if len(srv.Calls())-before != 1 || !strings.Contains(nextPage.out, "two") || strings.Contains(nextPage.out, "\"name\":\"one\"") {
		t.Fatalf("followed or wrong page: delta %d out %s", len(srv.Calls())-before, nextPage.out)
	}

	filtered := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "list", "-td-instance", "md1")
	if filtered.code != ExitValidationFailed || !strings.Contains(filtered.err, "/td_instance: is not a td instance") {
		t.Fatalf("%+v", filtered)
	}

	show := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "show", "2")
	if show.code != ExitOK || !strings.Contains(show.out, "two") || !strings.Contains(show.out, "td1") {
		t.Fatalf("%+v", show)
	}
	showJSON := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "-o", "json", "account", "show", "2")
	if showJSON.code != ExitOK || showJSON.out != string(srv.LastBody()) {
		t.Fatalf("show json %q body %q", showJSON.out, srv.LastBody())
	}
	missing := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "show", "99")
	if missing.code != ExitNotFound {
		t.Fatalf("%+v", missing)
	}
	bad := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "show", "0")
	if bad.code != ExitUsage {
		t.Fatalf("%+v", bad)
	}
	badCursor := runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "account", "list", "--cursor", "!!!")
	if badCursor.code != ExitValidationFailed || !strings.Contains(badCursor.err, "/cursor:") {
		t.Fatalf("%+v", badCursor)
	}
}

func TestAccountUsage(t *testing.T) {
	home := t.TempDir()
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "help")
	if got.code != ExitOK || !strings.Contains(got.out, "account") {
		t.Fatalf("%+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "help", "account")
	if got.code != ExitOK || !strings.Contains(got.out, "-key-file") || !strings.Contains(got.out, "-key-type") {
		t.Fatalf("%+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "account")
	if got.code != ExitUsage || !strings.Contains(got.err, "account list|show|create|update|delete") {
		t.Fatalf("%+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "account", "list", "extra")
	if got.code != ExitUsage {
		t.Fatalf("%+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "--name", "x")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -name") {
		t.Fatalf("%+v", got)
	}
}

func TestStripOneNewline(t *testing.T) {
	if stripOneNewline([]byte("test-secret-a\n")) != "test-secret-a" {
		t.Fatal("one")
	}
	if stripOneNewline([]byte("test-secret-a\n\n")) != "test-secret-a\n" {
		t.Fatal("two")
	}
	if stripOneNewline([]byte("test-secret-a\r\n")) != "test-secret-a" {
		t.Fatal("crlf")
	}
	if stripOneNewline([]byte("test-secret-a")) != "test-secret-a" {
		t.Fatal("none")
	}
}

func startAcct(t *testing.T, opt apitest.Option) (*apitest.Server, string) {
	t.Helper()
	if opt.Token == "" {
		opt.Token = tokenA
	}
	if opt.Kinds == nil {
		opt.Kinds = testKinds()
	}
	if opt.Instances == nil {
		opt.Instances = testInstances()
	}
	srv := apitest.Start(opt)
	t.Cleanup(srv.Close)
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	return srv, home
}

func testKinds() []apiv1.AccountKind {
	return []apiv1.AccountKind{
		{Kind: "Okx", Label: "OKX", Trades: []string{"Okx"}, KeyTypes: []apiv1.KeyType{apiv1.Hmac}, Passphrase: true, Supported: true},
		{Kind: "Binance", Label: "Binance", Trades: []string{"Binance"}, KeyTypes: []apiv1.KeyType{apiv1.Hmac, apiv1.Ed25519}, Passphrase: false, Supported: true},
		{Kind: "Ed", Label: "Ed", Trades: []string{"Ed"}, KeyTypes: []apiv1.KeyType{apiv1.Ed25519}, Passphrase: false, Supported: true},
		{Kind: "BinancePM", Label: "PM", Trades: []string{"a", "b"}, KeyTypes: []apiv1.KeyType{apiv1.Hmac}, Passphrase: false, Supported: false},
	}
}

func testInstances() []apiv1.Instance {
	return []apiv1.Instance{
		{Name: "td1", Plane: apiv1.Td, Region: "tw", RegionAvailable: true},
		{Name: "md1", Plane: apiv1.Md, Region: "tw", RegionAvailable: true},
	}
}

func seedAccount(id int64, name, kind string, refs ...string) apiv1.Account {
	sessions := make([]apiv1.SessionIDValue, len(refs))
	for i, ref := range refs {
		sessions[i] = ref
	}
	return apiv1.Account{
		ApiId: id, Name: name, Kind: kind, TdInstance: "td1", Region: "tw",
		Revision: 1, ReferencedBy: sessions,
	}
}

func writeCred(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "value")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testPEM(t *testing.T) []byte {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func newApp(home string, in io.Reader, opt options, tty bool) (*App, *bytes.Buffer, *bytes.Buffer) {
	var out, err bytes.Buffer
	if opt.timeout == 0 {
		opt.timeout = 30 * time.Second
	}
	app := &App{
		ctx:     context.Background(),
		Stdin:   in,
		Stdout:  &out,
		Stderr:  &err,
		env:     func(string) string { return "" },
		home:    home,
		opt:     opt,
		JSON:    opt.output == "json",
		Verbose: opt.verbose,
		Yes:     opt.yes,
		tty:     tty,
	}
	return app, &out, &err
}

func strOpt(v string) stringOpt { return stringOpt{v: v, set: true} }

func opCalls(calls []apitest.Call, op string) []apitest.Call {
	var out []apitest.Call
	for _, c := range calls {
		if c.Operation == op {
			out = append(out, c)
		}
	}
	return out
}

func createBody(t *testing.T, calls []apitest.Call) apiv1.AccountCreate {
	t.Helper()
	for _, c := range calls {
		if c.Operation == "CreateAccount" {
			var body apiv1.AccountCreate
			if err := json.Unmarshal(c.Body, &body); err != nil {
				t.Fatal(err)
			}
			return body
		}
	}
	t.Fatal("no create")
	return apiv1.AccountCreate{}
}
