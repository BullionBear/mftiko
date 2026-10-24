package cli

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
)

const idemOnes = "11111111-1111-1111-1111-111111111111"

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestRetryableNet(t *testing.T) {
	ctx := context.Background()
	if !retryableNet(ctx, io.EOF) || !retryableNet(ctx, io.ErrUnexpectedEOF) || !retryableNet(ctx, context.DeadlineExceeded) || !retryableNet(ctx, net.ErrClosed) {
		t.Fatal("connection errors must be retried")
	}
	if retryableNet(ctx, context.Canceled) || retryableNet(ctx, errors.New("nope")) {
		t.Fatal("unrelated errors must not be retried")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if retryableNet(canceled, context.DeadlineExceeded) || retryableNet(canceled, io.EOF) {
		t.Fatal("a canceled parent must not be retried")
	}
	reset := &url.Error{Op: "POST", URL: "http://127.0.0.1/api/v1/sessions", Err: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}}
	if !retryableNet(ctx, reset) {
		t.Fatal("connection reset must be retried")
	}
	timed := &url.Error{Op: "POST", URL: "http://127.0.0.1/api/v1/sessions", Err: context.DeadlineExceeded}
	if !retryableNet(ctx, timed) {
		t.Fatal("client timeout must be retried")
	}
	other := &url.Error{Op: "POST", URL: "http://127.0.0.1/api/v1/sessions", Err: errors.New("stopped")}
	if retryableNet(ctx, other) {
		t.Fatal("an unrelated client error must not be retried")
	}
}

func TestSessionCreatePreservesNumbers(t *testing.T) {
	srv, home := startSession(t, apitest.Option{RequestID: "req-sess"})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	calls := srv.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls %d", len(calls))
	}
	assertSessionCall(t, calls[0], calls[0].IdempotencyKey)
	if !uuidRe.MatchString(calls[0].IdempotencyKey) {
		t.Fatalf("key %q", calls[0].IdempotencyKey)
	}
	if string(calls[0].Body) != sessionJSON {
		t.Fatalf("body %s", calls[0].Body)
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("success printed the key: %q", got.err)
	}
	digest := "sha256:" + strings.Repeat("ab", 32)
	if !strings.Contains(got.out, "SESSION_ID") || !strings.Contains(got.out, "DIGEST") || !strings.Contains(got.out, "000001") || !strings.Contains(got.out, digest) {
		t.Fatalf("stdout %q", got.out)
	}
	if ids := srv.SessionIDs(); len(ids) != 1 || ids[0] != "000001" {
		t.Fatalf("ids %v", ids)
	}

	jsonPath := writeBody(t, "session.json", sessionJSON)
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", jsonPath, "--context", "dev", "-o", "json")
	if got.code != ExitOK || string(srv.Calls()[1].Body) != sessionJSON {
		t.Fatalf("json exit %d body %s stderr %q", got.code, srv.Calls()[1].Body, got.err)
	}
	if got.out != string(srv.LastBody()) {
		t.Fatalf("stdout is not the API body\n%s\n%s", got.out, srv.LastBody())
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(sessionYAML), "session", "create", "-f", "-", "--context", "dev", "-v")
	if got.code != ExitOK || string(srv.Calls()[2].Body) != sessionJSON {
		t.Fatalf("stdin exit %d body %s stderr %q", got.code, srv.Calls()[2].Body, got.err)
	}
	if !strings.Contains(got.err, "X-Request-Id: req-sess\n") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("verbose stderr %q", got.err)
	}
}

func TestSessionCreateIdempotencyKey(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if srv.Calls()[0].IdempotencyKey != idemOnes {
		t.Fatalf("key %q", srv.Calls()[0].IdempotencyKey)
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("stderr %q", got.err)
	}
	again := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if again.code != ExitOK || srv.Calls()[1].IdempotencyKey != idemOnes {
		t.Fatalf("replay exit %d key %q stderr %q", again.code, srv.Calls()[1].IdempotencyKey, again.err)
	}
	if ids := srv.SessionIDs(); len(ids) != 1 || !strings.Contains(again.out, ids[0]) {
		t.Fatalf("ids %v stdout %q", ids, again.out)
	}
}

func TestSessionCreateRetry(t *testing.T) {
	waits := installWaits(t)
	srv, home := startSession(t, apitest.Option{DropCreates: 1, RequestID: "req-sess"})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	calls := srv.Calls()
	if len(calls) != 2 || calls[0].IdempotencyKey == "" || calls[0].IdempotencyKey != calls[1].IdempotencyKey {
		t.Fatalf("calls %+v", calls)
	}
	if string(calls[0].Body) != sessionJSON || string(calls[1].Body) != sessionJSON {
		t.Fatal("retry changed the body")
	}
	if len(*waits) != 1 || (*waits)[0] != time.Second {
		t.Fatalf("waits %v", *waits)
	}
	if ids := srv.SessionIDs(); len(ids) != 1 || !strings.Contains(got.out, ids[0]) {
		t.Fatalf("ids %v stdout %q", ids, got.out)
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateRetryExhausted(t *testing.T) {
	waits := installWaits(t)
	srv, home := startSession(t, apitest.Option{DropCreates: 4})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	calls := srv.Calls()
	if len(calls) != 4 {
		t.Fatalf("calls %d", len(calls))
	}
	key := calls[0].IdempotencyKey
	for _, c := range calls {
		if c.IdempotencyKey != key || string(c.Body) != sessionJSON {
			t.Fatalf("call %+v", c)
		}
	}
	if len(*waits) != len(sessionBackoffs) {
		t.Fatalf("waits %v", *waits)
	}
	for i, d := range sessionBackoffs {
		if (*waits)[i] != d {
			t.Fatalf("waits %v", *waits)
		}
	}
	if len(srv.SessionIDs()) != 0 {
		t.Fatalf("stored %v", srv.SessionIDs())
	}
	if !strings.Contains(got.err, "network (2):") || strings.Count(got.err, "Idempotency-Key: "+key+"\n") != 1 {
		t.Fatalf("stderr %q", got.err)
	}
	if strings.Contains(got.err, "此 key 已用於不同內容") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateDoesNotRetryHTTP(t *testing.T) {
	waits := installWaits(t)
	srv, home := startSession(t, apitest.Option{StorageUnavailable: true, RequestID: "req-sess"})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitStorageUnavailable || len(srv.Calls()) != 1 || len(*waits) != 0 || len(srv.SessionIDs()) != 0 {
		t.Fatalf("exit %d calls %d waits %v ids %v stderr %q", got.code, len(srv.Calls()), *waits, srv.SessionIDs(), got.err)
	}
	if !strings.Contains(got.err, "storage_unavailable (18):") || strings.Contains(got.err, "此 key 已用於不同內容") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("stderr %q", got.err)
	}

	over, home := startSession(t, apitest.Option{AlgoOverlap: true})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitAlgoOverlap || len(over.Calls()) != 1 || strings.Contains(got.err, "此 key 已用於不同內容") {
		t.Fatalf("overlap exit %d calls %d stderr %q", got.code, len(over.Calls()), got.err)
	}
	if !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateConflict(t *testing.T) {
	srv, home := startSession(t, apitest.Option{RequestID: "req-sess"})
	first := writeBody(t, "a.yaml", sessionYAML)
	second := writeBody(t, "b.yaml", strings.Replace(sessionYAML, "name: demo", "name: other", 1))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", first, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("first exit %d stderr %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", second, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitConflict {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if !strings.Contains(got.err, "conflict (13):") || !strings.Contains(got.err, "此 key 已用於不同內容\n") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") || !strings.Contains(got.err, "X-Request-Id: req-sess\n") {
		t.Fatalf("stderr %q", got.err)
	}
	if len(srv.Calls()) != 2 || len(srv.SessionIDs()) != 1 {
		t.Fatalf("calls %d ids %v", len(srv.Calls()), srv.SessionIDs())
	}
}

func TestSessionCreateFieldErrors(t *testing.T) {
	const src = "name: demo\nsts_instance: sts1\nstrategy:\n  id: chase\n  ref: v1\nparams:\n  big: 123\n"
	_, locs, err := convertSession("ignored", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	pos := locs["/params/big"]
	srv, home := startSession(t, apitest.Option{
		RequestID: "req-sess",
		CreateFields: []apiv1.FieldError{
			{Path: "/params/big", Reason: "nope"},
			{Path: "/no/such", Reason: "gone"},
			{Path: "", Reason: "skip-me"},
		},
	})
	path := writeBody(t, "session.yaml", src)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitValidationFailed || len(srv.Calls()) != 1 || len(srv.SessionIDs()) != 0 {
		t.Fatalf("exit %d calls %d ids %v stderr %q", got.code, len(srv.Calls()), srv.SessionIDs(), got.err)
	}
	want := path + ":7:8 /params/big nope"
	if pos.Line != 7 || pos.Col != 8 || !strings.Contains(got.err, want+"\n") {
		t.Fatalf("pos %+v stderr %q", pos, got.err)
	}
	if !strings.Contains(got.err, "/no/such\n") || strings.Contains(got.err, "gone") || strings.Contains(got.err, "skip-me") {
		t.Fatalf("stderr %q", got.err)
	}
	if !strings.Contains(got.err, "validation_failed (10):") || !strings.Contains(got.err, "X-Request-Id: req-sess\n") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateRejectsFileLocally(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "bad.yaml", "mode: 0755\n")
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 {
		t.Fatalf("exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}
	if !strings.Contains(got.err, "quote it") || !strings.Contains(got.err, "0755") || !strings.Contains(got.err, path+":1:") {
		t.Fatalf("stderr %q", got.err)
	}
	if !regexp.MustCompile(`Idempotency-Key: [0-9a-f-]{36}\n`).MatchString(got.err) {
		t.Fatalf("stderr %q", got.err)
	}

	dup := writeBody(t, "dup.yaml", "name: a\nname: b\n")
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", dup, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 || !strings.Contains(got.err, `duplicate key "name"`) || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("dup exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader("mode: 0x1F\n"), "session", "create", "-f", "-", "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 || !strings.Contains(got.err, "-:1:") || !strings.Contains(got.err, "0x1F") {
		t.Fatalf("stdin exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}
}

func TestSessionCreateSendsUncheckedJSON(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "extra.yaml", "extra: 1\n")
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitOK || len(srv.Calls()) != 1 || string(srv.Calls()[0].Body) != `{"extra":1}` {
		t.Fatalf("exit %d body %s stderr %q", got.code, srv.Calls()[0].Body, got.err)
	}
}

func TestSessionCreateUsage(t *testing.T) {
	home := t.TempDir()
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--context", "dev")
	if got.code != ExitUsage || !strings.Contains(got.err, "session create requires -f") || !regexp.MustCompile(`Idempotency-Key: [0-9a-f-]{36}\n`).MatchString(got.err) {
		t.Fatalf("missing -f: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("explicit missing -f: %d %q", got.code, got.err)
	}
	path := writeBody(t, "session.yaml", sessionYAML)
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "extra", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "takes no arguments") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("args: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "-name", "demo", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -name") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("name: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "-limit", "1", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "-limit and -cursor are only for strategy list and strategy versions, and account list") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("limit: %d %q", got.code, got.err)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "-f", "x", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -f -idempotency-key") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("whoami: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "-f", "x")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -f") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("whoami: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "account", "create", "-f", "x")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -f") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("account: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "help", "session")
	if got.code != ExitOK || !strings.Contains(got.out, "session create -f") || !strings.Contains(got.out, "此 key 已用於不同內容") {
		t.Fatalf("help %d %q", got.code, got.out)
	}

	big := filepath.Join(t.TempDir(), "big.yaml")
	f, err := os.Create(big)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(maxBody) + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", big, "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "session file is too large") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("large: %d %q", got.code, got.err)
	}
}

func TestSessionCreateEmptyKey(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key=", "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if len(srv.Calls()) != 1 || srv.Calls()[0].IdempotencyKey != "" {
		t.Fatalf("calls %+v", srv.Calls())
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateCanceled(t *testing.T) {
	waits := installWaits(t)
	_, home := startSession(t, apitest.Option{})
	path := writeBody(t, "session.yaml", sessionYAML)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitNetwork || len(*waits) != 0 {
		t.Fatalf("exit %d waits %v stderr %q", got.code, *waits, got.err)
	}
	if !strings.Contains(got.err, "network (2):") || !regexp.MustCompile(`Idempotency-Key: [0-9a-f-]{36}\n`).MatchString(got.err) {
		t.Fatalf("stderr %q", got.err)
	}
}

func startSession(t *testing.T, opt apitest.Option) (*apitest.Server, string) {
	t.Helper()
	if opt.Token == "" {
		opt.Token = tokenA
	}
	if opt.Name == "" {
		opt.Name = "ada"
	}
	srv := apitest.Start(opt)
	t.Cleanup(srv.Close)
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	return srv, home
}

func writeBody(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func installWaits(t *testing.T) *[]time.Duration {
	t.Helper()
	var waits []time.Duration
	prev := sessionRetryWait
	sessionRetryWait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		waits = append(waits, d)
		return nil
	}
	t.Cleanup(func() { sessionRetryWait = prev })
	return &waits
}

func assertSessionCall(t *testing.T, c apitest.Call, key string) {
	t.Helper()
	if c.Operation != "CreateSession" || c.Method != "POST" || c.Path != "/api/v1/sessions" {
		t.Fatalf("call %+v", c)
	}
	if c.Authorization != "Bearer "+tokenA || c.ContentType != "application/json" || c.Cookie != "" {
		t.Fatalf("headers %+v", c)
	}
	if !strings.Contains(c.UserAgent, "mftiko-cli/") {
		t.Fatalf("ua %q", c.UserAgent)
	}
	if c.IdempotencyKey != key {
		t.Fatalf("key %q want %q", c.IdempotencyKey, key)
	}
}
