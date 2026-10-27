package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/api"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// envChild makes the re-executed test binary run mftiko-api instead of the tests.
const envChild = "MFTIKO_API_CHILD"

// testMainExtra handles a re-exec of this test binary that is not the API process. The linux
// account test sets it so the TD agent's shim and account worker can run in this binary. A nil
// hook, or one that reports not handled, falls through to the tests.
var testMainExtra func() (code int, handled bool)

func TestMain(m *testing.M) {
	if os.Getenv(envChild) == "1" {
		main()
		return
	}
	if testMainExtra != nil {
		if code, ok := testMainExtra(); ok {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

// pollInterval is how often a condition is sampled. It is not the bound.
const pollInterval = 20 * time.Millisecond

func until(t *testing.T, within time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for {
		last = cond()
		if last == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("waited %s for %s; last: %v", within, what, last)
		}
		time.Sleep(pollInterval)
	}
}

func tokenHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestParseArgs(t *testing.T) {
	getenv := func(k string) string {
		switch k {
		case api.EnvEtcdPassword:
			return "from-env"
		case api.EnvCookieKey:
			return "cookie-env"
		case api.EnvPostgresDSN:
			return "dsn-env"
		default:
			return ""
		}
	}
	opt, version, err := parseArgs(nil, getenv)
	if err != nil || version {
		t.Fatal(err)
	}
	if opt.etcdPassword != "from-env" || opt.cookieKey != "cookie-env" || opt.postgres != "dsn-env" {
		t.Fatalf("%+v", opt)
	}
	opt, version, err = parseArgs([]string{"-version", "-in", "deploy.json", "-etcd-password", "from-flag"}, getenv)
	if err != nil || !version || opt.etcdPassword != "from-flag" || opt.cookieKey != "cookie-env" || opt.in != "deploy.json" {
		t.Fatalf("version %v err %v opt %+v", version, err, opt)
	}
	if _, _, err := parseArgs([]string{"-no-such"}, nil); err == nil {
		t.Fatal("unknown flag accepted")
	}
	opt, _, err = parseArgs([]string{"-in", "d", "-config", "c", "-log-token", "/etc/mftiko/api/log-token"}, getenv)
	if err != nil || opt.logToken != "/etc/mftiko/api/log-token" {
		t.Fatalf("err %v opt %+v", err, opt)
	}
}

func TestReadLogToken(t *testing.T) {
	if tok, err := readLogToken(""); err != nil || tok != "" {
		t.Fatalf("empty path: %q %v", tok, err)
	}
	if _, err := readLogToken(filepath.Join(t.TempDir(), "missing")); err == nil || !strings.Contains(err.Error(), "-log-token") {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name, body string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if _, err := readLogToken(write("blank", " \n\t")); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatal(err)
	}
	if _, err := readLogToken(write("short", strings.Repeat("a", 31))); err == nil || !strings.Contains(err.Error(), "32") {
		t.Fatal(err)
	}
	got, err := readLogToken(write("ok", "\n"+strings.Repeat("b", 32)+"\n"))
	if err != nil || got != strings.Repeat("b", 32) {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := readLogToken(write("space", strings.Repeat("c", 16)+" "+strings.Repeat("c", 16))); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatal(err)
	}
	if _, err := readLogToken(write("cr", strings.Repeat("d", 16)+"\r"+strings.Repeat("d", 16))); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatal(err)
	}
	for _, err := range []error{
		readLogTokenErr(t, write("blank", " ")),
		readLogTokenErr(t, write("short", "abc")),
	} {
		if strings.Contains(err.Error(), "aaaa") || strings.Contains(err.Error(), strings.Repeat("d", 8)) {
			t.Fatalf("error includes token bytes: %v", err)
		}
	}
}

func readLogTokenErr(t *testing.T, path string) error {
	t.Helper()
	_, err := readLogToken(path)
	if err == nil {
		t.Fatal("expected error")
	}
	return err
}

func TestRunRejectsArgs(t *testing.T) {
	if err := run(context.Background(), options{}, strings.NewReader(""), zerolog.Nop()); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatal(err)
	}
	if err := run(context.Background(), options{in: "-", config: "-"}, strings.NewReader("{}"), zerolog.Nop()); err == nil || !strings.Contains(err.Error(), "stdin") {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.json")
	conf := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(conf, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), options{in: missing, config: conf}, strings.NewReader(""), zerolog.Nop()); err == nil {
		t.Fatal("missing deployment accepted")
	}
	tok := filepath.Join(t.TempDir(), "log-token")
	if err := os.WriteFile(tok, []byte("short\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), options{
		in: filepath.Join(t.TempDir(), "no-deploy"), config: filepath.Join(t.TempDir(), "no-config"), logToken: tok,
	}, strings.NewReader(""), zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "-log-token") {
		t.Fatal(err)
	}
}

func TestRunCanceled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + ln.Addr().String()
	ln.Close()
	dir := t.TempDir()
	deploy := filepath.Join(dir, "deploy.json")
	body := `{"regions":{"tw":{"endpoints":["` + endpoint + `"]}},"instances":{"controller":"tw"},"planes":{}}`
	if err := os.WriteFile(deploy, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	conf := `{"listen":"127.0.0.1:0","allow":["*"]}`
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, options{
			in: deploy, config: "-", etcdPassword: "pw", cookieKey: "cookie-key",
			postgres: "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable",
		}, strings.NewReader(conf), zerolog.Nop())
	}()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func TestProcessExitCodes(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runExe := func(args ...string) (int, string) {
		t.Helper()
		cmd := exec.Command(exe, args...)
		cmd.Env = append(os.Environ(), envChild+"=1")
		out, err := cmd.CombinedOutput()
		if err == nil {
			return 0, string(out)
		}
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatal(err)
		}
		return ee.ExitCode(), string(out)
	}
	if code, out := runExe("-version"); code != 0 || strings.TrimSpace(out) == "" {
		t.Fatalf("version %d %q", code, out)
	}
	if code, _ := runExe("-no-such"); code != 2 {
		t.Fatalf("bad flag exit %d", code)
	}
	if code, out := runExe(); code != 1 || !strings.Contains(out, "required") {
		t.Fatalf("missing args %d %q", code, out)
	}
}

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type child struct {
	t    *testing.T
	cmd  *exec.Cmd
	out  *safeBuf
	done chan struct{}

	mu       sync.Mutex
	finished bool
	waitErr  error
}

func startChild(t *testing.T, args ...string) *child {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	buf := &safeBuf{}
	cmd := exec.Command(exe, args...)
	cmd.Env = childEnv()
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &child{t: t, cmd: cmd, out: buf, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		c.mu.Lock()
		c.finished = true
		c.waitErr = err
		c.mu.Unlock()
		close(c.done)
	}()
	t.Cleanup(func() {
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
		}
		if t.Failed() {
			t.Logf("mftiko-api output:\n%s", c.out.String())
		}
	})
	return c
}

func childEnv() []string {
	out := []string{envChild + "=1", "MFTIKO_LOG_FORMAT=json", "MFTIKO_LOG_LEVEL=info"}
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "MFTIKO_LOG_LEVEL=") || strings.HasPrefix(e, "MFTIKO_LOG_FORMAT=") || strings.HasPrefix(e, envChild+"=") {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (c *child) signal(sig syscall.Signal) {
	c.t.Helper()
	if err := c.cmd.Process.Signal(sig); err != nil {
		c.t.Fatal(err)
	}
}

func (c *child) result() (error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished {
		return nil, false
	}
	return c.waitErr, true
}

func (c *child) exitCode(within time.Duration) int {
	c.t.Helper()
	select {
	case <-c.done:
	case <-time.After(within):
		c.t.Fatalf("still running after %s\n%s", within, c.out.String())
	}
	err, _ := c.result()
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	c.t.Fatal(err)
	return -1
}

func (c *child) running() bool {
	_, done := c.result()
	return !done
}

func listenAddr(out string) (string, bool) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] != '{' {
			continue
		}
		var ev struct {
			Addr string `json:"addr"`
			Msg  string `json:"message"`
		}
		if json.Unmarshal([]byte(line), &ev) != nil {
			continue
		}
		if ev.Msg == "listen" && ev.Addr != "" {
			return ev.Addr, true
		}
	}
	return "", false
}

type readyJSON struct {
	Regions map[string]struct {
		Available bool  `json:"available"`
		Synced    bool  `json:"synced"`
		Revision  int64 `json:"revision"`
	} `json:"regions"`
	UnavailableRegions []string `json:"unavailable_regions"`
}

func getJSON(t *testing.T, url string, out any) (int, []byte) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
	}
	return resp.StatusCode, b
}

func TestChildFollowsAndShutsDown(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo := state.Topology{
		Regions: map[string]state.Region{},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"md-tw-1":                "tw",
			"md-jp-1":                "jp",
		},
	}
	for region, srv := range servers {
		topo.Regions[region] = state.Region{Endpoints: srv.Endpoints()}
	}
	planes := map[string]state.Plane{"md-tw-1": state.PlaneMD, "md-jp-1": state.PlaneMD}
	tables, err := state.AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	for region, srv := range servers {
		if err := state.EnsureAuth(ctx, srv.Client(t), tables[region], etcdtest.Password); err != nil {
			t.Fatalf("auth %s: %v", region, err)
		}
	}
	dir := t.TempDir()
	dep, err := json.Marshal(struct {
		state.Topology
		Planes map[string]state.Plane `json:"planes"`
	}{Topology: topo, Planes: planes})
	if err != nil {
		t.Fatal(err)
	}
	deployPath := filepath.Join(dir, "deploy.json")
	if err := os.WriteFile(deployPath, dep, 0o644); err != nil {
		t.Fatal(err)
	}
	conf, err := json.Marshal(map[string]any{
		"listen": "127.0.0.1:0",
		"allow":  []string{"*"},
		"users":  []any{map[string]string{"name": "op", "token_sha256": tokenHash("op")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, conf, 0o644); err != nil {
		t.Fatal(err)
	}
	c := startChild(t,
		"-in", deployPath,
		"-config", configPath,
		"-etcd-password", etcdtest.Password(state.UserAPI),
		"-cookie-key", "cookie-key",
		"-postgres-dsn", "postgres://mftiko@127.0.0.1:5432/mftiko?sslmode=disable",
	)
	var addr string
	until(t, 30*time.Second, "the process listens", func() error {
		if !c.running() {
			return fmt.Errorf("exited\n%s", c.out.String())
		}
		got, ok := listenAddr(c.out.String())
		if !ok {
			return errors.New("no listen line")
		}
		addr = got
		return nil
	})
	until(t, 30*time.Second, "both regions ready", func() error {
		if !c.running() {
			return fmt.Errorf("exited\n%s", c.out.String())
		}
		var body readyJSON
		code, raw := getJSON(t, "http://"+addr+"/readyz", &body)
		if code != http.StatusOK || !body.Regions["tw"].Available || !body.Regions["jp"].Available ||
			len(body.UnavailableRegions) != 0 || !bytes.Contains(raw, []byte(`"unavailable_regions":[]`)) {
			return fmt.Errorf("status %d %s", code, raw)
		}
		hcode, hraw := getJSON(t, "http://"+addr+"/healthz", nil)
		if hcode != http.StatusOK || !bytes.Contains(hraw, []byte(`"ok":true`)) {
			return fmt.Errorf("healthz %d %s", hcode, hraw)
		}
		return nil
	})
	key := state.AgentKey(state.PlaneMD, "md-tw-1")
	resp, err := servers["tw"].Client(t).Put(ctx, key, "1")
	if err != nil {
		t.Fatal(err)
	}
	rev := resp.Header.Revision
	until(t, 15*time.Second, "the mirror reaches the write", func() error {
		var body readyJSON
		code, raw := getJSON(t, "http://"+addr+"/readyz", &body)
		if code != http.StatusOK || body.Regions["tw"].Revision < rev {
			return fmt.Errorf("status %d rev %d want >= %d %s", code, body.Regions["tw"].Revision, rev, raw)
		}
		return nil
	})
	if !c.running() {
		t.Fatal("process exited before SIGTERM")
	}
	c.signal(syscall.SIGTERM)
	if code := c.exitCode(15 * time.Second); code != 0 {
		t.Fatalf("SIGTERM exit %d", code)
	}
}
