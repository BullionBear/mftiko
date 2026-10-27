package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// cliFlow is E11-09 (#361): epic #13's end-to-end criterion with the mftiko CLI as the only
// client. The CLI talks to replica a through a reverse proxy that records each response's
// X-Request-Id, so every request it sends (pages, SSE, retries) is checked against audit. The
// flow's own checks that are not the CLI (readyz, the region's session list) go through the
// recorder as well.
//
//	cli main                  login, queries, account, session from YAML --wait running, log on disk,
//	                          end --wait, records, delete session and account
//	cli faults                validation_failed, algo_overlap, referenced, not_terminal, not_found,
//	                          unauthenticated and a refused delete off a terminal
//	cli region before|down|up region_unavailable while accept2's etcd is stopped
func cliFlow() error {
	which := popStep()
	step := ""
	if which == "region" {
		step = popStep()
	}
	bin := flag.String("cli", "/mftiko/mftiko", "the mftiko CLI binary")
	credsFile := flag.String("creds", "/work/binanceum.creds.json", "credentials JSON for the account ({key_type, key, secret[, passphrase]})")
	kind := flag.String("kind", "BinanceUM", "account kind")
	ticker := flag.String("ticker", "BinanceUM_Perp_BTCUSDT", "the ticker hello quotes")
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()
	c, err := f.newCLI(*bin)
	if err != nil {
		return err
	}
	defer c.close()
	if err := c.login(); err != nil {
		return err
	}
	switch which {
	case "main":
		err = c.mainSteps(*credsFile, *kind, *ticker)
	case "faults":
		err = c.faultSteps(*credsFile, *kind)
	case "region":
		err = c.regionSteps(step)
	default:
		return errors.New("cli main|faults|region before|down|up")
	}
	if err != nil {
		return err
	}
	if n := c.requests(); n == 0 {
		return errors.New("the CLI sent no request through the proxy")
	}
	return f.audited()
}

// cli runs the mftiko binary with its own HOME, whose one context points at the proxy.
type cli struct {
	f       *flow
	bin     string
	home    string
	dir     string // session files and credential files
	token   string
	secrets []string
	srv     *http.Server
	before  int
}

func (f *flow) newCLI(bin string) (*cli, error) {
	if _, err := os.Stat(bin); err != nil {
		return nil, fmt.Errorf("CLI binary: %w", err)
	}
	raw, err := os.ReadFile(*tokenAt)
	if err != nil {
		return nil, fmt.Errorf("token: %w", err)
	}
	target, err := url.Parse(strings.TrimRight(*apiA, "/"))
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1 // SSE
	proxy.ModifyResponse = func(resp *http.Response) error {
		f.rec.mu.Lock()
		f.rec.sent = append(f.rec.sent, sent{
			id: resp.Header.Get("X-Request-Id"), method: resp.Request.Method,
			path: resp.Request.URL.Path, status: resp.StatusCode,
		})
		f.rec.mu.Unlock()
		return nil
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()

	home, err := os.MkdirTemp("", "mftiko-cli-home-")
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(home, "files")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	cfg := fmt.Sprintf("contexts:\n  accept:\n    url: http://%s/api/v1\n    insecure_http: true\n", ln.Addr())
	path := filepath.Join(home, ".config", "mftiko", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		return nil, err
	}
	token := strings.TrimSpace(string(raw))
	c := &cli{f: f, bin: bin, home: home, dir: dir, token: token, secrets: []string{token}, srv: srv}
	c.before = len(f.rec.all())
	say("CLI %s, HOME %s, proxy %s -> %s", bin, home, ln.Addr(), target)
	return c, nil
}

func (c *cli) close() {
	_ = c.srv.Close()
	_ = os.RemoveAll(c.home)
}

func (c *cli) requests() int { return len(c.f.rec.all()) - c.before }

// out is one CLI run.
type out struct {
	code           int
	stdout, stderr string
}

func (o out) String() string {
	return fmt.Sprintf("exit %d\nstdout: %s\nstderr: %s", o.code, strings.TrimSpace(o.stdout), strings.TrimSpace(o.stderr))
}

// run executes mftiko args with stdin. MFTIKO_TOKEN is not set unless extra sets it, so the
// token is the one login stored. No token or credential may appear in what the CLI prints (E11-D003).
func (c *cli) run(stdin string, extra []string, args ...string) (out, error) {
	ctx, cancel := context.WithTimeout(c.f.ctx, 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Env = append([]string{"HOME=" + c.home, "MFTIKO_CONTEXT=accept", "PATH=" + os.Getenv("PATH")}, extra...)
	cmd.Stdin = strings.NewReader(stdin)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	o := out{stdout: so.String(), stderr: se.String()}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		o.code = exit.ExitCode()
	default:
		return o, fmt.Errorf("mftiko %s: %w", strings.Join(args, " "), err)
	}
	for _, s := range c.secrets {
		if s != "" && (strings.Contains(o.stdout, s) || strings.Contains(o.stderr, s)) {
			return o, fmt.Errorf("mftiko %s printed a token or a credential", strings.Join(args, " "))
		}
	}
	return o, nil
}

// ok runs mftiko and requires exit 0.
func (c *cli) ok(args ...string) (out, error) {
	o, err := c.run("", nil, args...)
	if err != nil {
		return o, err
	}
	if o.code != 0 {
		return o, fmt.Errorf("mftiko %s: %s", strings.Join(args, " "), o)
	}
	return o, nil
}

// okJSON runs mftiko -o json and decodes the first JSON value on stdout into v.
func (c *cli) okJSON(v any, args ...string) error {
	o, err := c.ok(append([]string{"-o", "json"}, args...)...)
	if err != nil {
		return err
	}
	if err := json.NewDecoder(strings.NewReader(o.stdout)).Decode(v); err != nil {
		return fmt.Errorf("mftiko %s: stdout is not JSON: %w\n%s", strings.Join(args, " "), err, o)
	}
	return nil
}

// fails runs mftiko and requires exit code and a stderr line that names it, "<name> (<code>)"
// (E11-D007, E11-D033).
func (c *cli) fails(code int, name string, args ...string) (out, error) {
	return c.failsWith(code, name, "", nil, args...)
}

func (c *cli) failsWith(code int, name, stdin string, extra []string, args ...string) (out, error) {
	o, err := c.run(stdin, extra, args...)
	if err != nil {
		return o, err
	}
	want := name
	if code >= 10 {
		want = fmt.Sprintf("%s (%d)", name, code)
	}
	if o.code != code || !strings.Contains(o.stderr, want) {
		return o, fmt.Errorf("mftiko %s: want exit %d and %q on stderr, got %s", strings.Join(args, " "), code, want, o)
	}
	say("mftiko %s: exit %d, %s", strings.Join(args, " "), o.code, firstLine(o.stderr))
	return o, nil
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	return s
}

func (c *cli) login() error {
	o, err := c.run(c.token+"\n", nil, "login")
	if err != nil {
		return err
	}
	if o.code != 0 {
		return fmt.Errorf("mftiko login: %s", o)
	}
	var me apiv1.Me
	if err := c.okJSON(&me, "whoami"); err != nil {
		return err
	}
	if me.Name != userName {
		return fmt.Errorf("whoami is %q, want %q", me.Name, userName)
	}
	v, err := c.ok("version")
	if err != nil {
		return err
	}
	say("logged in as %s; %s", me.Name, strings.ReplaceAll(strings.TrimSpace(v.stdout), "\n", "; "))
	return nil
}

// account creates an account from a creds JSON with the CLI's credential files (E11-D014).
func (c *cli) account(name, kind, credsFile string) (int64, error) {
	creds, err := readCreds(credsFile)
	if err != nil {
		return 0, err
	}
	c.secrets = append(c.secrets, creds.Secret)
	keyFile, secretFile := filepath.Join(c.dir, name+".key"), filepath.Join(c.dir, name+".secret")
	if err := os.WriteFile(keyFile, []byte(creds.Key), 0o600); err != nil {
		return 0, err
	}
	if err := os.WriteFile(secretFile, []byte(creds.Secret), 0o600); err != nil {
		return 0, err
	}
	args := []string{"account", "create", "-name", name, "-kind", kind, "-td-instance", tdInst,
		"-key-type", string(creds.KeyType), "-key-file", keyFile, "-secret-file", secretFile}
	if creds.Passphrase != nil {
		c.secrets = append(c.secrets, *creds.Passphrase)
		pass := filepath.Join(c.dir, name+".passphrase")
		if err := os.WriteFile(pass, []byte(*creds.Passphrase), 0o600); err != nil {
			return 0, err
		}
		args = append(args, "-passphrase-file", pass)
	}
	var acc apiv1.Account
	if err := c.okJSON(&acc, args...); err != nil {
		return 0, err
	}
	say("mftiko account create: api_id %d on %s", acc.ApiId, acc.TdInstance)
	return acc.ApiId, nil
}

// sessionFile writes a session file and returns its path.
func (c *cli) sessionFile(name, body string) (string, error) {
	p := filepath.Join(c.dir, name+".yaml")
	return p, os.WriteFile(p, []byte(body), 0o600)
}

func (c *cli) show(id string) (*apiv1.SessionDetail, error) {
	var s apiv1.SessionDetail
	if err := c.okJSON(&s, "session", "show", id); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *cli) mainSteps(credsFile, kind, ticker string) error {
	var kinds apiv1.AccountKindList
	if err := c.okJSON(&kinds, "account-kind", "list"); err != nil {
		return err
	}
	if !slices.ContainsFunc(kinds.Items, func(k apiv1.AccountKind) bool { return k.Kind == kind && k.Supported }) {
		return fmt.Errorf("account-kind list: %s is not supported", kind)
	}
	var strategies apiv1.StrategyRepository
	if err := c.okJSON(&strategies, "strategy", "list", strategyRepository); err != nil {
		return err
	}
	if !slices.ContainsFunc(strategies.Items, func(t apiv1.StrategyTag) bool { return t.Tag == strategyTag }) {
		return fmt.Errorf("strategy list %s has no %s", strategyRepository, strategyTag)
	}
	var insts apiv1.InstanceList
	if err := c.okJSON(&insts, "instance", "list"); err != nil {
		return err
	}
	for _, want := range []string{mdInst, tdInst, stsInst} {
		if !slices.ContainsFunc(insts.Items, func(i apiv1.Instance) bool { return i.Name == want }) {
			return fmt.Errorf("instance list has no %s", want)
		}
	}
	say("queries: %s supported, %s:%s, instances %s %s %s", kind, strategyRepository, strategyTag, mdInst, tdInst, stsInst)

	apiID, err := c.account("accept-cli-"+strings.ToLower(kind), kind, credsFile)
	if err != nil {
		return err
	}
	feed := "orderbook_delta." + ticker
	file, err := c.sessionFile("hello", fmt.Sprintf(`# hello through the CLI (E11-09)
name: accept-cli-main
sts_instance: %s
strategy:
  repository: %s
  ref: %s
params:
  tickers: [%s]
  api_id: %d
  algo_id: 1
  offset: 2000
  qty: 0.002
feeds:
  - feed: %s
    md_instance: %s
accounts:
  - name: main
    api_id: %d
    algo_ids: [1]
restart:
  mode: never
`, stsInst, strategyRepository, strategyTag, ticker, apiID, feed, mdInst, apiID))
	if err != nil {
		return err
	}
	var created apiv1.Session
	if err := c.okJSON(&created, "session", "create", "-f", file, "--wait", "running", "--wait-timeout", "3m"); err != nil {
		return err
	}
	id := created.Id
	say("mftiko session create --wait running: %s on %s, exit 0", id, stsInst)

	// "TD account READY": session show lists both dependencies ready, acked by dev and td1.
	var s *apiv1.SessionDetail
	if err := until(c.f.ctx, time.Minute, "dependencies ready in session show", func() error {
		var err error
		if s, err = c.show(id); err != nil {
			return stop(err)
		}
		var feedOK, accOK bool
		for _, d := range s.Dependencies {
			switch {
			case d.Kind == apiv1.DependencyKindFeed && d.Feed != nil && *d.Feed == feed:
				feedOK = d.State == apiv1.EntryStateReady && d.ProviderInstance == mdInst
			case d.Kind == apiv1.DependencyKindAccount && d.ApiId != nil && *d.ApiId == apiID:
				accOK = d.State == apiv1.EntryStateReady && d.ProviderInstance == tdInst
			}
		}
		if !feedOK || !accOK || s.Logs == nil {
			return fmt.Errorf("dependencies %s, logs %v", depsLine(s.Dependencies), s.Logs != nil)
		}
		return nil
	}); err != nil {
		return err
	}
	say("session show: phase %s, %s", phaseOf(s), depsLine(s.Dependencies))
	table, err := c.ok("session", "show", id)
	if err != nil {
		return err
	}
	if !strings.Contains(table.stdout, s.Logs.Stdout) {
		return fmt.Errorf("session show does not print the log path %s:\n%s", s.Logs.Stdout, table.stdout)
	}
	if err := checkLogs(s.Logs); err != nil {
		return err
	}
	if err := until(c.f.ctx, 2*time.Minute, "hello's order cancelled in its stdout", func() error {
		b, err := os.ReadFile(s.Logs.Stdout)
		if err != nil {
			return err
		}
		if !strings.Contains(string(b), `"hook":"OnRecon"`) || !strings.Contains(string(b), `"status":"ORDER_STATUS_CANCELLED"`) {
			return errors.New("no OnRecon and cancel lines yet")
		}
		return nil
	}); err != nil {
		return err
	}
	say("log %s:%s has hello's OnRecon and its cancelled order", s.Logs.Host, s.Logs.Stdout)

	if _, err := c.ok("session", "end", id, "--wait", "--yes"); err != nil {
		return err
	}
	if s, err = c.show(id); err != nil {
		return err
	}
	if phaseOf(s) != apiv1.PhaseDone || s.Status.Reason != "ended_by_spec" {
		return fmt.Errorf("after end --wait: phase %q reason %q", phaseOf(s), s.Status.Reason)
	}
	say("mftiko session end --wait: exit 0, phase done, reason ended_by_spec")

	var records apiv1.SessionRecordList
	if err := c.okJSON(&records, "record", "list", "-session-id", id); err != nil {
		return err
	}
	if len(records.Items) != 1 {
		return fmt.Errorf("record list -session-id %s: %d records", id, len(records.Items))
	}
	rid := records.Items[0].RecordId.String()
	var rec apiv1.SessionRecord
	if err := c.okJSON(&rec, "record", "show", rid); err != nil {
		return err
	}
	if rec.SessionId != id {
		return fmt.Errorf("record %s is session %s, want %s", rid, rec.SessionId, id)
	}
	say("record %s: state %s", rid, rec.State)

	if _, err := c.ok("session", "delete", id, "--yes"); err != nil {
		return err
	}
	if _, err := c.fails(exitNotFound, "not_found", "session", "show", id); err != nil {
		return err
	}
	if err := until(c.f.ctx, time.Minute, "the record closed", func() error {
		if err := c.okJSON(&rec, "record", "show", rid); err != nil {
			return stop(err)
		}
		if rec.State != apiv1.SessionRecordStateClosed || rec.ClosedAt == nil {
			return fmt.Errorf("record state %s", rec.State)
		}
		return nil
	}); err != nil {
		return err
	}
	// The row keeps the status it was closed with (E10-D117).
	if rec.Final == nil || rec.Final.Phase != apiv1.PhaseDone || rec.Final.Reason != "ended_by_spec" {
		return fmt.Errorf("record %s final %+v, want done and ended_by_spec", rid, rec.Final)
	}
	say("record %s closed at %s, final %s %s, still readable after the delete", rid, rec.ClosedAt.Format(time.RFC3339), rec.Final.Phase, rec.Final.Reason)

	var acc apiv1.Account
	if err := c.okJSON(&acc, "account", "show", strconv.FormatInt(apiID, 10)); err != nil {
		return err
	}
	if len(acc.ReferencedBy) != 0 {
		return fmt.Errorf("account %d still referenced by %v", apiID, acc.ReferencedBy)
	}
	if _, err := c.ok("account", "delete", strconv.FormatInt(apiID, 10), "--yes"); err != nil {
		return err
	}
	if _, err := c.fails(exitNotFound, "not_found", "account", "show", strconv.FormatInt(apiID, 10)); err != nil {
		return err
	}
	say("account %d deleted", apiID)
	return nil
}

// The CLI's exit codes the flows expect (E11-D007).
const (
	exitNotFound          = 12
	exitValidation        = 10
	exitUnauthenticated   = 11
	exitNotTerminal       = 14
	exitReferenced        = 15
	exitAlgoOverlap       = 16
	exitRegionUnavailable = 17
)

// idleFile is hello with no params on sts, optionally bound to one account: it runs until ended.
func idleFile(name, sts string, apiID int64, algo int) string {
	s := fmt.Sprintf("name: %s\nsts_instance: %s\nstrategy:\n  repository: %s\n  ref: %s\n", name, sts, strategyRepository, strategyTag)
	if apiID != 0 {
		s += fmt.Sprintf("accounts:\n  - name: main\n    api_id: %d\n    algo_ids: [%d]\n", apiID, algo)
	}
	return s
}

func (c *cli) faultSteps(credsFile, kind string) error {
	// Refused before any request: a number YAML and JSON read differently (E11-D020), and a
	// destructive command off a terminal without --yes (E11-D009).
	bad, err := c.sessionFile("octal", idleFile("accept-cli-octal", stsInst, 0, 0)+"start_timeout_s: 0755\n")
	if err != nil {
		return err
	}
	if _, err := c.fails(1, bad+":6:", "session", "create", "-f", bad); err != nil {
		return err
	}
	if _, err := c.fails(1, "--yes", "session", "end", "abcdef"); err != nil {
		return err
	}
	if _, err := c.failsWith(exitUnauthenticated, "unauthenticated", "", []string{"MFTIKO_TOKEN=not-the-token"}, "whoami"); err != nil {
		return err
	}
	if _, err := c.fails(exitNotFound, "not_found", "session", "show", "abcdef"); err != nil {
		return err
	}

	apiID, err := c.account("accept-cli-faults", kind, credsFile)
	if err != nil {
		return err
	}
	// validation_failed: algo_id 1024 is past the range; the error names the file's line.
	invalid, err := c.sessionFile("algo1024", idleFile("accept-cli-invalid", stsInst, apiID, 1024))
	if err != nil {
		return err
	}
	o, err := c.fails(exitValidation, "validation_failed", "session", "create", "-f", invalid)
	if err != nil {
		return err
	}
	if !strings.Contains(o.stderr, invalid+":") || !strings.Contains(o.stderr, "/accounts/0/algo_ids ") {
		return fmt.Errorf("validation_failed does not name the file line of algo_ids: %s", o)
	}

	first, err := c.sessionFile("algo7", idleFile("accept-cli-algo7", stsInst, apiID, 7))
	if err != nil {
		return err
	}
	var s apiv1.Session
	if err := c.okJSON(&s, "session", "create", "-f", first); err != nil {
		return err
	}
	say("session %s holds algo 7 on account %d", s.Id, apiID)
	second, err := c.sessionFile("algo7b", idleFile("accept-cli-algo7b", stsInst, apiID, 7))
	if err != nil {
		return err
	}
	if _, err := c.fails(exitAlgoOverlap, "algo_overlap", "session", "create", "-f", second); err != nil {
		return err
	}
	if _, err := c.fails(exitReferenced, "referenced", "account", "delete", strconv.FormatInt(apiID, 10), "--yes"); err != nil {
		return err
	}
	if _, err := c.fails(exitNotTerminal, "not_terminal", "session", "delete", s.Id, "--yes"); err != nil {
		return err
	}

	if _, err := c.ok("session", "end", s.Id, "--wait", "--yes"); err != nil {
		return err
	}
	if _, err := c.ok("session", "delete", s.Id, "--yes"); err != nil {
		return err
	}
	if _, err := c.ok("account", "delete", strconv.FormatInt(apiID, 10), "--yes"); err != nil {
		return err
	}
	say("cleaned up session %s and account %d", s.Id, apiID)
	return nil
}

func (c *cli) regionSteps(step string) error {
	switch step {
	case "before":
		if err := c.f.waitUnavailable(nil); err != nil {
			return err
		}
		file, err := c.sessionFile("sts2", idleFile("accept-cli-region2", sts2Inst, 0, 0))
		if err != nil {
			return err
		}
		var s apiv1.Session
		if err := c.okJSON(&s, "session", "create", "-f", file, "--wait", "running", "--wait-timeout", "2m"); err != nil {
			return err
		}
		say("session %s running on %s", s.Id, sts2Inst)
		return remember("cli-region2", s.Id)
	case "down":
		id, err := recall("cli-region2")
		if err != nil {
			return err
		}
		if err := c.f.waitUnavailable([]string{region2}); err != nil {
			return err
		}
		if _, err := c.fails(exitRegionUnavailable, "region_unavailable", "session", "show", id); err != nil {
			return err
		}
		file, err := c.sessionFile("sts2-down", idleFile("accept-cli-region2-down", sts2Inst, 0, 0))
		if err != nil {
			return err
		}
		if _, err := c.fails(exitRegionUnavailable, "region_unavailable", "session", "create", "-f", file); err != nil {
			return err
		}
		// A list still answers; the missing region is one warning on stderr, exit 0 (E11-D012).
		o, err := c.ok("session", "list")
		if err != nil {
			return err
		}
		if !strings.Contains(o.stderr, region2) {
			return fmt.Errorf("session list does not warn about %s: %s", region2, o)
		}
		say("mftiko session list: exit 0, %s", firstLine(o.stderr))
		return nil
	case "up":
		id, err := recall("cli-region2")
		if err != nil {
			return err
		}
		if err := c.f.waitUnavailable(nil); err != nil {
			return err
		}
		if _, err := c.ok("session", "end", id, "--wait", "--yes"); err != nil {
			return err
		}
		if _, err := c.ok("session", "delete", id, "--yes"); err != nil {
			return err
		}
		say("session %s on %s ended and deleted", id, sts2Inst)
		return nil
	default:
		return errors.New("cli region before|down|up")
	}
}
