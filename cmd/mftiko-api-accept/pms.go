package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// The PMS acceptance (E21-09, #521). pmsprobe is packed next to hello by accept.sh e2e-up.
const (
	probeRepository = "registry:5000/strategies/pmsprobe"
	pmsUID          = "accept-pms"
	pmsState        = "pms.json"
)

// pmsRun is what the steps hand each other, kept in /work/e2e/pms.json.
type pmsRun struct {
	A1, A2 int64
	// Sessions by role: exit (ended, all td.evt dropped), run (still running), never (never
	// spawned), retired (on sts2, which is then retired), after (algo 3 again, after the retire).
	Sessions map[string]string
	// Cloids each role's probe logged, in order.
	Cloids    map[string][]string
	RetiredAt time.Time
}

func loadPMS() (pmsRun, error) {
	b, err := os.ReadFile(filepath.Join(*stateDir, pmsState))
	if err != nil {
		return pmsRun{}, fmt.Errorf("no %s from pms before: %w", pmsState, err)
	}
	var r pmsRun
	return r, json.Unmarshal(b, &r)
}

func (r pmsRun) save() error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*stateDir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*stateDir, pmsState), b, 0o600)
}

// pmsFlow drives the PMS acceptance in three steps, around stopping sts2 (accept.sh pms):
//
//	before   two api_ids on one exchange account; sessions exit, run, never and retired
//	retire   sts2 is retired through the API; session after reuses the retired session's algo
//	check    a manual order, then mftiko-pms-sim, and its report against what the probes logged
func pmsFlow() error {
	step := popStep()
	credsFile := flag.String("creds", "/work/binanceum.creds.json", "BinanceUM testnet credentials ({key_type, key, secret})")
	ticker := flag.String("ticker", "BinanceUM_Perp_BTCUSDT", "the ticker the probes trade")
	exch := flag.String("exch-ticker", "BTCUSDT", "the same instrument as the venue spells it, for the manual order")
	qty := flag.Float64("qty", 0.002, "the probes' order quantity")
	rest := flag.String("rest", "https://testnet.binancefuture.com", "BinanceUM REST host (a testnet) for the manual order and the PMS")
	sim := flag.String("sim", "/mftiko/mftiko-pms-sim", "the PMS stand-in")
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://nats:4222"), "NATS for SYM")
	pmsCreds := flag.String("pms-creds", "/work/pms.creds", "the PMS NATS user (mftiko-md-accept creds -pms)")
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()
	switch step {
	case "before":
		return f.pmsBefore(*credsFile, *ticker, *qty)
	case "retire":
		return f.pmsRetire(*ticker, *qty)
	case "check":
		return f.pmsCheck(*credsFile, *exch, *qty, *rest, *sim, *natsURL, *pmsCreds)
	}
	return errors.New("pms needs a step: before, retire or check")
}

func (f *flow) pmsAccount(name string, creds apiv1.Credentials) (int64, error) {
	r, err := f.a.CreateAccountWithResponse(f.ctx, apiv1.AccountCreate{
		Name: name + "-" + randomHex(3), Kind: "BinanceUM", ExchAccountUid: pmsUID, Credentials: &creds,
	})
	if err != nil {
		return 0, err
	}
	if r.JSON201 == nil {
		return 0, fmt.Errorf("create account %s: %w", name, statusOf(r.HTTPResponse, r.Body))
	}
	return r.JSON201.ApiId, nil
}

func probeSession(name, sts, ticker string, apiID int64, algo int, qty float64, mode string) (apiv1.SessionCreate, error) {
	params, err := json.Marshal(map[string]any{"ticker": ticker, "api_id": apiID, "algo_id": algo, "qty": qty, "mode": mode})
	if err != nil {
		return apiv1.SessionCreate{}, err
	}
	raw := json.RawMessage(params)
	never := apiv1.Never
	return apiv1.SessionCreate{
		Name: name, StsInstance: sts,
		Strategy: apiv1.StrategySelector{Repository: probeRepository, Ref: strategyTag},
		Params:   &raw,
		Accounts: &[]apiv1.AccountDeclInput{{Name: "probe", ApiId: apiID, TdInstance: tdInst, AlgoIds: []int{algo}}},
		Restart:  &apiv1.RestartPolicy{Mode: &never},
	}, nil
}

// probe creates a pmsprobe session and waits for its stderr to show want lines of "probe":"submitted"
// (and, for a roundtrip, "roundtrip done"). It returns the session and the cloids it logged.
func (f *flow) probe(body apiv1.SessionCreate, submitted int, done bool) (string, []string, error) {
	s, err := f.create(f.a, body, randomHex(16))
	if err != nil {
		return "", nil, fmt.Errorf("create %s: %w", body.Name, err)
	}
	say("session %s (%s) on %s", s.Id, body.Name, body.StsInstance)
	var stderr string
	if err := until(f.ctx, 3*time.Minute, body.Name+" shows its log", func() error {
		d, _, err := f.get(f.a, s.Id)
		if err != nil {
			return err
		}
		if d.Logs == nil {
			return errors.New("no log location yet")
		}
		stderr = d.Logs.Stderr
		return nil
	}); err != nil {
		return "", nil, err
	}
	var cloids []string
	if err := until(f.ctx, 3*time.Minute, body.Name+"'s orders", func() error {
		b, err := os.ReadFile(stderr)
		if err != nil {
			return err
		}
		cloids = probeLines(b, "submitted")
		if len(cloids) < submitted {
			return fmt.Errorf("%d of %d orders submitted", len(cloids), submitted)
		}
		if done && len(probeLines(b, "roundtrip done")) == 0 {
			return errors.New("roundtrip not done")
		}
		return nil
	}); err != nil {
		return "", nil, err
	}
	say("%s submitted %v", body.Name, cloids)
	return s.Id, cloids, nil
}

// floodInput pushes a data artifact of 100,001 empty files, one more than the STS unpacks by
// default, and returns its digest.
func floodInput(ctx context.Context) (string, error) {
	dir, err := os.MkdirTemp("", "pms-flood-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	for i := range 100_001 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%06d", i)), nil, 0o644); err != nil {
			return "", err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/mftiko/mftiko-pack", "data", "-dir", dir, "-repo", inputRepository,
		"-tag", "pms-flood", "-plain-http", "-max-unpacked-files", "200000")
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("mftiko-pack data: %w\n%s", err, strings.TrimSpace(se.String()))
	}
	digest := strings.TrimSpace(so.String())
	if !strings.HasPrefix(digest, "sha256:") {
		return "", fmt.Errorf("mftiko-pack data printed %q", so.String())
	}
	return digest, nil
}

// probeLines is the cloid of every stderr line {"probe":kind,...}.
func probeLines(b []byte, kind string) []string {
	var out []string
	for _, line := range bytes.Split(b, []byte("\n")) {
		var l struct {
			Probe string `json:"probe"`
			Cloid string `json:"cloid"`
		}
		if json.Unmarshal(line, &l) == nil && l.Probe == kind && l.Cloid != "" {
			out = append(out, l.Cloid)
		}
	}
	return out
}

func (f *flow) pmsBefore(credsFile, ticker string, qty float64) error {
	creds, err := readCreds(credsFile)
	if err != nil {
		return err
	}
	run := pmsRun{Sessions: map[string]string{}, Cloids: map[string][]string{}}
	// One exchange account, two api_ids, the same uid: the PMS reads it once (E21-D008).
	if run.A1, err = f.pmsAccount("pms-a", creds); err != nil {
		return err
	}
	if run.A2, err = f.pmsAccount("pms-b", creds); err != nil {
		return err
	}
	say("api_ids %d and %d on exch_account_uid %s", run.A1, run.A2, pmsUID)

	// exit: the first order of A1 on this ticker. The probe exits on the ack, so nothing is
	// subscribed to td.evt when the order's events come, and the fill lands after last_exit.
	body, err := probeSession("pms-exit", stsInst, ticker, run.A1, 1, qty, "exit")
	if err != nil {
		return err
	}
	id, cloids, err := f.probe(body, 1, false)
	if err != nil {
		return err
	}
	run.Sessions["exit"], run.Cloids["exit"] = id, cloids
	end, err := f.waitPhase(f.a, id, 2*time.Minute, apiv1.PhaseDone, apiv1.PhaseFailed)
	if err != nil {
		return err
	}
	if end.Status.LastExit == nil {
		return fmt.Errorf("session exit is %s with no last_exit", end.Status.Phase)
	}
	// The probe logs "filled" from OnOrder, which only td.evt can drive. It must have seen none.
	if end.Logs != nil {
		b, err := os.ReadFile(end.Logs.Stderr)
		if err != nil {
			return err
		}
		if n := len(probeLines(b, "filled")); n != 0 {
			return fmt.Errorf("session exit saw %d td.evt fills before it exited", n)
		}
	}
	say("exit: %s at %s, no td.evt seen", end.Status.Phase, end.Status.LastExit.At.Format(time.RFC3339Nano))

	// run: A2, still running when the PMS reads.
	if body, err = probeSession("pms-run", stsInst, ticker, run.A2, 2, qty, "roundtrip"); err != nil {
		return err
	}
	if run.Sessions["run"], run.Cloids["run"], err = f.probe(body, 2, true); err != nil {
		return err
	}

	// never: an input the STS refuses before it spawns anything. mftiko-pack data is told the STS
	// takes more files than it does (default 100,000), so the session ends artifact_unsafe_layer
	// at incarnation 0 with no last_exit (docs/runbook.md §3.11). A feed SYM does not know is not
	// enough: the worker spawns while the dependencies settle.
	digest, err := floodInput(f.ctx)
	if err != nil {
		return err
	}
	if body, err = probeSession("pms-never", stsInst, ticker, run.A1, 4, qty, "roundtrip"); err != nil {
		return err
	}
	body.Inputs = &[]apiv1.InputDecl{{Name: "flood", Repository: inputRepository, Digest: digest}}
	s, err := f.create(f.a, body, randomHex(16))
	if err != nil {
		return fmt.Errorf("create pms-never: %w", err)
	}
	run.Sessions["never"] = s.Id
	never, err := f.waitPhase(f.a, s.Id, 3*time.Minute, apiv1.PhaseDone, apiv1.PhaseFailed)
	if err != nil {
		return err
	}
	if never.Status.LastExit != nil || never.Status.Incarnation != 0 {
		return fmt.Errorf("session never spawned after all: incarnation %d, last_exit %v", never.Status.Incarnation, never.Status.LastExit)
	}
	say("never: %s %s, incarnation 0, no last_exit", never.Status.Phase, never.Status.Reason)

	// retired: on sts2, which accept.sh stops and the next step retires.
	if body, err = probeSession("pms-retired", sts2Inst, ticker, run.A1, 3, qty, "roundtrip"); err != nil {
		return err
	}
	if run.Sessions["retired"], run.Cloids["retired"], err = f.probe(body, 2, true); err != nil {
		return err
	}
	if err := run.save(); err != nil {
		return err
	}
	say("next: stop the sts2 agent, then pms retire")
	return f.audited()
}

func (f *flow) pmsRetire(ticker string, qty float64) error {
	run, err := loadPMS()
	if err != nil {
		return err
	}
	if err := until(f.ctx, 2*time.Minute, "sts2 is retired", func() error {
		r, err := f.a.RetireInstanceWithResponse(f.ctx, sts2Inst)
		if err != nil {
			return err
		}
		if r.StatusCode() != http.StatusNoContent {
			return statusOf(r.HTTPResponse, r.Body)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := until(f.ctx, time.Minute, "sts_retired_at on the record", func() error {
		recs, err := f.records(run.A1)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if r.SessionId == run.Sessions["retired"] && r.StsRetiredAt != nil {
				run.RetiredAt = *r.StsRetiredAt
				return nil
			}
		}
		return errors.New("no sts_retired_at yet")
	}); err != nil {
		return err
	}
	say("sts2 retired at %s", run.RetiredAt.Format(time.RFC3339Nano))

	// after: algo 3 on A1 again. The retired session no longer holds it (E12-D034); without
	// sts_retired_at a PMS could not tell the two apart.
	body, err := probeSession("pms-after", stsInst, ticker, run.A1, 3, qty, "roundtrip")
	if err != nil {
		return err
	}
	if run.Sessions["after"], run.Cloids["after"], err = f.probe(body, 2, true); err != nil {
		return err
	}
	if err := run.save(); err != nil {
		return err
	}
	return f.audited()
}

func (f *flow) records(apiID int64) ([]apiv1.SessionRecord, error) {
	r, err := f.a.ListSessionRecordsWithResponse(f.ctx, &apiv1.ListSessionRecordsParams{ApiId: &apiID})
	if err != nil {
		return nil, err
	}
	if r.JSON200 == nil {
		return nil, statusOf(r.HTTPResponse, r.Body)
	}
	return r.JSON200.Items, nil
}

// simReport is the part of mftiko-pms-sim's report the check reads.
type simReport struct {
	Groups []struct {
		Exchange string       `json:"exchange"`
		UID      string       `json:"exch_account_uid"`
		APIIDs   []int64      `json:"api_ids"`
		Sessions []simSession `json:"sessions"`
		Symbols  []simSymbol  `json:"symbols"`
	} `json:"groups"`
}

type simSession struct {
	SessionID string     `json:"session_id"`
	APIID     int64      `json:"api_id"`
	End       *time.Time `json:"end"`
	Open      bool       `json:"open"`
	Empty     bool       `json:"empty"`
	EndSource string     `json:"end_source"`
}

type simSymbol struct {
	ExchTicker string    `json:"exch_ticker"`
	Since      time.Time `json:"since"`
	Skipped    string    `json:"skipped"`
	Orders     []struct {
		ClientOrderID string        `json:"client_order_id"`
		Status        string        `json:"status"`
		Attribution   simAttributed `json:"attribution"`
	} `json:"orders"`
	Fills []struct {
		ClientOrderID string        `json:"client_order_id"`
		Time          time.Time     `json:"time"`
		Attribution   simAttributed `json:"attribution"`
	} `json:"fills"`
}

type simAttributed struct {
	Kind      string `json:"kind"`
	SessionID string `json:"session_id"`
	TsMs      int64  `json:"ts_ms"`
}

func (f *flow) pmsCheck(credsFile, exch string, qty float64, rest, sim, natsURL, pmsCreds string) error {
	run, err := loadPMS()
	if err != nil {
		return err
	}
	if run.Sessions["after"] == "" {
		return errors.New("pms retire did not finish")
	}
	// A hand-placed order on the same account: the PMS sees it, and it belongs to no session.
	// It also closes the long the exit probe left.
	manual := "pms-manual-" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	if err := binanceOrder(f.ctx, credsFile, rest, url.Values{
		"symbol": {exch}, "side": {"SELL"}, "type": {"MARKET"}, "reduceOnly": {"true"},
		"quantity": {strconv.FormatFloat(qty, 'f', -1, 64)}, "newClientOrderId": {manual},
	}); err != nil {
		return fmt.Errorf("manual order: %w", err)
	}
	say("manual order %s", manual)

	token, err := os.ReadFile(*tokenAt)
	if err != nil {
		return err
	}
	out := filepath.Join(*stateDir, "pms-report.json")
	runSim := func() error {
		cmd := exec.CommandContext(f.ctx, sim, "-api", *apiA, "-nats", natsURL, "-nats-creds", pmsCreds,
			"-key", "binance/"+pmsUID+"="+credsFile, "-rest", "BinanceUM="+rest, "-out", out)
		// Only what a PMS has: its token. No etcd, Postgres or platform setting in the environment.
		cmd.Env = []string{"MFTIKO_TOKEN=" + strings.TrimSpace(string(token))}
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return cmd.Run()
	}
	var rep simReport
	// The venue's history can lag the fills by a moment.
	if err := until(f.ctx, 2*time.Minute, "the PMS report has every order and fill", func() error {
		if err := runSim(); err != nil {
			return stop(fmt.Errorf("mftiko-pms-sim: %w", err))
		}
		b, err := os.ReadFile(out)
		if err != nil {
			return err
		}
		rep = simReport{}
		if err := json.Unmarshal(b, &rep); err != nil {
			return stop(err)
		}
		return checkPMSReport(rep, run, manual)
	}); err != nil {
		return err
	}
	say("report %s matches", out)
	// The next run's probes reuse these algos on the same exchange account; two open sessions
	// with one algo there are ambiguous to a PMS (E21-D084).
	for _, role := range []string{"run", "after"} {
		if err := f.end(f.a, run.Sessions[role]); err != nil {
			return fmt.Errorf("end %s: %w", role, err)
		}
	}
	return f.audited()
}

// checkPMSReport is #521's done criteria against what the probes logged.
func checkPMSReport(rep simReport, run pmsRun, manual string) error {
	gi := -1
	for i, g := range rep.Groups {
		if g.Exchange == "binance" && g.UID == pmsUID {
			gi = i
		}
	}
	if gi < 0 {
		return stop(fmt.Errorf("no group binance/%s", pmsUID))
	}
	g := rep.Groups[gi]
	if !containsAll(g.APIIDs, run.A1, run.A2) {
		return stop(fmt.Errorf("group api_ids %v, want %d and %d together (E21-D008)", g.APIIDs, run.A1, run.A2))
	}

	// Intervals: running, ended, never spawned, retired (E21-D011, E21-D083).
	want := map[string]func(open, empty bool, end *time.Time, src string) error{
		"run": func(open, empty bool, _ *time.Time, _ string) error {
			if !open {
				return errors.New("a running session is not open")
			}
			return nil
		},
		"exit": func(open, empty bool, end *time.Time, src string) error {
			if open || empty || end == nil || src != "last_exit" {
				return fmt.Errorf("ended: open=%v empty=%v source %s", open, empty, src)
			}
			return nil
		},
		"never": func(open, empty bool, _ *time.Time, _ string) error {
			if !empty {
				return errors.New("a session that never spawned is not empty")
			}
			return nil
		},
		"retired": func(open, empty bool, end *time.Time, src string) error {
			if open || src != "sts_retired_at" || end == nil || !end.Equal(run.RetiredAt) {
				return fmt.Errorf("retired: open=%v end %v source %s, want %s", open, end, src, run.RetiredAt)
			}
			return nil
		},
		"after": func(open, _ bool, _ *time.Time, _ string) error {
			if !open {
				return errors.New("the session after the retire is not open")
			}
			return nil
		},
	}
	ends := map[string]*time.Time{}
	for role, check := range want {
		found := false
		for _, s := range g.Sessions {
			if s.SessionID != run.Sessions[role] {
				continue
			}
			found = true
			if err := check(s.Open, s.Empty, s.End, s.EndSource); err != nil {
				return stop(fmt.Errorf("session %s (%s): %w", s.SessionID, role, err))
			}
			ends[role] = s.End
		}
		if !found {
			return stop(fmt.Errorf("session %s (%s) is not in the report", run.Sessions[role], role))
		}
	}
	say("intervals: running open, ended at last_exit, never spawned empty, retired at sts_retired_at")

	var sym *simSymbol
	for i := range g.Symbols {
		if len(g.Symbols[i].Orders) > 0 || sym == nil {
			sym = &g.Symbols[i]
		}
	}
	if sym == nil || sym.Skipped != "" {
		return stop(fmt.Errorf("no symbol with history in the group: %+v", g.Symbols))
	}

	// Every probe order and its fills belong to the session that logged it.
	owner := map[string]string{}
	for role, cs := range run.Cloids {
		for _, c := range cs {
			owner[c] = run.Sessions[role]
		}
	}
	got := map[string]simAttributed{}
	for _, o := range sym.Orders {
		got[o.ClientOrderID] = o.Attribution
	}
	for c, sid := range owner {
		a, ok := got[c]
		if !ok {
			return fmt.Errorf("order %s is not in the venue history yet", c)
		}
		if a.Kind != "session" || a.SessionID != sid {
			return stop(fmt.Errorf("order %s attributed %+v, want session %s", c, a, sid))
		}
	}
	if a, ok := got[manual]; !ok {
		return fmt.Errorf("the manual order %s is not in the venue history yet", manual)
	} else if a.Kind != "manual" {
		return stop(fmt.Errorf("the manual order is %+v", a))
	}
	for _, o := range sym.Orders {
		if o.Attribution.Kind == "ambiguous" {
			return stop(fmt.Errorf("order %s is ambiguous: %+v", o.ClientOrderID, o.Attribution))
		}
	}
	filled := map[string]int{}
	var exitFill time.Time
	for _, fl := range sym.Fills {
		if sid, ok := owner[fl.ClientOrderID]; ok {
			if fl.Attribution.SessionID != sid {
				return stop(fmt.Errorf("fill of %s attributed %+v, want %s", fl.ClientOrderID, fl.Attribution, sid))
			}
			filled[fl.ClientOrderID]++
		}
		if fl.ClientOrderID == run.Cloids["exit"][0] && (exitFill.IsZero() || fl.Time.After(exitFill)) {
			exitFill = fl.Time
		}
	}
	for c := range owner {
		if filled[c] == 0 {
			return fmt.Errorf("no fill of %s yet", c)
		}
	}
	say("%d probe orders and their fills attributed to their sessions; the manual order to none", len(owner))

	// The exit probe's order: every td.evt was dropped, yet the scope is there and the fill is read
	// from the venue (#512). It filled after its session's last_exit and still belongs to it.
	first, ts := run.Cloids["exit"][0], got[run.Cloids["exit"][0]].TsMs
	if sym.Since.After(time.UnixMilli(ts).Add(time.Second)) {
		return stop(fmt.Errorf("scope since %s is after the first order %s at %d", sym.Since, first, ts))
	}
	if ends["exit"] == nil || !exitFill.After(*ends["exit"]) {
		return stop(fmt.Errorf("the exit order filled at %s, not after last_exit %v: run pms again", exitFill, ends["exit"]))
	}
	say("first order %s: scope since %s, all td.evt dropped, filled %s after last_exit %s",
		first, sym.Since.Format(time.RFC3339Nano), exitFill.Sub(*ends["exit"]), ends["exit"].Format(time.RFC3339Nano))
	return nil
}

func containsAll(have []int64, want ...int64) bool {
	for _, w := range want {
		ok := false
		for _, h := range have {
			ok = ok || h == w
		}
		if !ok {
			return false
		}
	}
	return true
}

// binanceOrder places one order on a BinanceUM testnet, signed with the creds file.
func binanceOrder(ctx context.Context, credsFile, rest string, q url.Values) error {
	if !strings.Contains(rest, "testnet") {
		return fmt.Errorf("%s is not a testnet host", rest)
	}
	b, err := os.ReadFile(credsFile)
	if err != nil {
		return err
	}
	var c struct {
		KeyType string `json:"key_type"`
		Key     string `json:"key"`
		Secret  string `json:"secret"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return errors.New("credentials are not JSON")
	}
	var sign func(string) string
	switch c.KeyType {
	case "ed25519":
		block, _ := pem.Decode([]byte(c.Secret))
		if block == nil {
			return errors.New("ed25519 secret is not PEM")
		}
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		priv, ok := k.(ed25519.PrivateKey)
		if err != nil || !ok {
			return errors.New("ed25519 secret does not parse")
		}
		sign = func(p string) string { return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(p))) }
	case "hmac":
		sign = func(p string) string {
			m := hmac.New(sha256.New, []byte(c.Secret))
			m.Write([]byte(p))
			return hex.EncodeToString(m.Sum(nil))
		}
	default:
		return fmt.Errorf("key type %q", c.KeyType)
	}
	q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	payload := q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rest+"/fapi/v1/order?"+payload+"&signature="+url.QueryEscape(sign(payload)), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", c.Key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST /fapi/v1/order: %d %s", resp.StatusCode, body)
	}
	return nil
}
