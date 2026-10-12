package account

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

const (
	apiKey = "test-key"
	// The secret is not a literal a scanner would flag, and the tests look for it in every reason.
	secretPart = "not-a-real"
)

var secret = secretPart + "-secret"

// recLedger records what the worker hands it.
type recLedger struct {
	mu     sync.Mutex
	recons []exchange.AccountSnapshot
	orders []string
	fills  []string
}

func (l *recLedger) Recon(s *exchange.AccountSnapshot) {
	l.mu.Lock()
	l.recons = append(l.recons, *s)
	l.mu.Unlock()
}
func (l *recLedger) Order(e *exchange.OrderUpdate) {
	l.mu.Lock()
	l.orders = append(l.orders, e.ClientOrderID)
	l.mu.Unlock()
}
func (l *recLedger) Fill(e *exchange.Fill) {
	l.mu.Lock()
	l.fills = append(l.fills, e.FillID)
	l.mu.Unlock()
}
func (l *recLedger) Balances([]exchange.Balance)   {}
func (l *recLedger) Positions([]exchange.Position) {}

func (l *recLedger) reconCount() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.recons) }

type harness struct {
	t      *testing.T
	v      *exchangetest.AccountVenue
	ledger *recLedger
	dials  atomic.Int32 // handshakes the private stream's server received

	conn net.Conn
	w    *frame.Writer
	ups  chan *workerv1.WorkerUp
	code chan int
}

type setup struct {
	opts  exchangetest.AccountOptions
	kind  string // the account kind Init names; "Fake" when empty
	creds *tdctlv1.Credentials
	cfg   func(*ServeConfig)
}

func start(t *testing.T, s setup) *harness {
	t.Helper()
	s.opts.Key = apiKey
	v := exchangetest.NewAccountVenue(s.opts)
	h := &harness{t: t, v: v, ledger: &recLedger{}, ups: make(chan *workerv1.WorkerUp, 256), code: make(chan int, 1)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.dials.Add(1)
		v.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	kinds := map[string]exchange.AccountKind{"Fake": v.Kind("Fake"), "Multi": v.Kind("Multi", "Fake", "Other")}
	cfg := ServeConfig{
		Kinds:          func(n string) (exchange.AccountKind, bool) { k, ok := kinds[n]; return k, ok },
		Doer:           v,
		StreamEndpoint: "ws" + strings.TrimPrefix(srv.URL, "http"),
		Ledger:         h.ledger,
		RetryAfter:     20 * time.Millisecond,
		RetryMax:       100 * time.Millisecond,
		ReadyTimeout:   2 * time.Second,
	}
	if s.cfg != nil {
		s.cfg(&cfg)
	}
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	h.conn, h.w = a, frame.NewWriter(a)
	go func() { h.code <- Serve(t.Context(), b, cfg) }()
	go func() {
		r := frame.NewReader(bufio.NewReader(a))
		for {
			m := &workerv1.WorkerUp{}
			if err := r.Read(m); err != nil {
				close(h.ups)
				return
			}
			if m.GetBeat() == nil {
				h.ups <- m
			}
		}
	}()
	h.next(func(m *workerv1.WorkerUp) bool { return m.GetHello().GetKind() == "account" })

	kind := s.kind
	if kind == "" {
		kind = "Fake"
	}
	creds := s.creds
	if creds == nil {
		creds = &tdctlv1.Credentials{KeyType: "hmac", Key: apiKey, Secret: secret}
	}
	payload, err := proto.Marshal(&tdctlv1.InitPayload{ApiId: 42, Kind: kind, TdInstance: "td-1", Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	h.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{Contract: Contract, Payload: payload}}})
	return h
}

func (h *harness) send(m *workerv1.WorkerDown) {
	h.t.Helper()
	if err := h.w.Write(m); err != nil {
		h.t.Fatal(err)
	}
}

// next returns the next frame, which must satisfy ok.
func (h *harness) next(ok func(*workerv1.WorkerUp) bool) *workerv1.WorkerUp {
	h.t.Helper()
	select {
	case m, open := <-h.ups:
		if !open {
			h.t.Fatal("the worker closed fd 3")
		}
		if !ok(m) {
			h.t.Fatalf("unexpected %v", m)
		}
		return m
	case <-time.After(5 * time.Second):
		h.t.Fatal("no frame")
	}
	return nil
}

// none fails when any frame but a Beat arrives within d.
func (h *harness) none(d time.Duration) {
	h.t.Helper()
	select {
	case m := <-h.ups:
		h.t.Fatalf("unexpected %v", m)
	case <-time.After(d):
	}
}

func (h *harness) report(state tdv1.AccountState, reason string) *tdctlv1.Report {
	h.t.Helper()
	m := h.next(func(m *workerv1.WorkerUp) bool { return m.GetReport() != nil })
	r := &tdctlv1.Report{}
	if m.GetReport().GetContract() != Contract || proto.Unmarshal(m.GetReport().GetPayload(), r) != nil {
		h.t.Fatalf("report %v", m)
	}
	if r.GetState() != state || r.GetReason() != reason {
		h.t.Fatalf("report %v %q, want %v %q", r.GetState(), r.GetReason(), state, reason)
	}
	return r
}

func (h *harness) initOK() {
	h.t.Helper()
	h.next(isDone(workerv1.Verb_VERB_INIT, true))
}

func (h *harness) startReady() *tdctlv1.Report {
	h.t.Helper()
	h.initOK()
	h.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: Contract}}})
	r := h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	if r.GetReconciledAtNs() == 0 {
		h.t.Fatal("READY without reconciled_at")
	}
	h.next(isDone(workerv1.Verb_VERB_START, true))
	h.next(func(m *workerv1.WorkerUp) bool { return m.GetStage().GetName() == "running" })
	return r
}

func (h *harness) startFails(reason string) {
	h.t.Helper()
	h.initOK()
	h.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: Contract}}})
	h.next(isDone(workerv1.Verb_VERB_START, false))
	h.ending(commonv1.Outcome_OUTCOME_INIT_FAILED, reason, c4.ExitInit)
}

func (h *harness) ending(o commonv1.Outcome, reason string, code int) {
	h.t.Helper()
	m := h.next(func(m *workerv1.WorkerUp) bool { return m.GetExiting() != nil })
	if m.GetExiting().GetOutcome() != o || m.GetExiting().GetReason() != reason {
		h.t.Fatalf("exiting %v, want %v %q", m.GetExiting(), o, reason)
	}
	h.next(func(m *workerv1.WorkerUp) bool { return m.GetStopped() != nil })
	select {
	case c := <-h.code:
		if c != code {
			h.t.Fatalf("exit %d, want %d", c, code)
		}
	case <-time.After(5 * time.Second):
		h.t.Fatal("serve did not return")
	}
}

func (h *harness) stop() {
	h.t.Helper()
	h.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: "ended_by_spec"}}})
	h.ending(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec", c4.ExitDone)
}

func isDone(v workerv1.Verb, ok bool) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool { return m.GetDone().GetVerb() == v && m.GetDone().GetOk() == ok }
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var btc = symbol.ExchKey{Venue: "Fake", ExchTicker: "BTCUSDT"}

// The acceptance: Init, Start, Stop against the fake account, with the exit code and outcome of
// PROTOCOL §5.3. READY is reported before Done(START), after a recon that read every part.
func TestLifecycle(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{PageSize: 1}})
	h.v.SetBalance(exchange.Balance{Asset: "USDT", Free: 100})
	h.v.SetBalance(exchange.Balance{Asset: "BTC", Free: 1})
	h.v.PutOrder(exchange.OrderUpdate{Exch: btc, ClientOrderID: "c1", Status: exchange.OrderStatusAccepted, Qty: 1, Price: 10})
	h.startReady()
	if n := h.ledger.reconCount(); n != 1 {
		t.Fatalf("%d recons", n)
	}
	r := h.ledger.recons[0]
	if len(r.Balances) != 2 || len(r.OpenOrders) != 1 || r.HedgeMode {
		t.Fatalf("recon %+v", r)
	}
	h.v.PutOrder(exchange.OrderUpdate{Exch: btc, ClientOrderID: "c2", Status: exchange.OrderStatusAccepted, Qty: 1, Price: 10})
	waitFor(t, "the event did not reach the ledger", func() bool {
		h.ledger.mu.Lock()
		defer h.ledger.mu.Unlock()
		return len(h.ledger.orders) == 1 && h.ledger.orders[0] == "c2"
	})
	h.stop()
	if open, _ := h.v.Connections(); open != 0 {
		waitFor(t, "the stream is still open", func() bool { o, _ := h.v.Connections(); return o == 0 })
	}
}

// A dropped private connection: DEGRADED, then READY after a new connection and a recon.
func TestDisconnect(t *testing.T) {
	h := start(t, setup{})
	first := h.startReady()
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	again := h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	if again.GetReconciledAtNs() <= first.GetReconciledAtNs() {
		t.Fatal("READY without a new recon")
	}
	if _, total := h.v.Connections(); total != 2 {
		t.Fatalf("%d connections", total)
	}
	h.stop()
}

// Dials that fail are retried; Start waits for them.
func TestDialRetried(t *testing.T) {
	h := start(t, setup{})
	h.v.FailDials(3)
	h.startReady()
	if n := h.dials.Load(); n != 4 {
		t.Fatalf("%d dials", n)
	}
	h.stop()
}

// A refused login is INIT_FAILED with reason auth, and the worker does not dial again.
func TestLoginRefused(t *testing.T) {
	h := start(t, setup{})
	h.v.RefuseLogins(1)
	h.startFails(ReasonAuth)
	if n := h.dials.Load(); n != 1 {
		t.Fatalf("%d dials", n)
	}
}

// A recon that fails part way is retried, and Start succeeds.
func TestReconRetried(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{PageSize: 1}})
	h.v.SetBalance(exchange.Balance{Asset: "USDT", Free: 1})
	h.v.SetBalance(exchange.Balance{Asset: "BTC", Free: 1})
	h.v.FailSnapshot(exchangetest.SnapshotBalances, 1, 2)
	h.startReady()
	if n := h.ledger.reconCount(); n != 1 || len(h.ledger.recons[0].Balances) != 2 {
		t.Fatalf("recons %+v", h.ledger.recons)
	}
	h.stop()
}

// Hedge mode at Start is INIT_FAILED (D607).
func TestHedgeModeAtStart(t *testing.T) {
	h := start(t, setup{})
	h.v.SetHedgeMode(true)
	h.startFails(ReasonHedgeMode)
}

// Hedge mode found by a later recon is UNAVAILABLE; the worker keeps running (D612).
func TestHedgeModeLater(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	h.v.SetHedgeMode(true)
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_UNAVAILABLE, ReasonHedgeMode)
	h.v.SetHedgeMode(false)
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	h.stop()
}

// Credentials revoked while running: the reconnect is refused, and the worker ends CRASHED with
// reason auth, which the TD agent's Policy does not restart (D608).
func TestCredentialsRevoked(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	h.v.SetKey("rotated")
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.ending(commonv1.Outcome_OUTCOME_CRASHED, ReasonAuth, c4.ExitCrash)
}

// A Binance-style stream: Prepare creates the listen key, Refresh extends it, and an expired key
// ends the connection and gets a new one.
func TestListenKey(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{ListenKey: true, Stream: exchange.StreamLimits{RefreshEvery: 30 * time.Millisecond}}})
	h.startReady()
	waitFor(t, "no refresh", func() bool { return count(h.v.Requests(), "PUT /listenKey") >= 2 })
	h.v.ExpireListenKey()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	if n := count(h.v.Requests(), "POST /listenKey"); n != 2 {
		t.Fatalf("%d listen keys", n)
	}
	h.stop()
}

// A connection about to reach its MaxLifetime is replaced before it closes: no DEGRADED, no recon.
func TestReplacedAheadOfLifetime(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Stream: exchange.StreamLimits{MaxLifetime: 600 * time.Millisecond}}})
	h.startReady()
	h.none(1500 * time.Millisecond)
	if _, total := h.v.Connections(); total < 3 {
		t.Fatalf("%d connections", total)
	}
	if n := h.ledger.reconCount(); n != 1 {
		t.Fatalf("%d recons", n)
	}
	h.stop()
}

// A trade stream that carries the events (Binance spot) is the only connection.
func TestTradeStreamWithEvents(t *testing.T) {
	tv := exchangetest.NewTradeVenue(apiKey, true)
	h := start(t, setup{opts: exchangetest.AccountOptions{Trade: tv}})
	srv := httptest.NewServer(tv.Handler())
	t.Cleanup(srv.Close)
	tv.SetURL("ws" + strings.TrimPrefix(srv.URL, "http"))
	h.startReady()
	if n := h.dials.Load(); n != 0 {
		t.Fatalf("%d private dials", n)
	}
	tv.Push(tv.Fill("f1", "o1", "BTCUSDT", exchange.SideBuy, "10", "1"))
	waitFor(t, "the fill did not reach the ledger", func() bool {
		h.ledger.mu.Lock()
		defer h.ledger.mu.Unlock()
		return len(h.ledger.fills) == 1
	})
	tv.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	h.stop()
}

// A trade stream without events (OKX, Bybit, Binance UM) is opened next to the private stream; its
// drop does not degrade the account (D614).
func TestTradeStreamWithoutEvents(t *testing.T) {
	tv := exchangetest.NewTradeVenue(apiKey, false)
	h := start(t, setup{opts: exchangetest.AccountOptions{Trade: tv}})
	srv := httptest.NewServer(tv.Handler())
	t.Cleanup(srv.Close)
	tv.SetURL("ws" + strings.TrimPrefix(srv.URL, "http"))
	h.startReady()
	waitFor(t, "no trade connection", func() bool { o, _ := tv.Connections(); return o == 1 })
	tv.Disconnect()
	waitFor(t, "the trade connection was not redialed", func() bool { o, total := tv.Connections(); return o == 1 && total == 2 })
	h.none(200 * time.Millisecond)
	h.stop()
}

// Init refuses what phase 1 does not run, and never quotes the credentials.
func TestInitRefused(t *testing.T) {
	for name, c := range map[string]struct {
		s      setup
		reason string
	}{
		"unknown kind": {setup{kind: "Nope"}, "unknown account kind Nope"},
		"multi venue":  {setup{kind: "Multi"}, ReasonKindUnsupported},
		"key type":     {setup{creds: &tdctlv1.Credentials{KeyType: "rsa", Key: apiKey, Secret: secret}}, `credentials: Fake does not take key type "rsa"`},
		"no secret":    {setup{creds: &tdctlv1.Credentials{KeyType: "hmac", Key: apiKey}}, "credentials: key and secret are required"},
	} {
		t.Run(name, func(t *testing.T) {
			h := start(t, c.s)
			m := h.next(isDone(workerv1.Verb_VERB_INIT, false))
			if strings.Contains(m.GetDone().GetError(), secretPart) {
				t.Fatal("the reason quotes the secret")
			}
			h.ending(commonv1.Outcome_OUTCOME_INIT_FAILED, c.reason, c4.ExitInit)
			if n := h.dials.Load(); n != 0 {
				t.Fatalf("%d dials", n)
			}
		})
	}
}

// A Live is applied as is: TD has no live config yet.
func TestLive(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	h.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Live{Live: &workerv1.Live{Contract: Contract, Digest: "d1"}}})
	h.next(func(m *workerv1.WorkerUp) bool { return m.GetApplied().GetDigest() == "d1" })
	h.stop()
}

// Stop during Start, while the venue refuses every dial, stops the worker.
func TestStopDuringStart(t *testing.T) {
	h := start(t, setup{})
	h.v.FailDials(1 << 20)
	h.initOK()
	h.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: Contract}}})
	waitFor(t, "no dial", func() bool { return h.dials.Load() > 1 })
	h.stop()
}

func count(reqs []string, want string) int {
	n := 0
	for _, r := range reqs {
		if r == want {
			n++
		}
	}
	return n
}
