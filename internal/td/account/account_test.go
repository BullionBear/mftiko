package account

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	"github.com/BullionBear/mftiko/contract/wire/decimal"
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

// fakeBus is the account's NATS in memory: it decodes what is published on td.evt, and calls the
// td.view responder directly.
type fakeBus struct {
	mu      sync.Mutex
	events  []*tdv1.AccountEvent
	respond map[string]func([]byte) []byte
}

func newFakeBus() *fakeBus { return &fakeBus{respond: map[string]func([]byte) []byte{}} }

func (b *fakeBus) Publish(subject string, data []byte) error {
	if subject != "td.evt.42" {
		return errors.New("published on " + subject)
	}
	ev := &tdv1.AccountEvent{}
	if err := proto.Unmarshal(data, ev); err != nil {
		return err
	}
	b.mu.Lock()
	b.events = append(b.events, ev)
	b.mu.Unlock()
	return nil
}

func (b *fakeBus) Respond(subject string, h func([]byte) []byte) (func(), error) {
	b.mu.Lock()
	b.respond[subject] = h
	b.mu.Unlock()
	return func() { b.mu.Lock(); delete(b.respond, subject); b.mu.Unlock() }, nil
}

func (b *fakeBus) all() []*tdv1.AccountEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*tdv1.AccountEvent(nil), b.events...)
}

// recons are the Recon events so far.
func (b *fakeBus) recons() []*tdv1.Recon {
	var out []*tdv1.Recon
	for _, e := range b.all() {
		if r := e.GetRecon(); r != nil {
			out = append(out, r)
		}
	}
	return out
}

func (b *fakeBus) view(t *testing.T, req *tdv1.ViewRequest) *tdv1.ViewReply {
	t.Helper()
	b.mu.Lock()
	h := b.respond["td.view.42"]
	b.mu.Unlock()
	if h == nil {
		t.Fatal("td.view is not served")
	}
	q, _ := proto.Marshal(req)
	r := &tdv1.ViewReply{}
	if err := proto.Unmarshal(h(q), r); err != nil {
		t.Fatal(err)
	}
	return r
}

// fakeInstruments is a symbol table keyed by exch_ticker alone: the fake venues fill the rest of an
// ExchKey unevenly. inactive are only found with includeInactive.
type fakeInstruments struct {
	active, inactive map[string]*symbol.Info
	asked            atomic.Int32 // questions with includeInactive
}

func (f *fakeInstruments) Reverse(_ context.Context, k symbol.ExchKey, includeInactive bool) (*symbol.Info, error) {
	if i, ok := f.active[k.ExchTicker]; ok {
		return i, nil
	}
	if includeInactive {
		f.asked.Add(1)
		if i, ok := f.inactive[k.ExchTicker]; ok {
			return i, nil
		}
	}
	return nil, errors.New("not found")
}

func (f *fakeInstruments) GetByUID(_ context.Context, uid uint32, includeInactive bool) (*symbol.Info, error) {
	for _, i := range f.active {
		if i.UID == uid {
			return i, nil
		}
	}
	if includeInactive {
		f.asked.Add(1)
		for _, i := range f.inactive {
			if i.UID == uid {
				return i, nil
			}
		}
	}
	return nil, errors.New("not found")
}

func testInst(uid uint32, ticker string, size float64) *symbol.Info {
	return &symbol.Info{
		UID: uid, ExchTicker: ticker, ContractSize: size, Status: symbol.Tradable,
		Ticker:  symbol.UniversalTicker{Venue: "Fake", Category: symbol.Perp},
		Filters: symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001, symbol.MinQty: 0.001},
	}
}

func instruments() *fakeInstruments {
	return &fakeInstruments{
		active: map[string]*symbol.Info{
			"BTCUSDT":  testInst(7, "BTCUSDT", 1),
			"BTC-SWAP": testInst(8, "BTC-SWAP", 0.01), // counted in contracts, as OKX's swaps
			"ETHUSDT":  testInst(10, "ETHUSDT", 1),
		},
		inactive: map[string]*symbol.Info{"OLD": {UID: 9}},
	}
}

type harness struct {
	t     *testing.T
	v     *exchangetest.AccountVenue
	bus   *fakeBus
	ins   *fakeInstruments
	dials atomic.Int32 // handshakes the private stream's server received

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
	h := &harness{t: t, v: v, bus: newFakeBus(), ins: instruments(), ups: make(chan *workerv1.WorkerUp, 256), code: make(chan int, 1)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.dials.Add(1)
		v.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	kinds := map[string]exchange.AccountKind{"Fake": v.Kind("Fake"), "Multi": v.Kind("Multi", "Fake", "Other")}
	cfg := ServeConfig{
		Kinds: func(n string) (exchange.AccountKind, bool) { k, ok := kinds[n]; return k, ok },
		Doer:  v,
		Connect: func(context.Context, *tdctlv1.NatsCredentials, string) (*Env, error) {
			return &Env{Bus: h.bus, Instruments: h.ins}, nil
		},
		WorkerKey:    "td-1/42",
		Incarnation:  3,
		RetryAfter:   20 * time.Millisecond,
		RetryMax:     100 * time.Millisecond,
		ReadyTimeout: 2 * time.Second,
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
	payload, err := proto.Marshal(&tdctlv1.InitPayload{ApiId: 42, Kind: kind, TdInstance: "td-1", Credentials: creds,
		StreamUrl: "ws" + strings.TrimPrefix(srv.URL, "http")})
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
	evs := h.bus.all()
	r := evs[0].GetRecon()
	if len(evs) != 1 || evs[0].GetSeq() != 1 || r.GetReason() != tdv1.ReconReason_RECON_REASON_INCARNATION ||
		r.GetIncarnation() != 3 || evs[0].GetSource().GetKey() != "td-1/42" ||
		len(r.GetBalances()) != 2 || len(r.GetOpenOrders()) != 1 || r.GetOpenOrders()[0].GetUid() != 7 || r.GetAsOfNs() == 0 {
		t.Fatalf("events %v", evs)
	}
	h.v.PutOrder(exchange.OrderUpdate{Exch: btc, ClientOrderID: "c2", Status: exchange.OrderStatusAccepted, Qty: 1, Price: 10})
	waitFor(t, "the event did not reach td.evt", func() bool { return len(h.bus.all()) == 2 })
	if u := h.bus.all()[1]; u.GetSeq() != 2 || u.GetOrderUpdate().GetOrder().GetClientOrderId() != "c2" ||
		u.GetOrderUpdate().GetCause() != tdv1.OrderCause_ORDER_CAUSE_ACCEPTED {
		t.Fatalf("event %v", u)
	}
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
	if rs := h.bus.recons(); len(rs) != 1 || len(rs[0].GetBalances()) != 2 {
		t.Fatalf("recons %v", rs)
	}
	h.stop()
}

// Hedge mode at Start is INIT_FAILED (D607).
func TestHedgeModeAtStart(t *testing.T) {
	h := start(t, setup{})
	h.v.SetHedgeMode(true)
	h.startFails(ReasonHedgeMode)
}

// The position mode is checked at Start only (D616): a later recon does not look at it.
func TestHedgeModeOnlyAtStart(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	h.v.SetHedgeMode(true)
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
	if n := len(h.bus.recons()); n != 1 {
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
	waitFor(t, "the fill did not reach td.evt", func() bool {
		evs := h.bus.all()
		return len(evs) == 2 && evs[1].GetFill().GetFillId() == "f1" && evs[1].GetFill().GetUid() == 7
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

// ---------- E6-07: td.view and td.evt against the venue ----------

func (h *harness) oms() *tdv1.ViewReply {
	return h.bus.view(h.t, &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Oms{Oms: &tdv1.OmsViewRequest{}}})
}

func (h *harness) ledgerView() *tdv1.ViewReply {
	return h.bus.view(h.t, &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Ledger{Ledger: &tdv1.LedgerViewRequest{}}})
}

// matches compares the worker's td.view with the venue's own state.
func (h *harness) matches() string {
	got := map[string]string{}
	for _, o := range h.oms().GetOms().GetOrders() {
		got[o.GetOrderId()] = o.GetStatus().String() + "/" + strconv.FormatFloat(o.GetFilledQty(), 'f', -1, 64)
	}
	want := map[string]string{}
	for _, o := range h.v.OpenOrders() {
		want[o.OrderID] = tdv1.OrderStatus(o.Status).String() + "/" + strconv.FormatFloat(o.FilledQty, 'f', -1, 64)
	}
	if !maps.Equal(got, want) {
		return "orders " + fmt.Sprint(got) + " vs " + fmt.Sprint(want)
	}
	l := h.ledgerView().GetLedger()
	gp, wp := map[uint32]float64{}, map[uint32]float64{}
	for _, p := range l.GetPositions() {
		gp[p.GetUid()] = p.GetSize()
	}
	for _, p := range h.v.Positions() {
		wp[h.ins.active[p.Exch.ExchTicker].UID] = p.Size
	}
	if !maps.Equal(gp, wp) {
		return "positions " + fmt.Sprint(gp) + " vs " + fmt.Sprint(wp)
	}
	gb, wb := map[string][2]float64{}, map[string][2]float64{}
	for _, b := range l.GetBalances() {
		gb[b.GetAsset()] = [2]float64{b.GetFree(), b.GetLocked()}
	}
	for _, b := range h.v.Balances() {
		wb[b.Asset] = [2]float64{b.Free, b.Locked}
	}
	if !maps.Equal(gb, wb) {
		return "balances " + fmt.Sprint(gb) + " vs " + fmt.Sprint(wb)
	}
	return ""
}

func (h *harness) waitMatch() {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		last := h.matches()
		if last == "" {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("td.view does not match the venue: %s\nevents %q", last, kinds(h.bus.all()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func limit(cid string, qty float64) exchange.OrderUpdate {
	return exchange.OrderUpdate{Exch: btc, ClientOrderID: cid, Side: exchange.SideBuy, Type: exchange.OrderTypeLimit,
		Status: exchange.OrderStatusAccepted, Qty: qty, Price: 10, QuoteQty: decimal.Absent(), AvgPrice: decimal.Absent()}
}

// The acceptance: after a run of events, td.view's OMS and ledger are the venue's state; and asking
// td.view, however often, sends the venue nothing.
func TestViewMatchesVenue(t *testing.T) {
	h := start(t, setup{})
	h.v.SetBalance(exchange.Balance{Asset: "USDT", Free: 1000})
	h.v.PutOrder(limit("a", 2))
	h.startReady()
	h.waitMatch()

	h.v.PutOrder(limit("b", 1))
	h.v.PutOrder(limit("c", 1))
	if _, err := h.v.Fill("a", 0.5, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := h.v.Fill("b", 1, 10); err != nil { // b fills and leaves
		t.Fatal(err)
	}
	for _, c := range h.v.OpenOrders() {
		if c.ClientOrderID == "c" {
			c.Status = exchange.OrderStatusCancelled
			h.v.PutOrder(c) // c is cancelled and leaves
		}
	}
	h.v.SetPosition(exchange.Position{Exch: btc, Size: 1.5, EntryPrice: 10, MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: 3})
	h.v.SetBalance(exchange.Balance{Asset: "USDT", Free: 985, Locked: 15})
	h.waitMatch()

	h.v.ResetRequests()
	for range 20 {
		h.oms()
		h.ledgerView()
		h.bus.view(t, &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Leverage{Leverage: &tdv1.LeverageRequest{Uid: 7}}})
	}
	if reqs := h.v.Requests(); len(reqs) != 0 {
		t.Fatalf("td.view sent the venue %v", reqs)
	}
	if v := h.oms(); v.GetState() != tdv1.AccountState_ACCOUNT_STATE_READY || v.GetIncarnation() != 3 || v.GetReconciledAtNs() == 0 {
		t.Fatalf("view header %v", v)
	}
	h.stop()
}

// The acceptance: a reconnect publishes one Recon (RECONNECT), and the seq goes on across it.
func TestReconAfterReconnect(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	h.v.PutOrder(limit("a", 1))
	waitFor(t, "no order event", func() bool { return len(h.bus.all()) == 2 })
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	h.v.PutOrder(limit("b", 1))
	waitFor(t, "no order event after the reconnect", func() bool { return len(h.bus.all()) == 4 })
	evs := h.bus.all()
	seqsFrom1(t, evs)
	rc := evs[2].GetRecon()
	if rc.GetReason() != tdv1.ReconReason_RECON_REASON_RECONNECT || len(rc.GetOpenOrders()) != 1 || len(h.bus.recons()) != 2 {
		t.Fatalf("events %v", kinds(evs))
	}
	h.stop()
}

// replay rebuilds an account from a td.view snapshot and the td.evt events after its seq: what a
// session does after it missed events (D606).
type replay struct {
	orders    map[string]*tdv1.Order
	positions map[uint32]float64
	balances  map[string][2]float64
}

func (r *replay) apply(e *tdv1.AccountEvent) {
	switch x := e.GetEvent().(type) {
	case *tdv1.AccountEvent_Recon:
		r.orders, r.positions, r.balances = map[string]*tdv1.Order{}, map[uint32]float64{}, map[string][2]float64{}
		for _, o := range x.Recon.GetOpenOrders() {
			r.orders[o.GetOrderId()] = o
		}
		for _, p := range x.Recon.GetPositions() {
			r.positions[p.GetUid()] = p.GetSize()
		}
		for _, b := range x.Recon.GetBalances() {
			r.balances[b.GetAsset()] = [2]float64{b.GetFree(), b.GetLocked()}
		}
	case *tdv1.AccountEvent_OrderUpdate:
		o := x.OrderUpdate.GetOrder()
		if terminal(o.GetStatus()) {
			delete(r.orders, o.GetOrderId())
		} else {
			r.orders[o.GetOrderId()] = o
		}
	case *tdv1.AccountEvent_PositionUpdate:
		for _, p := range x.PositionUpdate.GetPositions() {
			if p.GetSize() == 0 {
				delete(r.positions, p.GetUid())
			} else {
				r.positions[p.GetUid()] = p.GetSize()
			}
		}
	case *tdv1.AccountEvent_BalanceUpdate:
		for _, b := range x.BalanceUpdate.GetBalances() {
			r.balances[b.GetAsset()] = [2]float64{b.GetFree(), b.GetLocked()}
		}
	}
}

func (r *replay) state() string {
	o := map[string]string{}
	for id, x := range r.orders {
		o[id] = x.GetStatus().String() + "/" + strconv.FormatFloat(x.GetFilledQty(), 'f', -1, 64)
	}
	return fmt.Sprint(o, r.positions, r.balances)
}

func viewState(oms, ledger *tdv1.ViewReply) *replay {
	r := &replay{}
	r.apply(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_Recon{Recon: &tdv1.Recon{
		OpenOrders: oms.GetOms().GetOrders(), Positions: ledger.GetLedger().GetPositions(), Balances: ledger.GetLedger().GetBalances(),
	}}})
	return r
}

// The acceptance: a td.view snapshot plus the td.evt events after its seq rebuild the same state,
// across a reconnect and its Recon.
func TestRebuildFromViewAndEvents(t *testing.T) {
	h := start(t, setup{})
	h.v.SetBalance(exchange.Balance{Asset: "USDT", Free: 1000})
	h.v.PutOrder(limit("a", 2))
	h.startReady()
	h.v.PutOrder(limit("b", 1))
	h.waitMatch()

	// The session's snapshot: the OMS and the ledger at the same seq.
	oms, ledger := h.oms(), h.ledgerView()
	if oms.GetSeq() != ledger.GetSeq() {
		t.Fatalf("seq %d and %d", oms.GetSeq(), ledger.GetSeq())
	}
	base := oms.GetSeq()
	r := viewState(oms, ledger)

	if _, err := h.v.Fill("a", 2, 10); err != nil {
		t.Fatal(err)
	}
	h.v.SetPosition(exchange.Position{Exch: btc, Size: 2, EntryPrice: 10, MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()})
	// Let the events land: a fill lost with the connection leaves its order UNKNOWN until E6-08
	// asks the venue (TestOrderLostWithTheConnection).
	h.waitMatch()
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	h.v.PutOrder(limit("c", 1))
	h.v.SetBalance(exchange.Balance{Asset: "USDT", Free: 970, Locked: 10})
	h.waitMatch()

	final := viewState(h.oms(), h.ledgerView())
	for _, e := range h.bus.all() {
		if e.GetSeq() > base {
			r.apply(e)
		}
	}
	if got, want := r.state(), final.state(); got != want {
		t.Fatalf("rebuilt %s\nwant    %s", got, want)
	}
	h.stop()
}

// An order that finished while its event was lost with the connection is UNKNOWN in that recon
// (D615). The worker then asks the venue by client order id and the fill comes back (D605).
func TestOrderLostWithTheConnection(t *testing.T) {
	h := start(t, setup{})
	h.v.PutOrder(limit("a", 1))
	h.startReady()
	h.v.Hold() // the venue's events wait
	if _, err := h.v.Fill("a", 1, 10); err != nil {
		t.Fatal(err)
	}
	h.v.Disconnect() // and never reach the worker: the venue keeps holding them
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	if rc := h.bus.recons(); len(rc) != 2 || len(rc[1].GetOpenOrders()) != 1 || rc[1].GetOpenOrders()[0].GetClientOrderId() != "a" ||
		rc[1].GetOpenOrders()[0].GetStatus() != tdv1.OrderStatus_ORDER_STATUS_UNKNOWN {
		t.Fatalf("recons %v", rc)
	}
	// The recon leaves it UNKNOWN, then the worker asks by client order id and the fill comes back.
	waitFor(t, "the lost order was not queried", func() bool {
		for _, e := range h.bus.all() {
			u := e.GetOrderUpdate()
			if u.GetOrder().GetClientOrderId() == "a" && u.GetOrder().GetStatus() == tdv1.OrderStatus_ORDER_STATUS_FILLED &&
				u.GetCause() == tdv1.OrderCause_ORDER_CAUSE_RECONCILED {
				return true
			}
		}
		return false
	})
	if len(h.oms().GetOms().GetOrders()) != 0 {
		t.Fatalf("filled order still live: %v", h.oms().GetOms().GetOrders())
	}
	h.stop()
}
