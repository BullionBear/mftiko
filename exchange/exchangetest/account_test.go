package exchangetest

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

var (
	testNow  = time.Unix(1_800_000_000, 0).UTC()
	perpBTC  = &symbol.Info{UID: 7, Ticker: symbol.UniversalTicker{Venue: "Fake", Category: symbol.Perp, Symbol: "BTC-USDT"}, ExchTicker: "BTCUSDT"}
	perpETH  = &symbol.Info{UID: 8, Ticker: symbol.UniversalTicker{Venue: "Fake", Category: symbol.Perp, Symbol: "ETH-USDT"}, ExchTicker: "ETHUSDT"}
	testCfg  = exchange.AccountConfig{APIID: 1, Credentials: exchange.Credentials{Type: exchange.KeyHMAC, Key: "k"}}
	bgCtx    = context.Background()
	stepTime = func() time.Time { return testNow }
)

func newVenue(o AccountOptions) (*AccountVenue, exchange.Account) {
	if o.Key == "" {
		o.Key = "k"
	}
	o.Now = stepTime
	v := NewAccountVenue(o)
	return v, v.Account(testCfg)
}

func limit(cloid string, inst *symbol.Info, qty, px float64) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: inst, Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: exchange.TIFGTC,
		Qty: qty, QuoteQty: decimal.Absent(), Price: px, ClientOrderID: cloid,
	}
}

func submit(t *testing.T, v *AccountVenue, a exchange.Account, req *exchange.OrderRequest) (*exchange.OrderResult, error) {
	t.Helper()
	c, err := a.Submit(testNow, req)
	if err != nil {
		t.Fatal(err)
	}
	return exchange.Run(bgCtx, v, c)
}

func cancel(t *testing.T, v *AccountVenue, a exchange.Account, cloid string) error {
	t.Helper()
	c, err := a.Cancel(testNow, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: cloid})
	if err != nil {
		t.Fatal(err)
	}
	_, err = exchange.Run(bgCtx, v, c)
	return err
}

// snapshot runs every Snapshot Call to its last page and merges the pages, as the worker does.
func snapshot(v *AccountVenue, a exchange.Account) (exchange.AccountSnapshot, error) {
	calls, err := a.Snapshot(testNow)
	if err != nil {
		return exchange.AccountSnapshot{}, err
	}
	var out exchange.AccountSnapshot
	for _, c := range calls {
		pages, err := exchange.Collect(bgCtx, v, stepTime, c)
		if err != nil {
			return exchange.AccountSnapshot{}, err
		}
		for _, p := range pages {
			out.OpenOrders = append(out.OpenOrders, p.OpenOrders...)
			out.Positions = append(out.Positions, p.Positions...)
			out.Balances = append(out.Balances, p.Balances...)
			out.HedgeMode = out.HedgeMode || p.HedgeMode
		}
	}
	return out, nil
}

func wantReject(t *testing.T, err error, code int32) {
	t.Helper()
	var rej *exchange.Reject
	if !errors.As(err, &rej) || rej.Code != code {
		t.Fatalf("got %v, want a reject with code %d", err, code)
	}
}

func count(reqs []string, r string) int {
	n := 0
	for _, x := range reqs {
		if x == r {
			n++
		}
	}
	return n
}

func TestAccountSubmitAndCancel(t *testing.T) {
	v, a := newVenue(AccountOptions{})
	res, err := submit(t, v, a, limit("101", perpBTC, 0.5, 60000))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != exchange.OrderStatusNew || res.OrderID == "" || res.ExchTsNs != testNow.UnixNano() {
		t.Fatalf("ack %+v", res)
	}
	open := v.OpenOrders()
	if len(open) != 1 || open[0].Status != exchange.OrderStatusAccepted || open[0].ClientOrderID != "101" ||
		open[0].Qty != 0.5 || open[0].Price != 60000 || open[0].Exch != (symbol.ExchKey{Venue: "Fake", Category: symbol.Perp, ExchTicker: "BTCUSDT"}) {
		t.Fatalf("open %+v", open)
	}
	// The venue checks client order ids only among its open orders.
	_, err = submit(t, v, a, limit("101", perpBTC, 0.5, 60000))
	wantReject(t, err, -4116)
	if err := cancel(t, v, a, "101"); err != nil {
		t.Fatal(err)
	}
	if len(v.OpenOrders()) != 0 {
		t.Fatal("cancelled order still open")
	}
	wantReject(t, cancel(t, v, a, "101"), -2011)
	if _, err := submit(t, v, a, limit("101", perpBTC, 0.5, 60000)); err != nil {
		t.Fatalf("a closed order's id is free again: %v", err)
	}

	binance, ab := newVenue(AccountOptions{Ack: exchange.OrderStatusAccepted})
	if res, err := submit(t, binance, ab, limit("1", perpBTC, 1, 1)); err != nil || res.Status != exchange.OrderStatusAccepted {
		t.Fatalf("ack %+v %v", res, err)
	}
}

func TestAccountScriptedOutcomes(t *testing.T) {
	v, a := newVenue(AccountOptions{})

	v.OnSubmit("1", Outcome{Code: -2010})
	_, err := submit(t, v, a, limit("1", perpBTC, 1, 1))
	wantReject(t, err, 201)

	v.OnSubmit("2", Outcome{NoAnswer: true})
	if _, err := submit(t, v, a, limit("2", perpBTC, 1, 1)); !errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Fatalf("got %v, want an unknown outcome", err)
	}
	v.OnSubmit("3", Outcome{NoAnswer: true, Applied: true})
	if _, err := submit(t, v, a, limit("3", perpBTC, 1, 1)); !errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Fatalf("got %v, want an unknown outcome", err)
	}
	open := v.OpenOrders()
	if len(open) != 1 || open[0].ClientOrderID != "3" {
		t.Fatalf("only the applied order is on the venue: %+v", open)
	}

	// A script is used once.
	if _, err := submit(t, v, a, limit("1", perpBTC, 1, 1)); err != nil {
		t.Fatal(err)
	}

	v.OnCancel("3", Outcome{NoAnswer: true, Applied: true})
	if err := cancel(t, v, a, "3"); !errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Fatalf("got %v, want an unknown outcome", err)
	}
	v.OnCancel("1", Outcome{NoAnswer: true})
	if err := cancel(t, v, a, "1"); !errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Fatalf("got %v, want an unknown outcome", err)
	}
	open = v.OpenOrders()
	if len(open) != 1 || open[0].ClientOrderID != "1" {
		t.Fatalf("the unanswered, unapplied cancel leaves the order: %+v", open)
	}
	v.OnCancel("1", Outcome{Code: -5000})
	wantReject(t, cancel(t, v, a, "1"), -5000)
}

func TestAccountCancelBatch(t *testing.T) {
	v, a := newVenue(AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 3}})
	for _, id := range []string{"1", "2", "3"} {
		if _, err := submit(t, v, a, limit(id, perpBTC, 1, 1)); err != nil {
			t.Fatal(err)
		}
	}
	refs := func(ids ...string) []exchange.CancelRef {
		out := make([]exchange.CancelRef, len(ids))
		for i, id := range ids {
			out[i] = exchange.CancelRef{Instrument: perpBTC, ClientOrderID: id}
		}
		return out
	}
	run := func(ids ...string) ([]error, error) {
		c, err := a.CancelBatch(testNow, refs(ids...))
		if err != nil {
			t.Fatal(err)
		}
		return exchange.Run(bgCtx, v, c)
	}
	if _, err := a.CancelBatch(testNow, refs("1", "2", "3", "4")); err == nil {
		t.Fatal("a batch over the limit was built")
	}

	v.OnBatch(Outcome{Code: -1000})
	_, err := run("1")
	wantReject(t, err, -1000)

	v.OnBatch(Outcome{NoAnswer: true})
	if _, err := run("1"); !errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Fatalf("got %v, want an unknown outcome", err)
	}
	if len(v.OpenOrders()) != 3 {
		t.Fatal("an unanswered, unapplied batch cancelled orders")
	}

	v.OnCancel("2", Outcome{Code: -2011})
	errs, err := run("1", "2", "9")
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != nil {
		t.Fatalf("entry 1: %v", errs[0])
	}
	wantReject(t, errs[1], -2011)
	wantReject(t, errs[2], -2011)
	open := v.OpenOrders()
	if len(open) != 2 || open[0].ClientOrderID != "2" || open[1].ClientOrderID != "3" {
		t.Fatalf("open %+v", open)
	}
}

func TestAccountSnapshotPagesAndFailure(t *testing.T) {
	v, a := newVenue(AccountOptions{PageSize: 2})
	for _, id := range []string{"1", "2", "3"} {
		if _, err := submit(t, v, a, limit(id, perpBTC, 1, 1)); err != nil {
			t.Fatal(err)
		}
	}
	// A manual order has no client order id; it is on the snapshot all the same.
	v.PutOrder(exchange.OrderUpdate{Exch: symbol.ExchKey{Category: symbol.Perp, ExchTicker: "ETHUSDT"}, Side: exchange.SideSell,
		Type: exchange.OrderTypeLimit, Status: exchange.OrderStatusAccepted, Qty: 2, QuoteQty: decimal.Absent(), Price: 3000,
		AvgPrice: decimal.Absent()})
	v.SetPosition(exchange.Position{Exch: symbol.ExchKey{Category: symbol.Perp, ExchTicker: "BTCUSDT"}, Size: -0.5,
		EntryPrice: 60000, MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: 5})
	v.SetPosition(exchange.Position{Exch: symbol.ExchKey{Category: symbol.Perp, ExchTicker: "ETHUSDT"}, Size: 3,
		EntryPrice: decimal.Absent(), MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()})
	for _, asset := range []string{"USDT", "BTC", "ETH"} {
		v.SetBalance(exchange.Balance{Asset: asset, Free: 10, Locked: 1})
	}
	v.ResetRequests()

	got, err := snapshot(v, a)
	if err != nil {
		t.Fatal(err)
	}
	if !sameOrders(got.OpenOrders, v.OpenOrders()) || len(got.OpenOrders) != 4 {
		t.Fatalf("orders %+v\nwant %+v", got.OpenOrders, v.OpenOrders())
	}
	if !samePositions(got.Positions, v.Positions()) || !slices.Equal(got.Balances, v.Balances()) || got.HedgeMode {
		t.Fatalf("snapshot %+v", got)
	}
	reqs := v.Requests()
	if count(reqs, "GET /orders") != 2 || count(reqs, "GET /positions") != 1 || count(reqs, "GET /balances") != 2 {
		t.Fatalf("requests %v", reqs)
	}

	v.FailSnapshot(SnapshotOrders, 1, 1)
	if _, err := snapshot(v, a); err == nil {
		t.Fatal("a failed page made a snapshot")
	} else if errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Fatalf("a read is not an unknown outcome: %v", err)
	}
	if _, err := snapshot(v, a); err != nil {
		t.Fatalf("the failure was for one read: %v", err)
	}

	v.SetHedgeMode(true)
	if got, err := snapshot(v, a); err != nil || !got.HedgeMode {
		t.Fatalf("hedge mode %v %v", got.HedgeMode, err)
	}
}

func sameOrders(a, b []exchange.OrderUpdate) bool {
	return slices.EqualFunc(a, b, func(x, y exchange.OrderUpdate) bool {
		return fmt.Sprint(toWireOrder(&x)) == fmt.Sprint(toWireOrder(&y))
	})
}

func samePositions(a, b []exchange.Position) bool {
	return slices.EqualFunc(a, b, func(x, y exchange.Position) bool {
		return toWirePosition(x) == toWirePosition(y)
	})
}

func TestAccountThrottleUsageAndAuth(t *testing.T) {
	v, a := newVenue(AccountOptions{})
	reset := testNow.Add(40 * time.Second).Truncate(time.Millisecond)
	v.ReportUsage(exchange.Used{Bucket: "orders_10s", N: 7, ResetAt: reset}, exchange.Used{Bucket: "weight", N: 120})

	c, err := a.Submit(testNow, limit("1", perpBTC, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := v.Do(bgCtx, &c.Req)
	if err != nil {
		t.Fatal(err)
	}
	used := a.Usage(resp, testNow)
	want := []exchange.Used{{Bucket: "orders_10s", N: 7, ResetAt: reset}, {Bucket: "weight", N: 120}}
	if !slices.EqualFunc(used, want, func(x, y exchange.Used) bool {
		return x.Bucket == y.Bucket && x.N == y.N && x.ResetAt.Equal(y.ResetAt)
	}) {
		t.Fatalf("usage %+v", used)
	}

	v.Throttle(2, 3*time.Second)
	for range 2 {
		_, err := submit(t, v, a, limit("2", perpBTC, 1, 1))
		var rl *exchange.RateLimited
		if !errors.As(err, &rl) || rl.RetryAfterMs != 3000 {
			t.Fatalf("got %v, want 429 with Retry-After 3s", err)
		}
	}
	if _, err := submit(t, v, a, limit("2", perpBTC, 1, 1)); err != nil {
		t.Fatalf("throttle was for two requests: %v", err)
	}

	v.SetKey("rotated")
	if _, err := submit(t, v, a, limit("3", perpBTC, 1, 1)); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}
	if _, err := snapshot(v, a); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}
}

func TestAccountLeverageArmsAndTrade(t *testing.T) {
	v, a := newVenue(AccountOptions{})
	v.SetLeverage(symbol.ExchKey{Category: symbol.Perp, ExchTicker: "BTCUSDT"}, 20)
	c, err := a.Leverage(testNow, perpBTC)
	if err != nil {
		t.Fatal(err)
	}
	if lev, err := exchange.Run(bgCtx, v, c); err != nil || lev != 20 {
		t.Fatalf("leverage %v %v", lev, err)
	}
	c, err = a.Leverage(testNow, perpETH)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exchange.Run(bgCtx, v, c); err == nil {
		t.Fatal("leverage for an instrument the venue has none for")
	}

	cod, ok := a.(exchange.CancelOnDisconnect)
	if !ok {
		t.Fatal("no CancelOnDisconnect")
	}
	arm, err := cod.ArmCancelOnDisconnect(testNow, 30*time.Second, []*symbol.Info{perpBTC, perpETH})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exchange.Run(bgCtx, v, arm); err != nil {
		t.Fatal(err)
	}
	if got := v.Arms(); len(got) != 1 || got[0].Timeout != 30*time.Second || got[0].AtNs != testNow.UnixNano() ||
		!slices.Equal(got[0].Tickers, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Fatalf("arms %+v", got)
	}

	if _, err := a.NewTradeStream(); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("no TradeVenue: got %v, want ErrUnsupported", err)
	}
	_, withTrade := newVenue(AccountOptions{Trade: NewTradeVenue("k", false)})
	if ts, err := withTrade.NewTradeStream(); err != nil || ts == nil {
		t.Fatalf("trade stream %v %v", ts, err)
	}
}

// ---------- the private stream, over exchange/transport/ws ----------

// events is an AccountSink that copies what Decode reports and lets the test wait for it.
type events struct {
	mu     sync.Mutex
	got    []string
	wake   chan struct{}
	orders []exchange.OrderUpdate
	fills  []exchange.Fill
}

func newEvents() *events { return &events{wake: make(chan struct{}, 1)} }

func (e *events) add(s string) {
	e.mu.Lock()
	e.got = append(e.got, s)
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *events) Ready()           { e.add("ready") }
func (e *events) Expired(r string) { e.add("expired " + r) }
func (e *events) Order(o *exchange.OrderUpdate) {
	e.mu.Lock()
	e.orders = append(e.orders, *o)
	e.mu.Unlock()
	e.add(fmt.Sprintf("order %s %d", o.ClientOrderID, o.Status))
}
func (e *events) Fill(f *exchange.Fill) {
	e.mu.Lock()
	e.fills = append(e.fills, *f)
	e.mu.Unlock()
	e.add(fmt.Sprintf("fill %s %s %v@%v", f.ClientOrderID, f.FillID, f.Qty, f.Price))
}
func (e *events) Balances(b []exchange.Balance) {
	for _, x := range b {
		e.add(fmt.Sprintf("balance %s %v", x.Asset, x.Free))
	}
}
func (e *events) Positions(p []exchange.Position) {
	for _, x := range p {
		e.add(fmt.Sprintf("position %s %v", x.Exch.ExchTicker, x.Size))
	}
}

// wait returns the events once there are n of them.
func (e *events) wait(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		e.mu.Lock()
		if len(e.got) >= n {
			out := slices.Clone(e.got)
			e.mu.Unlock()
			return out
		}
		e.mu.Unlock()
		select {
		case <-e.wake:
		case <-deadline:
			e.mu.Lock()
			defer e.mu.Unlock()
			t.Fatalf("waited for %d events, have %v", n, e.got)
		}
	}
}

// stream is one connection of the worker's: dial with the codec, send its frames, decode into ev.
type stream struct {
	conn *ws.Conn
	ev   *events
	done chan error
}

func connect(ctx context.Context, a exchange.Account, token string) (*stream, error) {
	ps, err := a.NewStream()
	if err != nil {
		return nil, err
	}
	d, err := ps.Dial(testNow, token)
	if err != nil {
		return nil, err
	}
	lim := ps.Limits()
	conn, err := ws.Dial(ctx, d.URL, ws.Options{ReadLimit: lim.ReadLimit, Keepalive: lim.Keepalive})
	if err != nil {
		return nil, err
	}
	s := &stream{conn: conn, ev: newEvents(), done: make(chan error, 1)}
	go func() { s.done <- conn.Run(ctx, func(f []byte) error { return ps.Decode(f, s.ev) }) }()
	for _, f := range d.Frames {
		if err := conn.Write(ctx, f); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *stream) ended(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the connection did not end")
		return nil
	}
}

func serveVenue(t *testing.T, o AccountOptions) (*AccountVenue, exchange.Account) {
	t.Helper()
	if o.Key == "" {
		o.Key = "k"
	}
	o.Now = stepTime
	v := NewAccountVenue(o)
	srv := httptest.NewServer(v.Handler())
	t.Cleanup(srv.Close)
	cfg := testCfg
	cfg.StreamEndpoint = "ws" + strings.TrimPrefix(srv.URL, "http")
	return v, v.Account(cfg)
}

func TestAccountStreamEvents(t *testing.T) {
	v, a := serveVenue(t, AccountOptions{})
	ctx, stop := context.WithCancel(bgCtx)
	defer stop()
	s, err := connect(ctx, a, "")
	if err != nil {
		t.Fatal(err)
	}
	s.ev.wait(t, 1)
	if open, total := v.Connections(); open != 1 || total != 1 {
		t.Fatalf("connections %d %d", open, total)
	}

	if _, err := submit(t, v, a, limit("11", perpBTC, 1, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Fill("11", 0.25, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Fill("11", 0.75, 102); err != nil {
		t.Fatal(err)
	}
	v.SetBalance(exchange.Balance{Asset: "USDT", Free: 900, Locked: 0})
	v.SetPosition(exchange.Position{Exch: symbol.ExchKey{Category: symbol.Perp, ExchTicker: "BTCUSDT"}, Size: 1,
		EntryPrice: 101.5, MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()})
	got := s.ev.wait(t, 8)
	want := []string{
		"ready",
		fmt.Sprintf("order 11 %d", exchange.OrderStatusAccepted),
		fmt.Sprintf("order 11 %d", exchange.OrderStatusPartiallyFilled),
		"fill 11 f1 0.25@100",
		fmt.Sprintf("order 11 %d", exchange.OrderStatusFilled),
		"fill 11 f2 0.75@102",
		"balance USDT 900",
		"position BTCUSDT 1",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}
	s.ev.mu.Lock()
	last := s.ev.orders[len(s.ev.orders)-1]
	s.ev.mu.Unlock()
	if last.FilledQty != 1 || last.AvgPrice != 101.5 || decimal.Has(last.QuoteQty) || len(v.OpenOrders()) != 0 {
		t.Fatalf("filled order %+v", last)
	}

	// Hold lets the answer arrive before the event of the same change.
	v.Hold()
	if _, err := submit(t, v, a, limit("12", perpBTC, 1, 100)); err != nil {
		t.Fatal(err)
	}
	v.EmitOrder(exchange.OrderUpdate{Exch: symbol.ExchKey{Venue: "Fake", Category: symbol.Perp, ExchTicker: "BTCUSDT"},
		OrderID: "99", ClientOrderID: "12", Status: exchange.OrderStatusAccepted, Qty: 1, QuoteQty: decimal.Absent(),
		Price: decimal.Absent(), AvgPrice: decimal.Absent()})
	time.Sleep(50 * time.Millisecond)
	if n := len(s.ev.wait(t, 8)); n != 8 {
		t.Fatalf("held events arrived: %d", n)
	}
	v.Release()
	got = s.ev.wait(t, 10)
	if got[8] != fmt.Sprintf("order 12 %d", exchange.OrderStatusAccepted) || got[9] != got[8] {
		t.Fatalf("released %v", got[8:])
	}

	v.Disconnect()
	if err := s.ended(t); err == nil || errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("a dropped connection ends Run with a read error, got %v", err)
	}
	// What happens while no connection is there is lost, as on a venue; the next snapshot has it.
	v.SetBalance(exchange.Balance{Asset: "USDT", Free: 800})
	s2, err := connect(ctx, a, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.ev.wait(t, 1); !slices.Equal(got, []string{"ready"}) {
		t.Fatalf("a new connection starts with its login: %v", got)
	}
	if open, total := v.Connections(); open != 1 || total != 2 {
		t.Fatalf("connections %d %d", open, total)
	}
}

func TestAccountStreamRefusals(t *testing.T) {
	v, a := serveVenue(t, AccountOptions{})
	ctx, stop := context.WithCancel(bgCtx)
	defer stop()

	v.FailDials(1)
	if _, err := connect(ctx, a, ""); err == nil {
		t.Fatal("a failed handshake connected")
	}

	v.RefuseLogins(1)
	s, err := connect(ctx, a, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ended(t); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}

	s, err = connect(ctx, a, "")
	if err != nil {
		t.Fatal(err)
	}
	s.ev.wait(t, 1)

	v.SetKey("rotated")
	s2, err := connect(ctx, a, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.ended(t); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("a revoked key: got %v, want ErrAuth", err)
	}
	if open, total := v.Connections(); open != 1 || total != 1 {
		t.Fatalf("connections %d %d", open, total)
	}
}

func TestAccountStreamListenKey(t *testing.T) {
	v, a := serveVenue(t, AccountOptions{ListenKey: true})
	ctx, stop := context.WithCancel(bgCtx)
	defer stop()
	ps, err := a.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	if ps.Limits().RefreshEvery == 0 {
		t.Fatal("a listen key stream refreshes")
	}
	prep, err := ps.Prepare(testNow)
	if err != nil || prep == nil {
		t.Fatalf("prepare %v %v", prep, err)
	}
	key, err := exchange.Run(bgCtx, v, *prep)
	if err != nil || key == "" {
		t.Fatalf("listen key %q %v", key, err)
	}
	if _, err := connect(ctx, a, ""); err == nil {
		t.Fatal("dialled without the listen key")
	}
	s, err := connect(ctx, a, key)
	if err != nil {
		t.Fatal(err)
	}
	s.ev.wait(t, 1)
	ref, err := ps.Refresh(testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exchange.Run(bgCtx, v, *ref); err != nil {
		t.Fatal(err)
	}

	v.ExpireListenKey()
	if got := s.ev.wait(t, 2); got[1] != "expired listenKeyExpired" {
		t.Fatalf("events %v", got)
	}
	_, err = exchange.Run(bgCtx, v, *ref)
	wantReject(t, err, -1125)
	if _, err := connect(ctx, a, key); err == nil {
		t.Fatal("dialled with an expired listen key")
	}
	key2, err := exchange.Run(bgCtx, v, *prep)
	if err != nil || key2 == key {
		t.Fatalf("a new listen key %q %v", key2, err)
	}
	if _, err := connect(ctx, a, key2); err != nil {
		t.Fatal(err)
	}
}

func TestAccountKind(t *testing.T) {
	v := NewAccountVenue(AccountOptions{Venue: "Fake", Key: "k"})
	reg, err := exchange.NewRegistry(exchange.Declaration{
		Exchange:     "exchangetest",
		Venues:       []exchange.Venue{{Venue: symbol.Venue{Name: "Fake", Categories: []symbol.Category{symbol.Perp}}}},
		AccountKinds: []exchange.AccountKind{v.Kind("FakeUM")},
	})
	if err != nil {
		t.Fatal(err)
	}
	k, ok := reg.AccountKind("FakeUM")
	if !ok || !slices.Equal(k.Trades, []string{"Fake"}) {
		t.Fatalf("kind %+v", k)
	}
	a, err := k.Account(testCfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submit(t, v, a, limit("1", perpBTC, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if a.Limits().CancelBatch != 5 || a.Limits().ClientOrderIDMaxLen != 19 {
		t.Fatalf("limits %+v", a.Limits())
	}
}

func query(t *testing.T, v *AccountVenue, a exchange.Account, ref exchange.CancelRef) (*exchange.OrderUpdate, error) {
	t.Helper()
	c, err := a.QueryOrder(testNow, ref)
	if err != nil {
		t.Fatal(err)
	}
	return exchange.Run(bgCtx, v, c)
}

func TestAccountQueryOrder(t *testing.T) {
	v, a := newVenue(AccountOptions{})
	if _, err := submit(t, v, a, limit("101", perpBTC, 0.5, 60000)); err != nil {
		t.Fatal(err)
	}
	o, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "101"})
	if err != nil || o.Status != exchange.OrderStatusAccepted || o.ClientOrderID != "101" || o.Qty != 0.5 {
		t.Fatalf("open: %+v, %v", o, err)
	}
	if _, err := v.Fill("101", 0.5, 60000); err != nil {
		t.Fatal(err)
	}
	// A filled order is off the book and still found, by client order id and by order id.
	o, err = query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "101"})
	if err != nil || o.Status != exchange.OrderStatusFilled || o.FilledQty != 0.5 {
		t.Fatalf("filled: %+v, %v", o, err)
	}
	if o2, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, OrderID: o.OrderID}); err != nil || o2.ClientOrderID != "101" {
		t.Fatalf("by order id: %+v, %v", o2, err)
	}
	if _, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "999"}); !errors.Is(err, exchange.ErrNotFound) {
		t.Fatalf("missing: %v, want ErrNotFound", err)
	}
	if _, err := a.QueryOrder(testNow, exchange.CancelRef{Instrument: perpBTC}); err == nil {
		t.Fatal("a query naming no order was built")
	}
}

func TestAccountLateSubmit(t *testing.T) {
	v, a := newVenue(AccountOptions{})
	v.OnSubmit("101", Outcome{Late: true})
	v.OnSubmit("102", Outcome{Late: true})
	for _, cid := range []string{"101", "102"} {
		if _, err := submit(t, v, a, limit(cid, perpBTC, 0.5, 60000)); !errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Fatalf("%s: %v, want ErrUnknownOutcome", cid, err)
		}
	}
	if got := v.Held(); !slices.Equal(got, []string{"101", "102"}) {
		t.Fatalf("held %v", got)
	}
	// Not arrived yet: the venue has never seen it.
	if _, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "101"}); !errors.Is(err, exchange.ErrNotFound) {
		t.Fatalf("before arrival: %v", err)
	}
	// Within the window (testNow + DefaultValidity, inclusive) it is placed.
	if took, err := v.Arrive("101", testNow.Add(exchange.DefaultValidity)); err != nil || !took {
		t.Fatalf("on time: %v, %v", took, err)
	}
	if o, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "101"}); err != nil || o.Status != exchange.OrderStatusAccepted {
		t.Fatalf("after arrival: %+v, %v", o, err)
	}
	// Past it, refused: the order never exists.
	if took, err := v.Arrive("102", testNow.Add(exchange.DefaultValidity+time.Millisecond)); err != nil || took {
		t.Fatalf("late: %v, %v", took, err)
	}
	if _, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "102"}); !errors.Is(err, exchange.ErrNotFound) {
		t.Fatalf("after a late arrival: %v", err)
	}
	if _, err := v.Arrive("102", testNow); err == nil {
		t.Fatal("arrived twice")
	}
	if len(v.Held()) != 0 {
		t.Fatalf("still held %v", v.Held())
	}
}

func TestAccountLateCancel(t *testing.T) {
	v := NewAccountVenue(AccountOptions{Key: "k", Now: stepTime})
	cfg := testCfg
	cfg.Validity = 2 * time.Second
	a := v.Account(cfg)
	for _, cid := range []string{"101", "102"} {
		if _, err := submit(t, v, a, limit(cid, perpBTC, 0.5, 60000)); err != nil {
			t.Fatal(err)
		}
		v.OnCancel(cid, Outcome{Late: true})
		if err := cancel(t, v, a, cid); !errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Fatalf("%s: %v", cid, err)
		}
	}
	if len(v.OpenOrders()) != 2 {
		t.Fatalf("a held cancel took effect: %+v", v.OpenOrders())
	}
	// The window is the account's Validity, 2s here.
	if took, _ := v.Arrive("101", testNow.Add(2*time.Second)); !took {
		t.Fatal("an on-time cancel was refused")
	}
	if took, _ := v.Arrive("102", testNow.Add(2*time.Second+time.Millisecond)); took {
		t.Fatal("a late cancel took effect")
	}
	o, err := query(t, v, a, exchange.CancelRef{Instrument: perpBTC, ClientOrderID: "101"})
	if err != nil || o.Status != exchange.OrderStatusCancelled {
		t.Fatalf("101: %+v, %v", o, err)
	}
	open := v.OpenOrders()
	if len(open) != 1 || open[0].ClientOrderID != "102" {
		t.Fatalf("open %+v", open)
	}
}
