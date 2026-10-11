package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

// TestBybitTestnet places orders on the Bybit testnet through the adapter and checks the account
// afterwards. It does not run in CI.
//
//	MFTIKO_BYBIT_LIVE=1 go test ./exchange/bybit/ -run TestBybitTestnet -v -count=1
//
// MFTIKO_BYBIT_RECORD=1 also writes the responses and the private session into testdata, with the
// key and the secret redacted.
func TestBybitTestnet(t *testing.T) {
	if os.Getenv("MFTIKO_BYBIT_LIVE") != "1" {
		t.Skip("set MFTIKO_BYBIT_LIVE=1 to place orders on the Bybit testnet")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	creds := loadBybitCreds(t)
	acct, err := Declare().AccountKinds[0].Account(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: testREST,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := acct.(*account)
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 15 * time.Second})
	inst, tick, step, qty := btcLinear(t)

	h := &live{
		t: t, ctx: ctx, acct: raw, doer: doer, inst: inst, tick: tick, step: step, qty: qty,
		creds: creds, feed: newFeed(), record: os.Getenv("MFTIKO_BYBIT_RECORD") == "1",
	}
	var passed bool
	t.Cleanup(func() {
		h.cleanup()
		if passed && h.record {
			h.writeFixtures()
		}
	})
	defer func() {
		fmt.Println("--- bybit testnet ---")
		for _, line := range h.lines {
			fmt.Println(line)
		}
	}()

	h.note("book bid=%s ask=%s tick=%s step=%s qty=%s",
		decimal.Format(h.bid(), tick), decimal.Format(h.ask(), tick),
		decimal.Format(tick, tick), decimal.Format(step, step), decimal.Format(qty, step))

	h.cancelOpen()
	h.flatten("setup")
	h.until(15*time.Second, "setup leaves BTCUSDT flat", func() bool {
		return h.openBTC() == 0 && math.Abs(h.size()) < step
	})

	levCall := mustCall(raw.Leverage(time.Now(), inst))
	lev, levResp, err := call(h, levCall)
	if err != nil {
		t.Fatal(err)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: "leverage", req: levCall.Req, resp: levResp})
	}
	if !(lev > 0) {
		t.Fatalf("leverage %v", lev)
	}
	h.note("leverage BTCUSDT=%s", decimal.Format(lev, 1))

	h.openSocket()

	restPx := decimal.SnapDown(h.bid()*0.90, tick)
	if !(restPx < h.bid()) {
		t.Fatalf("resting price %s is not below the bid", decimal.Format(restPx, tick))
	}
	rest := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, restPx, false)
	res := h.place("submit_limit", rest, exchange.OrderStatusAccepted)
	h.until(10*time.Second, "snapshot shows the resting order", func() bool { return h.hasOrder(res.OrderID) })
	h.keepSnap("open_orders", "/v5/order/realtime", "settleCoin=USDT")
	if sz := math.Abs(h.size()); sz >= step {
		t.Fatalf("resting order opened a position %s", decimal.Format(sz, step))
	}
	h.note("GTC rest px=%s order=%s cloid=%s", decimal.Format(restPx, tick), res.OrderID, rest.ClientOrderID)

	h.cancel("cancel", rest.ClientOrderID)
	h.until(10*time.Second, "snapshot drops the cancelled order", func() bool { return !h.hasOrder(res.OrderID) })
	h.note("GTC rest cancelled")

	po := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFPostOnly, qty, restPx, false)
	poRes := h.place("submit_postonly", po, exchange.OrderStatusAccepted)
	h.cancel("", po.ClientOrderID)
	h.note("post-only px=%s order=%s", decimal.Format(restPx, tick), poRes.OrderID)

	before := h.size()
	mkt := h.order(exchange.SideBuy, exchange.OrderTypeMarket, exchange.TIFUnspecified, qty, decimal.Absent(), false)
	mktRes := h.place("submit_market", mkt, exchange.OrderStatusFilled)
	fill, err := h.feed.waitFill(mkt.ClientOrderID, 20*time.Second)
	if err != nil {
		t.Fatalf("stream fill %s: %v\n%s", mkt.ClientOrderID, err, h.feed.seen())
	}
	if fill.OrderID != mktRes.OrderID || fill.Qty <= 0 || fill.FillID == "" {
		t.Fatalf("fill %+v does not match order %s", fill, mktRes.OrderID)
	}
	var after float64
	h.until(10*time.Second, "position grows by the market fill", func() bool {
		after = h.size()
		return decimal.Equal(after, before+qty, step)
	})
	h.note("market order=%s fill=%s qty=%s size %s -> %s", mktRes.OrderID, fill.FillID, decimal.Format(fill.Qty, step), decimal.Format(before, step), decimal.Format(after, step))
	h.keepSnap("positions", "/v5/position/list", "settleCoin=USDT")

	flat := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, decimal.SnapDown(math.Abs(h.size()), step), decimal.Absent(), true)
	flatRes := h.place("", flat, exchange.OrderStatusFilled)
	h.until(10*time.Second, "flatten", func() bool { return math.Abs(h.size()) < step })
	h.note("flatten order=%s", flatRes.OrderID)

	px := restPx
	a := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, px, false)
	b := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, px, false)
	ra := h.place("", a, exchange.OrderStatusAccepted)
	rb := h.place("", b, exchange.OrderStatusAccepted)
	batchCall := mustCall(raw.CancelBatch(time.Now(), []exchange.CancelRef{
		{Instrument: inst, ClientOrderID: a.ClientOrderID},
		{Instrument: inst, ClientOrderID: b.ClientOrderID},
	}))
	batch, resp, err := call(h, batchCall)
	if err != nil {
		t.Fatal(err)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: "cancel_batch", req: batchCall.Req, resp: resp})
	}
	for i, e := range batch {
		if e != nil {
			t.Fatalf("batch item %d: %v", i, e)
		}
	}
	if _, err := h.feed.waitOrder(a.ClientOrderID, exchange.OrderStatusCancelled, 20*time.Second); err != nil {
		t.Fatalf("stream cancel %s: %v\n%s", a.ClientOrderID, err, h.feed.seen())
	}
	if _, err := h.feed.waitOrder(b.ClientOrderID, exchange.OrderStatusCancelled, 20*time.Second); err != nil {
		t.Fatalf("stream cancel %s: %v\n%s", b.ClientOrderID, err, h.feed.seen())
	}
	var open int
	h.until(10*time.Second, "batch cancel clears BTCUSDT", func() bool {
		open = h.openBTC()
		return open == 0
	})
	h.note("batch cancel %s %s open=%d", ra.OrderID, rb.OrderID, open)

	cheap := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, tick, false)
	rej := h.placeReject("submit_reject", cheap)
	h.note("reject px=%s code=%d native=%s reason=%s", decimal.Format(tick, tick), rej.Code, rej.Native, rej.Reason)

	dcpCall := mustCall(raw.ArmCancelOnDisconnect(time.Now(), 10*time.Second, nil))
	_, dcpResp, dcpErr := call(h, dcpCall)
	if dcpErr == nil {
		h.note("dcp armed timeWindow=10")
	} else {
		var rej *exchange.Reject
		if !errors.As(dcpErr, &rej) || rej.Native != "3500716" {
			t.Fatalf("dcp: %v", dcpErr)
		}
		h.note("dcp reject %d %s %s", rej.Code, rej.Native, rej.Reason)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: "dcp", req: dcpCall.Req, resp: dcpResp})
	}

	h.keepSnap("balances", "/v5/account/wallet-balance", "accountType=UNIFIED")
	if bals := h.balances(); len(bals) == 0 {
		t.Fatal("wallet snapshot is empty")
	}
	h.note("balances %d coins, position %s", len(h.balances()), decimal.Format(h.size(), step))

	// Let the last private frames arrive before the socket closes.
	time.Sleep(500 * time.Millisecond)
	passed = true
}

type live struct {
	t      *testing.T
	ctx    context.Context
	acct   *account
	doer   exchange.Doer
	inst   *symbol.Info
	tick   float64
	step   float64
	qty    float64
	creds  exchange.Credentials
	feed   *feed
	conn   *ws.Conn
	seq    uint64
	lines  []string
	record bool
	saved  []saved
	rawMu  sync.Mutex
	raw    [][]byte
}

type saved struct {
	name string
	req  exchange.HTTPRequest
	resp *exchange.HTTPResponse
}

func (h *live) note(format string, args ...any) {
	h.lines = append(h.lines, fmt.Sprintf(format, args...))
}

func (h *live) openSocket() {
	h.t.Helper()
	ps, err := h.acct.NewStream()
	if err != nil {
		h.t.Fatal(err)
	}
	d, err := ps.Dial(time.Now(), "")
	if err != nil {
		h.t.Fatal(err)
	}
	lim := ps.Limits()
	conn, err := ws.Dial(h.ctx, d.URL, ws.Options{
		ReadLimit: lim.ReadLimit, Keepalive: lim.Keepalive, MaxLifetime: lim.MaxLifetime, Ping: ps.Ping(),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.conn = conn
	go func() {
		err := conn.Run(h.ctx, func(frame []byte) error {
			buf := append([]byte(nil), frame...)
			h.rawMu.Lock()
			h.raw = append(h.raw, buf)
			h.rawMu.Unlock()
			if err := ps.Decode(frame, h.feed); err != nil {
				return fmt.Errorf("%w (frame %.240q)", err, frame)
			}
			return nil
		})
		h.feed.die(err)
	}()
	for _, f := range d.Frames {
		if err := conn.Write(h.ctx, f); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := h.feed.waitReady(15 * time.Second); err != nil {
		h.t.Fatal(err)
	}
	h.note("private stream ready")
}

func (h *live) place(fixture string, req *exchange.OrderRequest, want exchange.OrderStatus) *exchange.OrderResult {
	h.t.Helper()
	c := mustCall(h.acct.Submit(time.Now(), req))
	res, resp, err := call(h, c)
	if err != nil {
		h.t.Fatalf("%s: %v", req.ClientOrderID, err)
	}
	if res == nil || res.Status != exchange.OrderStatusNew || res.OrderID == "" {
		h.t.Fatalf("%s: ack %+v, want New", req.ClientOrderID, res)
	}
	if fixture != "" {
		h.saved = append(h.saved, saved{name: fixture, req: c.Req, resp: resp})
	}
	if _, err := h.feed.waitOrder(req.ClientOrderID, want, 20*time.Second); err != nil {
		h.t.Fatalf("stream %s %s: %v\n%s", req.ClientOrderID, statusName(want), err, h.feed.seen())
	}
	return res
}

func (h *live) placeReject(fixture string, req *exchange.OrderRequest) *exchange.Reject {
	h.t.Helper()
	c := mustCall(h.acct.Submit(time.Now(), req))
	_, resp, err := call(h, c)
	var rej *exchange.Reject
	if !errors.As(err, &rej) {
		h.t.Fatalf("%s: want a reject, got %v", req.ClientOrderID, err)
	}
	if fixture != "" {
		h.saved = append(h.saved, saved{name: fixture, req: c.Req, resp: resp})
	}
	return rej
}

func (h *live) cancel(fixture, cloid string) {
	h.t.Helper()
	c := mustCall(h.acct.Cancel(time.Now(), exchange.CancelRef{Instrument: h.inst, ClientOrderID: cloid}))
	if _, resp, err := call(h, c); err != nil {
		h.t.Fatalf("cancel %s: %v", cloid, err)
	} else if fixture != "" {
		h.saved = append(h.saved, saved{name: fixture, req: c.Req, resp: resp})
	}
	if _, err := h.feed.waitOrder(cloid, exchange.OrderStatusCancelled, 20*time.Second); err != nil {
		h.t.Fatalf("stream cancel %s: %v\n%s", cloid, err, h.feed.seen())
	}
}

func (h *live) order(side exchange.Side, typ exchange.OrderType, tif exchange.TimeInForce, qty, price float64, reduce bool) *exchange.OrderRequest {
	h.seq++
	id := d602(h.seq)
	if len(id) > 19 || len(id) > h.acct.Limits().ClientOrderIDMaxLen {
		h.t.Fatalf("client order id %q length %d", id, len(id))
	}
	return &exchange.OrderRequest{
		Instrument: h.inst, Side: side, Type: typ, TIF: tif,
		Qty: qty, QuoteQty: decimal.Absent(), Price: price,
		ReduceOnly: reduce, ClientOrderID: id,
	}
}

func call[T any](h *live, c exchange.Call[T]) (T, *exchange.HTTPResponse, error) {
	h.t.Helper()
	resp, err := h.doer.Do(h.ctx, &c.Req)
	if err != nil {
		var zero T
		return zero, nil, err
	}
	v, perr := c.Parse(resp)
	return v, resp, perr
}

func (h *live) keepSnap(name, path, query string) {
	h.t.Helper()
	calls, err := h.acct.Snapshot(time.Now())
	if err != nil {
		h.t.Fatal(err)
	}
	var found exchange.Call[exchange.Page[exchange.AccountSnapshot]]
	ok := false
	for _, c := range calls {
		if c.Req.Path == path && strings.Contains(string(c.Req.Query), query) {
			found, ok = c, true
			break
		}
	}
	if !ok {
		h.t.Fatalf("snapshot has no %s %s", path, query)
	}
	page, resp, err := call(h, found)
	if err != nil {
		h.t.Fatal(err)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: name, req: found.Req, resp: resp})
	}
	_ = page
}

func (h *live) one(path, query string) exchange.AccountSnapshot {
	h.t.Helper()
	calls, err := h.acct.Snapshot(time.Now())
	if err != nil {
		h.t.Fatal(err)
	}
	var out exchange.AccountSnapshot
	for _, c := range calls {
		if c.Req.Path != path || !strings.Contains(string(c.Req.Query), query) {
			continue
		}
		items, err := exchange.Collect(h.ctx, h.doer, time.Now, c)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, item := range items {
			out.OpenOrders = append(out.OpenOrders, item.OpenOrders...)
			out.Positions = append(out.Positions, item.Positions...)
			out.Balances = append(out.Balances, item.Balances...)
		}
		return out
	}
	h.t.Fatalf("snapshot has no %s %s", path, query)
	return out
}

func (h *live) size() float64 {
	var n float64
	for _, p := range h.one("/v5/position/list", "settleCoin=USDT").Positions {
		if p.Exch.ExchTicker == "BTCUSDT" && p.Exch.Category == symbol.Perp {
			n += p.Size
		}
	}
	return n
}

func (h *live) balances() []exchange.Balance {
	return h.one("/v5/account/wallet-balance", "accountType=UNIFIED").Balances
}

func (h *live) hasOrder(id string) bool {
	for _, o := range h.one("/v5/order/realtime", "settleCoin=USDT").OpenOrders {
		if o.OrderID == id && o.Exch.ExchTicker == "BTCUSDT" {
			return true
		}
	}
	return false
}

func (h *live) openBTC() int {
	n := 0
	for _, o := range h.one("/v5/order/realtime", "settleCoin=USDT").OpenOrders {
		if o.Exch.ExchTicker == "BTCUSDT" && o.Exch.Category == symbol.Perp {
			n++
		}
	}
	return n
}

func (h *live) until(d time.Duration, what string, ok func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(d)
	for {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out after %s: %s", d, what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (h *live) cancelOpen() {
	h.t.Helper()
	for _, o := range h.one("/v5/order/realtime", "settleCoin=USDT").OpenOrders {
		if o.Exch.ExchTicker != "BTCUSDT" || o.Exch.Category != symbol.Perp {
			continue
		}
		ref := exchange.CancelRef{Instrument: h.inst, OrderID: o.OrderID, ClientOrderID: o.ClientOrderID}
		if _, _, err := call(h, mustCall(h.acct.Cancel(time.Now(), ref))); err != nil {
			h.t.Fatalf("cancel open %s: %v", o.OrderID, err)
		}
	}
}

func (h *live) flatten(why string) {
	h.t.Helper()
	sz := h.size()
	if math.Abs(sz) < h.step {
		return
	}
	side := exchange.SideSell
	if sz < 0 {
		side = exchange.SideBuy
	}
	req := h.order(side, exchange.OrderTypeMarket, exchange.TIFUnspecified, decimal.SnapDown(math.Abs(sz), h.step), decimal.Absent(), true)
	res, _, err := call(h, mustCall(h.acct.Submit(time.Now(), req)))
	if err != nil {
		h.t.Fatalf("flatten %s: %v", why, err)
	}
	h.note("flatten %s order=%s status=%s", why, res.OrderID, statusName(res.Status))
}

func (h *live) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	old := h.ctx
	h.ctx = ctx
	defer func() { h.ctx = old }()
	for _, o := range h.one("/v5/order/realtime", "settleCoin=USDT").OpenOrders {
		if o.Exch.ExchTicker != "BTCUSDT" || o.Exch.Category != symbol.Perp {
			continue
		}
		ref := exchange.CancelRef{Instrument: h.inst, OrderID: o.OrderID}
		if _, _, err := call(h, mustCall(h.acct.Cancel(time.Now(), ref))); err != nil {
			h.t.Errorf("cleanup cancel %s: %v", o.OrderID, err)
		}
	}
	if sz := h.size(); math.Abs(sz) >= h.step {
		side := exchange.SideSell
		if sz < 0 {
			side = exchange.SideBuy
		}
		req := h.order(side, exchange.OrderTypeMarket, exchange.TIFUnspecified, decimal.SnapDown(math.Abs(sz), h.step), decimal.Absent(), true)
		if _, _, err := call(h, mustCall(h.acct.Submit(time.Now(), req))); err != nil {
			h.t.Errorf("cleanup flatten: %v", err)
		}
	}
	if h.conn != nil {
		_ = h.conn.Close()
	}
}

func (h *live) bid() float64 { return h.book("bid1Price") }
func (h *live) ask() float64 { return h.book("ask1Price") }

func (h *live) book(field string) float64 {
	h.t.Helper()
	body := publicGet(h.t, testREST+"/v5/market/tickers?category=linear&symbol=BTCUSDT")
	var resp struct {
		Code   int    `json:"retCode"`
		Msg    string `json:"retMsg"`
		Result struct {
			List []map[string]string `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		h.t.Fatal(err)
	}
	if resp.Code != 0 || len(resp.Result.List) == 0 {
		h.t.Fatalf("tickers: %d %s", resp.Code, resp.Msg)
	}
	v, err := strconv.ParseFloat(resp.Result.List[0][field], 64)
	if err != nil || !(v > 0) {
		h.t.Fatalf("ticker %s %q", field, resp.Result.List[0][field])
	}
	return v
}

func (h *live) writeFixtures() {
	h.t.Helper()
	dir := filepath.Join("testdata", "conformance", "Bybit")
	secrets := []string{h.creds.Key, h.creds.Secret}
	for _, s := range h.saved {
		path := filepath.Join(dir, "http", s.name+".json")
		writeExchange(h.t, path, &s.req, s.resp, secrets)
	}
	h.rawMu.Lock()
	frames := append([][]byte(nil), h.raw...)
	h.rawMu.Unlock()
	if len(frames) == 0 {
		h.t.Fatal("private session recorded no frames")
	}
	path := filepath.Join(dir, "ws", "private.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		h.t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		h.t.Fatal(err)
	}
	defer f.Close()
	w, err := conformance.NewWriter(f, conformance.Record{
		Venue: Name, Side: conformance.SidePriv, Note: "bybit testnet private session",
	}, time.Now, secrets...)
	if err != nil {
		h.t.Fatal(err)
	}
	for _, frame := range frames {
		if err := w.Frame(conformance.KindIn, frame); err != nil {
			h.t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		h.t.Fatal(err)
	}
	if strings.Contains(string(b), h.creds.Key) || strings.Contains(string(b), h.creds.Secret) {
		_ = os.Remove(path)
		h.t.Fatal("private fixture still contains a credential")
	}
}

func writeExchange(t *testing.T, path string, req *exchange.HTTPRequest, resp *exchange.HTTPResponse, secrets []string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	if err := conformance.SaveExchange(&buf, req, resp, secrets...); err != nil {
		t.Fatal(err)
	}
	for _, s := range secrets {
		if s != "" && strings.Contains(buf.String(), s) {
			t.Fatalf("%s still contains a credential", path)
		}
	}
	if err := os.WriteFile(path, []byte(buf.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustCall[T any](c exchange.Call[T], err error) exchange.Call[T] {
	if err != nil {
		panic(err)
	}
	return c
}

func d602(seq uint64) string {
	const ver = uint64(1)
	const algo = uint64(1)
	ts := uint64(time.Now().UnixMilli()) & ((1 << 40) - 1)
	id := (ver << 60) | (algo << 50) | (ts << 10) | (seq & 1023)
	return strconv.FormatUint(id, 10)
}

func statusName(s exchange.OrderStatus) string {
	switch s {
	case exchange.OrderStatusPendingNew:
		return "PENDING_NEW"
	case exchange.OrderStatusNew:
		return "NEW"
	case exchange.OrderStatusAccepted:
		return "ACCEPTED"
	case exchange.OrderStatusPartiallyFilled:
		return "PARTIALLY_FILLED"
	case exchange.OrderStatusFilled:
		return "FILLED"
	case exchange.OrderStatusCancelled:
		return "CANCELED"
	case exchange.OrderStatusRejected:
		return "REJECTED"
	default:
		return fmt.Sprintf("status(%d)", s)
	}
}

type feed struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ready  bool
	orders []exchange.OrderUpdate
	fills  []exchange.Fill
	dead   error
}

func newFeed() *feed {
	f := &feed{}
	f.cond = sync.NewCond(&f.mu)
	go func() {
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		for range tk.C {
			f.mu.Lock()
			f.cond.Broadcast()
			f.mu.Unlock()
		}
	}()
	return f
}

func (f *feed) Ready() { f.mu.Lock(); f.ready = true; f.cond.Broadcast(); f.mu.Unlock() }
func (f *feed) Expired(string) {
	f.die(errors.New("bybit: private stream expired"))
}
func (f *feed) Order(e *exchange.OrderUpdate) {
	o := *e
	o.OrderID = strings.Clone(o.OrderID)
	o.ClientOrderID = strings.Clone(o.ClientOrderID)
	o.RejectReason = strings.Clone(o.RejectReason)
	o.Exch.ExchTicker = strings.Clone(o.Exch.ExchTicker)
	f.mu.Lock()
	f.orders = append(f.orders, o)
	f.cond.Broadcast()
	f.mu.Unlock()
}
func (f *feed) Fill(e *exchange.Fill) {
	c := *e
	c.FillID = strings.Clone(c.FillID)
	c.OrderID = strings.Clone(c.OrderID)
	c.ClientOrderID = strings.Clone(c.ClientOrderID)
	c.FeeAsset = strings.Clone(c.FeeAsset)
	c.Liquidity = strings.Clone(c.Liquidity)
	c.Exch.ExchTicker = strings.Clone(c.Exch.ExchTicker)
	f.mu.Lock()
	f.fills = append(f.fills, c)
	f.cond.Broadcast()
	f.mu.Unlock()
}
func (f *feed) Balances([]exchange.Balance)   {}
func (f *feed) Positions([]exchange.Position) {}

func (f *feed) die(err error) {
	if err == nil {
		err = errors.New("bybit: private stream closed")
	}
	f.mu.Lock()
	if f.dead == nil {
		f.dead = err
	}
	f.cond.Broadcast()
	f.mu.Unlock()
}

func (f *feed) waitReady(d time.Duration) error { return f.wait(d, func() bool { return f.ready }) }

func (f *feed) waitOrder(cloid string, st exchange.OrderStatus, d time.Duration) (exchange.OrderUpdate, error) {
	var got exchange.OrderUpdate
	err := f.wait(d, func() bool {
		for _, o := range f.orders {
			if o.ClientOrderID == cloid && o.Status == st {
				got = o
				return true
			}
		}
		return false
	})
	return got, err
}

func (f *feed) waitFill(cloid string, d time.Duration) (exchange.Fill, error) {
	var got exchange.Fill
	err := f.wait(d, func() bool {
		for _, fill := range f.fills {
			if fill.ClientOrderID == cloid {
				got = fill
				return true
			}
		}
		return false
	})
	return got, err
}

func (f *feed) wait(d time.Duration, done func() bool) error {
	deadline := time.Now().Add(d)
	f.mu.Lock()
	defer f.mu.Unlock()
	for !done() {
		if f.dead != nil {
			return f.dead
		}
		if time.Now().After(deadline) {
			if f.dead != nil {
				return f.dead
			}
			return fmt.Errorf("timed out after %s", d)
		}
		f.cond.Wait()
	}
	return nil
}

func (f *feed) seen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var b strings.Builder
	for _, o := range f.orders {
		fmt.Fprintf(&b, "order %s %s %s\n", o.ClientOrderID, o.OrderID, statusName(o.Status))
	}
	for _, fill := range f.fills {
		fmt.Fprintf(&b, "fill %s %s\n", fill.ClientOrderID, fill.FillID)
	}
	return b.String()
}

func loadBybitCreds(t *testing.T) exchange.Credentials {
	t.Helper()
	path := os.Getenv("MFTIKO_BYBIT_KEYFILE")
	if path == "" {
		path = findUp(t, "bybit_testnet.json")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Key    string `json:"api_key"`
		Secret string `json:"api_secret"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if f.Key == "" || f.Secret == "" {
		t.Fatal("bybit_testnet.json has no api_key or api_secret")
	}
	return exchange.Credentials{Type: exchange.KeyHMAC, Key: f.Key, Secret: f.Secret}
}

func findUp(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("%s not found from %s", name, dir)
		}
		dir = parent
	}
}

func btcLinear(t *testing.T) (*symbol.Info, float64, float64, float64) {
	t.Helper()
	body := publicGet(t, testREST+"/v5/market/instruments-info?category=linear&symbol=BTCUSDT")
	var resp struct {
		Code   int    `json:"retCode"`
		Msg    string `json:"retMsg"`
		Result struct {
			List []struct {
				Symbol string `json:"symbol"`
				Price  struct {
					Tick string `json:"tickSize"`
				} `json:"priceFilter"`
				Lot struct {
					Step string `json:"qtyStep"`
					Min  string `json:"minOrderQty"`
				} `json:"lotSizeFilter"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != 0 || len(resp.Result.List) != 1 || resp.Result.List[0].Symbol != "BTCUSDT" {
		t.Fatalf("instruments-info: %d %s", resp.Code, resp.Msg)
	}
	row := resp.Result.List[0]
	tick := mustFloat(t, row.Price.Tick)
	step := mustFloat(t, row.Lot.Step)
	qty := mustFloat(t, row.Lot.Min)
	inst := &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: Name, Category: symbol.Perp},
		ExchTicker: "BTCUSDT",
		Filters:    symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step},
	}
	return inst, tick, step, qty
}

func publicGet(t *testing.T, url string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", url, resp.Status)
	}
	return b
}

func mustFloat(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || !(v > 0) {
		t.Fatalf("bad number %q", s)
	}
	return v
}
