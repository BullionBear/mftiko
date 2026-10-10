package binance

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
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

// TestBinanceUMTestnet places orders on the USD-M testnet through the adapter and checks the
// account afterwards. It does not run in CI.
//
//	MFTIKO_BINANCE_UM_LIVE=1 go test ./exchange/binance/ -run TestBinanceUMTestnet -v -count=1
func TestBinanceUMTestnet(t *testing.T) {
	if os.Getenv("MFTIKO_BINANCE_UM_LIVE") != "1" {
		t.Skip("set MFTIKO_BINANCE_UM_LIVE=1 to place orders on the USD-M testnet")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	creds := loadUMCreds(t)
	acct, err := accountFactory(kindUM)(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: "https://" + testHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := asAccount(t, acct)
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 15 * time.Second})
	inst, tick, step, qty := btcFilters(t)

	h := &live{
		t: t, ctx: ctx, acct: raw, doer: doer, inst: inst, tick: tick, step: step, qty: qty,
		feed: newFeed(),
	}
	t.Cleanup(func() { h.cleanup() })
	defer func() {
		fmt.Println("--- binance um testnet ---")
		for _, line := range h.lines {
			fmt.Println(line)
		}
	}()

	h.note("book bid=%s ask=%s tick=%s step=%s qty=%s",
		decimal.Format(h.bid(), tick), decimal.Format(h.ask(), tick),
		decimal.Format(tick, tick), decimal.Format(step, step), decimal.Format(qty, step))

	h.cancelAll()
	h.flatten("setup")
	h.note("start %s", h.fmtSnap(h.snap()))
	if open := h.openBTC(); open != 0 {
		t.Fatalf("setup left %d open BTCUSDT orders", open)
	}
	if sz := h.size(); math.Abs(sz) >= step {
		t.Fatalf("setup left BTCUSDT size %s", decimal.Format(sz, step))
	}
	if lev, err := exchange.Run(ctx, doer, mustCall(raw.Leverage(time.Now(), inst))); err != nil {
		t.Fatal(err)
	} else {
		h.note("leverage BTCUSDT=%s", decimal.Format(lev, 1))
	}

	h.openSockets()

	// Resting GTC, under the bid so it does not cross. Margin locks, the position does not.
	before := h.snap()
	restPx := decimal.SnapDown(h.bid()*0.996, tick)
	if !(restPx < h.bid()) {
		t.Fatalf("resting price %s is not below the bid", decimal.Format(restPx, tick))
	}
	rest := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, restPx, false)
	res := h.place(rest, exchange.OrderStatusNew)
	mid := h.snap()
	h.note("GTC rest px=%s order=%s cloid=%s NEW %s", decimal.Format(restPx, tick), res.OrderID, rest.ClientOrderID, h.fmtSnap(mid))
	if free, _, ok := usdt(mid); !ok || free > usdtFree(before)-0.5 {
		t.Fatalf("available did not fall while the order rested: before %s mid %s", h.fmtSnap(before), h.fmtSnap(mid))
	}
	if !decimal.Equal(h.sizeOf(mid), h.sizeOf(before), step) {
		t.Fatalf("position changed on a resting order: %s -> %s", h.fmtSnap(before), h.fmtSnap(mid))
	}
	h.cancel(rest.ClientOrderID)
	after := h.snap()
	h.note("GTC rest cancel %s", h.fmtSnap(after))
	if free, _, ok := usdt(after); !ok || free < usdtFree(mid)+0.5 {
		t.Fatalf("available did not return after cancel: resting %s after %s", h.fmtSnap(mid), h.fmtSnap(after))
	}

	// Crossing limits and a market buy. Each one that fills grows the position by qty.
	want := 0.0
	cross := func(name string, typ exchange.OrderType, tif exchange.TimeInForce, wantSt exchange.OrderStatus) {
		t.Helper()
		px := decimal.Absent()
		if typ == exchange.OrderTypeLimit {
			px = decimal.SnapUp(h.ask()*1.002, tick)
			if px < h.ask() {
				t.Fatalf("%s price %s does not reach the ask", name, decimal.Format(px, tick))
			}
		}
		req := h.order(exchange.SideBuy, typ, tif, qty, px, false)
		res := h.place(req, wantSt)
		if wantSt == exchange.OrderStatusFilled {
			want += qty
		}
		snap := h.snap()
		h.note("%s px=%s order=%s status=%s %s", name, pxText(px, tick), res.OrderID, statusName(res.Status), h.fmtSnap(snap))
		if !decimal.Equal(h.sizeOf(snap), want, step) {
			t.Fatalf("%s position %s, want %s", name, decimal.Format(h.sizeOf(snap), step), decimal.Format(want, step))
		}
	}
	cross("GTC overlap", exchange.OrderTypeLimit, exchange.TIFGTC, exchange.OrderStatusFilled)
	cross("IOC overlap", exchange.OrderTypeLimit, exchange.TIFIOC, exchange.OrderStatusFilled)
	away := func(name string, tif exchange.TimeInForce) {
		t.Helper()
		px := decimal.SnapDown(h.bid()*0.996, tick)
		req := h.order(exchange.SideBuy, exchange.OrderTypeLimit, tif, qty, px, false)
		res := h.place(req, exchange.OrderStatusExpired)
		snap := h.snap()
		h.note("%s px=%s order=%s status=%s %s", name, decimal.Format(px, tick), res.OrderID, statusName(res.Status), h.fmtSnap(snap))
		if !decimal.Equal(h.sizeOf(snap), want, step) {
			t.Fatalf("%s position %s, want %s", name, decimal.Format(h.sizeOf(snap), step), decimal.Format(want, step))
		}
	}
	away("IOC away", exchange.TIFIOC)
	cross("FOK overlap", exchange.OrderTypeLimit, exchange.TIFFOK, exchange.OrderStatusFilled)
	// A FOK that does not cross is rejected with -5021. The venue stores no order.
	fokPx := decimal.SnapDown(h.bid()*0.996, h.tick)
	fok := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFFOK, qty, fokPx, false)
	held := h.snap()
	rej := h.placeReject(fok)
	afterFOK := h.snap()
	h.note("FOK away px=%s reject %d %s %s", decimal.Format(fokPx, h.tick), rej.Code, rej.Native, h.fmtSnap(afterFOK))
	if rej.Native != "-5021" {
		t.Fatalf("FOK away native %s, want -5021 (%s)", rej.Native, rej.Reason)
	}
	if !decimal.Equal(h.sizeOf(afterFOK), want, step) || h.openBTC() != 0 {
		t.Fatalf("FOK away changed the account: %s", h.fmtSnap(afterFOK))
	}
	if free, _, ok := usdt(afterFOK); !ok || math.Abs(free-usdtFree(held)) > 1 {
		t.Fatalf("FOK away moved the balance: before %s after %s", h.fmtSnap(held), h.fmtSnap(afterFOK))
	}
	cross("market", exchange.OrderTypeMarket, exchange.TIFUnspecified, exchange.OrderStatusFilled)

	flat := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, decimal.SnapDown(h.size(), step), decimal.Absent(), true)
	res = h.place(flat, exchange.OrderStatusFilled)
	want = 0
	snap := h.snap()
	h.note("market reduce-only order=%s %s", res.OrderID, h.fmtSnap(snap))
	if math.Abs(h.sizeOf(snap)) >= step {
		t.Fatalf("flatten left size %s", decimal.Format(h.sizeOf(snap), step))
	}

	// Two resting orders, one batch cancel, then the recon snapshot.
	px := decimal.SnapDown(h.bid()*0.996, tick)
	a := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, px, false)
	b := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, px, false)
	ra := h.place(a, exchange.OrderStatusNew)
	rb := h.place(b, exchange.OrderStatusNew)
	errs, err := exchange.Run(ctx, doer, mustCall(raw.CancelBatch(time.Now(), []exchange.CancelRef{
		{Instrument: inst, ClientOrderID: a.ClientOrderID},
		{Instrument: inst, ClientOrderID: b.ClientOrderID},
	})))
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("batch item %d: %v", i, e)
		}
	}
	if _, err := h.feed.waitOrder(a.ClientOrderID, exchange.OrderStatusCancelled, 20*time.Second); err != nil {
		t.Fatalf("stream cancel %s: %v", a.ClientOrderID, err)
	}
	if _, err := h.feed.waitOrder(b.ClientOrderID, exchange.OrderStatusCancelled, 20*time.Second); err != nil {
		t.Fatalf("stream cancel %s: %v", b.ClientOrderID, err)
	}
	final := h.snap()
	h.note("batch cancel %s %s open=%d %s", ra.OrderID, rb.OrderID, h.openBTC(), h.fmtSnap(final))
	if h.openBTC() != 0 {
		t.Fatalf("batch cancel left %d open orders", h.openBTC())
	}
	if math.Abs(h.sizeOf(final)) >= step {
		t.Fatalf("final size %s", decimal.Format(h.sizeOf(final), step))
	}
	if h.feed.fillsN() < 1 {
		t.Fatal("the private stream saw no fill")
	}
}

func (h *live) openSockets() {
	h.t.Helper()
	ts, err := h.acct.NewTradeStream()
	if err != nil {
		h.t.Fatal(err)
	}
	h.ts = ts
	dial, err := ts.Dial(time.Now())
	if err != nil {
		h.t.Fatal(err)
	}
	lim := ts.Limits()
	conn, err := ws.Dial(h.ctx, dial.URL, ws.Options{ReadLimit: lim.ReadLimit, Keepalive: lim.Keepalive, MaxLifetime: lim.MaxLifetime})
	if err != nil {
		h.t.Fatal(err)
	}
	h.trade = conn
	go func() {
		err := conn.Run(h.ctx, func(frame []byte) error {
			buf := append([]byte(nil), frame...)
			return ts.Decode(buf, h.feed)
		})
		h.feed.die(err)
	}()
	for _, f := range dial.Frames {
		if err := conn.Write(h.ctx, f); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := h.feed.waitReady(15 * time.Second); err != nil {
		h.t.Fatal(err)
	}

	ps, err := h.acct.NewStream()
	if err != nil {
		h.t.Fatal(err)
	}
	h.priv = ps.(*privateStream)
	prep, err := h.priv.Prepare(time.Now())
	if err != nil {
		h.t.Fatal(err)
	}
	key, err := exchange.Run(h.ctx, h.doer, *prep)
	if err != nil {
		h.t.Fatal(err)
	}
	h.listenKey = key
	pd, err := h.priv.Dial(time.Now(), key)
	if err != nil {
		h.t.Fatal(err)
	}
	sl := h.priv.Limits()
	pconn, err := ws.Dial(h.ctx, pd.URL, ws.Options{ReadLimit: sl.ReadLimit, Keepalive: sl.Keepalive, MaxLifetime: sl.MaxLifetime})
	if err != nil {
		h.t.Fatal(err)
	}
	h.pconn = pconn
	go func() {
		err := pconn.Run(h.ctx, func(frame []byte) error {
			buf := append([]byte(nil), frame...)
			h.rawMu.Lock()
			h.raw = append(h.raw, buf)
			h.rawMu.Unlock()
			return h.priv.Decode(buf, h.feed)
		})
		h.feed.die(err)
	}()
}

func (h *live) place(req *exchange.OrderRequest, want exchange.OrderStatus) *exchange.OrderResult {
	h.t.Helper()
	h.next++
	frame, err := h.ts.Submit(time.Now(), h.next, req)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.trade.Write(h.ctx, frame); err != nil {
		h.t.Fatal(err)
	}
	ans, err := h.feed.waitAns(h.next, 20*time.Second)
	if err != nil {
		h.t.Fatal(err)
	}
	if ans.err != nil {
		h.t.Fatalf("%s: %v", req.ClientOrderID, ans.err)
	}
	if ans.res == nil || ans.res.Status != want {
		got := exchange.OrderStatusUnknown
		id := ""
		if ans.res != nil {
			got, id = ans.res.Status, ans.res.OrderID
		}
		h.t.Fatalf("%s: status %s, want %s (order %s)", req.ClientOrderID, statusName(got), statusName(want), id)
	}
	if _, err := h.feed.waitOrder(req.ClientOrderID, want, 20*time.Second); err != nil {
		h.t.Fatalf("stream %s %s: %v\n%s", req.ClientOrderID, statusName(want), err, h.feed.seen())
	}
	if want == exchange.OrderStatusFilled {
		fill, err := h.feed.waitFill(req.ClientOrderID, 20*time.Second)
		if err != nil {
			h.t.Fatalf("stream fill %s: %v", req.ClientOrderID, err)
		}
		if fill.OrderID != ans.res.OrderID || fill.Qty <= 0 {
			h.t.Fatalf("fill %+v does not match order %s", fill, ans.res.OrderID)
		}
	}
	return ans.res
}

func (h *live) placeReject(req *exchange.OrderRequest) *exchange.Reject {
	h.t.Helper()
	h.next++
	frame, err := h.ts.Submit(time.Now(), h.next, req)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.trade.Write(h.ctx, frame); err != nil {
		h.t.Fatal(err)
	}
	ans, err := h.feed.waitAns(h.next, 20*time.Second)
	if err != nil {
		h.t.Fatal(err)
	}
	var rej *exchange.Reject
	if ans.err == nil || !errors.As(ans.err, &rej) {
		h.t.Fatalf("%s: want a reject, got %v", req.ClientOrderID, ans.err)
	}
	return rej
}

func (h *live) cancel(cloid string) {
	h.t.Helper()
	h.next++
	frame, err := h.ts.Cancel(time.Now(), h.next, exchange.CancelRef{Instrument: h.inst, ClientOrderID: cloid})
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.trade.Write(h.ctx, frame); err != nil {
		h.t.Fatal(err)
	}
	ans, err := h.feed.waitAns(h.next, 20*time.Second)
	if err != nil {
		h.t.Fatal(err)
	}
	if ans.err != nil {
		h.t.Fatalf("cancel %s: %v", cloid, ans.err)
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

func (h *live) snap() exchange.AccountSnapshot {
	h.t.Helper()
	calls, err := h.acct.Snapshot(time.Now())
	if err != nil {
		h.t.Fatal(err)
	}
	var out exchange.AccountSnapshot
	for _, c := range calls {
		page, err := exchange.Run(h.ctx, h.doer, c)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, item := range page.Items {
			out.OpenOrders = append(out.OpenOrders, item.OpenOrders...)
			out.Positions = append(out.Positions, item.Positions...)
			out.Balances = append(out.Balances, item.Balances...)
		}
	}
	return out
}

func (h *live) size() float64 { return h.sizeOf(h.snap()) }

func (h *live) sizeOf(s exchange.AccountSnapshot) float64 {
	var n float64
	for _, p := range s.Positions {
		if p.Exch.ExchTicker == "BTCUSDT" && p.Exch.Category == symbol.Perp {
			n += p.Size
		}
	}
	return n
}

func (h *live) openBTC() int {
	n := 0
	for _, o := range h.snap().OpenOrders {
		if o.Exch.ExchTicker == "BTCUSDT" {
			n++
		}
	}
	return n
}

func (h *live) cancelAll() {
	h.t.Helper()
	if _, err := exchange.Run(h.ctx, h.doer, mustCall(h.acct.cancelAll(time.Now(), "BTCUSDT"))); err != nil {
		h.t.Fatal(err)
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
	q := decimal.SnapDown(math.Abs(sz), h.step)
	req := h.order(side, exchange.OrderTypeMarket, exchange.TIFUnspecified, q, decimal.Absent(), true)
	call, err := h.acct.Submit(time.Now(), req)
	if err != nil {
		h.t.Fatal(err)
	}
	res, err := exchange.Run(h.ctx, h.doer, call)
	if err != nil {
		h.t.Fatalf("flatten %s: %v", why, err)
	}
	h.note("flatten %s order=%s status=%s", why, res.OrderID, statusName(res.Status))
}

func (h *live) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := exchange.Run(ctx, h.doer, mustCall(h.acct.cancelAll(time.Now(), "BTCUSDT"))); err != nil {
		h.t.Errorf("cleanup cancel: %v", err)
	}
	if sz := h.size(); math.Abs(sz) >= h.step {
		side := exchange.SideSell
		if sz < 0 {
			side = exchange.SideBuy
		}
		req := h.order(side, exchange.OrderTypeMarket, exchange.TIFUnspecified, decimal.SnapDown(math.Abs(sz), h.step), decimal.Absent(), true)
		call, err := h.acct.Submit(time.Now(), req)
		if err != nil {
			h.t.Errorf("cleanup flatten: %v", err)
		} else if _, err := exchange.Run(ctx, h.doer, call); err != nil {
			h.t.Errorf("cleanup flatten: %v", err)
		}
	}
	if h.priv != nil && h.listenKey != "" {
		if _, err := exchange.Run(ctx, h.doer, h.priv.release()); err != nil {
			h.t.Errorf("cleanup listenKey: %v", err)
		}
	}
	if h.trade != nil {
		_ = h.trade.Write(ctx, []byte(`{"id":"logout","method":"session.logout","params":{}}`))
		_ = h.trade.Close()
	}
	if h.pconn != nil {
		_ = h.pconn.Close()
	}
}

func (h *live) bid() float64 { return h.book(true) }
func (h *live) ask() float64 { return h.book(false) }

func (h *live) book(bid bool) float64 {
	h.t.Helper()
	var q struct {
		Bid string `json:"bidPrice"`
		Ask string `json:"askPrice"`
	}
	getJSON(h.t, "https://"+testHost+"/fapi/v1/ticker/bookTicker?symbol=BTCUSDT", &q)
	s := q.Ask
	if bid {
		s = q.Bid
	}
	v, err := decimal.Parse(s)
	if err != nil || !(v > 0) {
		h.t.Fatalf("book %q: %v", s, err)
	}
	return v
}

func (h *live) note(format string, args ...any) {
	h.lines = append(h.lines, fmt.Sprintf(format, args...))
}

func (h *live) fmtSnap(s exchange.AccountSnapshot) string {
	var b strings.Builder
	free, locked, ok := usdt(s)
	if ok {
		fmt.Fprintf(&b, "USDT free=%.4f locked=%.4f", free, locked)
	}
	fmt.Fprintf(&b, " BTCUSDT size=%s", decimal.Format(h.sizeOf(s), h.step))
	n := 0
	for _, o := range s.OpenOrders {
		if o.Exch.ExchTicker == "BTCUSDT" {
			n++
		}
	}
	fmt.Fprintf(&b, " open=%d", n)
	return b.String()
}

type live struct {
	t         *testing.T
	ctx       context.Context
	acct      *account
	doer      exchange.Doer
	ts        exchange.TradeStream
	trade     *ws.Conn
	priv      *privateStream
	pconn     *ws.Conn
	listenKey string
	inst      *symbol.Info
	tick      float64
	step      float64
	qty       float64
	next      uint64
	seq       uint64
	feed      *feed
	lines     []string
	rawMu     sync.Mutex
	raw       [][]byte // private frames, copied
}

func asAccount(t *testing.T, a exchange.Account) *account {
	t.Helper()
	switch v := a.(type) {
	case *umAccount:
		return v.account
	case *account:
		return v
	default:
		t.Fatalf("account type %T", a)
		return nil
	}
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
	case exchange.OrderStatusNew:
		return "NEW"
	case exchange.OrderStatusPartiallyFilled:
		return "PARTIALLY_FILLED"
	case exchange.OrderStatusFilled:
		return "FILLED"
	case exchange.OrderStatusCancelled:
		return "CANCELED"
	case exchange.OrderStatusExpired:
		return "EXPIRED"
	case exchange.OrderStatusRejected:
		return "REJECTED"
	default:
		return fmt.Sprintf("status(%d)", s)
	}
}

func pxText(px, tick float64) string {
	if !decimal.Has(px) {
		return "market"
	}
	return decimal.Format(px, tick)
}

func usdt(s exchange.AccountSnapshot) (free, locked float64, ok bool) {
	for _, b := range s.Balances {
		if b.Asset == "USDT" {
			return b.Free, b.Locked, true
		}
	}
	return 0, 0, false
}

func usdtFree(s exchange.AccountSnapshot) float64 {
	f, _, _ := usdt(s)
	return f
}

func mustCall[T any](c exchange.Call[T], err error) exchange.Call[T] {
	if err != nil {
		panic(err)
	}
	return c
}

type tradeAns struct {
	id  uint64
	res *exchange.OrderResult
	err error
}

type feed struct {
	mu      sync.Mutex
	cond    *sync.Cond
	ready   bool
	orders  []exchange.OrderUpdate
	fills   []exchange.Fill
	answers []tradeAns
	dead    error
}

func newFeed() *feed {
	f := &feed{}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *feed) die(err error) {
	f.mu.Lock()
	f.dead = err
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *feed) Ready() {
	f.mu.Lock()
	f.ready = true
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *feed) Expired(string) {}

func (f *feed) Order(e *exchange.OrderUpdate) {
	u := *e
	u.OrderID = strings.Clone(e.OrderID)
	u.ClientOrderID = strings.Clone(e.ClientOrderID)
	u.RejectReason = strings.Clone(e.RejectReason)
	u.Exch.ExchTicker = strings.Clone(e.Exch.ExchTicker)
	f.mu.Lock()
	f.orders = append(f.orders, u)
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *feed) Fill(e *exchange.Fill) {
	c := *e
	c.FillID = strings.Clone(e.FillID)
	c.OrderID = strings.Clone(e.OrderID)
	c.ClientOrderID = strings.Clone(e.ClientOrderID)
	c.FeeAsset = strings.Clone(e.FeeAsset)
	c.Liquidity = strings.Clone(e.Liquidity)
	c.Exch.ExchTicker = strings.Clone(e.Exch.ExchTicker)
	f.mu.Lock()
	f.fills = append(f.fills, c)
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *feed) Balances([]exchange.Balance)   {}
func (f *feed) Positions([]exchange.Position) {}

func (f *feed) Submitted(id uint64, res *exchange.OrderResult, err error) {
	if res != nil {
		cp := *res
		cp.OrderID = strings.Clone(res.OrderID)
		res = &cp
	}
	f.mu.Lock()
	f.answers = append(f.answers, tradeAns{id: id, res: res, err: err})
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *feed) Canceled(id uint64, err error) {
	f.mu.Lock()
	f.answers = append(f.answers, tradeAns{id: id, err: err})
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *feed) Used([]exchange.Used) {}

func (f *feed) waitReady(d time.Duration) error {
	return f.wait(d, func() bool { return f.ready })
}

func (f *feed) waitAns(id uint64, d time.Duration) (tradeAns, error) {
	var out tradeAns
	err := f.wait(d, func() bool {
		for _, a := range f.answers {
			if a.id == id {
				out = a
				return true
			}
		}
		return false
	})
	return out, err
}

func (f *feed) waitOrder(cloid string, st exchange.OrderStatus, d time.Duration) (exchange.OrderUpdate, error) {
	var out exchange.OrderUpdate
	err := f.wait(d, func() bool {
		for _, u := range f.orders {
			if u.ClientOrderID == cloid && u.Status == st {
				out = u
				return true
			}
		}
		return false
	})
	return out, err
}

func (f *feed) waitFill(cloid string, d time.Duration) (exchange.Fill, error) {
	var out exchange.Fill
	err := f.wait(d, func() bool {
		for _, fill := range f.fills {
			if fill.ClientOrderID == cloid {
				out = fill
				return true
			}
		}
		return false
	})
	return out, err
}

func (f *feed) fillsN() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.fills)
}

func (f *feed) fillsOf(cloid string) []exchange.Fill {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []exchange.Fill
	for _, fill := range f.fills {
		if fill.ClientOrderID == cloid {
			out = append(out, fill)
		}
	}
	return out
}

func (f *feed) seen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var b strings.Builder
	for _, u := range f.orders {
		fmt.Fprintf(&b, "  order %s %s %s\n", u.ClientOrderID, u.OrderID, statusName(u.Status))
	}
	return b.String()
}

func (f *feed) wait(d time.Duration, done func() bool) error {
	deadline := time.Now().Add(d)
	timer := time.AfterFunc(d, func() { f.cond.Broadcast() })
	defer timer.Stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	for {
		if done() {
			return nil
		}
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
}

func loadUMCreds(t *testing.T) exchange.Credentials {
	t.Helper()
	path := os.Getenv("MFTIKO_BINANCE_UM_KEYFILE")
	if path == "" {
		path = findUp(t, "binanceum_testnet.json")
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
	secret := f.Secret
	if !strings.Contains(secret, "BEGIN") {
		p := secret
		if !filepath.IsAbs(p) {
			p = filepath.Join(filepath.Dir(path), p)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read signing key file: %v", err)
		}
		secret = string(raw)
	}
	return exchange.Credentials{Type: exchange.KeyEd25519, Key: f.Key, Secret: secret}
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

func btcFilters(t *testing.T) (*symbol.Info, float64, float64, float64) {
	t.Helper()
	var info struct {
		Symbols []struct {
			Symbol       string `json:"symbol"`
			Status       string `json:"status"`
			ContractType string `json:"contractType"`
			Filters      []struct {
				Type     string `json:"filterType"`
				Tick     string `json:"tickSize"`
				Step     string `json:"stepSize"`
				MinQty   string `json:"minQty"`
				Notional string `json:"notional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	getJSON(t, "https://"+testHost+"/fapi/v1/exchangeInfo?symbol=BTCUSDT", &info)
	if len(info.Symbols) == 0 || info.Symbols[0].Symbol != "BTCUSDT" {
		t.Fatal("exchangeInfo has no BTCUSDT")
	}
	s := info.Symbols[0]
	if s.Status != "TRADING" || s.ContractType != "PERPETUAL" {
		t.Fatalf("BTCUSDT %s %s", s.Status, s.ContractType)
	}
	var tick, step, minQty, minNotional float64
	for _, f := range s.Filters {
		switch f.Type {
		case "PRICE_FILTER":
			tick = mustNum(t, f.Tick)
		case "LOT_SIZE", "MARKET_LOT_SIZE":
			st, mq := mustNum(t, f.Step), mustNum(t, f.MinQty)
			if st > step {
				step = st
			}
			if mq > minQty {
				minQty = mq
			}
		case "MIN_NOTIONAL", "NOTIONAL":
			if n := mustNum(t, f.Notional); n > minNotional {
				minNotional = n
			}
		}
	}
	if !(tick > 0) || !(step > 0) {
		t.Fatalf("tick %v step %v", tick, step)
	}
	var book struct {
		Ask string `json:"askPrice"`
	}
	getJSON(t, "https://"+testHost+"/fapi/v1/ticker/bookTicker?symbol=BTCUSDT", &book)
	ask := mustNum(t, book.Ask)
	if !(minNotional > 0) {
		minNotional = 100
	}
	qty := minNotional / ask
	if qty < minQty {
		qty = minQty
	}
	qty = decimal.SnapUp(qty, step)
	if qty*ask < minNotional {
		qty = decimal.SnapUp(qty+step, step)
	}
	inst := &symbol.Info{
		ExchTicker: "BTCUSDT",
		Ticker:     symbol.UniversalTicker{Venue: VenueUM, Category: symbol.Perp, Symbol: "BTCUSDT"},
		Filters:    symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step},
	}
	return inst, tick, step, qty
}

func mustNum(t *testing.T, s string) float64 {
	t.Helper()
	if s == "" {
		return 0
	}
	v, err := decimal.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func getJSON(t *testing.T, rawURL string, dest any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s", rawURL, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		t.Fatal(err)
	}
}
