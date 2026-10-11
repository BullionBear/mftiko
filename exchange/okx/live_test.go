package okx

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

// TestOkxDemo places orders on OKX demo trading through the adapter and checks the account
// afterwards. It does not run in CI.
//
//	MFTIKO_OKX_TRADE=1 go test ./exchange/okx/ -run TestOkxDemo -v -count=1
//
// MFTIKO_OKX_RECORD=1 also writes the responses and the private session into testdata, with the
// key, the secret and the passphrase redacted. The account is spot: orders are BTC-USDT, and a
// buy is sold back down to the BTC balance from before the test.
func TestOkxDemo(t *testing.T) {
	if os.Getenv("MFTIKO_OKX_TRADE") != "1" {
		t.Skip("set MFTIKO_OKX_TRADE=1 to place orders on OKX demo trading")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	creds := loadOkxCreds(t)
	acct, err := Declare().AccountKinds[0].Account(exchange.AccountConfig{
		APIID: 1, Credentials: creds, Simulated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := acct.(*account)
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 20 * time.Second})
	inst, tick, step, minSz := btcSpot(t)
	qty := decimal.SnapDown(math.Max(minSz, 0.0001), step)
	if qty < minSz {
		qty = minSz
	}

	h := &live{
		t: t, ctx: ctx, acct: raw, doer: doer, inst: inst,
		tick: tick, step: step, minSz: minSz, qty: qty,
		creds: creds, feed: newFeed(ctx), record: os.Getenv("MFTIKO_OKX_RECORD") == "1",
	}
	var passed bool
	t.Cleanup(func() {
		h.cleanup()
		if passed && h.record {
			h.writeFixtures()
		}
	})
	defer func() {
		fmt.Println("--- okx demo ---")
		for _, line := range h.lines {
			fmt.Println(line)
		}
	}()

	h.disarm()
	h.cancelOpen()
	h.baseBTC = h.btcFree()
	h.baseSet = true
	h.note("book bid=%s ask=%s tick=%s step=%s qty=%s btc=%s",
		decimal.Format(h.bid(), tick), decimal.Format(h.ask(), tick),
		decimal.Format(tick, tick), decimal.Format(step, step), decimal.Format(qty, step),
		decimal.Format(h.baseBTC, step))

	levCall := mustCall(raw.Leverage(time.Now(), swapInst()))
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
	h.note("leverage BTC-USDT-SWAP=%s", decimal.Format(lev, 1))

	h.openSocket()

	restPx := h.restPx()
	rest := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, restPx)
	res := h.place("submit_limit", rest, exchange.OrderStatusAccepted)
	h.until(10*time.Second, "snapshot shows the resting order", func() bool { return h.hasOrder(res.OrderID) })
	h.keepSnap("open_orders", "/api/v5/trade/orders-pending")
	h.note("GTC rest px=%s order=%s cloid=%s", decimal.Format(restPx, tick), res.OrderID, rest.ClientOrderID)
	h.cancel("cancel", rest.ClientOrderID)
	h.until(10*time.Second, "snapshot drops the cancelled order", func() bool { return !h.hasOrder(res.OrderID) })

	po := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFPostOnly, qty, restPx)
	poRes := h.place("submit_postonly", po, exchange.OrderStatusAccepted)
	h.cancel("", po.ClientOrderID)
	h.note("post-only px=%s order=%s", decimal.Format(restPx, tick), poRes.OrderID)

	mkt := h.order(exchange.SideBuy, exchange.OrderTypeMarket, exchange.TIFUnspecified, qty, decimal.Absent())
	mktRes := h.place("submit_market", mkt, exchange.OrderStatusFilled)
	fill, err := h.feed.waitFill(mkt.ClientOrderID, 20*time.Second)
	if err != nil {
		t.Fatalf("stream fill %s: %v\n%s", mkt.ClientOrderID, err, h.feed.seen())
	}
	if fill.OrderID != mktRes.OrderID || fill.Qty <= 0 || fill.FillID == "" {
		t.Fatalf("fill %+v does not match order %s", fill, mktRes.OrderID)
	}
	h.note("market order=%s fill=%s qty=%s fee=%s %s liq=%s",
		mktRes.OrderID, fill.FillID, decimal.Format(fill.Qty, step), decimal.Format(fill.Fee, 1e-8), fill.FeeAsset, fill.Liquidity)
	h.flatten("market")

	quote := h.quoteBuy(10)
	qRes := h.place("submit_quote", quote, exchange.OrderStatusFilled)
	h.note("quote order=%s cloid=%s", qRes.OrderID, quote.ClientOrderID)
	h.flatten("quote")

	a := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, restPx)
	b := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, restPx)
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
	h.until(10*time.Second, "batch cancel clears BTC-USDT", func() bool { return h.openBTC() == 0 })
	h.note("batch cancel %s %s", ra.OrderID, rb.OrderID)

	huge := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, 10, restPx)
	rej := h.placeReject("submit_reject", huge)
	if rej.Code != 201 || rej.Native != "51008" {
		t.Fatalf("reject code=%d native=%s reason=%s", rej.Code, rej.Native, rej.Reason)
	}
	h.note("reject code=%d native=%s reason=%s", rej.Code, rej.Native, rej.Reason)

	h.recordAuth()
	h.armAndRate()

	h.keepSnap("balances", "/api/v5/account/balance")
	h.keepSnap("positions", "/api/v5/account/positions")
	if len(h.balances()) == 0 {
		t.Fatal("balance snapshot is empty")
	}
	h.note("balances %d coins, extra btc=%s", len(h.balances()), decimal.Format(h.extra(), step))

	time.Sleep(500 * time.Millisecond)
	passed = true
}

type live struct {
	t       *testing.T
	ctx     context.Context
	acct    *account
	doer    exchange.Doer
	inst    *symbol.Info
	tick    float64
	step    float64
	minSz   float64
	qty     float64
	creds   exchange.Credentials
	feed    *feed
	conn    *ws.Conn
	seq     uint64
	lines   []string
	record  bool
	saved   []saved
	baseBTC float64
	baseSet bool
	loginMu sync.Mutex
	logged  bool
	rawMu   sync.Mutex
	raw     [][]byte
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
		Header: http.Header{"User-Agent": []string{userAgent}},
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
			if loginAccepted(frame) {
				h.loginMu.Lock()
				h.logged = true
				h.loginMu.Unlock()
			}
			if err := ps.Decode(frame, h.feed); err != nil {
				return fmt.Errorf("%w (frame %.240q)", err, h.redact(string(frame)))
			}
			return nil
		})
		h.feed.die(err)
	}()
	// The worker sends each dial frame only after the previous one is answered.
	if err := conn.Write(h.ctx, d.Frames[0]); err != nil {
		h.t.Fatal(err)
	}
	h.waitLogin()
	if err := conn.Write(h.ctx, d.Frames[1]); err != nil {
		h.t.Fatal(err)
	}
	if err := h.feed.waitReady(15 * time.Second); err != nil {
		h.t.Fatalf("private stream: %v\n%s", err, h.recent())
	}
	h.note("private stream ready %s", d.URL)
}

func (h *live) waitLogin() {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		h.loginMu.Lock()
		ok := h.logged
		h.loginMu.Unlock()
		if ok {
			return
		}
		if err := h.feed.deadErr(); err != nil {
			h.t.Fatalf("login: %v\n%s", err, h.recent())
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("login timed out\n%s", h.recent())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func loginAccepted(frame []byte) bool {
	var head struct {
		Event string `json:"event"`
		Code  string `json:"code"`
	}
	if json.Unmarshal(frame, &head) != nil {
		return false
	}
	return head.Event == "login" && head.Code == "0"
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
	if fixture != "" && h.record {
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
	if fixture != "" && h.record {
		h.saved = append(h.saved, saved{name: fixture, req: c.Req, resp: resp})
	}
	return rej
}

func (h *live) cancel(fixture, cloid string) {
	h.t.Helper()
	c := mustCall(h.acct.Cancel(time.Now(), exchange.CancelRef{Instrument: h.inst, ClientOrderID: cloid}))
	_, resp, err := call(h, c)
	if err != nil {
		h.t.Fatalf("cancel %s: %v", cloid, err)
	}
	if fixture != "" && h.record {
		h.saved = append(h.saved, saved{name: fixture, req: c.Req, resp: resp})
	}
	if _, err := h.feed.waitOrder(cloid, exchange.OrderStatusCancelled, 20*time.Second); err != nil {
		h.t.Fatalf("stream cancel %s: %v\n%s", cloid, err, h.feed.seen())
	}
}

func (h *live) recordAuth() {
	h.t.Helper()
	req := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, h.qty, h.restPx())
	c := mustCall(h.acct.Submit(time.Now(), req))
	found := false
	for i := range c.Req.Header {
		if c.Req.Header[i].Name == "OK-ACCESS-PASSPHRASE" {
			c.Req.Header[i].Value = "wrong-passphrase"
			found = true
		}
	}
	if !found {
		h.t.Fatal("request has no passphrase header")
	}
	resp, err := h.doer.Do(h.ctx, &c.Req)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, perr := c.Parse(resp); !errors.Is(perr, exchange.ErrAuth) {
		h.t.Fatalf("auth: %v", perr)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: "submit_auth", req: c.Req, resp: resp})
	}
	h.note("auth refused")
}

func (h *live) armAndRate() {
	h.t.Helper()
	c := mustCall(h.acct.ArmCancelOnDisconnect(time.Now(), 10*time.Second, nil))
	_, resp, err := call(h, c)
	if err != nil {
		h.t.Fatalf("caa: %v", err)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: "caa", req: c.Req, resp: resp})
	}
	var limited bool
	for i := 0; i < 4 && !limited; i++ {
		c2 := mustCall(h.acct.ArmCancelOnDisconnect(time.Now(), 10*time.Second, nil))
		_, resp2, err2 := call(h, c2)
		var rl *exchange.RateLimited
		if errors.As(err2, &rl) {
			limited = true
			if h.record {
				h.saved = append(h.saved, saved{name: "submit_rate", req: c2.Req, resp: resp2})
			}
			break
		}
		if err2 != nil {
			h.t.Fatalf("caa repeat: %v", err2)
		}
	}
	if !limited {
		h.t.Fatal("cancel-all-after did not return a rate limit")
	}
	h.disarm()
	h.note("caa armed, rate limited, then off")
}

func (h *live) disarm() {
	h.t.Helper()
	var last error
	for i := 0; i < 6; i++ {
		_, _, err := call(h, mustCall(h.acct.ArmCancelOnDisconnect(time.Now(), 0, nil)))
		if err == nil {
			return
		}
		last = err
		var rl *exchange.RateLimited
		if !errors.As(err, &rl) {
			h.t.Fatalf("caa off: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	h.t.Fatalf("caa off: %v", last)
}

func (h *live) order(side exchange.Side, typ exchange.OrderType, tif exchange.TimeInForce, qty, price float64) *exchange.OrderRequest {
	h.seq++
	id := d602(h.seq)
	if len(id) > h.acct.Limits().ClientOrderIDMaxLen {
		h.t.Fatalf("client order id %q length %d", id, len(id))
	}
	return &exchange.OrderRequest{
		Instrument: h.inst, Side: side, Type: typ, TIF: tif,
		Qty: qty, QuoteQty: decimal.Absent(), Price: price, ClientOrderID: id,
	}
}

func (h *live) quoteBuy(usdt float64) *exchange.OrderRequest {
	h.seq++
	return &exchange.OrderRequest{
		Instrument: h.inst, Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: decimal.Absent(), QuoteQty: usdt, Price: decimal.Absent(), ClientOrderID: d602(h.seq),
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

func (h *live) keepSnap(name, path string) {
	h.t.Helper()
	found := h.snap(path)
	page, resp, err := call(h, found)
	if err != nil {
		h.t.Fatal(err)
	}
	if h.record {
		h.saved = append(h.saved, saved{name: name, req: found.Req, resp: resp})
	}
	_ = page
}

func (h *live) snap(path string) exchange.Call[exchange.Page[exchange.AccountSnapshot]] {
	h.t.Helper()
	calls, err := h.acct.Snapshot(time.Now())
	if err != nil {
		h.t.Fatal(err)
	}
	for _, c := range calls {
		if c.Req.Path == path {
			return c
		}
	}
	h.t.Fatalf("snapshot has no %s", path)
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}
}

func (h *live) one(path string) exchange.AccountSnapshot {
	h.t.Helper()
	items, err := exchange.Collect(h.ctx, h.doer, time.Now, h.snap(path))
	if err != nil {
		h.t.Fatal(err)
	}
	var out exchange.AccountSnapshot
	for _, item := range items {
		out.OpenOrders = append(out.OpenOrders, item.OpenOrders...)
		out.Positions = append(out.Positions, item.Positions...)
		out.Balances = append(out.Balances, item.Balances...)
	}
	return out
}

func (h *live) btcFree() float64 {
	for _, b := range h.one("/api/v5/account/balance").Balances {
		if b.Asset == "BTC" {
			return b.Free
		}
	}
	return 0
}

func (h *live) balances() []exchange.Balance {
	return h.one("/api/v5/account/balance").Balances
}

func (h *live) extra() float64 {
	if !h.baseSet {
		return 0
	}
	return h.btcFree() - h.baseBTC
}

func (h *live) hasOrder(id string) bool {
	for _, o := range h.one("/api/v5/trade/orders-pending").OpenOrders {
		if o.OrderID == id && o.Exch.ExchTicker == "BTC-USDT" {
			return true
		}
	}
	return false
}

func (h *live) openBTC() int {
	n := 0
	for _, o := range h.one("/api/v5/trade/orders-pending").OpenOrders {
		if o.Exch.ExchTicker == "BTC-USDT" && o.Exch.Category == symbol.Spot {
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
	for _, o := range h.one("/api/v5/trade/orders-pending").OpenOrders {
		if o.Exch.ExchTicker != "BTC-USDT" {
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
	var extra float64
	h.until(10*time.Second, "btc balance moves after "+why, func() bool {
		extra = h.extra()
		return extra >= h.minSz || extra < h.step
	})
	qty := decimal.SnapDown(extra, h.step)
	if qty < h.minSz {
		h.note("flatten %s leaves dust %s", why, decimal.Format(extra, h.step))
		return
	}
	req := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, qty, decimal.Absent())
	res := h.place("", req, exchange.OrderStatusFilled)
	h.note("flatten %s order=%s qty=%s", why, res.OrderID, decimal.Format(qty, h.step))
	h.until(10*time.Second, "btc back to the starting balance after "+why, func() bool {
		return h.extra() < h.minSz
	})
}

func (h *live) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	old := h.ctx
	h.ctx = ctx
	defer func() { h.ctx = old }()
	for _, o := range h.one("/api/v5/trade/orders-pending").OpenOrders {
		if o.Exch.ExchTicker != "BTC-USDT" {
			continue
		}
		ref := exchange.CancelRef{Instrument: h.inst, OrderID: o.OrderID}
		if _, _, err := call(h, mustCall(h.acct.Cancel(time.Now(), ref))); err != nil {
			h.t.Errorf("cleanup cancel %s: %v", o.OrderID, err)
		}
	}
	if h.baseSet {
		extra := h.extra()
		qty := decimal.SnapDown(extra, h.step)
		if qty >= h.minSz {
			req := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, qty, decimal.Absent())
			if _, _, err := call(h, mustCall(h.acct.Submit(time.Now(), req))); err != nil {
				h.t.Errorf("cleanup flatten: %v", err)
			}
		}
	}
	var last error
	for i := 0; i < 6; i++ {
		if _, _, err := call(h, mustCall(h.acct.ArmCancelOnDisconnect(time.Now(), 0, nil))); err == nil {
			last = nil
			break
		} else {
			last = err
			var rl *exchange.RateLimited
			if !errors.As(err, &rl) {
				break
			}
			time.Sleep(2 * time.Second)
		}
	}
	if last != nil {
		h.t.Errorf("cleanup caa off: %v", last)
	}
	if h.conn != nil {
		_ = h.conn.Close()
	}
}

func (h *live) restPx() float64 {
	h.t.Helper()
	px := decimal.SnapDown(h.bid()*0.5, h.tick)
	if !(px > 0) || !(px < h.bid()) {
		h.t.Fatalf("resting price %s", decimal.Format(px, h.tick))
	}
	return px
}

func (h *live) bid() float64 { return h.book("bidPx") }
func (h *live) ask() float64 { return h.book("askPx") }

func (h *live) book(field string) float64 {
	h.t.Helper()
	body := publicGet(h.t, prodREST+"/api/v5/market/ticker?instId=BTC-USDT")
	var resp struct {
		Code string              `json:"code"`
		Msg  string              `json:"msg"`
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		h.t.Fatal(err)
	}
	if resp.Code != "0" || len(resp.Data) == 0 {
		h.t.Fatalf("ticker: %s %s", resp.Code, resp.Msg)
	}
	v, err := strconv.ParseFloat(resp.Data[0][field], 64)
	if err != nil || !(v > 0) {
		h.t.Fatalf("ticker %s %q", field, resp.Data[0][field])
	}
	return v
}

func (h *live) recent() string {
	h.rawMu.Lock()
	defer h.rawMu.Unlock()
	var b strings.Builder
	from := 0
	if len(h.raw) > 8 {
		from = len(h.raw) - 8
	}
	for _, frame := range h.raw[from:] {
		s := h.redact(string(frame))
		if len(s) > 240 {
			s = s[:240]
		}
		fmt.Fprintf(&b, "%s\n", s)
	}
	return b.String()
}

func (h *live) redact(s string) string {
	for _, sec := range []string{h.creds.Key, h.creds.Secret, h.creds.Passphrase} {
		if sec != "" {
			s = strings.ReplaceAll(s, sec, "REDACTED")
		}
	}
	return s
}

func (h *live) writeFixtures() {
	h.t.Helper()
	dir := filepath.Join("testdata", "conformance", "Okx")
	secrets := []string{h.creds.Key, h.creds.Secret, h.creds.Passphrase}
	for _, s := range h.saved {
		writeExchange(h.t, filepath.Join(dir, "http", s.name+".json"), &s.req, s.resp, secrets)
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
		Venue: Name, Side: conformance.SidePriv, Note: "okx demo private session",
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
	for _, s := range secrets {
		if s != "" && strings.Contains(string(b), s) {
			_ = os.Remove(path)
			h.t.Fatal("private fixture still contains a credential")
		}
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

func loadOkxCreds(t *testing.T) exchange.Credentials {
	t.Helper()
	path := os.Getenv("MFTIKO_OKX_KEYFILE")
	if path == "" {
		path = findUp(t, "okx_testnet.json")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Key        string `json:"api_key"`
		Secret     string `json:"api_secret"`
		Passphrase string `json:"passphrase"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if f.Key == "" || f.Secret == "" || f.Passphrase == "" {
		t.Fatal("okx_testnet.json needs api_key, api_secret and passphrase")
	}
	return exchange.Credentials{Type: exchange.KeyHMAC, Key: f.Key, Secret: f.Secret, Passphrase: f.Passphrase}
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

func btcSpot(t *testing.T) (inst *symbol.Info, tick, step, minSz float64) {
	t.Helper()
	body := publicGet(t, prodREST+"/api/v5/public/instruments?instType=SPOT&instId=BTC-USDT")
	var resp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID string `json:"instId"`
			Tick   string `json:"tickSz"`
			Lot    string `json:"lotSz"`
			Min    string `json:"minSz"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != "0" || len(resp.Data) != 1 || resp.Data[0].InstID != "BTC-USDT" {
		t.Fatalf("instruments: %s %s", resp.Code, resp.Msg)
	}
	row := resp.Data[0]
	tick, step, minSz = mustFloat(t, row.Tick), mustFloat(t, row.Lot), mustFloat(t, row.Min)
	inst = &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: Name, Category: symbol.Spot},
		ExchTicker: "BTC-USDT",
		Filters:    symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step},
	}
	return inst, tick, step, minSz
}

func publicGet(t *testing.T, url string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", userAgent)
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
		t.Fatalf("number %q", s)
	}
	return v
}

type feed struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ready  bool
	orders []exchange.OrderUpdate
	fills  []exchange.Fill
	dead   error
}

func newFeed(ctx context.Context) *feed {
	f := &feed{}
	f.cond = sync.NewCond(&f.mu)
	go func() {
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				f.mu.Lock()
				f.cond.Broadcast()
				f.mu.Unlock()
				return
			case <-tk.C:
				f.mu.Lock()
				f.cond.Broadcast()
				f.mu.Unlock()
			}
		}
	}()
	return f
}

func (f *feed) Ready() { f.mu.Lock(); f.ready = true; f.cond.Broadcast(); f.mu.Unlock() }
func (f *feed) Expired(string) {
	f.die(errors.New("okx: private stream expired"))
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

func (f *feed) deadErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dead
}

func (f *feed) die(err error) {
	if err == nil {
		err = errors.New("okx: private stream closed")
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
		fmt.Fprintf(&b, "fill %s %s qty=%s\n", fill.ClientOrderID, fill.FillID, decimal.Format(fill.Qty, 1e-8))
	}
	return b.String()
}
