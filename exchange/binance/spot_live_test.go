package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

// TestBinanceSpotPrivate places orders on the spot testnet through one WebSocket API connection
// and checks that myTrades reports the same trade id as executionReport (D601). It does not run in CI.
//
//	MFTIKO_BINANCE_SPOT_LIVE=1 go test ./exchange/binance/ -run TestBinanceSpotPrivate -v -count=1
//
// MFTIKO_BINANCE_SPOT_RECORD=1 also writes that segment into testdata, with the key redacted.
func TestBinanceSpotPrivate(t *testing.T) {
	if os.Getenv("MFTIKO_BINANCE_SPOT_LIVE") != "1" {
		t.Skip("set MFTIKO_BINANCE_SPOT_LIVE=1 to place orders on the spot testnet")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	creds := loadSpotCreds(t)
	acct, err := accountFactory(kindSpot)(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: "https://" + spotTestHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := asAccount(t, acct)
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 15 * time.Second})
	inst, tick, step, minNotional := spotFilters(t)
	h := &live{
		t: t, ctx: ctx, acct: raw, doer: doer, inst: inst, tick: tick, step: step,
		feed: newFeed(),
	}
	t.Cleanup(func() { spotCleanup(h) })
	defer func() {
		fmt.Println("--- binance spot testnet ---")
		for _, line := range h.lines {
			fmt.Println(line)
		}
	}()

	bid, ask := spotBook(t)
	h.note("book bid=%s ask=%s tick=%s step=%s minNotional=%s",
		decimal.Format(bid, tick), decimal.Format(ask, tick),
		decimal.Format(tick, tick), decimal.Format(step, step), decimal.Format(minNotional, tick))

	if _, err := exchange.Run(ctx, doer, mustCall(raw.spotCancelAll(time.Now(), "BTCUSDT"))); err != nil {
		t.Fatal(err)
	}
	spotSellFree(h, "setup")
	start := h.snap()
	free, locked, _ := usdt(start)
	h.note("start %s", spotSnapText(start, step))

	qty := spotQty(t, ask, step, minNotional)
	rest := decimal.SnapDown(bid*0.99, tick)
	if !(rest > 0) || !(rest < bid) {
		t.Fatalf("resting price %s bid %s", decimal.Format(rest, tick), decimal.Format(bid, tick))
	}
	for qty*rest < minNotional {
		qty = decimal.SnapUp(qty+step, step)
	}
	cross := decimal.SnapUp(ask*1.002, tick)
	need := cross * qty
	if free < need {
		t.Fatalf("USDT free %s is below the %s this test buys; balances %s",
			decimal.Format(free, 0.01), decimal.Format(need, 0.01), spotSnapText(start, step))
	}
	h.qty = qty
	h.note("qty=%s rest=%s cross=%s", decimal.Format(qty, step), decimal.Format(rest, tick), decimal.Format(cross, tick))

	openSpotTrade(h)

	h.place(h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFIOC, qty, rest, false), exchange.OrderStatusExpired)
	h.note("IOC below the bid expired")
	h.place(h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFFOK, qty, rest, false), exchange.OrderStatusExpired)
	h.note("FOK below the bid expired")

	resting := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, rest, false)
	h.place(resting, exchange.OrderStatusAccepted)
	held := h.snap()
	_, lockedNow, _ := usdt(held)
	if lockedNow < locked+rest*qty-0.01 {
		t.Fatalf("USDT locked %s, was %s, order notionally %s",
			decimal.Format(lockedNow, 0.01), decimal.Format(locked, 0.01), decimal.Format(rest*qty, 0.01))
	}
	h.note("resting locked USDT %s -> %s", decimal.Format(locked, 0.01), decimal.Format(lockedNow, 0.01))
	h.cancel(resting.ClientOrderID)
	_, lockedAfter, _ := usdt(h.snap())
	if lockedAfter > locked+0.01 {
		t.Fatalf("USDT locked %s after cancel, was %s", decimal.Format(lockedAfter, 0.01), decimal.Format(locked, 0.01))
	}
	h.note("cancel released the lock")

	maker := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFPostOnly, qty, rest, false)
	h.place(maker, exchange.OrderStatusAccepted)
	h.cancel(maker.ClientOrderID)
	h.note("post-only rested and was canceled")

	req := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, cross, false)
	res := h.place(req, exchange.OrderStatusFilled)
	stream := h.feed.fillsOf(req.ClientOrderID)
	if len(stream) == 0 {
		t.Fatal("the trade stream reported no fill")
	}
	fromID := stream[0].FillID
	for _, f := range stream[1:] {
		if idLess(f.FillID, fromID) {
			fromID = f.FillID
		}
	}

	hist, err := historyFactory(kindSpot)(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: "https://" + spotTestHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	q := exchange.FillQuery{Instrument: inst, FromID: fromID, Limit: len(stream)}
	var page *exchange.FillPage
	var fillResp *exchange.HTTPResponse
	var fillCall exchange.Call[*exchange.FillPage]
	deadline := time.Now().Add(15 * time.Second)
	for {
		fillCall, err = hist.Fills(time.Now(), q)
		if err != nil {
			t.Fatal(err)
		}
		fillResp, err = doer.Do(ctx, &fillCall.Req)
		if err != nil {
			t.Fatal(err)
		}
		page, err = fillCall.Parse(fillResp)
		if err != nil {
			t.Fatal(err)
		}
		if covers(page.Fills, stream) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("myTrades never returned the stream fills %s; page has %d", fromID, len(page.Fills))
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, f := range stream {
		got, ok := findFill(page.Fills, f.FillID)
		if !ok {
			t.Fatalf("fill %s is on the stream and not in myTrades", f.FillID)
		}
		if got.OrderID != f.OrderID || got.Side != f.Side || got.Price != f.Price || got.Qty != f.Qty {
			t.Fatalf("fill %s differs: stream %+v history %+v", f.FillID, f, got)
		}
		h.note("fill %s order %s side %s price %s qty %s", f.FillID, f.OrderID, sideName(f.Side),
			decimal.Format(f.Price, tick), decimal.Format(f.Qty, step))
	}

	orderCall, err := hist.Order(time.Now(), inst, res.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	orderResp, err := doer.Do(ctx, &orderCall.Req)
	if err != nil {
		t.Fatal(err)
	}
	up, err := orderCall.Parse(orderResp)
	if err != nil {
		t.Fatal(err)
	}
	if up.OrderID != res.OrderID || up.Status != exchange.OrderStatusFilled {
		t.Fatalf("order %s status %s, want FILLED", up.OrderID, statusName(up.Status))
	}
	var sum float64
	for _, f := range stream {
		sum += f.Qty
	}
	if !decimal.Equal(up.FilledQty, sum, step) {
		t.Fatalf("order filled %s, stream fills sum %s", decimal.Format(up.FilledQty, step), decimal.Format(sum, step))
	}
	h.note("order %s FILLED filled=%s", up.OrderID, decimal.Format(up.FilledQty, step))

	if os.Getenv("MFTIKO_BINANCE_SPOT_RECORD") == "1" {
		recordSpot(t, creds, h, req.ClientOrderID, fillCall.Req, fillResp, orderCall.Req, orderResp)
	}

	sold := spotSellFree(h, "flatten")
	h.note("flatten order %s", sold)
	if _, err := exchange.Run(ctx, doer, mustCall(raw.spotCancelAll(time.Now(), "BTCUSDT"))); err != nil {
		t.Fatal(err)
	}
	left := btcFree(h.snap())
	if left >= step && left*bid >= minNotional {
		t.Fatalf("BTC free %s still sells", decimal.Format(left, step))
	}
	h.note("flat BTC free=%s", decimal.Format(left, step))
}

func openSpotTrade(h *live) {
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
			h.rawMu.Lock()
			h.raw = append(h.raw, buf)
			h.rawMu.Unlock()
			return ts.Decode(buf, h.feed)
		})
		h.feed.die(err)
	}()
	// The worker sends each dial frame only after the previous one is answered. Pipelining
	// session.logon with the next request leaves the session unauthenticated.
	for i, f := range dial.Frames {
		if err := conn.Write(h.ctx, f); err != nil {
			h.t.Fatal(err)
		}
		if i+1 == len(dial.Frames) {
			break
		}
		if err := waitControl(h, dialID(f), 15*time.Second); err != nil {
			h.t.Fatal(err)
		}
		h.note("answered %s", dialID(f))
	}
	if err := h.feed.waitReady(15 * time.Second); err != nil {
		h.t.Fatal(err)
	}
}

func dialID(frame []byte) string {
	var head struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(frame, &head)
	return head.ID
}

func waitControl(h *live, id string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for {
		h.rawMu.Lock()
		var found string
		for _, f := range h.raw {
			var head struct {
				ID     string `json:"id"`
				Status int    `json:"status"`
				Error  *struct {
					Code int    `json:"code"`
					Msg  string `json:"msg"`
				} `json:"error"`
			}
			if json.Unmarshal(f, &head) != nil || head.ID != id {
				continue
			}
			if head.Status == 200 && head.Error == nil {
				h.rawMu.Unlock()
				return nil
			}
			found = fmt.Sprintf("id=%s status=%d", head.ID, head.Status)
			if head.Error != nil {
				found = fmt.Sprintf("%s code=%d %s", found, head.Error.Code, head.Error.Msg)
			}
		}
		h.rawMu.Unlock()
		if found != "" {
			return fmt.Errorf("binance: %s", found)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("binance: no answer to %s", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func spotCleanup(h *live) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if h.acct != nil {
		if _, err := exchange.Run(ctx, h.doer, mustCall(h.acct.spotCancelAll(time.Now(), "BTCUSDT"))); err != nil {
			h.t.Errorf("cleanup cancel: %v", err)
		}
		if q := decimal.SnapDown(btcFree(h.snap()), h.step); q >= h.step {
			req := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, q, decimal.Absent(), false)
			call, err := h.acct.Submit(time.Now(), req)
			if err != nil {
				h.t.Errorf("cleanup sell: %v", err)
			} else if _, err := exchange.Run(ctx, h.doer, call); err != nil {
				h.t.Errorf("cleanup sell: %v", err)
			}
		}
	}
	if h.trade != nil {
		_ = h.trade.Write(ctx, []byte(`{"id":"logout","method":"session.logout","params":{}}`))
		_ = h.trade.Close()
	}
}

func spotSellFree(h *live, why string) string {
	h.t.Helper()
	q := decimal.SnapDown(btcFree(h.snap()), h.step)
	if q < h.step {
		return ""
	}
	req := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, q, decimal.Absent(), false)
	call, err := h.acct.Submit(time.Now(), req)
	if err != nil {
		h.t.Fatal(err)
	}
	res, err := exchange.Run(h.ctx, h.doer, call)
	if err != nil {
		h.t.Fatalf("sell %s: %v", why, err)
	}
	h.note("sell %s order=%s status=%s qty=%s", why, res.OrderID, statusName(res.Status), decimal.Format(q, h.step))
	return res.OrderID
}

func btcFree(s exchange.AccountSnapshot) float64 {
	for _, b := range s.Balances {
		if b.Asset == "BTC" {
			return b.Free
		}
	}
	return 0
}

func spotSnapText(s exchange.AccountSnapshot, step float64) string {
	free, locked, ok := usdt(s)
	btcLocked := 0.0
	for _, bal := range s.Balances {
		if bal.Asset == "BTC" {
			btcLocked = bal.Locked
		}
	}
	if !ok {
		return fmt.Sprintf("no USDT; BTC free=%s locked=%s", decimal.Format(btcFree(s), step), decimal.Format(btcLocked, step))
	}
	return fmt.Sprintf("USDT free=%s locked=%s BTC free=%s locked=%s",
		decimal.Format(free, 0.01), decimal.Format(locked, 0.01),
		decimal.Format(btcFree(s), step), decimal.Format(btcLocked, step))
}

func sideName(s exchange.Side) string {
	switch s {
	case exchange.SideBuy:
		return "BUY"
	case exchange.SideSell:
		return "SELL"
	default:
		return "SIDE"
	}
}

func loadSpotCreds(t *testing.T) exchange.Credentials {
	t.Helper()
	path := os.Getenv("MFTIKO_BINANCE_SPOT_KEYFILE")
	if path == "" {
		path = findUp(t, "binance_testnet.json")
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

func spotFilters(t *testing.T) (*symbol.Info, float64, float64, float64) {
	t.Helper()
	var info struct {
		Symbols []struct {
			Symbol  string `json:"symbol"`
			Status  string `json:"status"`
			Filters []struct {
				Type        string `json:"filterType"`
				Tick        string `json:"tickSize"`
				Step        string `json:"stepSize"`
				MinQty      string `json:"minQty"`
				MinPrice    string `json:"minPrice"`
				Notional    string `json:"notional"`
				MinNotional string `json:"minNotional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	getJSON(t, "https://"+spotTestHost+"/api/v3/exchangeInfo?symbol=BTCUSDT", &info)
	if len(info.Symbols) == 0 || info.Symbols[0].Symbol != "BTCUSDT" {
		t.Fatal("exchangeInfo has no BTCUSDT")
	}
	s := info.Symbols[0]
	if s.Status != "TRADING" {
		t.Fatalf("BTCUSDT %s", s.Status)
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
			n := mustNum(t, f.MinNotional)
			if v := mustNum(t, f.Notional); v > n {
				n = v
			}
			if n > minNotional {
				minNotional = n
			}
		}
	}
	if !(tick > 0) || !(step > 0) || !(minNotional > 0) {
		t.Fatalf("tick %v step %v minNotional %v", tick, step, minNotional)
	}
	inst := &symbol.Info{
		ExchTicker: "BTCUSDT",
		Ticker:     symbol.UniversalTicker{Venue: VenueSpot, Category: symbol.Spot, Symbol: "BTCUSDT"},
		Filters:    symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step},
	}
	return inst, tick, step, minNotional
}

func spotBook(t *testing.T) (bid, ask float64) {
	t.Helper()
	var q struct {
		Bid string `json:"bidPrice"`
		Ask string `json:"askPrice"`
	}
	getJSON(t, "https://"+spotTestHost+"/api/v3/ticker/bookTicker?symbol=BTCUSDT", &q)
	bid, ask = mustNum(t, q.Bid), mustNum(t, q.Ask)
	if !(bid > 0) || !(ask >= bid) {
		t.Fatalf("book bid %s ask %s", q.Bid, q.Ask)
	}
	return bid, ask
}

func spotQty(t *testing.T, ask, step, minNotional float64) float64 {
	t.Helper()
	qty := decimal.SnapUp(minNotional/ask, step)
	if qty*ask < minNotional {
		qty = decimal.SnapUp(qty+step, step)
	}
	if !(qty > 0) {
		t.Fatalf("qty %v", qty)
	}
	return qty
}

func recordSpot(t *testing.T, creds exchange.Credentials, h *live, cloid string, fillReq exchange.HTTPRequest, fillResp *exchange.HTTPResponse, orderReq exchange.HTTPRequest, orderResp *exchange.HTTPResponse) {
	t.Helper()
	dir := filepath.Join("testdata", "conformance", "Binance")
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "http"), 0o755); err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	h.rawMu.Lock()
	for _, f := range h.raw {
		if spotExecFrame(f, cloid) {
			frames = append(frames, f)
		}
	}
	n := len(h.raw)
	h.rawMu.Unlock()
	if len(frames) == 0 {
		t.Fatalf("no executionReport for the fill (%d trade frames)", n)
	}
	var buf bytes.Buffer
	w, err := conformance.NewWriter(&buf, conformance.Record{
		Venue: VenueSpot, Side: conformance.SideTrade,
		Note: "Spot testnet BTCUSDT fill. The trade id matches my_trades_testnet (D601).",
	}, time.Now, creds.Key, creds.Secret)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		if err := w.Frame(conformance.KindIn, f); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(buf.String(), creds.Key) || strings.Contains(buf.String(), "BEGIN ") {
		t.Fatal("the trade recording still contains a credential")
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", "testnet.jsonl"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	writeExchange(t, filepath.Join(dir, "http", "my_trades_testnet.json"), creds, "", &fillReq, fillResp)
	writeExchange(t, filepath.Join(dir, "http", "order_testnet.json"), creds, "", &orderReq, orderResp)
	t.Logf("recorded %d trade frames", len(frames))
}

func spotExecFrame(frame []byte, cloid string) bool {
	s := string(frame)
	if !strings.Contains(s, `"e":"executionReport"`) {
		return false
	}
	return strings.Contains(s, `"c":"`+cloid+`"`) || strings.Contains(s, `"C":"`+cloid+`"`)
}
