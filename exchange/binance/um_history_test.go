package binance

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
)

// TestBinanceUMHistory places one fill on the USD-M testnet and checks that History.Fills and
// History.Order describe the same trade the private stream reported (D601). It does not run in CI.
//
//	MFTIKO_BINANCE_UM_LIVE=1 go test ./exchange/binance/ -run TestBinanceUMHistory -v -count=1
//
// MFTIKO_BINANCE_UM_RECORD=1 also writes that segment into testdata, with the key and the
// listenKey redacted, for the conformance fill check.
func TestBinanceUMHistory(t *testing.T) {
	if os.Getenv("MFTIKO_BINANCE_UM_LIVE") != "1" {
		t.Skip("set MFTIKO_BINANCE_UM_LIVE=1 to check fill ids against the USD-M testnet")
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

	h.cancelAll()
	h.flatten("setup")
	h.openSockets()

	px := decimal.SnapUp(h.ask()*1.002, tick)
	req := h.order(exchange.SideBuy, exchange.OrderTypeLimit, exchange.TIFGTC, qty, px, false)
	res := h.place(req, exchange.OrderStatusFilled)
	stream := h.feed.fillsOf(req.ClientOrderID)
	if len(stream) == 0 {
		t.Fatal("the private stream reported no fill")
	}
	fromID := stream[0].FillID
	for _, f := range stream[1:] {
		if idLess(f.FillID, fromID) {
			fromID = f.FillID
		}
	}

	hist, err := historyFactory(kindUM)(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: "https://" + testHost,
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
			t.Fatalf("userTrades never returned the stream fills %s; page has %d", fromID, len(page.Fills))
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, f := range stream {
		got, ok := findFill(page.Fills, f.FillID)
		if !ok {
			t.Fatalf("fill %s is on the stream and not in userTrades", f.FillID)
		}
		if got.OrderID != f.OrderID || got.Side != f.Side || got.Price != f.Price || got.Qty != f.Qty {
			t.Fatalf("fill %s differs: stream %+v history %+v", f.FillID, f, got)
		}
		h.note("fill %s order %s price %s qty %s", f.FillID, f.OrderID, decimal.Format(f.Price, tick), decimal.Format(f.Qty, step))
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

	flat := h.order(exchange.SideSell, exchange.OrderTypeMarket, exchange.TIFUnspecified, decimal.SnapDown(h.size(), step), decimal.Absent(), true)
	flatRes := h.place(flat, exchange.OrderStatusFilled)
	h.note("flatten %s", flatRes.OrderID)
	if sz := h.size(); sz < -step || sz >= step {
		t.Fatalf("size left %s", decimal.Format(sz, step))
	}

	if os.Getenv("MFTIKO_BINANCE_UM_RECORD") == "1" {
		recordSegment(t, creds, h, req.ClientOrderID, fillCall.Req, fillResp, orderCall.Req, orderResp)
	}
	fmt.Println("--- binance um history ---")
	for _, line := range h.lines {
		fmt.Println(line)
	}
}

func recordSegment(t *testing.T, creds exchange.Credentials, h *live, cloid string, fillReq exchange.HTTPRequest, fillResp *exchange.HTTPResponse, orderReq exchange.HTTPRequest, orderResp *exchange.HTTPResponse) {
	t.Helper()
	dir := filepath.Join("testdata", "conformance", "BinanceUM")
	var frames [][]byte
	h.rawMu.Lock()
	for _, f := range h.raw {
		if orderFrame(f, cloid) {
			frames = append(frames, f)
		}
	}
	h.rawMu.Unlock()
	if len(frames) == 0 {
		h.rawMu.Lock()
		n := len(h.raw)
		h.rawMu.Unlock()
		t.Fatalf("no ORDER_TRADE_UPDATE frame for the fill (%d private frames)", n)
	}
	var buf bytes.Buffer
	w, err := conformance.NewWriter(&buf, conformance.Record{
		Venue: VenueUM, Side: conformance.SidePriv,
		Note: "USD-M testnet BTCUSDT fill. The trade id matches user_trades_testnet (D601).",
	}, time.Now, creds.Key, creds.Secret, h.listenKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		if err := w.Frame(conformance.KindIn, f); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(buf.String(), creds.Key) || strings.Contains(buf.String(), "BEGIN ") {
		t.Fatal("the private recording still contains a credential")
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", "testnet.jsonl"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	writeExchange(t, filepath.Join(dir, "http", "user_trades_testnet.json"), creds, h.listenKey, &fillReq, fillResp)
	writeExchange(t, filepath.Join(dir, "http", "order_testnet.json"), creds, h.listenKey, &orderReq, orderResp)
	t.Logf("recorded %d private frames", len(frames))
}

func writeExchange(t *testing.T, path string, creds exchange.Credentials, listenKey string, req *exchange.HTTPRequest, resp *exchange.HTTPResponse) {
	t.Helper()
	var secrets []string
	for _, s := range []string{creds.Key, creds.Secret, listenKey} {
		if s != "" {
			secrets = append(secrets, s)
		}
	}
	var buf bytes.Buffer
	if err := conformance.SaveExchange(&buf, req, resp, secrets...); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), creds.Key) || strings.Contains(buf.String(), "BEGIN ") {
		t.Fatal("the REST recording still contains a credential")
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func orderFrame(frame []byte, cloid string) bool {
	// encoding/json folds case, and these frames carry both "e" and "E", so a struct
	// decode can fail. The adapter's codec does not fold case.
	s := string(frame)
	return strings.Contains(s, `"e":"ORDER_TRADE_UPDATE"`) && strings.Contains(s, `"c":"`+cloid+`"`)
}

func covers(page, want []exchange.Fill) bool {
	for _, f := range want {
		if _, ok := findFill(page, f.FillID); !ok {
			return false
		}
	}
	return true
}

func findFill(page []exchange.Fill, id string) (exchange.Fill, bool) {
	for _, f := range page {
		if f.FillID == id {
			return f, true
		}
	}
	return exchange.Fill{}, false
}

func idLess(a, b string) bool {
	x, errX := strconv.ParseInt(a, 10, 64)
	y, errY := strconv.ParseInt(b, 10, 64)
	if errX != nil || errY != nil {
		return a < b
	}
	return x < y
}
