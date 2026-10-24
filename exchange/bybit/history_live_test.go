package bybit

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
)

// TestBybitHistory places one linear fill on the testnet and checks that the execution list and
// the order history describe the same trade the private stream reported (E06-D001). It does not run
// in CI.
//
//	MFTIKO_BYBIT_HISTORY=1 go test ./exchange/bybit/ -run TestBybitHistory -v -count=1
//
// MFTIKO_BYBIT_RECORD=1 also writes that fill into testdata, with the key redacted.
func TestBybitHistory(t *testing.T) {
	if os.Getenv("MFTIKO_BYBIT_HISTORY") != "1" {
		t.Skip("set MFTIKO_BYBIT_HISTORY=1 to match a Bybit testnet fill to the execution list")
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
		creds: creds, feed: newFeed(),
	}
	t.Cleanup(func() { h.cleanup() })

	h.cancelOpen()
	h.flatten("setup")
	h.openSocket()

	req := h.order(exchange.SideBuy, exchange.OrderTypeMarket, exchange.TIFUnspecified, qty, decimal.Absent(), false)
	res := h.place("", req, exchange.OrderStatusFilled)
	fill, err := h.feed.waitFill(req.ClientOrderID, 20*time.Second)
	if err != nil {
		t.Fatalf("stream fill: %v\n%s", err, h.feed.seen())
	}
	if fill.OrderID != res.OrderID || fill.FillID == "" || fill.Qty != qty {
		t.Fatalf("stream fill %+v order %s", fill, res.OrderID)
	}

	hist, err := Declare().AccountKinds[0].History(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: testREST,
	})
	if err != nil {
		t.Fatal(err)
	}
	q := exchange.FillQuery{
		Instrument: inst,
		StartNs:    fill.ExchTsNs - int64(2*time.Second),
		EndNs:      fill.ExchTsNs + int64(2*time.Second),
		Limit:      50,
	}
	var page *exchange.FillPage
	var fillCall exchange.Call[*exchange.FillPage]
	var fillResp *exchange.HTTPResponse
	deadline := time.Now().Add(20 * time.Second)
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
		if historyHas(page.Fills, fill) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution list never returned %s (%d rows)", fill.FillID, len(page.Fills))
		}
		time.Sleep(300 * time.Millisecond)
	}
	got := historyFill(page.Fills, fill.FillID)
	if got.OrderID != fill.OrderID || got.Side != fill.Side || got.Price != fill.Price || got.Qty != fill.Qty {
		t.Fatalf("fill %s differs: stream %+v history %+v", fill.FillID, fill, got)
	}
	t.Logf("fill %s order %s price %s qty %s", fill.FillID, fill.OrderID, decimal.Format(fill.Price, tick), decimal.Format(fill.Qty, step))

	var up *exchange.OrderUpdate
	var orderCall exchange.Call[*exchange.OrderUpdate]
	var orderResp *exchange.HTTPResponse
	deadline = time.Now().Add(20 * time.Second)
	for {
		orderCall, err = hist.Order(time.Now(), inst, res.OrderID)
		if err != nil {
			t.Fatal(err)
		}
		orderResp, err = doer.Do(ctx, &orderCall.Req)
		if err != nil {
			t.Fatal(err)
		}
		up, err = orderCall.Parse(orderResp)
		if err == nil && up.Status == exchange.OrderStatusFilled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("order history: %v %+v", err, up)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if up.OrderID != res.OrderID || up.FilledQty != qty {
		t.Fatalf("order %+v", up)
	}
	t.Logf("order %s FILLED filled=%s", up.OrderID, decimal.Format(up.FilledQty, step))

	h.flatten("done")
	if os.Getenv("MFTIKO_BYBIT_RECORD") == "1" {
		recordHistory(t, h, creds, res.OrderID, fillCall.Req, fillResp, orderCall.Req, orderResp)
	}
}

func historyHas(fills []exchange.Fill, want exchange.Fill) bool {
	for _, f := range fills {
		if f.FillID == want.FillID {
			return true
		}
	}
	return false
}

func historyFill(fills []exchange.Fill, id string) exchange.Fill {
	for _, f := range fills {
		if f.FillID == id {
			return f
		}
	}
	return exchange.Fill{}
}

func recordHistory(t *testing.T, h *live, creds exchange.Credentials, orderID string, fillReq exchange.HTTPRequest, fillResp *exchange.HTTPResponse, orderReq exchange.HTTPRequest, orderResp *exchange.HTTPResponse) {
	t.Helper()
	dir := filepath.Join("testdata", "conformance", "Bybit")
	secrets := []string{creds.Key, creds.Secret}
	h.rawMu.Lock()
	var frames [][]byte
	for _, f := range h.raw {
		if bytes.Contains(f, []byte(orderID)) {
			frames = append(frames, append([]byte(nil), f...))
		}
	}
	h.rawMu.Unlock()
	if len(frames) == 0 {
		t.Fatal("no private frame for the fill")
	}
	path := filepath.Join(dir, "ws", "fills.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := conformance.NewWriter(f, conformance.Record{
		Venue: Name, Side: conformance.SidePriv,
		Note: "bybit testnet linear BTCUSDT fill. execId matches executions (E06-D001).",
	}, time.Now, secrets...)
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		if err := w.Frame(conformance.KindIn, frame); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), creds.Key) || strings.Contains(string(b), creds.Secret) {
		_ = os.Remove(path)
		t.Fatal("fills fixture still contains a credential")
	}
	writeExchange(t, filepath.Join(dir, "http", "executions.json"), &fillReq, fillResp, secrets)
	writeExchange(t, filepath.Join(dir, "http", "order_lookup.json"), &orderReq, orderResp, secrets)
	t.Logf("recorded %d private frames for %s", len(frames), orderID)
}
