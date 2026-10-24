package okx

import (
	"bytes"
	"context"
	"math"
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

// TestOkxHistory places one spot fill on demo trading and checks that fills-history and the
// order lookup describe the same trade the orders channel reported (E06-D001). It does not run in CI.
//
//	MFTIKO_OKX_HISTORY=1 go test ./exchange/okx/ -run TestOkxHistory -v -count=1
//
// MFTIKO_OKX_RECORD=1 also writes that fill into testdata, with the key, the secret and the
// passphrase redacted. The buy is sold back down to the BTC balance from before the test.
func TestOkxHistory(t *testing.T) {
	if os.Getenv("MFTIKO_OKX_HISTORY") != "1" {
		t.Skip("set MFTIKO_OKX_HISTORY=1 to match an OKX demo fill to fills-history")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	creds := loadOkxCreds(t)
	cfg := exchange.AccountConfig{APIID: 1, Credentials: creds, Simulated: true}
	acct, err := Declare().AccountKinds[0].Account(cfg)
	if err != nil {
		t.Fatal(err)
	}
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 20 * time.Second})
	inst, tick, step, minSz := btcSpot(t)
	qty := decimal.SnapDown(math.Max(minSz, 0.0001), step)
	if qty < minSz {
		qty = minSz
	}
	h := &live{
		t: t, ctx: ctx, acct: acct.(*account), doer: doer, inst: inst,
		tick: tick, step: step, minSz: minSz, qty: qty,
		creds: creds, feed: newFeed(ctx),
	}
	t.Cleanup(func() { h.cleanup() })

	h.disarm()
	h.cancelOpen()
	h.baseBTC = h.btcFree()
	h.baseSet = true
	h.openSocket()

	req := h.order(exchange.SideBuy, exchange.OrderTypeMarket, exchange.TIFUnspecified, qty, decimal.Absent())
	res := h.place("", req, exchange.OrderStatusFilled)
	fill, err := h.feed.waitFill(req.ClientOrderID, 20*time.Second)
	if err != nil {
		t.Fatalf("stream fill: %v\n%s", err, h.feed.seen())
	}
	if fill.OrderID != res.OrderID || fill.FillID == "" || fill.Qty != qty {
		t.Fatalf("stream fill %+v order %s qty %s", fill, res.OrderID, decimal.Format(qty, step))
	}

	hist, err := Declare().AccountKinds[0].History(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var page *exchange.FillPage
	var fillCall exchange.Call[*exchange.FillPage]
	var fillResp *exchange.HTTPResponse
	deadline := time.Now().Add(90 * time.Second)
	for {
		q := exchange.FillQuery{
			Instrument: inst,
			StartNs:    time.Now().Add(-2 * time.Minute).UnixNano(),
			EndNs:      time.Now().Add(time.Minute).UnixNano(),
			Limit:      100,
		}
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
			t.Fatalf("fills-history: %v body %.300q", err, fillResp.Body)
		}
		if historyHas(page.Fills, fill) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fills-history never returned %s (%d rows)", fill.FillID, len(page.Fills))
		}
		time.Sleep(time.Second)
	}
	got := historyFill(page.Fills, fill.FillID)
	if got.OrderID != fill.OrderID || got.Side != fill.Side || got.Price != fill.Price || got.Qty != fill.Qty {
		t.Fatalf("fill %s differs: stream %+v history %+v", fill.FillID, fill, got)
	}
	if got.Fee != fill.Fee || got.FeeAsset != fill.FeeAsset || got.Liquidity != fill.Liquidity {
		t.Fatalf("fill %s fee or liquidity differs: stream %+v history %+v", fill.FillID, fill, got)
	}
	t.Logf("fill %s order %s price %s qty %s fee %s %s %s", fill.FillID, fill.OrderID,
		decimal.Format(fill.Price, tick), decimal.Format(fill.Qty, step), decimal.Format(fill.Fee, 1e-8), fill.FeeAsset, fill.Liquidity)

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
		if err == nil && up.Status == exchange.OrderStatusFilled && up.OrderID == res.OrderID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("order lookup: %v %+v", err, up)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if up.FilledQty != fill.Qty {
		t.Fatalf("order %+v fill qty %s", up, decimal.Format(fill.Qty, step))
	}
	t.Logf("order %s FILLED filled=%s", up.OrderID, decimal.Format(up.FilledQty, step))

	h.flatten("done")
	if os.Getenv("MFTIKO_OKX_RECORD") == "1" {
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
	dir := filepath.Join("testdata", "conformance", "Okx")
	secrets := []string{creds.Key, creds.Secret, creds.Passphrase}
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
		Note: "okx demo spot BTC-USDT fill. tradeId matches fills-history (E06-D001).",
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
	for _, s := range secrets {
		if s != "" && strings.Contains(string(b), s) {
			_ = os.Remove(path)
			t.Fatal("fills fixture still contains a credential")
		}
	}
	writeExchange(t, filepath.Join(dir, "http", "fills.json"), &fillReq, fillResp, secrets)
	writeExchange(t, filepath.Join(dir, "http", "order_lookup.json"), &orderReq, orderResp, secrets)
	t.Logf("recorded %d private frames for %s", len(frames), orderID)
}
