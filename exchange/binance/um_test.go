package binance

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestOtherKindsUnimplemented(t *testing.T) {
	creds := conformance.TestCredentials(exchange.KeyEd25519, false)
	a, err := accountFactory(kindCM)(exchange.AccountConfig{Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.NewTradeStream(); !errors.Is(err, exchange.ErrNotImplemented) {
		t.Fatalf("NewTradeStream: %v", err)
	}
	if _, err := a.NewStream(); !errors.Is(err, exchange.ErrNotImplemented) {
		t.Fatalf("NewStream: %v", err)
	}
	if _, err := a.Submit(time.Unix(0, 0), limitOrder(exchange.TIFGTC, cloidGTC)); !errors.Is(err, exchange.ErrNotImplemented) {
		t.Fatalf("Submit: %v", err)
	}
	h, err := historyFactory(kindCM)(exchange.AccountConfig{Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Fills(time.Unix(0, 0), exchange.FillQuery{Instrument: btcInst()}); !errors.Is(err, exchange.ErrNotImplemented) {
		t.Fatalf("Fills: %v", err)
	}
}

func TestHMACRefused(t *testing.T) {
	a, err := accountFactory(kindUM)(exchange.AccountConfig{Credentials: exchange.Credentials{
		Type: exchange.KeyHMAC, Key: "k", Secret: "secret",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.NewTradeStream(); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("NewTradeStream: %v", err)
	}
	if _, err := a.Submit(time.Unix(0, 0), limitOrder(exchange.TIFGTC, cloidGTC)); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("Submit: %v", err)
	}
}

func TestClientOrderID(t *testing.T) {
	a := mustUM(t)
	if lim := a.Limits().ClientOrderIDMaxLen; lim < 19 || len(cloidGTC) > lim {
		t.Fatalf("ClientOrderIDMaxLen %d, D602 id length %d", lim, len(cloidGTC))
	}
	req := limitOrder(exchange.TIFGTC, strings.Repeat("a", 37))
	if _, err := a.Submit(time.Unix(0, 0), req); err == nil {
		t.Fatal("a 37-character client order id was accepted")
	}
	req.ClientOrderID = "has space"
	if _, err := a.Submit(time.Unix(0, 0), req); err == nil {
		t.Fatal("a client order id with a space was accepted")
	}
	req = marketOrder(cloidMKT)
	req.TIF = exchange.TIFIOC
	if _, err := a.Submit(time.Unix(0, 0), req); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("market IOC: %v", err)
	}
	req.TIF = exchange.TIFUnspecified
	req.QuoteQty = 1
	if _, err := a.Submit(time.Unix(0, 0), req); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("quote qty: %v", err)
	}
}

func TestPlatformCodes(t *testing.T) {
	err := binanceErr(400, -2010, "Account has insufficient balance")
	var rej *exchange.Reject
	if !errors.As(err, &rej) || rej.Code != 201 || rej.Native != "-2010" {
		t.Fatalf("got %v", err)
	}
	err = binanceErr(400, -2011, "Unknown order sent.")
	if !errors.As(err, &rej) || rej.Code != 200 || rej.Native != "-2011" {
		t.Fatalf("got %v", err)
	}
	if err := binanceErr(429, -1003, "Too many requests"); !errors.As(err, new(*exchange.RateLimited)) {
		t.Fatalf("got %v", err)
	}
	if err := binanceErr(400, -1022, "Signature for this request is not valid."); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("got %v", err)
	}
	if err := binanceErr(400, -5021, "FOK"); !errors.As(err, &rej) || rej.Code != -5021 || rej.Native != "-5021" {
		t.Fatalf("got %v", err)
	}
	if err := binanceErr(400, -9999, "no such code"); !errors.As(err, &rej) || rej.Code != -9999 || rej.Native != "-9999" {
		t.Fatalf("got %v", err)
	}
}

func TestBatchAndWindow(t *testing.T) {
	a := mustUM(t)
	now := time.Unix(0, 0)
	if _, err := a.CancelBatch(now, nil); err == nil {
		t.Fatal("empty batch was accepted")
	}
	if _, err := a.CancelBatch(now, []exchange.CancelRef{
		{Instrument: btcInst(), ClientOrderID: cloidGTC},
		{Instrument: btcInst(), OrderID: "1001"},
	}); err == nil {
		t.Fatal("a mixed batch was accepted")
	}
	other := btcInst()
	other.ExchTicker = "ETHUSDT"
	if _, err := a.CancelBatch(now, []exchange.CancelRef{
		{Instrument: btcInst(), ClientOrderID: cloidGTC},
		{Instrument: other, ClientOrderID: cloidIOC},
	}); err == nil {
		t.Fatal("a two-symbol batch was accepted")
	}
	if _, err := a.(exchange.CancelOnDisconnect).ArmCancelOnDisconnect(now, time.Second, nil); err == nil {
		t.Fatal("countdown with no symbol was accepted")
	}
	h := mustHist(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	_, err := h.Fills(now, exchange.FillQuery{
		Instrument: btcInst(),
		StartNs:    start,
		EndNs:      start + int64(8*24*time.Hour),
	})
	if err == nil {
		t.Fatal("an 8-day window was accepted")
	}
	if _, err := h.Fills(now, exchange.FillQuery{
		Instrument: btcInst(), FromID: "100", StartNs: start,
	}); err == nil {
		t.Fatal("fromId combined with a time window was accepted")
	}
}

func TestPositionSide(t *testing.T) {
	if g := positionSize(0.002, "SHORT"); g != -0.002 {
		t.Fatalf("SHORT size %v", g)
	}
	if g := positionSize(-0.002, "SHORT"); g != -0.002 {
		t.Fatalf("negative SHORT size %v", g)
	}
	if g := positionSize(0.002, "BOTH"); g != 0.002 {
		t.Fatalf("BOTH size %v", g)
	}
	if g := positionSize(-0.002, "BOTH"); g != -0.002 {
		t.Fatalf("negative BOTH size %v", g)
	}
}

func TestQuoteAbsent(t *testing.T) {
	if decimal.Has(marketOrder(cloidMKT).Price) {
		t.Fatal("market price is present")
	}
}

func mustUM(t *testing.T) exchange.Account {
	t.Helper()
	a, err := accountFactory(kindUM)(exchange.AccountConfig{Credentials: conformance.TestCredentials(exchange.KeyEd25519, false)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustHist(t *testing.T) exchange.History {
	t.Helper()
	h, err := historyFactory(kindUM)(exchange.AccountConfig{Credentials: conformance.TestCredentials(exchange.KeyEd25519, false)})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestValidity: AccountConfig.Validity is recvWindow on REST, on the trade socket's frames and on
// its logon (D605). A window Binance does not take is refused when the account is built.
func TestValidity(t *testing.T) {
	cfg := exchange.AccountConfig{Credentials: conformance.TestCredentials(exchange.KeyEd25519, false), Validity: 2 * time.Second}
	now := conformance.DefaultNow
	for _, k := range []kind{kindUM, kindSpot} {
		a, err := accountFactory(k)(cfg)
		if err != nil {
			t.Fatal(err)
		}
		inst := btcInst()
		if k.name == kindSpot.name {
			inst = spotInst()
		}
		sub, err := a.Submit(now, &exchange.OrderRequest{
			Instrument: inst, Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: exchange.TIFGTC,
			Qty: 0.001, QuoteQty: decimal.Absent(), Price: 65000, ClientOrderID: cloidGTC,
		})
		if err != nil {
			t.Fatal(err)
		}
		q, err := a.QueryOrder(now, exchange.CancelRef{Instrument: inst, ClientOrderID: cloidGTC})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range []exchange.HTTPRequest{sub.Req, q.Req} {
			if !strings.Contains(string(r.Query), "recvWindow=2000&") {
				t.Errorf("%s %s: %s", k.name, r.Path, r.Query)
			}
		}
		ts, err := a.NewTradeStream()
		if err != nil {
			t.Fatal(err)
		}
		d, err := ts.Dial(now)
		if err != nil {
			t.Fatal(err)
		}
		c, err := ts.Cancel(now, 1, exchange.CancelRef{Instrument: inst, ClientOrderID: cloidGTC})
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range [][]byte{d.Frames[0], c} {
			if !strings.Contains(string(f), `"recvWindow":2000`) {
				t.Errorf("%s frame %s", k.name, f)
			}
		}
	}
	cfg.Validity = 61 * time.Second
	if _, err := accountFactory(kindUM)(cfg); err == nil {
		t.Error("a 61s window was taken")
	}
}
