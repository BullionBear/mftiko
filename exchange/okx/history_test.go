package okx

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestHistoryTraits(t *testing.T) {
	tr := mustHist(t).Traits()
	if !tr.Descending || tr.ByID || tr.MaxWindow != 0 || tr.Retention != fillKeep || tr.PageLimit != fillLimit {
		t.Fatalf("traits %+v", tr)
	}
}

func TestHistoryRequests(t *testing.T) {
	h := mustHist(t)
	now := conformance.DefaultNow
	call, err := h.Fills(now, exchange.FillQuery{
		Instrument: spotInst(),
		StartNs:    now.UnixNano(),
		EndNs:      now.Add(time.Hour).UnixNano(),
		Limit:      50,
		Cursor:     "680800019754098688",
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Req.Path != "/api/v5/trade/fills-history" {
		t.Fatalf("path %s", call.Req.Path)
	}
	want := "instType=SPOT&instId=BTC-USDT&begin=1767225600000&end=1767229200000&limit=50&after=680800019754098688"
	if string(call.Req.Query) != want {
		t.Fatalf("query\n%s\nwant\n%s", call.Req.Query, want)
	}
	if call.Req.Base != prodREST {
		t.Fatalf("base %s", call.Req.Base)
	}
	if headerValue(call.Req.Header, "x-simulated-trading") != "" {
		t.Fatal("production request carries the demo header")
	}

	if _, err := h.Fills(now, exchange.FillQuery{Instrument: spotInst(), FromID: "1"}); err == nil || !strings.Contains(err.Error(), "trade id") {
		t.Fatalf("fromId %v", err)
	}
	back := exchange.FillQuery{Instrument: spotInst(), StartNs: now.Add(time.Hour).UnixNano(), EndNs: now.UnixNano()}
	if _, err := h.Fills(now, back); err == nil || !strings.Contains(err.Error(), "ends before") {
		t.Fatalf("reversed window %v", err)
	}
	if _, err := h.Fills(now, exchange.FillQuery{}); err == nil {
		t.Fatal("missing instrument")
	}
	wide, err := h.Fills(now, exchange.FillQuery{
		Instrument: spotInst(),
		StartNs:    now.UnixNano(),
		EndNs:      now.Add(90 * 24 * time.Hour).UnixNano(),
		Limit:      1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wide.Req.Query), "limit=100") {
		t.Fatalf("limit not clamped: %s", wide.Req.Query)
	}

	swap, err := h.Fills(now, exchange.FillQuery{Instrument: swapInst()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(swap.Req.Query), "instType=SWAP&instId=BTC-USDT-SWAP&limit=100") {
		t.Fatalf("swap %s", swap.Req.Query)
	}
	inv := &symbol.Info{Ticker: symbol.UniversalTicker{Venue: Name, Category: symbol.Inverse}, ExchTicker: "BTC-USD-SWAP"}
	coin, err := h.Fills(now, exchange.FillQuery{Instrument: inv})
	if err != nil || !strings.HasPrefix(string(coin.Req.Query), "instType=SWAP&instId=BTC-USD-SWAP") {
		t.Fatalf("inverse %v %s", err, coin.Req.Query)
	}
	dated := &symbol.Info{
		Ticker: symbol.UniversalTicker{Venue: Name, Category: symbol.Inverse}, ExchTicker: "BTC-USD-261030", ExpiryNs: 1,
	}
	fut, err := h.Fills(now, exchange.FillQuery{Instrument: dated})
	if err != nil || !strings.HasPrefix(string(fut.Req.Query), "instType=FUTURES&instId=BTC-USD-261030") {
		t.Fatalf("dated inverse %v %s", err, fut.Req.Query)
	}

	ord, err := h.Order(now, spotInst(), "680800019749904384")
	if err != nil {
		t.Fatal(err)
	}
	if ord.Req.Path != "/api/v5/trade/order" || string(ord.Req.Query) != "instId=BTC-USDT&ordId=680800019749904384" {
		t.Fatalf("order %s %s", ord.Req.Path, ord.Req.Query)
	}
	if _, err := h.Order(now, spotInst(), ""); err == nil {
		t.Fatal("empty order id")
	}

	demo, err := Declare().AccountKinds[0].History(exchange.AccountConfig{
		APIID: 1, Credentials: conformance.TestCredentials(exchange.KeyHMAC, true), Simulated: true,
		RESTEndpoint: "https://example.test/",
	})
	if err != nil {
		t.Fatal(err)
	}
	dcall, err := demo.Fills(now, exchange.FillQuery{Instrument: spotInst()})
	if err != nil {
		t.Fatal(err)
	}
	if dcall.Req.Base != "https://example.test" || headerValue(dcall.Req.Header, "x-simulated-trading") != "1" {
		t.Fatalf("demo %+v", dcall.Req)
	}
}

func TestParseFills(t *testing.T) {
	body := []byte(`{"code":"0","msg":"","data":[
		{"instType":"SPOT","instId":"BTC-USDT","tradeId":"744876980","ordId":"680800019749904384","clOrdId":"1767225600000000005","billId":"680800019754098688","fillPx":"51858","fillSz":"0.001","side":"buy","execType":"T","feeCcy":"USDT","fee":"-0.01","fillTime":"1708587373361","ts":"1708587373362"},
		{"instType":"SPOT","instId":"BTC-USDT","tradeId":"","ordId":"1","billId":"680800019754098687","fillPx":"1","fillSz":"1","side":"buy","ts":"1"},
		{"instType":"SPOT","instId":"BTC-USDT","tradeId":"-7","ordId":"2","billId":"680800019754098686","fillPx":"50000","fillSz":"0.01","side":"sell","execType":"","feeCcy":"USDT","fee":"-0.2","fillTime":"1708587373000"}
	]}`)
	page, err := parseFills(&exchange.HTTPResponse{Status: 200, Body: body}, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	if page.Next != "680800019754098686" || len(page.Fills) != 2 {
		t.Fatalf("page next=%q fills=%d", page.Next, len(page.Fills))
	}
	f := page.Fills[0]
	if f.FillID != "744876980" || f.OrderID != "680800019749904384" || f.ClientOrderID != "1767225600000000005" ||
		f.Side != exchange.SideBuy || f.Price != 51858 || f.Qty != 0.001 || f.Liquidity != "taker" ||
		f.Fee != -0.01 || f.FeeAsset != "USDT" || f.Exch.ExchTicker != "BTC-USDT" {
		t.Fatalf("fill %+v", f)
	}
	if page.Fills[1].FillID != "-7" || page.Fills[1].Liquidity != "" || page.Fills[1].Side != exchange.SideSell {
		t.Fatalf("liquidation %+v", page.Fills[1])
	}
	again, err := parseFills(&exchange.HTTPResponse{Status: 200, Body: body}, page.Next, 3)
	if err != nil {
		t.Fatal(err)
	}
	if again.Next != "" {
		t.Fatalf("same cursor continued %q", again.Next)
	}
	short, err := parseFills(&exchange.HTTPResponse{Status: 200, Body: body}, "", 100)
	if err != nil || short.Next != "" {
		t.Fatalf("short page next=%q %v", short.Next, err)
	}

	_, err = parseFills(&exchange.HTTPResponse{Status: 200, Body: []byte(`{"code":"50011","msg":"Rate limit reached","data":[]}`)}, "", 100)
	var rl *exchange.RateLimited
	if !errors.As(err, &rl) {
		t.Fatalf("rate %v", err)
	}

	missing, err := parseHistoryOrder(&exchange.HTTPResponse{Status: 200, Body: []byte(`{"code":"0","msg":"","data":[]}`)}, "missing")
	if err == nil || missing != nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing %+v %v", missing, err)
	}
	got, err := parseHistoryOrder(&exchange.HTTPResponse{Status: 200, Body: []byte(`{"code":"0","data":[{"instType":"SPOT","instId":"BTC-USDT","ordId":"680800019749904384","clOrdId":"1767225600000000005","side":"buy","ordType":"market","state":"filled","sz":"0.001","accFillSz":"0.001","avgPx":"51858","tgtCcy":"base_ccy","cTime":"1708587373361","uTime":"1708587373362"}]}`)}, "680800019749904384")
	if err != nil {
		t.Fatal(err)
	}
	if got.OrderID != "680800019749904384" || got.Status != exchange.OrderStatusFilled || got.FilledQty != 0.001 || got.ClientOrderID != "1767225600000000005" {
		t.Fatalf("order %+v", got)
	}
}

func mustHist(t *testing.T) *history {
	t.Helper()
	h, err := Declare().AccountKinds[0].History(exchange.AccountConfig{
		APIID: 1, Credentials: conformance.TestCredentials(exchange.KeyHMAC, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h.(*history)
}
