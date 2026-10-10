package bybit

import (
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestHistoryTraits(t *testing.T) {
	tr := mustHist(t).Traits()
	if !tr.Descending || tr.ByID || tr.MaxWindow != fillWindow || tr.Retention != fillKeep || tr.PageLimit != fillLimit {
		t.Fatalf("traits %+v", tr)
	}
}

func TestHistoryRequests(t *testing.T) {
	h := mustHist(t)
	now := conformance.DefaultNow
	call, err := h.Fills(now, exchange.FillQuery{
		Instrument: btcInst(),
		StartNs:    now.UnixNano(),
		EndNs:      now.Add(time.Hour).UnixNano(),
		Limit:      50,
		Cursor:     "132766:2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Req.Path != "/v5/execution/list" {
		t.Fatalf("path %s", call.Req.Path)
	}
	want := "category=linear&symbol=BTCUSDT&startTime=1767225600000&endTime=1767229200000&limit=50&cursor=132766%3A2"
	if string(call.Req.Query) != want {
		t.Fatalf("query\n%s\nwant\n%s", call.Req.Query, want)
	}
	if _, err := h.Fills(now, exchange.FillQuery{Instrument: btcInst(), FromID: "1"}); err == nil {
		t.Fatal("fromId was accepted")
	}
	long := exchange.FillQuery{Instrument: btcInst(), StartNs: now.UnixNano(), EndNs: now.Add(8 * 24 * time.Hour).UnixNano()}
	if _, err := h.Fills(now, long); err == nil || !strings.Contains(err.Error(), "longer than") {
		t.Fatalf("long window %v", err)
	}
	back := exchange.FillQuery{Instrument: btcInst(), StartNs: now.Add(time.Hour).UnixNano(), EndNs: now.UnixNano()}
	if _, err := h.Fills(now, back); err == nil || !strings.Contains(err.Error(), "ends before") {
		t.Fatalf("reversed window %v", err)
	}
	ord, err := h.Order(now, btcInst(), "90f08fa4-4426-474b-8dea-f68e2b0fbd31")
	if err != nil {
		t.Fatal(err)
	}
	if ord.Req.Path != "/v5/order/history" || string(ord.Req.Query) != "category=linear&symbol=BTCUSDT&orderId=90f08fa4-4426-474b-8dea-f68e2b0fbd31" {
		t.Fatalf("order %s %s", ord.Req.Path, ord.Req.Query)
	}
	if _, err := h.Order(now, btcInst(), ""); err == nil {
		t.Fatal("empty order id")
	}
}

func TestParseFills(t *testing.T) {
	body := []byte(`{"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"132766%3A2%2C132766%3A2","list":[
		{"symbol":"BTCUSDT","orderId":"9953777d-8b64-4044-b754-6ce72220c2bb","orderLinkId":"1154756132126860291","side":"Buy","execFee":"0.04427759","execId":"92ff1578-ccd3-5a89-beb4-72872e7dd82f","execPrice":"80504.7","execQty":"0.001","execType":"Trade","execTime":"1791628437611","feeCurrency":"USDT","isMaker":false},
		{"symbol":"BTCUSDT","orderId":"oid","execId":"fund","execType":"Funding","execPrice":"1","execQty":"1","side":"Buy","execTime":"1791628437611"}
	]},"time":1791628437700}`)
	page, err := parseFills(&exchange.HTTPResponse{Status: 200, Body: body}, "")
	if err != nil {
		t.Fatal(err)
	}
	if page.Next != "132766%3A2%2C132766%3A2" || len(page.Fills) != 1 {
		t.Fatalf("page next=%q fills=%d", page.Next, len(page.Fills))
	}
	f := page.Fills[0]
	if f.FillID != "92ff1578-ccd3-5a89-beb4-72872e7dd82f" || f.OrderID != "9953777d-8b64-4044-b754-6ce72220c2bb" ||
		f.Side != exchange.SideBuy || f.Price != 80504.7 || f.Qty != 0.001 || f.Liquidity != "taker" || f.FeeAsset != "USDT" {
		t.Fatalf("fill %+v", f)
	}
	again, err := parseFills(&exchange.HTTPResponse{Status: 200, Body: body}, page.Next)
	if err != nil {
		t.Fatal(err)
	}
	if again.Next != "" {
		t.Fatalf("same cursor continued %q", again.Next)
	}
	missing, err := parseHistoryOrder(&exchange.HTTPResponse{Status: 200, Body: []byte(`{"retCode":0,"retMsg":"OK","result":{"list":[],"category":"linear"},"time":1}`)}, "missing")
	if err == nil || missing != nil {
		t.Fatalf("missing %+v %v", missing, err)
	}
	got, err := parseHistoryOrder(&exchange.HTTPResponse{Status: 200, Body: []byte(`{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","orderId":"9953777d-8b64-4044-b754-6ce72220c2bb","orderLinkId":"1154756132126860291","side":"Buy","orderStatus":"Filled","orderType":"Market","qty":"0.001","price":"82114.6","avgPrice":"80504.7","cumExecQty":"0.001","createdTime":"1791628437600","updatedTime":"1791628437611"}]},"time":1791628437700}`)}, "9953777d-8b64-4044-b754-6ce72220c2bb")
	if err != nil {
		t.Fatal(err)
	}
	if got.OrderID != "9953777d-8b64-4044-b754-6ce72220c2bb" || got.Status != exchange.OrderStatusFilled || got.FilledQty != 0.001 {
		t.Fatalf("order %+v", got)
	}
}

func mustHist(t *testing.T) *history {
	t.Helper()
	h, err := Declare().AccountKinds[0].History(exchange.AccountConfig{
		APIID: 1, Credentials: conformance.TestCredentials(exchange.KeyHMAC, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h.(*history)
}
