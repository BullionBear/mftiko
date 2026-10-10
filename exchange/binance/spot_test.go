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

func TestSpotAccount(t *testing.T) {
	a := mustSpot(t)
	if _, ok := a.(exchange.CancelOnDisconnect); ok {
		t.Fatal("spot implements cancel on disconnect")
	}
	if lim := a.Limits(); lim.CancelBatch != 1 || lim.ClientOrderIDMaxLen < 19 {
		t.Fatalf("limits %+v", lim)
	}
	if _, err := a.NewStream(); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("NewStream: %v", err)
	}
	if _, err := a.Leverage(time.Unix(0, 0), spotInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("Leverage: %v", err)
	}
	now := time.Unix(0, 0)
	if _, err := a.CancelBatch(now, nil); err == nil {
		t.Fatal("an empty batch was accepted")
	}
	if _, err := a.CancelBatch(now, []exchange.CancelRef{
		{Instrument: spotInst(), ClientOrderID: cloidGTC},
		{Instrument: spotInst(), ClientOrderID: cloidIOC},
	}); err == nil {
		t.Fatal("a two-order batch was accepted")
	}
	one, err := a.CancelBatch(now, []exchange.CancelRef{{Instrument: spotInst(), ClientOrderID: cloidGTC}})
	if err != nil {
		t.Fatal(err)
	}
	if one.Req.Path != "/api/v3/order" || one.Req.Method != "DELETE" {
		t.Fatalf("batch cancel %s %s", one.Req.Method, one.Req.Path)
	}

	req := spotLimit(exchange.TIFGTC, cloidGTC)
	req.ReduceOnly = true
	if _, err := a.Submit(now, req); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("reduce-only: %v", err)
	}
	req = spotLimit(exchange.TIFPostOnly, cloidGTX)
	call, err := a.Submit(now, req)
	if err != nil {
		t.Fatal(err)
	}
	q := string(call.Req.Query)
	if !strings.Contains(q, "type=LIMIT_MAKER") || strings.Contains(q, "timeInForce") {
		t.Fatalf("post-only query %s", q)
	}
	req = spotMarket(cloidMKT)
	req.Qty = decimal.Absent()
	req.QuoteQty = 10
	call, err = a.Submit(now, req)
	if err != nil {
		t.Fatal(err)
	}
	if q = string(call.Req.Query); !strings.Contains(q, "quoteOrderQty=10.00") || strings.Contains(q, "quantity=") {
		t.Fatalf("quote query %s", q)
	}

	ts, err := a.NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	if !ts.Limits().Events {
		t.Fatal("spot trade stream does not carry events")
	}
	d, err := ts.Dial(time.UnixMilli(1767225600000))
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != spotProdTrade || len(d.Frames) != 2 {
		t.Fatalf("dial %s frames %d", d.URL, len(d.Frames))
	}
	if !strings.Contains(string(d.Frames[0]), `"method":"session.logon"`) ||
		!strings.Contains(string(d.Frames[1]), `"method":"userDataStream.subscribe"`) {
		t.Fatalf("frames %s %s", d.Frames[0], d.Frames[1])
	}
	frame, err := ts.Submit(time.UnixMilli(1767225600000), 1, spotLimit(exchange.TIFPostOnly, cloidGTX))
	if err != nil {
		t.Fatal(err)
	}
	s := string(frame)
	if !strings.Contains(s, `"type":"LIMIT_MAKER"`) || strings.Contains(s, "timeInForce") ||
		strings.Contains(s, "apiKey") || strings.Contains(s, "signature") {
		t.Fatalf("post-only frame %s", s)
	}
	frame, err = ts.Submit(time.UnixMilli(1767225600000), 2, &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: decimal.Absent(), QuoteQty: 10, Price: decimal.Absent(), ClientOrderID: cloidMKT,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(frame), `"quoteOrderQty":"10.00"`) {
		t.Fatalf("quote frame %s", frame)
	}

	testnet, err := accountFactory(kindSpot)(exchange.AccountConfig{
		Credentials:  conformance.TestCredentials(exchange.KeyEd25519, false),
		RESTEndpoint: "https://" + spotTestHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	tts, err := testnet.NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	td, err := tts.Dial(time.UnixMilli(1767225600000))
	if err != nil {
		t.Fatal(err)
	}
	if td.URL != spotTestTrade {
		t.Fatalf("testnet url %s", td.URL)
	}
}

func TestSpotHistoryWindow(t *testing.T) {
	h := mustSpotHist(t)
	tr := h.Traits()
	if !tr.ByID || tr.Descending || tr.MaxWindow != 24*time.Hour || tr.PageLimit != 1000 || tr.Retention != 0 {
		t.Fatalf("traits %+v", tr)
	}
	now := time.Unix(0, 0)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	if _, err := h.Fills(now, exchange.FillQuery{
		Instrument: spotInst(), StartNs: start, EndNs: start + int64(25*time.Hour),
	}); err == nil {
		t.Fatal("a 25-hour window was accepted")
	}
	if _, err := h.Fills(now, exchange.FillQuery{
		Instrument: spotInst(), FromID: "100", StartNs: start,
	}); err == nil {
		t.Fatal("fromId combined with a time window was accepted")
	}
	call, err := h.Fills(now, exchange.FillQuery{Instrument: spotInst(), FromID: "100", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if call.Req.Path != "/api/v3/myTrades" || !strings.Contains(string(call.Req.Query), "fromId=100") {
		t.Fatalf("fills %s %s", call.Req.Path, call.Req.Query)
	}
	order, err := h.Order(now, spotInst(), "1001")
	if err != nil {
		t.Fatal(err)
	}
	if order.Req.Path != "/api/v3/order" {
		t.Fatalf("order path %s", order.Req.Path)
	}
}

func TestSpotDecode(t *testing.T) {
	ts := mustSpotTrade(t)
	var s spotCapture
	if err := ts.Decode([]byte(`{"id":"logon","status":200,"result":{"apiKey":"k"}}`), &s); err != nil || s.ready != 0 {
		t.Fatalf("logon ready %d err %v", s.ready, err)
	}
	if err := ts.Decode([]byte(`{"id":"subscribe","status":200,"result":{"subscriptionId":0}}`), &s); err != nil || s.ready != 1 {
		t.Fatalf("subscribe ready %d err %v", s.ready, err)
	}
	if err := ts.Decode([]byte(`{"id":"subscribe","status":200}`), &s); err != nil || s.ready != 1 {
		t.Fatalf("second subscribe ready %d err %v", s.ready, err)
	}
	if err := ts.Decode([]byte(`{"id":"logon","status":401,"error":{"code":-2015,"msg":"Invalid API-key"}}`), &s); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("logon auth: %v", err)
	}

	rep := []byte(`{"subscriptionId":0,"event":{"e":"executionReport","E":1,"s":"BTCUSDT","c":"new","C":"","S":"BUY","o":"LIMIT","X":"FILLED","x":"TRADE","r":"NONE","i":9,"q":"0.001","p":"65000","z":"0.001","Z":"65","l":"0.001","L":"65000","n":"0","N":null,"T":2,"t":77,"m":false}}`)
	if err := ts.Decode(rep, &s); err != nil {
		t.Fatal(err)
	}
	if s.orders != 1 || s.fills != 1 || s.lastFill.FillID != "77" || s.lastFill.OrderID != "9" ||
		s.lastFill.Side != exchange.SideBuy || !decimal.Equal(s.lastFill.Price, 65000, 0.01) ||
		!decimal.Equal(s.lastFill.Qty, 0.001, 0.00001) || s.lastFill.FeeAsset != "" ||
		s.lastOrder.ClientOrderID != "new" {
		t.Fatalf("fill %+v order %+v counts %d %d", s.lastFill, s.lastOrder, s.orders, s.fills)
	}
	if !decimal.Equal(s.lastOrder.AvgPrice, 65000, 0.01) {
		t.Fatalf("avg %v", s.lastOrder.AvgPrice)
	}

	cancel := []byte(`{"e":"executionReport","E":3,"s":"BTCUSDT","c":"cancel","C":"orig","S":"BUY","o":"LIMIT","X":"CANCELED","x":"CANCELED","r":"NONE","i":9,"q":"0.001","p":"65000","z":"0","Z":"0","l":"0","L":"0","n":"0","N":null,"T":3,"t":-1,"m":false}`)
	if err := ts.Decode(cancel, &s); err != nil {
		t.Fatal(err)
	}
	if s.fills != 1 || s.lastOrder.ClientOrderID != "orig" || s.lastOrder.Status != exchange.OrderStatusCancelled {
		t.Fatalf("cancel order %+v fills %d", s.lastOrder, s.fills)
	}

	bal := []byte(`{"e":"outboundAccountPosition","B":[{"a":"USDT","f":"0.00000000","l":"0.00000000"}]}`)
	if err := ts.Decode(bal, &s); err != nil || s.balances != 1 || len(s.lastBal) != 1 || s.lastBal[0].Asset != "USDT" {
		t.Fatalf("balances %d %+v err %v", s.balances, s.lastBal, err)
	}
	if err := ts.Decode([]byte(`{"e":"balanceUpdate","a":"USDT","d":"1"}`), &s); err != nil || s.balances != 1 {
		t.Fatalf("balanceUpdate balances %d err %v", s.balances, err)
	}
	if err := ts.Decode([]byte(`{"e":"eventStreamTerminated"}`), &s); err != nil || s.expired != "eventStreamTerminated" {
		t.Fatalf("expired %q err %v", s.expired, err)
	}
	if err := ts.Decode([]byte(`{"conformance":"a frame no venue sends"}`), &s); err == nil {
		t.Fatal("an unknown object was accepted")
	}
	if err := ts.Decode([]byte(`["conformance",1]`), &s); err == nil {
		t.Fatal("an unknown array was accepted")
	}

	page, err := parseFills(&exchange.HTTPResponse{Status: 200, Body: []byte(`[{"symbol":"BTCUSDT","id":77,"orderId":9,"price":"65000","qty":"0.001","commission":"0","commissionAsset":null,"time":2,"isBuyer":true,"isMaker":false}]`)}, 1, VenueSpot)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Fills) != 1 || page.Fills[0].FillID != "77" || page.Fills[0].Side != exchange.SideBuy || page.Fills[0].FeeAsset != "" {
		t.Fatalf("myTrades %+v", page.Fills)
	}
}

func mustSpot(t *testing.T) exchange.Account {
	t.Helper()
	a, err := accountFactory(kindSpot)(exchange.AccountConfig{Credentials: conformance.TestCredentials(exchange.KeyEd25519, false)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustSpotHist(t *testing.T) exchange.History {
	t.Helper()
	h, err := historyFactory(kindSpot)(exchange.AccountConfig{Credentials: conformance.TestCredentials(exchange.KeyEd25519, false)})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func mustSpotTrade(t *testing.T) exchange.TradeStream {
	t.Helper()
	ts, err := mustSpot(t).NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

type spotCapture struct {
	ready     int
	orders    int
	fills     int
	balances  int
	expired   string
	lastOrder exchange.OrderUpdate
	lastFill  exchange.Fill
	lastBal   []exchange.Balance
}

func (s *spotCapture) Ready() { s.ready++ }
func (s *spotCapture) Expired(reason string) {
	s.expired = reason
}
func (s *spotCapture) Order(e *exchange.OrderUpdate) {
	s.orders++
	s.lastOrder = *e
}
func (s *spotCapture) Fill(e *exchange.Fill) {
	s.fills++
	s.lastFill = *e
}
func (s *spotCapture) Balances(e []exchange.Balance) {
	s.balances++
	s.lastBal = append([]exchange.Balance(nil), e...)
}
func (s *spotCapture) Positions([]exchange.Position) {}
func (s *spotCapture) Submitted(uint64, *exchange.OrderResult, error) {
}
func (s *spotCapture) Canceled(uint64, error) {}
func (s *spotCapture) Used([]exchange.Used)   {}
