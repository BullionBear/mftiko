package bybit

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestAuthFrame(t *testing.T) {
	a := mustAcct(t)
	s, err := a.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.Dial(conformance.DefaultNow, "")
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != prodPrivate {
		t.Fatalf("url %s", d.URL)
	}
	if len(d.Frames) != 2 {
		t.Fatalf("frames %d", len(d.Frames))
	}
	auth := string(d.Frames[0])
	want := `{"op":"auth","args":["conformance-api-key",1767225610000,"80dd9d3b567890c6be44d5c1e3d1cec9ec67083e3f8d1cc84f3cc70baafacbad"]}`
	if auth != want {
		t.Fatalf("auth\n%s\nwant\n%s", auth, want)
	}
	sub := string(d.Frames[1])
	if sub != `{"op":"subscribe","args":["order","execution","position","wallet","dcp"]}` {
		t.Fatalf("subscribe %s", sub)
	}
	if prep, err := s.Prepare(conformance.DefaultNow); err != nil || prep != nil {
		t.Fatalf("prepare %v %v", prep, err)
	}
	if ref, err := s.Refresh(conformance.DefaultNow); err != nil || ref != nil {
		t.Fatalf("refresh %v %v", ref, err)
	}
	ts, err := a.NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	td, err := ts.Dial(conformance.DefaultNow)
	if err != nil {
		t.Fatal(err)
	}
	if td.URL != prodTrade || len(td.Frames) != 1 || string(td.Frames[0]) != auth {
		t.Fatalf("trade dial %s %q", td.URL, td.Frames)
	}
	if ts.Limits().Events || string(ts.Ping()) != `{"op":"ping"}` {
		t.Fatalf("limits %+v ping %s", ts.Limits(), ts.Ping())
	}
}

func TestTestnetSocket(t *testing.T) {
	a := mustAcct(t)
	a.cfg.RESTEndpoint = testREST
	s, err := a.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.Dial(conformance.DefaultNow, "")
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != testPrivate {
		t.Fatalf("url %s", d.URL)
	}
}

func TestOrderBodies(t *testing.T) {
	cases := []struct {
		name string
		req  *exchange.OrderRequest
		want string
	}{
		{"limit", limitOrder(exchange.TIFGTC, cloidGTC),
			`{"category":"linear","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","qty":"0.001","price":"65000.0","timeInForce":"GTC","orderLinkId":"1767225600000000001"}`},
		{"postonly", limitOrder(exchange.TIFPostOnly, cloidGTX),
			`{"category":"linear","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","qty":"0.001","price":"65000.0","timeInForce":"PostOnly","orderLinkId":"1767225600000000004"}`},
		{"market", marketOrder(cloidMKT),
			`{"category":"linear","symbol":"BTCUSDT","side":"Buy","orderType":"Market","qty":"0.001","orderLinkId":"1767225600000000005"}`},
		{"spot quote", spotQuote(cloidGTC),
			`{"category":"spot","symbol":"BTCUSDT","side":"Buy","orderType":"Market","qty":"200.00000000","orderLinkId":"1767225600000000001","marketUnit":"quoteCoin"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, err := createBodyOf(tc.req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := marshalBody(body)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != tc.want {
				t.Fatalf("body\n%s\nwant\n%s", raw, tc.want)
			}
		})
	}
}

func TestCreateRefuses(t *testing.T) {
	if _, err := createBodyOf(limitOrder(exchange.TIFGTC, cloidGTC)); err != nil {
		t.Fatal(err)
	}
	spot := limitOrder(exchange.TIFGTC, cloidGTC)
	spot.Instrument = spotInst()
	spot.ReduceOnly = true
	if _, err := createBodyOf(spot); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("spot reduce-only %v", err)
	}
	m := marketOrder(cloidMKT)
	m.TIF = exchange.TIFGTC
	if _, err := createBodyOf(m); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("market gtc %v", err)
	}
	a := mustAcct(t)
	refs := []exchange.CancelRef{
		{Instrument: btcInst(), ClientOrderID: cloidGTC},
		{Instrument: spotInst(), ClientOrderID: cloidIOC},
	}
	if _, err := a.CancelBatch(conformance.DefaultNow, refs); err == nil || !strings.Contains(err.Error(), "one category") {
		t.Fatalf("mixed batch %v", err)
	}
	if _, err := a.ArmCancelOnDisconnect(conformance.DefaultNow, 0, nil); err == nil {
		t.Fatal("zero window was accepted")
	}
	if _, err := a.ArmCancelOnDisconnect(conformance.DefaultNow, 1500*time.Millisecond, nil); err == nil {
		t.Fatal("fractional window was accepted")
	}
}

func TestClassify(t *testing.T) {
	if err := classify(200, 10004, "Error sign"); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("auth %v", err)
	}
	if _, ok := classify(200, 10006, "Too many visits").(*exchange.RateLimited); !ok {
		t.Fatalf("rate %T", classify(200, 10006, "x"))
	}
	if _, ok := classify(200, 10403, "exceed ip rate limit").(*exchange.RateLimited); !ok {
		t.Fatal("trade ip limit")
	}
	if !errors.Is(classify(200, 10019, "ws trade service is restarting"), exchange.ErrUnknownOutcome) {
		t.Fatal("trade restart")
	}
	if !errors.Is(classify(200, 10000, "Server Timeout"), exchange.ErrUnknownOutcome) {
		t.Fatal("timeout")
	}
	rej, ok := classify(200, 110007, "Available balance is insufficient").(*exchange.Reject)
	if !ok || rej.Code != 201 || rej.Native != "110007" {
		t.Fatalf("reject %+v", rej)
	}
	rej, ok = classify(200, 3500716, "DCP of this UID is inactive").(*exchange.Reject)
	if !ok || rej.Code != 3500716 {
		t.Fatalf("dcp %+v", rej)
	}
}

func TestStatusAndCategory(t *testing.T) {
	if statusOf("PartiallyFilledCanceled") != exchange.OrderStatusCancelled {
		t.Fatal("partial cancel")
	}
	if statusOf("Untriggered") != exchange.OrderStatusPendingNew {
		t.Fatal("untriggered")
	}
	if got := exchKey("linear", "BTCUSDT-16OCT26"); got.Category != symbol.Future {
		t.Fatalf("future %s", got.Category)
	}
	if got := exchKey("linear", "BTCPERP"); got.Category != symbol.Perp {
		t.Fatalf("perp %s", got.Category)
	}
	if got := exchKey("inverse", "BTCUSDH27"); got.Category != symbol.Inverse {
		t.Fatalf("inverse %s", got.Category)
	}
}

func TestDecodePrivate(t *testing.T) {
	s := &privateStream{}
	sink := &capture{}
	auth := []byte(`{"success":true,"ret_msg":"","op":"auth","conn_id":"c1"}`)
	if err := s.Decode(auth, sink); err != nil || sink.ready {
		t.Fatalf("auth ready=%v err=%v", sink.ready, err)
	}
	bad := []byte(`{"success":false,"ret_msg":"Params Error","op":"auth","conn_id":"c1"}`)
	if err := s.Decode(bad, sink); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("bad auth %v", err)
	}
	sub := []byte(`{"success":true,"ret_msg":"","op":"subscribe","conn_id":"c1"}`)
	if err := s.Decode(sub, sink); err != nil || !sink.ready {
		t.Fatalf("subscribe ready=%v err=%v", sink.ready, err)
	}
	pong := []byte(`{"success":true,"ret_msg":"pong","op":"ping","conn_id":"c1"}`)
	if err := s.Decode(pong, sink); err != nil {
		t.Fatal(err)
	}
	order := []byte(`{"topic":"order","creationTime":1672364262474,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"oid","orderLinkId":"1767225600000000001","side":"Buy","orderType":"Limit","orderStatus":"Filled","qty":"0.001","price":"65000","avgPrice":"65010.5","cumExecQty":"0.001","rejectReason":"EC_NoError","updatedTime":"1672364262457"}]}`)
	if err := s.Decode(order, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.orders) != 1 || sink.orders[0].Status != exchange.OrderStatusFilled || sink.orders[0].Exch.Category != symbol.Perp {
		t.Fatalf("order %+v", sink.orders)
	}
	if sink.orders[0].ExchTsNs != 1672364262457*int64(time.Millisecond) {
		t.Fatalf("ts %d", sink.orders[0].ExchTsNs)
	}
	exec := []byte(`{"topic":"execution","data":[{"category":"linear","symbol":"BTCUSDT","execId":"eid","execType":"Trade","execPrice":"65010.5","execQty":"0.001","execFee":"0.03","feeCurrency":"USDT","orderId":"oid","orderLinkId":"1767225600000000001","side":"Buy","isMaker":false,"execTime":"1672364262457"},{"category":"linear","symbol":"BTCUSDT","execId":"fund","execType":"Funding","execPrice":"1","execQty":"1","orderId":"oid","side":"Buy","execTime":"1672364262457"}]}`)
	if err := s.Decode(exec, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.fills) != 1 || sink.fills[0].FillID != "eid" || sink.fills[0].Liquidity != "taker" || sink.fills[0].FeeAsset != "USDT" {
		t.Fatalf("fills %+v", sink.fills)
	}
	pos := []byte(`{"topic":"position","data":[{"category":"linear","symbol":"BTCUSDT","side":"Sell","size":"0.002","avgPrice":"65000","markPrice":"65100","unrealisedPnl":"-0.2","leverage":"10","updatedTime":"1672364262457"}]}`)
	if err := s.Decode(pos, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.pos) != 1 || sink.pos[0].Size != -0.002 || sink.pos[0].Leverage != 10 || sink.pos[0].EntryPrice != 65000 {
		t.Fatalf("pos %+v", sink.pos)
	}
	// The private topic sends entryPrice and omits avgPrice. A missing number decodes as 0.
	wsPos := []byte(`{"topic":"position","data":[{"category":"linear","symbol":"BTCUSDT","side":"Buy","size":"0.001","entryPrice":"80504.7","markPrice":"80504.6","unrealisedPnl":"-0.0001","leverage":"10"}]}`)
	sink.pos = nil
	if err := s.Decode(wsPos, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.pos) != 1 || sink.pos[0].Size != 0.001 || sink.pos[0].EntryPrice != 80504.7 {
		t.Fatalf("ws pos %+v", sink.pos)
	}
	wal := []byte(`{"topic":"wallet","data":[{"coin":[{"coin":"USDT","walletBalance":"100","locked":"10","totalOrderIM":"5"},{"coin":"BTC","walletBalance":"0","locked":"0","totalOrderIM":"0"}]}]}`)
	if err := s.Decode(wal, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.bals) != 1 || sink.bals[0].Asset != "USDT" || sink.bals[0].Free != 85 || sink.bals[0].Locked != 15 {
		t.Fatalf("bal %+v", sink.bals)
	}
	if err := s.Decode([]byte(`{"conformance":"no"}`), sink); err == nil {
		t.Fatal("unknown frame accepted")
	}
	// The frame buffer is reused. What the sink kept must not point into it.
	frame := append([]byte(nil), order...)
	sink.orders = nil
	if err := s.Decode(frame, sink); err != nil {
		t.Fatal(err)
	}
	got := sink.orders[0].OrderID
	for i := range frame {
		frame[i] = 'x'
	}
	if sink.orders[0].OrderID != got || got != "oid" {
		t.Fatal("order id aliased the frame")
	}
}

func TestUsage(t *testing.T) {
	// X-Bapi-Limit-Reset-Timestamp is the value recorded on the trade socket
	// (order.create). REST reports the same header, in milliseconds.
	resp := &exchange.HTTPResponse{Header: []exchange.Header{
		{Name: "X-Bapi-Limit", Value: "10"},
		{Name: "X-Bapi-Limit-Status", Value: "9"},
		{Name: "X-Bapi-Limit-Reset-Timestamp", Value: "1711001595208"},
	}}
	got := usedFrom(resp, time.Time{})
	want := time.UnixMilli(1711001595208).UTC()
	if len(got) != 1 || got[0].Bucket != "endpoint" || got[0].N != 1 || !got[0].ResetAt.Equal(want) {
		t.Fatalf("%+v", got)
	}
	missing := usedFrom(&exchange.HTTPResponse{Header: []exchange.Header{
		{Name: "X-Bapi-Limit", Value: "10"},
		{Name: "X-Bapi-Limit-Status", Value: "9"},
	}}, time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC))
	if len(missing) != 1 || !missing[0].ResetAt.IsZero() {
		t.Fatalf("missing reset %+v", missing)
	}
	_, err := readEnv(&exchange.HTTPResponse{
		Status: http.StatusTooManyRequests,
		Header: []exchange.Header{{Name: "Retry-After", Value: "8"}},
		Body:   []byte(`{"retCode":10006,"retMsg":"Too many visits"}`),
	})
	var rl *exchange.RateLimited
	if !errors.As(err, &rl) || rl.RetryAfterMs != 8000 {
		t.Fatalf("retry %v", err)
	}
}

func TestSignedCreate(t *testing.T) {
	a := mustAcct(t)
	c, err := a.Submit(conformance.DefaultNow, limitOrder(exchange.TIFGTC, cloidGTC))
	if err != nil {
		t.Fatal(err)
	}
	if c.Req.Base != prodREST || c.Req.Path != "/v5/order/create" || !c.Req.Mutates {
		t.Fatalf("req %+v", c.Req)
	}
	if !bytes.Contains(c.Req.Body, []byte(`"orderLinkId":"1767225600000000001"`)) {
		t.Fatalf("body %s", c.Req.Body)
	}
	var key, ts, sign string
	for _, h := range c.Req.Header {
		switch h.Name {
		case "X-BAPI-API-KEY":
			key = h.Value
		case "X-BAPI-TIMESTAMP":
			ts = h.Value
		case "X-BAPI-SIGN":
			sign = h.Value
		}
	}
	if key != conformance.TestKey || ts != "1767225600000" || len(sign) != 64 {
		t.Fatalf("headers key=%s ts=%s sign=%s", key, ts, sign)
	}
	if strings.Contains(sign, conformance.TestHMACSecret) {
		t.Fatal("signature contains the secret")
	}
}

type capture struct {
	ready  bool
	orders []exchange.OrderUpdate
	fills  []exchange.Fill
	pos    []exchange.Position
	bals   []exchange.Balance
}

func (c *capture) Ready()         { c.ready = true }
func (c *capture) Expired(string) {}
func (c *capture) Order(e *exchange.OrderUpdate) {
	o := *e
	o.OrderID = strings.Clone(o.OrderID)
	o.ClientOrderID = strings.Clone(o.ClientOrderID)
	o.RejectReason = strings.Clone(o.RejectReason)
	o.Exch.ExchTicker = strings.Clone(o.Exch.ExchTicker)
	c.orders = append(c.orders, o)
}
func (c *capture) Fill(e *exchange.Fill) {
	f := *e
	f.FillID = strings.Clone(f.FillID)
	f.OrderID = strings.Clone(f.OrderID)
	f.ClientOrderID = strings.Clone(f.ClientOrderID)
	f.FeeAsset = strings.Clone(f.FeeAsset)
	f.Liquidity = strings.Clone(f.Liquidity)
	f.Exch.ExchTicker = strings.Clone(f.Exch.ExchTicker)
	c.fills = append(c.fills, f)
}
func (c *capture) Balances(e []exchange.Balance) {
	for _, b := range e {
		b.Asset = strings.Clone(b.Asset)
		c.bals = append(c.bals, b)
	}
}
func (c *capture) Positions(e []exchange.Position) {
	for _, p := range e {
		p.Exch.ExchTicker = strings.Clone(p.Exch.ExchTicker)
		c.pos = append(c.pos, p)
	}
}

func mustAcct(t *testing.T) *account {
	t.Helper()
	a, err := Declare().AccountKinds[0].Account(exchange.AccountConfig{
		APIID: 1, Credentials: conformance.TestCredentials(exchange.KeyHMAC, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	return a.(*account)
}

func btcInst() *symbol.Info {
	return &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: Name, Category: symbol.Perp},
		ExchTicker: "BTCUSDT",
		Filters:    symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001},
	}
}

func spotInst() *symbol.Info {
	return &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: Name, Category: symbol.Spot},
		ExchTicker: "BTCUSDT",
		Filters:    symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001},
	}
}

func limitOrder(tif exchange.TimeInForce, cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: btcInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: tif,
		Qty: 0.001, QuoteQty: decimal.Absent(), Price: 65000, ClientOrderID: cloid,
	}
}

func marketOrder(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: btcInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: 0.001, QuoteQty: decimal.Absent(), Price: decimal.Absent(), ClientOrderID: cloid,
	}
}

func spotQuote(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: decimal.Absent(), QuoteQty: 200, Price: decimal.Absent(), ClientOrderID: cloid,
	}
}
