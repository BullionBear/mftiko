package okx

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestDial(t *testing.T) {
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
	login := string(d.Frames[0])
	want := `{"op":"login","args":[{"apiKey":"conformance-api-key","passphrase":"conformance-passphrase-9a8b","timestamp":"1767225600","sign":"IwPxl1qrO0GDAlZPkR5Qi+gPFmS5fto6C/diEDSUjvY="}]}`
	if login != want {
		t.Fatalf("login\n%s\nwant\n%s", login, want)
	}
	sub := string(d.Frames[1])
	if sub != `{"op":"subscribe","args":[{"channel":"orders","instType":"ANY"},{"channel":"positions","instType":"ANY"},{"channel":"account"}]}` {
		t.Fatalf("subscribe %s", sub)
	}
	if prep, err := s.Prepare(conformance.DefaultNow); err != nil || prep != nil {
		t.Fatalf("prepare %v %v", prep, err)
	}
	if ref, err := s.Refresh(conformance.DefaultNow); err != nil || ref != nil {
		t.Fatalf("refresh %v %v", ref, err)
	}
	if string(s.Ping()) != "ping" {
		t.Fatalf("ping %s", s.Ping())
	}
	if _, err := a.NewTradeStream(); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("trade stream %v", err)
	}
}

func TestSimulated(t *testing.T) {
	a := mustAcct(t)
	a.cfg.Simulated = true
	s, err := a.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.Dial(conformance.DefaultNow, "")
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != demoPrivate {
		t.Fatalf("url %s", d.URL)
	}
	req, err := a.Submit(conformance.DefaultNow, limitOrder(exchange.TIFGTC, cloidGTC))
	if err != nil {
		t.Fatal(err)
	}
	if headerValue(req.Req.Header, "x-simulated-trading") != "1" {
		t.Fatalf("header %v", req.Req.Header)
	}
	a.cfg.StreamEndpoint = "wss://example.invalid/ws/v5/private"
	s, err = a.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err = s.Dial(conformance.DefaultNow, "")
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != "wss://example.invalid/ws/v5/private" {
		t.Fatalf("override %s", d.URL)
	}
}

func TestOrderBodies(t *testing.T) {
	cases := []struct {
		name string
		req  *exchange.OrderRequest
		want string
	}{
		{"limit", limitOrder(exchange.TIFGTC, cloidGTC),
			`{"instId":"BTC-USDT","tdMode":"cash","side":"buy","ordType":"limit","sz":"0.00002","px":"41000.0","clOrdId":"1767225600000000001"}`},
		{"postonly", limitOrder(exchange.TIFPostOnly, cloidGTX),
			`{"instId":"BTC-USDT","tdMode":"cash","side":"buy","ordType":"post_only","sz":"0.00002","px":"41000.0","clOrdId":"1767225600000000004"}`},
		{"market", marketOrder(cloidMKT),
			`{"instId":"BTC-USDT","tdMode":"cash","side":"buy","ordType":"market","sz":"0.00002","clOrdId":"1767225600000000005","tgtCcy":"base_ccy"}`},
		{"quote", spotQuote(cloidGTC),
			`{"instId":"BTC-USDT","tdMode":"cash","side":"buy","ordType":"market","sz":"10.00000000","clOrdId":"1767225600000000001","tgtCcy":"quote_ccy"}`},
		{"swap", swapOrder(cloidGTC),
			`{"instId":"BTC-USDT-SWAP","tdMode":"cross","side":"buy","ordType":"limit","sz":"1.00","px":"65000.0","clOrdId":"1767225600000000001"}`},
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
	spot := limitOrder(exchange.TIFGTC, cloidGTC)
	spot.ReduceOnly = true
	if _, err := createBodyOf(spot); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("spot reduce-only %v", err)
	}
	m := marketOrder(cloidMKT)
	m.TIF = exchange.TIFGTC
	if _, err := createBodyOf(m); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("market gtc %v", err)
	}
	swap := swapOrder(cloidGTC)
	swap.Instrument.ContractSize = 0
	if _, err := createBodyOf(swap); err == nil || !strings.Contains(err.Error(), "contract size") {
		t.Fatalf("missing contract size %v", err)
	}
	a := mustAcct(t)
	refs := make([]exchange.CancelRef, 21)
	for i := range refs {
		refs[i] = exchange.CancelRef{Instrument: spotInst(), ClientOrderID: cloidGTC}
	}
	call, err := a.CancelBatch(conformance.DefaultNow, refs)
	if err != nil || !strings.Contains(call.Req.Path, "cancel-batch-orders") {
		t.Fatalf("over-cap batch was not sent: %v", err)
	}
	if _, err := a.ArmCancelOnDisconnect(conformance.DefaultNow, 9*time.Second, nil); err == nil {
		t.Fatal("9s window was accepted")
	}
	if _, err := a.ArmCancelOnDisconnect(conformance.DefaultNow, 1500*time.Millisecond, nil); err == nil {
		t.Fatal("fractional window was accepted")
	}
	if _, err := a.ArmCancelOnDisconnect(conformance.DefaultNow, 10*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Leverage(conformance.DefaultNow, spotInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("spot leverage %v", err)
	}
}

func TestClassify(t *testing.T) {
	if err := classify(200, "50113", "Invalid Sign"); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("auth %v", err)
	}
	if _, ok := classify(429, "50011", "Too Many Requests").(*exchange.RateLimited); !ok {
		t.Fatal("rate")
	}
	if !errors.Is(classify(200, "50004", "timeout"), exchange.ErrUnknownOutcome) {
		t.Fatal("timeout")
	}
	rej, ok := classify(200, "51008", "insufficient").(*exchange.Reject)
	if !ok || rej.Code != 201 || rej.Native != "51008" {
		t.Fatalf("balance %+v", rej)
	}
	rej, ok = classify(200, "51000", "Parameter mgnMode error").(*exchange.Reject)
	if !ok || rej.Code != 51000 {
		t.Fatalf("native %+v", rej)
	}
	if classify(200, "0", "Order placed") != nil {
		t.Fatal("success")
	}
}

func TestCategory(t *testing.T) {
	if got := categoryOf("SPOT", "BTC-USDT"); got != symbol.Spot {
		t.Fatalf("spot %s", got)
	}
	if got := categoryOf("MARGIN", "BTC-USDT"); got != symbol.Spot {
		t.Fatalf("margin %s", got)
	}
	if got := categoryOf("SWAP", "BTC-USDT-SWAP"); got != symbol.Perp {
		t.Fatalf("perp %s", got)
	}
	if got := categoryOf("SWAP", "BTC-USD-SWAP"); got != symbol.Inverse {
		t.Fatalf("inverse %s", got)
	}
	if got := categoryOf("FUTURES", "BTC-USDT-260327"); got != symbol.Future {
		t.Fatalf("future %s", got)
	}
	if got := categoryOf("FUTURES", "BTC-USD-260327"); got != symbol.Inverse {
		t.Fatalf("inverse future %s", got)
	}
	if got := categoryOf("OPTION", "BTC-USD-260327-100000-C"); got != symbol.Option {
		t.Fatalf("option %s", got)
	}
}

func TestDecodePrivate(t *testing.T) {
	s := &privateStream{}
	sink := &capture{}
	if err := s.Decode([]byte("pong"), sink); err != nil {
		t.Fatal(err)
	}
	login := []byte(`{"event":"login","code":"0","msg":""}`)
	if err := s.Decode(login, sink); err != nil || sink.ready {
		t.Fatalf("login ready=%v err=%v", sink.ready, err)
	}
	bad := []byte(`{"event":"error","code":"60009","msg":"Login failed."}`)
	if err := s.Decode(bad, sink); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("bad login %v", err)
	}
	for _, ch := range []string{"orders", "positions", "account"} {
		frame := []byte(`{"event":"subscribe","arg":{"channel":"` + ch + `"}}`)
		if err := s.Decode(frame, sink); err != nil {
			t.Fatal(err)
		}
	}
	if !sink.ready {
		t.Fatal("not ready")
	}
	order := []byte(`{"arg":{"channel":"orders","instType":"SPOT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","ordId":"1","clOrdId":"9","side":"buy","ordType":"limit","state":"live","px":"41000","sz":"0.00002","accFillSz":"0","avgPx":"","fillSz":"0","tradeId":"","uTime":"1791633374675"}]}`)
	if err := s.Decode(order, sink); err != nil || len(sink.orders) != 1 {
		t.Fatalf("order %v %+v", err, sink.orders)
	}
	if sink.orders[0].Status != exchange.OrderStatusAccepted || sink.orders[0].Exch.Category != symbol.Spot {
		t.Fatalf("parsed %+v", sink.orders[0])
	}
	fill := []byte(`{"arg":{"channel":"orders","instType":"SPOT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","ordId":"1","clOrdId":"9","side":"buy","ordType":"market","state":"filled","px":"","sz":"0.00002","accFillSz":"0.00002","avgPx":"83000","fillPx":"83000","fillSz":"0.00002","fillFee":"-0.01","fillFeeCcy":"USDT","tradeId":"t1","execType":"T","fillTime":"1791633374675","uTime":"1791633374675","tgtCcy":"base_ccy"}]}`)
	if err := s.Decode(fill, sink); err != nil || len(sink.fills) != 1 {
		t.Fatalf("fill %v %+v", err, sink.fills)
	}
	if sink.fills[0].FillID != "t1" || sink.fills[0].Liquidity != "taker" || sink.fills[0].Fee != -0.01 {
		t.Fatalf("fill %+v", sink.fills[0])
	}
	pos := []byte(`{"arg":{"channel":"positions","instType":"SWAP"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"net","pos":"-1","avgPx":"80000","markPx":"80100","upl":"-1","lever":"3"}]}`)
	if err := s.Decode(pos, sink); err != nil || len(sink.pos) != 1 || sink.pos[0].Size != -1 || sink.pos[0].Exch.Category != symbol.Perp {
		t.Fatalf("pos %v %+v", err, sink.pos)
	}
	flat := []byte(`{"arg":{"channel":"positions"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"net","pos":"0"}]}`)
	// A closed position is passed on with size 0: the worker forgets it.
	sink.pos = nil
	if err := s.Decode(flat, sink); err != nil || len(sink.pos) != 1 || sink.pos[0].Size != 0 || sink.pos[0].Exch.ExchTicker != "BTC-USDT-SWAP" {
		t.Fatalf("flat %v %+v", err, sink.pos)
	}
	bal := []byte(`{"arg":{"channel":"account"},"data":[{"details":[{"ccy":"USDT","availBal":"5000","frozenBal":"1"},{"ccy":"BTC","availBal":"0","frozenBal":"0"}]}]}`)
	if err := s.Decode(bal, sink); err != nil || len(sink.bals) != 1 || sink.bals[0].Asset != "USDT" || sink.bals[0].Locked != 1 {
		t.Fatalf("bal %v %+v", err, sink.bals)
	}
	if err := s.Decode([]byte(`{"conformance":"a frame no venue sends"}`), sink); err == nil {
		t.Fatal("unknown frame")
	}
}

func headerValue(h []exchange.Header, name string) string {
	for _, x := range h {
		if x.Name == name {
			return x.Value
		}
	}
	return ""
}

func mustAcct(t *testing.T) *account {
	t.Helper()
	a, err := Declare().AccountKinds[0].Account(exchange.AccountConfig{
		APIID: 1, Credentials: conformance.TestCredentials(exchange.KeyHMAC, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	return a.(*account)
}

func spotInst() *symbol.Info {
	return &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: Name, Category: symbol.Spot},
		ExchTicker: "BTC-USDT",
		Filters:    symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.00001},
	}
}

func swapInst() *symbol.Info {
	return &symbol.Info{
		Ticker:       symbol.UniversalTicker{Venue: Name, Category: symbol.Perp},
		ExchTicker:   "BTC-USDT-SWAP",
		ContractSize: 0.01,
		Filters:      symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.0001},
	}
}

func limitOrder(tif exchange.TimeInForce, cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: tif,
		Qty: 0.00002, QuoteQty: decimal.Absent(), Price: 41000, ClientOrderID: cloid,
	}
}

func marketOrder(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: 0.00002, QuoteQty: decimal.Absent(), Price: decimal.Absent(), ClientOrderID: cloid,
	}
}

func spotQuote(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: decimal.Absent(), QuoteQty: 10, Price: decimal.Absent(), ClientOrderID: cloid,
	}
}

func swapOrder(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: swapInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: exchange.TIFGTC,
		Qty: 0.01, QuoteQty: decimal.Absent(), Price: 65000, ClientOrderID: cloid,
	}
}

const (
	cloidGTC = "1767225600000000001"
	cloidIOC = "1767225600000000002"
	cloidGTX = "1767225600000000004"
	cloidMKT = "1767225600000000005"
)

type capture struct {
	ready  bool
	orders []exchange.OrderUpdate
	fills  []exchange.Fill
	pos    []exchange.Position
	bals   []exchange.Balance
}

func (c *capture) Ready()                          { c.ready = true }
func (c *capture) Expired(string)                  {}
func (c *capture) Order(e *exchange.OrderUpdate)   { c.orders = append(c.orders, *e) }
func (c *capture) Fill(e *exchange.Fill)           { c.fills = append(c.fills, *e) }
func (c *capture) Balances(e []exchange.Balance)   { c.bals = append([]exchange.Balance(nil), e...) }
func (c *capture) Positions(e []exchange.Position) { c.pos = append([]exchange.Position(nil), e...) }

// TestExpTime: Submit, Cancel and CancelBatch carry expTime, now plus AccountConfig.Validity in Unix
// ms (E06-D005); reads do not. A window out of range is refused when the account is built.
func TestExpTime(t *testing.T) {
	kind := Declare().AccountKinds[0]
	cfg := exchange.AccountConfig{Credentials: conformance.TestCredentials(exchange.KeyHMAC, true), Validity: 3 * time.Second}
	a, err := kind.Account(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := conformance.DefaultNow
	want := strconv.FormatInt(now.Add(3*time.Second).UnixMilli(), 10)
	ref := exchange.CancelRef{Instrument: spotInst(), ClientOrderID: cloidGTC}
	sub, err := a.Submit(now, limitOrder(exchange.TIFGTC, cloidGTC))
	if err != nil {
		t.Fatal(err)
	}
	can, err := a.Cancel(now, ref)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := a.CancelBatch(now, []exchange.CancelRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []exchange.HTTPRequest{sub.Req, can.Req, batch.Req} {
		if got := expTimeOf(r); got != want {
			t.Errorf("%s expTime %q, want %s", r.Path, got, want)
		}
	}
	q, err := a.QueryOrder(now, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := expTimeOf(q.Req); got != "" {
		t.Errorf("a read carries expTime %q", got)
	}
	cfg.Validity = time.Hour
	if _, err := kind.Account(cfg); err == nil {
		t.Error("an hour's window was taken")
	}
}

func expTimeOf(r exchange.HTTPRequest) string {
	for _, h := range r.Header {
		if h.Name == "expTime" {
			return h.Value
		}
	}
	return ""
}
