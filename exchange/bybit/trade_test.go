package bybit

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestTradeFrames(t *testing.T) {
	ts := mustTrade(t)
	now := conformance.DefaultNow
	limit, err := ts.Submit(now, 1, limitOrder(exchange.TIFGTC, cloidGTC))
	if err != nil {
		t.Fatal(err)
	}
	wantLimit := `{"reqId":"1","header":{"X-BAPI-TIMESTAMP":"1767225600000","X-BAPI-RECV-WINDOW":"5000"},"op":"order.create","args":[{"category":"linear","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","qty":"0.001","price":"65000.0","timeInForce":"GTC","orderLinkId":"1767225600000000001"}]}`
	if string(limit) != wantLimit {
		t.Fatalf("limit\n%s\nwant\n%s", limit, wantLimit)
	}
	mkt, err := ts.Submit(now, 2, marketOrder(cloidMKT))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mkt), `"op":"order.create"`) || !strings.Contains(string(mkt), `"orderType":"Market"`) {
		t.Fatalf("market %s", mkt)
	}
	spot, err := ts.Submit(now, 3, spotQuote(cloidGTC))
	if err != nil {
		t.Fatal(err)
	}
	wantSpot := `{"reqId":"3","header":{"X-BAPI-TIMESTAMP":"1767225600000","X-BAPI-RECV-WINDOW":"5000"},"op":"order.create","args":[{"category":"spot","symbol":"BTCUSDT","side":"Buy","orderType":"Market","qty":"200.00000000","orderLinkId":"1767225600000000001","marketUnit":"quoteCoin"}]}`
	if string(spot) != wantSpot {
		t.Fatalf("spot\n%s\nwant\n%s", spot, wantSpot)
	}
	cancel, err := ts.Cancel(now, 4, exchange.CancelRef{Instrument: btcInst(), ClientOrderID: cloidGTC})
	if err != nil {
		t.Fatal(err)
	}
	wantCancel := `{"reqId":"4","header":{"X-BAPI-TIMESTAMP":"1767225600000","X-BAPI-RECV-WINDOW":"5000"},"op":"order.cancel","args":[{"category":"linear","symbol":"BTCUSDT","orderLinkId":"1767225600000000001"}]}`
	if string(cancel) != wantCancel {
		t.Fatalf("cancel\n%s\nwant\n%s", cancel, wantCancel)
	}
	if _, err := ts.Submit(now, 0, limitOrder(exchange.TIFGTC, cloidGTC)); err == nil {
		t.Fatal("id 0 was accepted")
	}
}

func TestTradeURLFollowsREST(t *testing.T) {
	a := mustAcct(t)
	a.cfg.StreamEndpoint = "wss://example.invalid/v5/private"
	ts, err := a.NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err := ts.Dial(conformance.DefaultNow)
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != prodTrade {
		t.Fatalf("prod %s", d.URL)
	}
	a.cfg.RESTEndpoint = testREST
	ts, err = a.NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err = ts.Dial(conformance.DefaultNow)
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != testTrade {
		t.Fatalf("testnet %s", d.URL)
	}
}

func TestDecodeTrade(t *testing.T) {
	ts := mustTrade(t)
	sink := &tradeCap{}
	auth := []byte(`{"retCode":0,"retMsg":"OK","op":"auth","connId":"c1"}`)
	if err := ts.Decode(auth, sink); err != nil || !sink.ready {
		t.Fatalf("auth ready=%v err=%v", sink.ready, err)
	}
	bad := []byte(`{"retCode":10004,"retMsg":"Error sign","op":"auth"}`)
	if err := ts.Decode(bad, sink); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("bad auth %v", err)
	}
	pong := []byte(`{"retCode":0,"retMsg":"OK","op":"pong","data":["1711002002529"]}`)
	if err := ts.Decode(pong, sink); err != nil {
		t.Fatal(err)
	}
	create := []byte(`{"reqId":"1","retCode":0,"retMsg":"OK","op":"order.create","data":{"orderId":"a4c1718e-fe53-4659-a118-1f6ecce04ad9","orderLinkId":""},"header":{"X-Bapi-Limit":"10","X-Bapi-Limit-Status":"9","X-Bapi-Limit-Reset-Timestamp":"1711001595208","Traceid":"38b7977b430f9bd228f4b19724794dfd","Timenow":"1711001595209"},"connId":"c1"}`)
	if err := ts.Decode(create, sink); err != nil {
		t.Fatal(err)
	}
	if !sink.gotSub || sink.subID != 1 || sink.sub == nil || sink.sub.OrderID != "a4c1718e-fe53-4659-a118-1f6ecce04ad9" ||
		sink.sub.Status != exchange.OrderStatusNew || sink.sub.ExchTsNs != 1711001595209*int64(time.Millisecond) {
		t.Fatalf("create %+v id=%d err=%v", sink.sub, sink.subID, sink.subErr)
	}
	if len(sink.used) != 1 || sink.used[0].Bucket != bucketEndpoint || sink.used[0].N != 1 ||
		!sink.used[0].ResetAt.Equal(time.UnixMilli(1711001595208).UTC()) {
		t.Fatalf("used %+v", sink.used)
	}
	for i := range create {
		create[i] = 'x'
	}
	if sink.sub.OrderID != "a4c1718e-fe53-4659-a118-1f6ecce04ad9" {
		t.Fatalf("scribbled order id %q", sink.sub.OrderID)
	}

	rej := []byte(`{"reqId":"2","retCode":110007,"retMsg":"Available balance is insufficient","op":"order.create"}`)
	if err := ts.Decode(rej, sink); err != nil || !sink.gotSub || sink.subID != 2 {
		t.Fatalf("reject id=%d err=%v", sink.subID, err)
	}
	var r *exchange.Reject
	if !errors.As(sink.subErr, &r) || r.Native != "110007" {
		t.Fatalf("reject %v", sink.subErr)
	}
	for i := range rej {
		rej[i] = 'x'
	}
	if r.Reason != "Available balance is insufficient" {
		t.Fatalf("scribbled reason %q", r.Reason)
	}

	num := []byte(`{"op":"order.cancel","reqId":7,"retCode":0,"retMsg":"OK"}`)
	if err := ts.Decode(num, sink); err != nil || !sink.gotCan || sink.canID != 7 || sink.canErr != nil {
		t.Fatalf("numeric cancel id=%d err=%v decode=%v", sink.canID, sink.canErr, err)
	}
	limited := []byte(`{"op":"order.cancel","reqId":"8","retCode":10429,"retMsg":"System level frequency protection"}`)
	if err := ts.Decode(limited, sink); err != nil || sink.canID != 8 {
		t.Fatalf("limited %v id=%d", err, sink.canID)
	}
	if _, ok := sink.canErr.(*exchange.RateLimited); !ok {
		t.Fatalf("limited %T", sink.canErr)
	}
	unknown := []byte(`{"op":"order.create","reqId":"9","retCode":10019,"retMsg":"ws trade service is restarting"}`)
	if err := ts.Decode(unknown, sink); err != nil || sink.subID != 9 || !errors.Is(sink.subErr, exchange.ErrUnknownOutcome) {
		t.Fatalf("restart id=%d err=%v decode=%v", sink.subID, sink.subErr, err)
	}
	if err := ts.Decode([]byte(`{"op":"order.create","reqId":"1","retCode":0,"retMsg":"OK","data":{}}`), sink); err == nil {
		t.Fatal("empty order id")
	}
	for _, frame := range []string{
		`{"conformance":"a frame no venue sends"}`,
		`["conformance",1]`,
		`not json`,
		`{"op":"order.amend","reqId":"1"}`,
		`{"op":"order.create"}`,
	} {
		if err := ts.Decode([]byte(frame), sink); err == nil {
			t.Fatalf("accepted %s", frame)
		}
	}
}

func mustTrade(t *testing.T) exchange.TradeStream {
	t.Helper()
	ts, err := mustAcct(t).NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

type tradeCap struct {
	ready  bool
	used   []exchange.Used
	sub    *exchange.OrderResult
	subID  uint64
	subErr error
	gotSub bool
	canID  uint64
	canErr error
	gotCan bool
}

func (c *tradeCap) Ready()                        { c.ready = true }
func (c *tradeCap) Expired(string)                {}
func (c *tradeCap) Order(*exchange.OrderUpdate)   {}
func (c *tradeCap) Fill(*exchange.Fill)           {}
func (c *tradeCap) Balances([]exchange.Balance)   {}
func (c *tradeCap) Positions([]exchange.Position) {}
func (c *tradeCap) Used(u []exchange.Used) {
	c.used = append(c.used, u...)
}
func (c *tradeCap) Submitted(id uint64, res *exchange.OrderResult, err error) {
	c.gotSub, c.subID, c.subErr = true, id, err
	if res != nil {
		cp := *res
		c.sub = &cp
	} else {
		c.sub = nil
	}
}
func (c *tradeCap) Canceled(id uint64, err error) {
	c.gotCan, c.canID, c.canErr = true, id, err
}
