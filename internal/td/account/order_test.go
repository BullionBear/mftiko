package account

import (
	"context"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
	"google.golang.org/protobuf/proto"
)

func mustCloid(t *testing.T, algo, seq uint32) string {
	t.Helper()
	s, err := cloid.Encode(algo, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), seq)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (h *harness) order(req *tdv1.OrderRequest) *tdv1.OrderReply {
	h.t.Helper()
	h.bus.mu.Lock()
	fn := h.bus.respond["td.order.42"]
	h.bus.mu.Unlock()
	if fn == nil {
		h.t.Fatal("td.order is not served")
	}
	q, err := proto.Marshal(req)
	if err != nil {
		h.t.Fatal(err)
	}
	r := &tdv1.OrderReply{}
	if err := proto.Unmarshal(fn(q), r); err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *harness) submit(uid, algo uint32, cid string, qty, price float64) *tdv1.OrderReply {
	q, p := qty, price
	return h.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
		Uid: uid, AlgoId: algo, ClientOrderId: cid,
		Side: commonv1.Side_SIDE_BUY, OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT,
		Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC, Qty: &q, Price: &p,
	}}})
}

func (h *harness) cancel(algo uint32, cid, orderID string) *tdv1.OrderReply {
	return h.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{
		AlgoId: algo, ClientOrderId: cid, OrderId: orderID,
	}}})
}

func (h *harness) updates(cid string) []*tdv1.OrderUpdate {
	var out []*tdv1.OrderUpdate
	for _, e := range h.bus.all() {
		if u := e.GetOrderUpdate(); u.GetOrder().GetClientOrderId() == cid {
			out = append(out, u)
		}
	}
	return out
}

func (h *harness) waitStatus(cid string, st tdv1.OrderStatus) *tdv1.OrderUpdate {
	h.t.Helper()
	var got *tdv1.OrderUpdate
	waitFor(h.t, "order "+cid+" did not become "+st.String(), func() bool {
		for _, u := range h.updates(cid) {
			if u.GetOrder().GetStatus() == st {
				got = u
				return true
			}
		}
		return false
	})
	return got
}

func rejectCode(r *tdv1.OrderReply) int32 { return r.GetCode() }

// A local reject sends nothing and publishes nothing for that client order id.
func TestOrderLocalRejects(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	h.ins.active["OTHER"] = testInst(11, "OTHER", 1)
	h.ins.active["OTHER"].Ticker.Venue = "Other"
	h.ins.active["ETHUSDT"].Status = symbol.Halted

	cid := func(seq uint32) string { return mustCloid(t, 7, seq) }
	q, p := 0.01, 100.0
	badQty := 0.0001
	residual := 0.015
	cases := []struct {
		name string
		req  *tdv1.OrderRequest
		code int32
	}{
		{"algo", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 7, AlgoId: 8, ClientOrderId: cid(1), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_ALGO_MISMATCH)},
		{"cloid", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 7, AlgoId: 7, ClientOrderId: "nope", Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
		{"missing", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 999, AlgoId: 7, ClientOrderId: cid(2), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
		{"halted", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 10, AlgoId: 7, ClientOrderId: cid(3), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
		{"venue", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 11, AlgoId: 7, ClientOrderId: cid(4), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_VENUE_NOT_TRADABLE)},
		{"qty", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 7, AlgoId: 7, ClientOrderId: cid(5), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &badQty, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
		{"contracts", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 8, AlgoId: 7, ClientOrderId: cid(6), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &residual, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
		{"side", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 7, AlgoId: 7, ClientOrderId: cid(7), OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
		}}}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
		{"hedge", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 7, AlgoId: 7, ClientOrderId: cid(8), Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Qty: &q, Price: &p,
			Params: map[string]string{"positionSide": "LONG"},
		}}}, int32(tdv1.RejectCode_REJECT_CODE_HEDGE_MODE)},
		{"not open", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{
			AlgoId: 7, ClientOrderId: cid(9),
		}}}, int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN)},
		{"cancel without cloid", &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{AlgoId: 7}}}, int32(tdv1.RejectCode_REJECT_CODE_ALGO_MISMATCH)},
		{"empty", &tdv1.OrderRequest{}, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			posts := count(h.v.Requests(), "POST /order") + count(h.v.Requests(), "DELETE /order")
			r := h.order(c.req)
			if r.GetAccepted() || rejectCode(r) != c.code {
				t.Fatalf("reply %+v, want code %d", r, c.code)
			}
			if n := count(h.v.Requests(), "POST /order") + count(h.v.Requests(), "DELETE /order"); n != posts {
				t.Fatalf("venue saw %d order requests, was %d", n, posts)
			}
			cid := ""
			switch x := c.req.GetReq().(type) {
			case *tdv1.OrderRequest_Submit:
				cid = x.Submit.GetClientOrderId()
			case *tdv1.OrderRequest_Cancel:
				cid = x.Cancel.GetClientOrderId()
			}
			if cid != "" && len(h.updates(cid)) != 0 {
				t.Fatalf("td.evt %v", h.updates(cid))
			}
		})
	}
}

func TestOrderSubmitAndIdempotent(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Ack: exchange.OrderStatusAccepted}})
	h.startReady()
	cid := mustCloid(t, 7, 1)
	r := h.submit(7, 7, cid, 0.01, 100)
	if !r.GetAccepted() || r.GetClientOrderId() != cid {
		t.Fatalf("reply %+v", r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW)
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	waitFor(t, "venue did not see the order", func() bool { return count(h.v.Requests(), "POST /order") == 1 })

	again := h.submit(7, 7, cid, 0.01, 100)
	if !proto.Equal(r, again) {
		t.Fatalf("replay %+v, want %+v", again, r)
	}
	if n := count(h.v.Requests(), "POST /order"); n != 1 {
		t.Fatalf("%d submits", n)
	}
	q := 0.02
	p := 100.0
	diff := h.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
		Uid: 7, AlgoId: 7, ClientOrderId: cid, Side: commonv1.Side_SIDE_BUY,
		OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		Qty: &q, Price: &p,
	}}})
	if diff.GetAccepted() || diff.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER) {
		t.Fatalf("different content %+v", diff)
	}
	if n := count(h.v.Requests(), "POST /order"); n != 1 {
		t.Fatalf("%d submits after a different resend", n)
	}
	accepted := 0
	for _, u := range h.updates(cid) {
		if u.GetOrder().GetStatus() == tdv1.OrderStatus_ORDER_STATUS_ACCEPTED {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted updates %d, events %v", accepted, h.updates(cid))
	}
}

func TestOrderPriceSnaps(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	cid := mustCloid(t, 7, 2)
	sell := 100.01
	q := 0.01
	r := h.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
		Uid: 7, AlgoId: 7, ClientOrderId: cid, Side: commonv1.Side_SIDE_SELL,
		OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		Qty: &q, Price: &sell,
	}}})
	if !r.GetAccepted() {
		t.Fatalf("reply %+v", r)
	}
	waitFor(t, "venue did not take the snapped order", func() bool {
		for _, o := range h.v.OpenOrders() {
			if o.ClientOrderID == cid && decimal.Equal(o.Price, 100.1, 0.1) {
				return true
			}
		}
		return false
	})
	buy := mustCloid(t, 7, 3)
	px := 100.07
	if r := h.submit(7, 7, buy, 0.01, px); !r.GetAccepted() {
		t.Fatalf("buy %+v", r)
	}
	waitFor(t, "buy was not snapped down", func() bool {
		for _, o := range h.v.OpenOrders() {
			if o.ClientOrderID == buy && decimal.Equal(o.Price, 100, 0.1) {
				return true
			}
		}
		return false
	})
}

func TestOrderLiveCloid(t *testing.T) {
	cid := mustCloid(t, 7, 4)
	h := start(t, setup{})
	h.v.PutOrder(exchange.OrderUpdate{Exch: btc, ClientOrderID: cid, Side: exchange.SideBuy, Type: exchange.OrderTypeLimit,
		Status: exchange.OrderStatusAccepted, Qty: 0.01, Price: 100, QuoteQty: decimal.Absent(), AvgPrice: decimal.Absent()})
	h.startReady()
	h.v.ResetRequests()
	if r := h.submit(7, 7, cid, 0.02, 100); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER) {
		t.Fatalf("mismatch %+v", r)
	}
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatalf("match %+v", r)
	}
	if n := count(h.v.Requests(), "POST /order"); n != 0 {
		t.Fatalf("venue saw %d submits", n)
	}
}

func TestOrderRateAndPause(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{
		opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{OrderRate: []exchange.RateLimit{{N: 1, Per: time.Hour}}}},
		cfg: func(c *ServeConfig) {
			c.Now = clock.Now
			c.After = clock.After
		},
	})
	h.startReady()
	cid := mustCloid(t, 7, 5)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatalf("first %+v", r)
	}
	waitFor(t, "the first order was not sent", func() bool { return count(h.v.Requests(), "POST /order") == 1 })
	second := mustCloid(t, 7, 6)
	if r := h.submit(7, 7, second, 0.01, 100); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED) {
		t.Fatalf("limited %+v", r)
	}
	if n := count(h.v.Requests(), "POST /order"); n != 1 {
		t.Fatalf("%d submits", n)
	}

	h.v.Throttle(1, 2*time.Second)
	third := mustCloid(t, 7, 7)
	// The local window still has room once an hour has passed on the fake clock.
	clock.Advance(time.Hour)
	if r := h.submit(7, 7, third, 0.01, 100); !r.GetAccepted() {
		t.Fatalf("after the window %+v", r)
	}
	h.waitStatus(third, tdv1.OrderStatus_ORDER_STATUS_REJECTED)
	fourth := mustCloid(t, 7, 8)
	posts := count(h.v.Requests(), "POST /order")
	if r := h.submit(7, 7, fourth, 0.01, 100); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED) {
		t.Fatalf("paused %+v", r)
	}
	if n := count(h.v.Requests(), "POST /order"); n != posts {
		t.Fatalf("paused submit was sent (%d, was %d)", n, posts)
	}
}

func TestOrderDegraded(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	cid := mustCloid(t, 7, 9)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatalf("submit %+v", r)
	}
	if rep := h.nextReport(); rep.GetFirstSubmitNs()[7] == 0 {
		t.Fatalf("first submit report %+v", rep)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	posts := count(h.v.Requests(), "POST /order")
	next := mustCloid(t, 7, 10)
	if r := h.submit(7, 7, next, 0.01, 100); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_DEGRADED) {
		t.Fatalf("degraded submit %+v", r)
	}
	if n := count(h.v.Requests(), "POST /order"); n != posts {
		t.Fatalf("degraded submit was sent")
	}
	if r := h.cancel(7, cid, ""); !r.GetAccepted() {
		t.Fatalf("degraded cancel %+v", r)
	}
	waitFor(t, "cancel was not sent", func() bool { return count(h.v.Requests(), "DELETE /order") == 1 })
}

func TestOrderCancel(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Ack: exchange.OrderStatusAccepted}})
	h.startReady()
	cid := mustCloid(t, 7, 11)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatalf("submit %+v", r)
	}
	u := h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	if r := h.cancel(7, cid, "other"); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN) {
		t.Fatalf("mismatched id %+v", r)
	}
	if n := count(h.v.Requests(), "DELETE /order"); n != 0 {
		t.Fatalf("mismatched cancel was sent")
	}
	if r := h.cancel(7, cid, u.GetOrder().GetOrderId()); !r.GetAccepted() {
		t.Fatalf("cancel %+v", r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if r := h.cancel(7, cid, ""); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN) {
		t.Fatalf("cancel of a finished order %+v", r)
	}
}

func TestOrderCancelInFlight(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	cid := mustCloid(t, 7, 12)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	h.v.OnCancel(cid, exchangetest.Outcome{NoAnswer: true})
	if r := h.cancel(7, cid, ""); !r.GetAccepted() {
		t.Fatalf("first %+v", r)
	}
	if r := h.cancel(7, cid, ""); !r.GetAccepted() {
		t.Fatalf("second %+v", r)
	}
	waitFor(t, "cancel was not sent once", func() bool { return count(h.v.Requests(), "DELETE /order") == 1 })
	if n := count(h.v.Requests(), "DELETE /order"); n != 1 {
		t.Fatalf("%d cancels", n)
	}
}

func TestOrderUnknownCancel(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	cid := mustCloid(t, 7, 13)
	h.v.OnSubmit(cid, exchangetest.Outcome{NoAnswer: true})
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)
	if n := count(h.v.Requests(), "GET /order"); n != 0 {
		t.Fatalf("queried before the line: %d", n)
	}
	if r := h.cancel(7, cid, ""); !r.GetAccepted() {
		t.Fatalf("cancel unknown %+v", r)
	}
	waitFor(t, "unknown order was not cancelled", func() bool { return count(h.v.Requests(), "DELETE /order") == 1 })
}

func TestOrderQueryAfterTheLine(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	cid := mustCloid(t, 7, 14)
	h.v.OnSubmit(cid, exchangetest.Outcome{NoAnswer: true})
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)
	armed := waitArmed(t, clock)
	if n := count(h.v.Requests(), "GET /order"); n != 0 {
		t.Fatalf("queried before the line: %d", n)
	}
	// Still inside the window plus skew: the query must not go out.
	clock.Advance(time.Second)
	if n := count(h.v.Requests(), "GET /order"); n != 0 {
		t.Fatalf("queried early: %d", n)
	}
	clock.Advance(armed.Sub(clock.Now()))
	u := h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_REJECTED)
	if u.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_NOT_PLACED) || u.GetCause() != tdv1.OrderCause_ORDER_CAUSE_REJECTED {
		t.Fatalf("update %+v", u)
	}
}

func TestOrderLateAccept(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	cid := mustCloid(t, 7, 15)
	h.v.OnSubmit(cid, exchangetest.Outcome{Late: true})
	sent := clock.Now()
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)
	waitArmed(t, clock)
	if n := count(h.v.Requests(), "GET /order"); n != 0 {
		t.Fatalf("queried before the line: %d", n)
	}
	if took, err := h.v.Arrive(cid, sent.Add(time.Second)); err != nil || !took {
		t.Fatalf("arrive %v %v", took, err)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	clock.Advance(10 * time.Second)
	waitFor(t, "late order did not stay accepted", func() bool {
		for _, u := range h.updates(cid) {
			if u.GetCode() == int32(tdv1.RejectCode_REJECT_CODE_NOT_PLACED) {
				return false
			}
		}
		return h.omsOrder(cid).GetStatus() == tdv1.OrderStatus_ORDER_STATUS_ACCEPTED
	})
}

func TestOrderQueryWhileDisconnected(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	cid := mustCloid(t, 7, 16)
	h.v.OnSubmit(cid, exchangetest.Outcome{NoAnswer: true})
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.nextReport()
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	armed := waitArmed(t, clock)
	clock.Advance(armed.Sub(clock.Now()))
	u := h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_REJECTED)
	if u.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_NOT_PLACED) {
		t.Fatalf("update %+v", u)
	}
}

func TestOrderQueryFailureDegrades(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	cid := mustCloid(t, 7, 17)
	h.v.OnSubmit(cid, exchangetest.Outcome{NoAnswer: true})
	h.v.FailQueries(1000)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.nextReport()
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)
	for h.oms().GetState() != tdv1.AccountState_ACCOUNT_STATE_DEGRADED {
		armed := waitArmed(t, clock)
		n := count(h.v.Requests(), "GET /order")
		clock.Advance(armed.Sub(clock.Now()))
		waitFor(t, "query did not run", func() bool {
			return count(h.v.Requests(), "GET /order") > n || h.oms().GetState() == tdv1.AccountState_ACCOUNT_STATE_DEGRADED
		})
	}
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, reasonQuery)
	h.v.FailQueries(0)
	armed := waitArmed(t, clock)
	clock.Advance(armed.Sub(clock.Now()))
	u := h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_REJECTED)
	if u.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_NOT_PLACED) {
		t.Fatalf("update %+v", u)
	}
	h.report(tdv1.AccountState_ACCOUNT_STATE_READY, "")
}

func TestOrderRecvWindow(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	q, p := 0.01, 100.0
	submitAt := func(cid string, window int64) time.Time {
		h.v.OnSubmit(cid, exchangetest.Outcome{Late: true})
		sent := clock.Now()
		r := h.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			Uid: 7, AlgoId: 7, ClientOrderId: cid, Side: commonv1.Side_SIDE_BUY,
			OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT, Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
			Qty: &q, Price: &p, RecvWindowMs: window,
		}}})
		if !r.GetAccepted() {
			t.Fatal(r)
		}
		h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)
		return sent
	}
	late := mustCloid(t, 7, 18)
	sent := submitAt(late, 1000)
	if took, err := h.v.Arrive(late, sent.Add(2*time.Second)); err != nil || took {
		t.Fatalf("past the window: took %v %v", took, err)
	}
	okid := mustCloid(t, 7, 23)
	sent = submitAt(okid, 1000)
	if took, err := h.v.Arrive(okid, sent.Add(500*time.Millisecond)); err != nil || !took {
		t.Fatalf("inside the window: took %v %v", took, err)
	}
	h.waitStatus(okid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
}

func TestOrderFirstSubmitReportsBeforeSend(t *testing.T) {
	hold := make(chan struct{})
	h := start(t, setup{cfg: func(c *ServeConfig) {
		inner := c.Doer
		c.Doer = blockDoer{inner: inner, hold: hold}
	}})
	h.startReady()
	cid := mustCloid(t, 7, 19)
	got := make(chan *tdv1.OrderReply, 1)
	go func() { got <- h.submit(7, 7, cid, 0.01, 100) }()
	m := h.next(func(m *workerv1.WorkerUp) bool { return m.GetReport() != nil })
	rep := readReport(h.t, m)
	if rep.GetFirstSubmitNs()[7] == 0 {
		t.Fatalf("report %+v", rep)
	}
	if n := count(h.v.Requests(), "POST /order"); n != 0 {
		t.Fatalf("sent before the report: %d", n)
	}
	close(hold)
	r := <-got
	if !r.GetAccepted() {
		t.Fatalf("reply %+v", r)
	}
	waitFor(t, "the order was not sent", func() bool { return count(h.v.Requests(), "POST /order") == 1 })
	cid2 := mustCloid(t, 7, 20)
	if r := h.submit(7, 7, cid2, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	select {
	case m := <-h.ups:
		t.Fatalf("second order reported %v", m)
	default:
	}
}

func TestOrderTradeStream(t *testing.T) {
	tv := exchangetest.NewTradeVenue(apiKey, false)
	// Connections() flips at Accept, before logon and before the worker records the stream up.
	// onOrders fires only after that record, which is what tryWS reads.
	var streamUp atomic.Bool
	h := start(t, setup{opts: exchangetest.AccountOptions{Trade: tv}, cfg: func(c *ServeConfig) {
		c.onOrders = func(up bool) { streamUp.Store(up) }
	}})
	srv := httptest.NewServer(tv.Handler())
	t.Cleanup(srv.Close)
	tv.SetURL("ws" + strings.TrimPrefix(srv.URL, "http"))
	h.startReady()
	waitFor(t, "the worker has not observed the trade stream up", streamUp.Load)
	h.v.ResetRequests()
	cid := mustCloid(t, 7, 21)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_NEW)
	if n := count(h.v.Requests(), "POST /order"); n != 0 {
		t.Fatalf("REST submit while the trade stream was up: %d", n)
	}
}

func TestOrderSeenExpires(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{cfg: func(c *ServeConfig) {
		c.Now = clock.Now
		c.After = clock.After
	}})
	h.startReady()
	cid := mustCloid(t, 7, 22)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	if r := h.cancel(7, cid, ""); !r.GetAccepted() {
		t.Fatalf("cancel %+v", r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	waitFor(t, "not sent", func() bool { return count(h.v.Requests(), "POST /order") == 1 })
	clock.Advance(25 * time.Hour)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatalf("after expiry %+v", r)
	}
	waitFor(t, "expired cloid was not sent again", func() bool { return count(h.v.Requests(), "POST /order") == 2 })
}

func (h *harness) batch(algo uint32, uids ...uint32) *tdv1.OrderReply {
	return h.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_BatchCancel{BatchCancel: &tdv1.BatchCancel{
		AlgoId: algo, Uids: uids,
	}}})
}

func (h *harness) place(uid, algo, seq uint32) string {
	h.t.Helper()
	cid := mustCloid(h.t, algo, seq)
	if r := h.submit(uid, algo, cid, 0.01, 100); !r.GetAccepted() {
		h.t.Fatalf("submit %+v", r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	return cid
}

func (h *harness) hasCause(cid string, c tdv1.OrderCause) bool {
	for _, u := range h.updates(cid) {
		if u.GetCause() == c {
			return true
		}
	}
	return false
}

func openCloid(orders []exchange.OrderUpdate, cid string) bool {
	for _, o := range orders {
		if o.ClientOrderID == cid {
			return true
		}
	}
	return false
}

// Two algos share the book: BatchCancel takes only the one it names, on every symbol.
func TestBatchCancelOwnsItsAlgo(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 5}}})
	h.v.PutOrder(exchange.OrderUpdate{Exch: btc, ClientOrderID: "manual", Side: exchange.SideBuy,
		Type: exchange.OrderTypeLimit, Status: exchange.OrderStatusAccepted, Qty: 0.01, Price: 100})
	h.startReady()
	own := h.place(7, 7, 30)
	ownETH := h.place(10, 7, 31)
	other := h.place(7, 8, 32)
	if r := h.batch(7); !r.GetAccepted() || r.GetClientOrderId() != "" {
		t.Fatalf("reply %+v", r)
	}
	h.waitStatus(own, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	h.waitStatus(ownETH, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	waitFor(t, "the other algo was cancelled", func() bool {
		return h.omsOrder(other) != nil && openCloid(h.v.OpenOrders(), other) && openCloid(h.v.OpenOrders(), "manual") &&
			!openCloid(h.v.OpenOrders(), own) && !openCloid(h.v.OpenOrders(), ownETH)
	})
	if !h.hasCause(own, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) || !h.hasCause(ownETH, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) {
		t.Fatal("missing CANCEL_REQUESTED")
	}
	if h.hasCause(other, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) || h.hasCause("manual", tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) {
		t.Fatal("cancelled an order that is not this algo's")
	}
	// One instrument per chunk, even though both would fit in CancelBatch. Chunks go out together,
	// so their arrival order is not the book order.
	got := map[string]bool{}
	for _, b := range h.v.Batches() {
		if len(b) != 1 {
			t.Fatalf("batches %v", h.v.Batches())
		}
		got[b[0]] = true
	}
	if len(got) != 2 || !got[own] || !got[ownETH] {
		t.Fatalf("batches %v", h.v.Batches())
	}
}

// More orders than one CancelBatch: several requests, and each order gets its own update.
func TestBatchCancelChunks(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 2}}})
	h.startReady()
	var ids []string
	for seq := uint32(40); seq < 43; seq++ {
		ids = append(ids, h.place(7, 7, seq))
	}
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatal(r)
	}
	for _, cid := range ids {
		h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
		if !h.hasCause(cid, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) {
			t.Fatalf("%s has no CANCEL_REQUESTED", cid)
		}
	}
	batches := h.v.Batches()
	if len(batches) != 2 {
		t.Fatalf("batches %v", batches)
	}
	got := map[string]int{}
	var sizes []int
	for _, b := range batches {
		sizes = append(sizes, len(b))
		for _, cid := range b {
			got[cid]++
		}
	}
	sort.Ints(sizes)
	if sizes[0] != 1 || sizes[1] != 2 {
		t.Fatalf("batches %v", batches)
	}
	for _, cid := range ids {
		if got[cid] != 1 {
			t.Fatalf("%s sent %d times, batches %v", cid, got[cid], batches)
		}
	}
}

// CancelBatch of 1 is one REST cancel per order, still not the single-cancel path.
func TestBatchCancelOneByOne(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 1}}})
	h.startReady()
	a := h.place(7, 7, 50)
	b := h.place(7, 7, 51)
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(a, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	h.waitStatus(b, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	waitFor(t, "the two cancels were not sent", func() bool { return len(h.v.Batches()) == 2 })
	if n := count(h.v.Requests(), "DELETE /order"); n != 0 {
		t.Fatalf("single cancel path used: %d", n)
	}
}

// uids limits the instruments. An algo with nothing open is accepted and sends nothing.
func TestBatchCancelFilters(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	btcID := h.place(7, 7, 60)
	ethID := h.place(10, 7, 61)
	if r := h.batch(9); !r.GetAccepted() {
		t.Fatal(r)
	}
	if len(h.v.Batches()) != 0 {
		t.Fatalf("empty batch sent %v", h.v.Batches())
	}
	if r := h.batch(7, 10); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(ethID, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	waitFor(t, "btc was cancelled too", func() bool {
		return h.omsOrder(btcID) != nil && openCloid(h.v.OpenOrders(), btcID) && !openCloid(h.v.OpenOrders(), ethID)
	})
	if batches := h.v.Batches(); len(batches) != 1 || len(batches[0]) != 1 || batches[0][0] != ethID {
		t.Fatalf("batches %v", batches)
	}
}

// An order that already has a cancel in flight is not sent again.
func TestBatchCancelSkipsInFlight(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 5}}})
	h.startReady()
	first := h.place(7, 7, 70)
	second := h.place(7, 7, 71)
	h.v.OnCancel(first, exchangetest.Outcome{NoAnswer: true})
	if r := h.cancel(7, first, ""); !r.GetAccepted() {
		t.Fatal(r)
	}
	waitFor(t, "the single cancel was not sent", func() bool { return count(h.v.Requests(), "DELETE /order") == 1 })
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(second, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	waitFor(t, "the in-flight order was sent again", func() bool {
		b := h.v.Batches()
		return len(b) == 1 && len(b[0]) == 1 && b[0][0] == second && count(h.v.Requests(), "DELETE /order") == 1
	})
}

// One order in the batch can be refused while the others cancel.
func TestBatchCancelPerOrder(t *testing.T) {
	h := start(t, setup{opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 5}}})
	h.startReady()
	bad := h.place(7, 7, 80)
	good := h.place(7, 7, 81)
	h.v.OnCancel(bad, exchangetest.Outcome{Code: -2011})
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(good, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	u := h.waitCause(bad, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REJECTED)
	if u.GetOrder().GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || u.GetOrder().GetCancelSentNs() != 0 || u.GetCode() != -2011 {
		t.Fatalf("update %+v", u)
	}
	if h.omsOrder(bad).GetCancelSentNs() != 0 {
		t.Fatal("refused cancel left the flag set")
	}
}

// A batch whose answer never comes is queried per order and not sent again (D626).
func TestBatchCancelUnknownOutcome(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{
		opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{CancelBatch: 5}},
		cfg: func(c *ServeConfig) {
			c.Now = clock.Now
			c.After = clock.After
		},
	})
	h.startReady()
	a := h.place(7, 7, 90)
	b := h.place(7, 7, 91)
	h.v.OnBatch(exchangetest.Outcome{NoAnswer: true})
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatal(r)
	}
	waitFor(t, "batch was not sent", func() bool { return len(h.v.Batches()) == 1 })
	if !h.hasCause(a, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) || !h.hasCause(b, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED) {
		t.Fatal("missing CANCEL_REQUESTED")
	}
	armed := waitArmed(t, clock)
	if n := count(h.v.Requests(), "GET /order"); n != 0 {
		t.Fatalf("queried before the line: %d", n)
	}
	clock.Advance(armed.Sub(clock.Now()))
	waitFor(t, "the batch was not queried per order", func() bool { return count(h.v.Requests(), "GET /order") >= 2 })
	if len(h.v.Batches()) != 1 {
		t.Fatalf("resent %v", h.v.Batches())
	}
	for _, cid := range []string{a, b} {
		o := h.omsOrder(cid)
		if o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || o.GetCancelSentNs() == 0 {
			t.Fatalf("%s after the query: %+v", cid, o)
		}
	}
}

// DEGRADED does not stop BatchCancel (D627). It still goes out over REST.
func TestBatchCancelDegraded(t *testing.T) {
	h := start(t, setup{})
	h.startReady()
	cid := h.place(7, 7, 100)
	if rep := h.nextReport(); rep.GetFirstSubmitNs()[7] == 0 {
		t.Fatalf("first submit report %+v", rep)
	}
	h.v.Disconnect()
	h.report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatalf("degraded batch %+v", r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	waitFor(t, "degraded batch was not sent", func() bool { return len(h.v.Batches()) == 1 })
}

// Submit stops at 90%. Cancel and BatchCancel may use the rest of the window, and stop at 100% (D628).
func TestCancelUsesFullRate(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	h := start(t, setup{
		opts: exchangetest.AccountOptions{Limits: exchange.AccountLimits{
			CancelBatch: 5,
			OrderRate:   []exchange.RateLimit{{N: 10, Per: time.Hour}},
		}},
		cfg: func(c *ServeConfig) {
			c.Now = clock.Now
			c.After = clock.After
		},
	})
	h.startReady()
	var ids []string
	for seq := uint32(110); seq < 119; seq++ {
		ids = append(ids, h.place(7, 7, seq))
	}
	extra := mustCloid(t, 7, 119)
	if r := h.submit(7, 7, extra, 0.01, 100); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED) {
		t.Fatalf("submit at 90%% %+v", r)
	}
	if r := h.batch(7, 7); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED) {
		t.Fatalf("batch that does not fit %+v", r)
	}
	if len(h.v.Batches()) != 0 {
		t.Fatalf("rate-limited batch was sent %v", h.v.Batches())
	}
	if r := h.cancel(7, ids[0], ""); !r.GetAccepted() {
		t.Fatalf("cancel in the last slot %+v", r)
	}
	h.waitStatus(ids[0], tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if r := h.batch(7); r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED) {
		t.Fatalf("batch at 100%% %+v", r)
	}
	if len(h.v.Batches()) != 0 {
		t.Fatalf("full window sent a batch %v", h.v.Batches())
	}
}

// A batch does not ride the trade stream, even while that stream is up.
func TestBatchCancelRestWhileStreamUp(t *testing.T) {
	tv := exchangetest.NewTradeVenue(apiKey, false)
	var streamUp atomic.Bool
	h := start(t, setup{opts: exchangetest.AccountOptions{Trade: tv, Limits: exchange.AccountLimits{CancelBatch: 5}}, cfg: func(c *ServeConfig) {
		c.onOrders = func(up bool) { streamUp.Store(up) }
	}})
	srv := httptest.NewServer(tv.Handler())
	t.Cleanup(srv.Close)
	tv.SetURL("ws" + strings.TrimPrefix(srv.URL, "http"))
	h.startReady()
	waitFor(t, "the worker has not observed the trade stream up", streamUp.Load)
	cid := mustCloid(t, 7, 130)
	if r := h.submit(7, 7, cid, 0.01, 100); !r.GetAccepted() {
		t.Fatal(r)
	}
	h.waitStatus(cid, tdv1.OrderStatus_ORDER_STATUS_NEW)
	h.v.ResetRequests()
	if r := h.batch(7); !r.GetAccepted() {
		t.Fatal(r)
	}
	waitFor(t, "batch did not go out on REST", func() bool { return len(h.v.Batches()) == 1 })
	if n := count(h.v.Requests(), "DELETE /order"); n != 0 {
		t.Fatalf("single cancel path used: %d", n)
	}
}

func (h *harness) waitCause(cid string, c tdv1.OrderCause) *tdv1.OrderUpdate {
	h.t.Helper()
	var got *tdv1.OrderUpdate
	waitFor(h.t, "order "+cid+" did not get "+c.String(), func() bool {
		for _, u := range h.updates(cid) {
			if u.GetCause() == c {
				got = u
				return true
			}
		}
		return false
	})
	return got
}

func (h *harness) omsOrder(cid string) *tdv1.Order {
	for _, o := range h.oms().GetOms().GetOrders() {
		if o.GetClientOrderId() == cid {
			return o
		}
	}
	return nil
}

func waitArmed(t *testing.T, c *fakeClock) time.Time {
	t.Helper()
	var next time.Time
	waitFor(t, "query was not scheduled", func() bool {
		n, ok := c.Next()
		if ok {
			next = n
		}
		return ok
	})
	return next
}

func (h *harness) nextReport() *tdctlv1.Report {
	h.t.Helper()
	return readReport(h.t, h.next(func(m *workerv1.WorkerUp) bool { return m.GetReport() != nil }))
}

func readReport(t *testing.T, m *workerv1.WorkerUp) *tdctlv1.Report {
	t.Helper()
	r := &tdctlv1.Report{}
	if err := proto.Unmarshal(m.GetReport().GetPayload(), r); err != nil {
		t.Fatal(err)
	}
	return r
}

// blockDoer holds POST /order until hold is closed, so a test can observe the report that must
// go out before the order is sent.
type blockDoer struct {
	inner exchange.Doer
	hold  <-chan struct{}
}

func (d blockDoer) Do(ctx context.Context, req *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	if req.Method == "POST" && req.Path == "/order" {
		select {
		case <-d.hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return d.inner.Do(ctx, req)
}

// fakeClock is a clock whose After waits for Advance. Query deadlines use it; nothing sleeps a
// fixed time.
type fakeClock struct {
	mu      sync.Mutex
	t       time.Time
	waiters []fakeWait
}

type fakeWait struct {
	until time.Time
	ch    chan time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) Next() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var next time.Time
	ok := false
	for _, w := range f.waiters {
		if !ok || w.until.Before(next) {
			next, ok = w.until, true
		}
	}
	return next, ok
}

func (f *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	f.mu.Lock()
	now := f.t
	if d <= 0 {
		f.mu.Unlock()
		ch <- now
		return ch
	}
	f.waiters = append(f.waiters, fakeWait{until: now.Add(d), ch: ch})
	f.mu.Unlock()
	return ch
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
	var keep []fakeWait
	for _, w := range f.waiters {
		if f.t.Before(w.until) {
			keep = append(keep, w)
			continue
		}
		w.ch <- f.t
	}
	f.waiters = keep
}
