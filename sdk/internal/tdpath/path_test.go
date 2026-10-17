package tdpath

import (
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
)

func TestGrow(t *testing.T) {
	d := backoffMin
	var got []time.Duration
	for range 7 {
		got = append(got, d)
		d = grow(d)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step %d: %v", i, got)
		}
	}
}

func TestClassify(t *testing.T) {
	acc := &account{inc: 3, seq: 5}
	cases := []struct {
		inc, want int
		seq       uint64
	}{
		{inc: 3, seq: 4, want: classDrop},
		{inc: 3, seq: 5, want: classDrop},
		{inc: 3, seq: 6, want: classDeliver},
		{inc: 3, seq: 7, want: classGap},
		{inc: 9, seq: 6, want: classDrop},
	}
	for _, c := range cases {
		ev := &tdv1.AccountEvent{Seq: c.seq, Source: &commonv1.WorkerRef{Incarnation: uint32(c.inc)}}
		if got := classify(acc, ev); got != c.want {
			t.Fatalf("inc %d seq %d: %d", c.inc, c.seq, got)
		}
	}
}

func TestOpenRejects(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("nil conn was accepted")
	}
	url := startNATS(t)
	nc := connect(t, url)
	if _, err := Open(Config{NC: nc}); err == nil {
		t.Fatal("empty session was accepted")
	}
	if _, err := Open(Config{NC: nc, SessionID: "s"}); err == nil {
		t.Fatal("zero recv window was accepted")
	}
	if _, err := Open(Config{NC: nc, SessionID: "s", RecvWindow: time.Second, Accounts: []*sessionv1.AccountDecl{nil}}); err == nil {
		t.Fatal("nil account was accepted")
	}
	if _, err := Open(Config{NC: nc, SessionID: "s", RecvWindow: time.Second, Accounts: []*sessionv1.AccountDecl{decl(0, 1)}}); err == nil {
		t.Fatal("api_id 0 was accepted")
	}
	if _, err := Open(Config{NC: nc, SessionID: "s", RecvWindow: time.Second, Accounts: []*sessionv1.AccountDecl{decl(7, 1), decl(7, 2)}}); err == nil {
		t.Fatal("duplicate api_id was accepted")
	}
}

func TestCallsBeforeListen(t *testing.T) {
	p := openPath(t, time.Second, Hooks{}, decl(7, 1))
	o := Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1}
	if _, err := p.Submit(o); !errors.Is(err, ErrNotListening) {
		t.Fatalf("submit %v", err)
	}
	if err := p.Cancel(7, mustCloid(1, 1)); !errors.Is(err, ErrNotListening) {
		t.Fatalf("cancel %v", err)
	}
	if err := p.BatchCancel(7, 1); !errors.Is(err, ErrNotListening) {
		t.Fatalf("batch %v", err)
	}
	if err := p.Recon(); !errors.Is(err, ErrNotListening) {
		t.Fatalf("recon %v", err)
	}
	if err := p.RequestRecon(7); !errors.Is(err, ErrNotListening) {
		t.Fatalf("request %v", err)
	}
}

func TestSyncErrors(t *testing.T) {
	_, p := session(t, time.Second, decl(7, 1))
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Submit(Order{APIID: 99, AlgoID: 1, UID: 1, Qty: 1}); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("account %v", err)
	}
	if _, err := p.Submit(Order{APIID: 7, AlgoID: 9, UID: 1, Qty: 1}); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("algo %v", err)
	}
	if err := p.Cancel(7, "manual"); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("manual %v", err)
	}
	if err := p.Cancel(7, mustCloid(2, 1)); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("other algo %v", err)
	}
	if err := p.BatchCancel(7, 9); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("batch algo %v", err)
	}
	if err := p.RequestRecon(99); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("request %v", err)
	}
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1, Qty: 1}); !errors.Is(err, ErrStopping) {
		t.Fatalf("closed %v", err)
	}
}

func TestBatchCancelUnknown(t *testing.T) {
	f, p := session(t, time.Second, decl(7, 1))
	f.listenOrders()
	f.onOrder = accept
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	if err := p.BatchCancel(7, 1); !errors.Is(err, ErrNoCancelBatch) {
		t.Fatalf("%v", err)
	}
	if n := f.orderCount(); n != 0 {
		t.Fatalf("sent %d", n)
	}
	if err := p.BatchCancel(7, 1, 1001); !errors.Is(err, ErrNoCancelBatch) {
		t.Fatalf("uids %v", err)
	}
}

func TestSubmitReplies(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		f.onOrder = accept
		listenRecon(t, f, p)
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 2, RecvWindowMs: 250})
		if err != nil {
			t.Fatal(err)
		}
		if o, ok := p.Order(7, id); !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
			t.Fatalf("before reply %+v %v", o, ok)
		}
		waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 })
		o, ok := p.Order(7, id)
		if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
			t.Fatalf("after ack %+v %v", o, ok)
		}
		if len(rec.orders()) != 0 {
			t.Fatalf("ack emitted %+v", rec.orders())
		}
		got := f.orders()
		if len(got) != 1 || got[0].req.GetSubmit().GetRecvWindowMs() != 250 {
			t.Fatalf("wire %+v", got)
		}
	})

	t.Run("reject", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, Hooks{}, decl(7, 1))
		f.serve()
		var during bool
		p.hooks.Order = func(ev OrderEvent) {
			rec.mu.Lock()
			rec.ordersV = append(rec.ordersV, ev)
			rec.mu.Unlock()
			o, ok := p.Order(7, ev.Order.GetClientOrderId())
			open := p.OpenOrders(7, 0)
			during = ok && o.GetStatus() == tdv1.OrderStatus_ORDER_STATUS_REJECTED && len(open) == 0
		}
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		f.onOrder = func(int64, *tdv1.OrderRequest) *tdv1.OrderReply {
			return &tdv1.OrderReply{Code: int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), Reason: "nope"}
		}
		listenRecon(t, f, p)
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 })
		if !during {
			t.Fatal("hook did not see the rejected order")
		}
		if _, ok := p.Order(7, id); ok {
			t.Fatal("rejected order stayed in the book")
		}
		ev := rec.orders()
		if len(ev) != 1 || ev[0].Origin != LocalReject || ev[0].Code != 106 || ev[0].Reason != "nope" {
			t.Fatalf("%+v", ev)
		}
	})

	t.Run("no responder", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		f.listenViews()
		listenRecon(t, f, p)
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 })
		if _, ok := p.Order(7, id); ok {
			t.Fatal("offline order stayed in the book")
		}
		ev := rec.orders()
		if len(ev) != 1 || ev[0].Origin != LocalReject || ev[0].Code != int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_OFFLINE) || ev[0].Reason != reasonOffline {
			t.Fatalf("%+v", ev)
		}
		if ev[0].Seq != 0 || ev[0].Incarnation != 0 || ev[0].APIID != 7 {
			t.Fatalf("stamp %+v", ev[0].TD)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, 40*time.Millisecond, rec.hooks(), decl(7, 1))
		f.serve()
		f.listenOrders()
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		listenRecon(t, f, p)
		views := f.viewCount()
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool {
			o, ok := p.Order(7, id)
			return ok && o.GetStatus() == tdv1.OrderStatus_ORDER_STATUS_UNKNOWN
		})
		ev := rec.orders()
		if len(ev) != 1 || ev[0].Origin != Timeout || ev[0].APIID != 7 {
			t.Fatalf("%+v", ev)
		}
		if f.orderCount() != 1 {
			t.Fatalf("resent: %d", f.orderCount())
		}
		if f.viewCount() != views {
			t.Fatalf("queried view: %d -> %d", views, f.viewCount())
		}
	})
}

func TestAckVersusOrderUpdate(t *testing.T) {
	t.Run("update then ack", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		f.onOrder = func(_ int64, req *tdv1.OrderRequest) *tdv1.OrderReply {
			publish(t, f.nc, 7, orderEv(1, 1, req.GetSubmit().GetClientOrderId(), tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
			if !until(time.Second, func() bool { return len(rec.orders()) == 1 }) {
				t.Errorf("update was not applied before the ack")
			}
			return accept(0, nil)
		}
		listenRecon(t, f, p)
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 && len(rec.orders()) == 1 })
		o, ok := p.Order(7, id)
		if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED {
			t.Fatalf("%+v %v", o, ok)
		}
		if rec.orders()[0].Origin != FromTD || rec.orders()[0].Incarnation != 1 || rec.orders()[0].Seq != 1 {
			t.Fatalf("%+v", rec.orders())
		}
	})

	t.Run("ack then update", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		f.onOrder = accept
		listenRecon(t, f, p)
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 })
		if len(rec.orders()) != 0 {
			t.Fatal(rec.orders())
		}
		publish(t, f.nc, 7, orderEv(1, 1, id, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		waitFor(t, 5*time.Second, func() bool { return len(rec.orders()) == 1 })
		o, ok := p.Order(7, id)
		if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || rec.orders()[0].Origin != FromTD {
			t.Fatalf("%+v %+v", o, rec.orders())
		}
	})
}

func TestTerminalVisibleDuringHook(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, Hooks{}, decl(7, 1))
	f.serve()
	var during atomic.Bool
	p.hooks.Order = func(ev OrderEvent) {
		o, ok := p.Order(7, ev.Order.GetClientOrderId())
		during.Store(ok && o.GetStatus() == tdv1.OrderStatus_ORDER_STATUS_CANCELLED && len(p.OpenOrders(7, 0)) == 0)
		rec.addOrder(ev)
	}
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
	f.onOrder = accept
	listenRecon(t, f, p)
	id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 })
	publish(t, f.nc, 7, orderEv(1, 1, id, tdv1.OrderStatus_ORDER_STATUS_CANCELLED))
	waitFor(t, 5*time.Second, func() bool { return len(rec.orders()) == 1 })
	if !during.Load() {
		t.Fatal("hook did not see the cancelled order")
	}
	if _, ok := p.Order(7, id); ok {
		t.Fatal("terminal order stayed after the hook")
	}
}

func TestSubmitSurvivesEmptyRecon(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	f.onOrder = accept
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 4, nil) }
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.OpenOrders(7, 1)) != 1 {
		t.Fatal(p.OpenOrders(7, 1))
	}
	if err := p.Recon(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
	o, ok := p.Order(7, id)
	if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
		t.Fatalf("snapshot dropped the local order %+v %v", o, ok)
	}
	if rec.recons()[0].Recon.GetReason() != tdv1.ReconReason_RECON_REASON_REQUESTED {
		t.Fatalf("reason %v", rec.recons()[0].Recon.GetReason())
	}
}

func TestBatchCancelSplit(t *testing.T) {
	ids := make([]string, 5)
	rows := make([]*tdv1.Order, 5)
	for i := range ids {
		ids[i] = mustCloid(1, uint32(i+1))
		rows[i] = &tdv1.Order{ClientOrderId: ids[i], Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: uint32(100 + i)}
	}
	// liveIDs sorts the decimal strings. That is the chunk order on the wire.
	sorted := append([]string(nil), ids...)
	slices.Sort(sorted)

	t.Run("chunks", func(t *testing.T) {
		f, p := session(t, time.Second, decl(7, 1))
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 2, rows) }
		f.onOrder = accept
		listenRecon(t, f, p)
		if err := p.BatchCancel(7, 1); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return f.orderCount() == 3 && p.settled.Load() == 3 })
		got := f.orders()
		want := [][]string{sorted[0:2], sorted[2:4], sorted[4:5]}
		for i, w := range want {
			b := got[i].req.GetBatchCancel().GetOrders()
			if got[i].req.GetCancel() != nil || len(b) != len(w) {
				t.Fatalf("chunk %d %+v", i, got[i].req)
			}
			for k := range w {
				if b[k].GetClientOrderId() != w[k] {
					t.Fatalf("chunk %d %+v", i, b)
				}
			}
		}
		if len(p.OpenOrders(7, 1)) != 5 {
			t.Fatalf("reject would have changed orders, count %d", len(p.OpenOrders(7, 1)))
		}
	})

	t.Run("single", func(t *testing.T) {
		f, p := session(t, time.Second, decl(7, 1))
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, rows) }
		f.onOrder = accept
		listenRecon(t, f, p)
		if err := p.BatchCancel(7, 1); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return f.orderCount() == 5 })
		for _, g := range f.orders() {
			if g.req.GetBatchCancel() != nil || g.req.GetCancel() == nil {
				t.Fatalf("%+v", g.req)
			}
			if g.req.GetCancel().GetRecvWindowMs() != 0 {
				t.Fatalf("recv %d", g.req.GetCancel().GetRecvWindowMs())
			}
		}
	})

	t.Run("one chunk refused", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 2, rows) }
		f.onOrder = func(_ int64, req *tdv1.OrderRequest) *tdv1.OrderReply {
			if req.GetBatchCancel() != nil && req.GetBatchCancel().GetOrders()[0].GetClientOrderId() == sorted[0] {
				return &tdv1.OrderReply{Code: 106, Reason: "batch"}
			}
			return accept(0, nil)
		}
		listenRecon(t, f, p)
		if err := p.BatchCancel(7, 1); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 3 })
		rj := rec.rejects()
		if len(rj) != 1 || rj[0].Request != RejectedBatchCancel || rj[0].Code != 106 {
			t.Fatalf("%+v", rj)
		}
		if strings.Join(rj[0].ClientOrderIDs, ",") != strings.Join(sorted[0:2], ",") {
			t.Fatalf("ids %v", rj[0].ClientOrderIDs)
		}
		if len(p.OpenOrders(7, 1)) != 5 {
			t.Fatalf("orders changed: %d", len(p.OpenOrders(7, 1)))
		}
	})

	t.Run("uid and empty", func(t *testing.T) {
		f, p := session(t, time.Second, decl(7, 1))
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 2, rows) }
		f.onOrder = accept
		listenRecon(t, f, p)
		if err := p.BatchCancel(7, 1, 999); err != nil {
			t.Fatal(err)
		}
		if f.orderCount() != 0 {
			t.Fatalf("empty selection sent %d", f.orderCount())
		}
		if err := p.BatchCancel(7, 1, rows[0].GetUid()); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return f.orderCount() == 1 })
		b := f.orders()[0].req.GetBatchCancel().GetOrders()
		if len(b) != 1 || b[0].GetClientOrderId() != rows[0].GetClientOrderId() {
			t.Fatalf("%+v", b)
		}
	})
}

func TestWatermark(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, 2*time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	own := mustCloid(1, 1)
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
		publish(t, f.nc, 7, orderEv(3, 4, own, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		publish(t, f.nc, 7, orderEv(3, 6, own, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		publish(t, f.nc, 7, orderEv(9, 6, own, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		if !until(time.Second, func() bool { return held(p, 7) >= 3 }) {
			t.Errorf("events were not held before the reply, hold %d", held(p, 7))
		}
		return snap(3, 5, 1, nil)
	}
	listenRecon(t, f, p)
	waitFor(t, 5*time.Second, func() bool { return len(rec.orderKeys()) == 1 })
	if k := rec.orderKeys(); len(k) != 1 || k[0] != key(3, 6) {
		t.Fatalf("early %v", k)
	}
	publish(t, f.nc, 7, orderEv(3, 5, own, tdv1.OrderStatus_ORDER_STATUS_NEW))
	publish(t, f.nc, 7, orderEv(3, 7, own, tdv1.OrderStatus_ORDER_STATUS_NEW))
	waitFor(t, 5*time.Second, func() bool { return len(rec.orderKeys()) == 2 })
	got := map[string]bool{}
	for _, k := range rec.orderKeys() {
		got[k] = true
	}
	if !got[key(3, 6)] || !got[key(3, 7)] || got[key(3, 5)] || got[key(9, 6)] {
		t.Fatalf("%v", got)
	}
	if n := len(rec.recons()); n != 1 {
		t.Fatalf("recons %d", n)
	}
}

func TestAlgoFilter(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	own := mustCloid(1, 1)
	other := mustCloid(2, 1)
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
		return snap(3, 5, 1, []*tdv1.Order{
			{ClientOrderId: own, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: 1},
			{ClientOrderId: other, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: 2},
			{ClientOrderId: "c", Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: 3},
		})
	}
	listenRecon(t, f, p)
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
	r := rec.recons()[0]
	if r.APIID != 7 || len(r.Recon.GetOpenOrders()) != 1 || r.Recon.GetOpenOrders()[0].GetClientOrderId() != own {
		t.Fatalf("%+v", r.Recon.GetOpenOrders())
	}
	if len(p.OpenOrders(7, 0)) != 1 {
		t.Fatal(p.OpenOrders(7, 0))
	}
	publish(t, f.nc, 7, orderEv(3, 6, other, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
	publish(t, f.nc, 7, fillEv(3, 7, other))
	publish(t, f.nc, 7, fillEv(3, 8, own))
	publish(t, f.nc, 7, &tdv1.AccountEvent{
		Seq: 9, Source: &commonv1.WorkerRef{Incarnation: 3},
		Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{Positions: []*tdv1.Position{{Uid: 9, Size: 1.5}}}},
	})
	publish(t, f.nc, 7, &tdv1.AccountEvent{
		Seq: 10, Source: &commonv1.WorkerRef{Incarnation: 3},
		Event: &tdv1.AccountEvent_BalanceUpdate{BalanceUpdate: &tdv1.BalanceUpdate{Balances: []*tdv1.Balance{{Asset: "USDT", Free: 3}}}},
	})
	publish(t, f.nc, 7, orderEv(3, 11, own, tdv1.OrderStatus_ORDER_STATUS_NEW))
	waitFor(t, 5*time.Second, func() bool {
		return len(rec.orders()) == 1 && len(rec.fills()) == 1 && len(rec.positions()) == 1 && len(rec.balances()) == 1
	})
	if rec.orders()[0].Seq != 11 || rec.fills()[0].Seq != 8 || rec.fills()[0].APIID != 7 {
		t.Fatalf("orders %+v fills %+v", rec.orders(), rec.fills())
	}
	if rec.positions()[0].APIID != 7 || rec.positions()[0].Positions[0].GetUid() != 9 {
		t.Fatalf("pos %+v", rec.positions())
	}
	if rec.balances()[0].APIID != 7 || rec.balances()[0].Balances[0].GetAsset() != "USDT" {
		t.Fatalf("bal %+v", rec.balances())
	}
	if f.viewCount() != 1 {
		t.Fatalf("filtered events started another recon: %d", f.viewCount())
	}
}

func TestTwoAccounts(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1), decl(8, 2))
	f.serve()
	f.onView = func(api int64, _ *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
	f.onOrder = accept
	listenRecon(t, f, p)
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 2 })
	id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Submit(Order{APIID: 8, AlgoID: 1, UID: 1001, Qty: 1}); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("cross algo %v", err)
	}
	if len(p.OpenOrders(8, 0)) != 0 || len(p.OpenOrders(7, 1)) != 1 {
		t.Fatalf("7 %v 8 %v", p.OpenOrders(7, 1), p.OpenOrders(8, 0))
	}
	publish(t, f.nc, 7, orderEv(1, 1, id, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
	publish(t, f.nc, 8, &tdv1.AccountEvent{
		Seq: 1, Source: &commonv1.WorkerRef{Incarnation: 1},
		Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{Positions: []*tdv1.Position{{Uid: 4, Size: 1}}}},
	})
	waitFor(t, 5*time.Second, func() bool { return len(rec.orders()) == 1 && len(rec.positions()) == 1 })
	if rec.orders()[0].APIID != 7 || rec.positions()[0].APIID != 8 {
		t.Fatalf("order %d pos %d", rec.orders()[0].APIID, rec.positions()[0].APIID)
	}
	if len(p.OpenOrders(8, 2)) != 0 {
		t.Fatal(p.OpenOrders(8, 2))
	}
}

func TestReconGapAndIncarnation(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		var n int
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
			n++
			if n == 1 {
				return snap(1, 1, 1, nil)
			}
			return snap(1, 2, 1, nil)
		}
		listenRecon(t, f, p)
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
		id := mustCloid(1, 1)
		publish(t, f.nc, 7, orderEv(1, 3, id, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) >= 2 && len(rec.orders()) == 1 })
		if rec.recons()[1].Incarnation != 1 || rec.recons()[1].Seq != 2 {
			t.Fatalf("second %+v", rec.recons()[1].TD)
		}
		if rec.orders()[0].Seq != 3 {
			t.Fatalf("%+v", rec.orders()[0].TD)
		}
	})

	t.Run("incarnation", func(t *testing.T) {
		rec := &tape{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		var n int
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
			n++
			if n == 1 {
				return snap(1, 1, 1, nil)
			}
			return snap(2, 0, 1, nil)
		}
		listenRecon(t, f, p)
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
		id := mustCloid(1, 2)
		publish(t, f.nc, 7, orderEv(2, 1, id, tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) >= 2 && len(rec.orders()) == 1 })
		if rec.recons()[1].Incarnation != 2 || rec.orders()[0].Incarnation != 2 || rec.orders()[0].Seq != 1 {
			t.Fatalf("recon %+v order %+v", rec.recons()[1].TD, rec.orders()[0].TD)
		}
	})
}

func TestStuckSnapshotBacksOff(t *testing.T) {
	rec := &tape{}
	clock := &manual{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	p.after = clock.After
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 1, 1, nil) }
	listenRecon(t, f, p)
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
	publish(t, f.nc, 7, orderEv(1, 10, mustCloid(1, 1), tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
	waitFor(t, 5*time.Second, func() bool { return len(clock.waits()) >= 1 || f.viewCount() >= 3 })
	if f.viewCount() != 2 || len(clock.waits()) != 1 || clock.waits()[0] != time.Second {
		t.Fatalf("views %d waits %v", f.viewCount(), clock.waits())
	}
	if len(rec.orders()) != 0 {
		t.Fatal(rec.orders())
	}
}

func TestReconBackoff(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		rec := &tape{}
		clock := &manual{}
		f, p := sessionHooks(t, 40*time.Millisecond, rec.hooks(), decl(7, 1))
		p.after = clock.After
		f.listenViews()
		reply := false
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
			f.mu.Lock()
			ok := reply
			f.mu.Unlock()
			if !ok {
				return nil
			}
			return snap(3, 5, 1, nil)
		}
		if err := p.Listen(); err != nil {
			t.Fatal(err)
		}
		publish(t, f.nc, 7, orderEv(3, 6, mustCloid(1, 1), tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		waitFor(t, 5*time.Second, func() bool { return held(p, 7) == 1 })
		if err := p.Recon(); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return len(clock.waits()) >= 1 })
		if clock.waits()[0] != time.Second || len(rec.orders()) != 0 {
			t.Fatalf("waits %v orders %d", clock.waits(), len(rec.orders()))
		}
		clock.fire(0)
		waitFor(t, 5*time.Second, func() bool { return len(clock.waits()) >= 2 })
		if clock.waits()[1] != 2*time.Second {
			t.Fatalf("%v", clock.waits())
		}
		f.mu.Lock()
		reply = true
		f.mu.Unlock()
		clock.fire(1)
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 && len(rec.orders()) == 1 })
		if rec.orders()[0].Seq != 6 || rec.orders()[0].Incarnation != 3 {
			t.Fatalf("%+v", rec.orders()[0].TD)
		}
	})

	t.Run("no responder", func(t *testing.T) {
		rec := &tape{}
		clock := &manual{}
		f, p := sessionHooks(t, 40*time.Millisecond, rec.hooks(), decl(7, 1))
		p.after = clock.After
		if err := p.Listen(); err != nil {
			t.Fatal(err)
		}
		publish(t, f.nc, 7, orderEv(1, 1, mustCloid(1, 1), tdv1.OrderStatus_ORDER_STATUS_NEW))
		waitFor(t, 5*time.Second, func() bool { return held(p, 7) == 1 })
		if err := p.Recon(); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return len(clock.waits()) >= 1 })
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
		f.listenViews()
		clock.fire(0)
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 && len(rec.orders()) == 1 })
	})

	t.Run("not recon", func(t *testing.T) {
		rec := &tape{}
		clock := &manual{}
		f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
		f.serve()
		p.after = clock.After
		var n int
		f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
			n++
			if n == 1 {
				return &tdv1.ViewReply{}
			}
			return snap(1, 0, 1, nil)
		}
		if err := p.Listen(); err != nil {
			t.Fatal(err)
		}
		if err := p.Recon(); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return len(clock.waits()) >= 1 })
		clock.fire(0)
		waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
	})
}

func TestCancelTimeout(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, 40*time.Millisecond, rec.hooks(), decl(7, 1))
	f.serve()
	id := mustCloid(1, 1)
	row := &tdv1.Order{ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: 1001}
	f.listenOrders()
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, []*tdv1.Order{row}) }
	listenRecon(t, f, p)
	if err := p.Cancel(7, id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 1 })
	if len(rec.rejects()) != 0 || len(rec.orders()) != 0 {
		t.Fatalf("reject %+v order %+v", rec.rejects(), rec.orders())
	}
	o, ok := p.Order(7, id)
	if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED {
		t.Fatalf("%+v %v", o, ok)
	}
	if f.orderCount() != 1 {
		t.Fatalf("requests %d", f.orderCount())
	}
	publish(t, f.nc, 7, orderEv(1, 1, id, tdv1.OrderStatus_ORDER_STATUS_CANCELLED))
	waitFor(t, 5*time.Second, func() bool { return len(rec.orders()) == 1 })
	if _, ok := p.Order(7, id); ok {
		t.Fatal("cancelled order stayed")
	}
	if rec.orders()[0].Origin != FromTD {
		t.Fatal(rec.orders()[0].Origin)
	}
}

func TestPushedRecon(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	a := mustCloid(1, 1)
	b := mustCloid(1, 2)
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
		return snap(1, 0, 4, []*tdv1.Order{{ClientOrderId: a, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: 1}})
	}
	f.onOrder = accept
	listenRecon(t, f, p)
	publish(t, f.nc, 7, &tdv1.AccountEvent{
		Seq: 1, Source: &commonv1.WorkerRef{Incarnation: 1},
		Event: &tdv1.AccountEvent_Recon{Recon: &tdv1.Recon{
			OpenOrders: []*tdv1.Order{{ClientOrderId: b, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Uid: 2}},
		}},
	})
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 2 })
	open := p.OpenOrders(7, 1)
	if len(open) != 1 || open[0].GetClientOrderId() != b {
		t.Fatalf("%v", open)
	}
	if err := p.BatchCancel(7, 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return f.orderCount() == 1 })
	if f.orders()[0].req.GetBatchCancel() == nil {
		t.Fatal("cancel_batch was cleared by the pushed recon")
	}
}

func TestRequestReconHolds(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	var n int
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
		n++
		if n == 1 {
			return snap(1, 1, 1, nil)
		}
		publish(t, f.nc, 7, orderEv(1, 5, mustCloid(1, 1), tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
		if !until(time.Second, func() bool { return held(p, 7) >= 1 }) {
			t.Errorf("event during RequestRecon was not held")
		}
		return snap(1, 4, 1, nil)
	}
	listenRecon(t, f, p)
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 1 })
	if err := p.RequestRecon(7); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(rec.recons()) == 2 && len(rec.orders()) == 1 })
	if rec.recons()[1].Recon.GetReason() != tdv1.ReconReason_RECON_REASON_REQUESTED || rec.orders()[0].Seq != 5 {
		t.Fatalf("recon %+v order %+v", rec.recons()[1], rec.orders()[0].TD)
	}
}

func TestMalformedDropped(t *testing.T) {
	rec := &tape{}
	var mu sync.Mutex
	var logs []string
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	p.logf = func(s string) {
		mu.Lock()
		logs = append(logs, s)
		mu.Unlock()
	}
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
	listenRecon(t, f, p)
	if err := f.nc.Publish(evtSubject(7), []byte{0x0a}); err != nil {
		t.Fatal(err)
	}
	publish(t, f.nc, 7, orderEv(1, 1, mustCloid(1, 1), tdv1.OrderStatus_ORDER_STATUS_NEW))
	waitFor(t, 5*time.Second, func() bool { return len(rec.orders()) == 1 })
	mu.Lock()
	n := len(logs)
	mu.Unlock()
	if n == 0 {
		t.Fatal("malformed event was not logged")
	}
	if f.viewCount() != 1 {
		t.Fatalf("views %d", f.viewCount())
	}
}

func TestHoldCap(t *testing.T) {
	var mu sync.Mutex
	var reason string
	var fails int
	f, p := session(t, time.Second, decl(7, 1))
	p.fail = func(r string) {
		mu.Lock()
		fails++
		reason = r
		mu.Unlock()
	}
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return nil }
	f.listenViews()
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	if err := p.Recon(); err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(&tdv1.AccountEvent{Seq: 1, Source: &commonv1.WorkerRef{Incarnation: 1}})
	if err != nil {
		t.Fatal(err)
	}
	subj := evtSubject(7)
	for range dispatch.TDCap + 1 {
		if err := f.nc.Publish(subj, body); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return fails >= 1
	})
	mu.Lock()
	got, n := reason, fails
	mu.Unlock()
	if got != dispatch.ReasonTDFull {
		t.Fatalf("reason %q", got)
	}
	if held(p, 7) != dispatch.TDCap {
		t.Fatalf("held %d", held(p, 7))
	}
	for range 10 {
		if err := f.nc.Publish(subj, body); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return held(p, 7) == dispatch.TDCap })
	mu.Lock()
	n = fails
	mu.Unlock()
	if n != 1 {
		t.Fatalf("fail calls %d", n)
	}
}

type tape struct {
	mu       sync.Mutex
	ordersV  []OrderEvent
	fillsV   []FillEvent
	posV     []PositionEvent
	balV     []BalanceEvent
	reconsV  []ReconEvent
	rejectsV []RejectEvent
}

func (t *tape) hooks() Hooks {
	return Hooks{
		Order:    func(e OrderEvent) { t.addOrder(e) },
		Fill:     func(e FillEvent) { t.mu.Lock(); t.fillsV = append(t.fillsV, e); t.mu.Unlock() },
		Position: func(e PositionEvent) { t.mu.Lock(); t.posV = append(t.posV, e); t.mu.Unlock() },
		Balance:  func(e BalanceEvent) { t.mu.Lock(); t.balV = append(t.balV, e); t.mu.Unlock() },
		Recon:    func(e ReconEvent) { t.mu.Lock(); t.reconsV = append(t.reconsV, e); t.mu.Unlock() },
		Reject:   func(e RejectEvent) { t.mu.Lock(); t.rejectsV = append(t.rejectsV, e); t.mu.Unlock() },
	}
}

func (t *tape) addOrder(e OrderEvent) {
	t.mu.Lock()
	t.ordersV = append(t.ordersV, e)
	t.mu.Unlock()
}

func (t *tape) orders() []OrderEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]OrderEvent(nil), t.ordersV...)
}

func (t *tape) fills() []FillEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]FillEvent(nil), t.fillsV...)
}

func (t *tape) positions() []PositionEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]PositionEvent(nil), t.posV...)
}

func (t *tape) balances() []BalanceEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]BalanceEvent(nil), t.balV...)
}

func (t *tape) recons() []ReconEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ReconEvent(nil), t.reconsV...)
}

func (t *tape) rejects() []RejectEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]RejectEvent(nil), t.rejectsV...)
}

func (t *tape) orderKeys() []string {
	ev := t.orders()
	out := make([]string, len(ev))
	for i := range ev {
		out[i] = key(ev[i].Incarnation, ev[i].Seq)
	}
	return out
}

func key(inc uint32, seq uint64) string {
	return strconv.FormatUint(uint64(inc), 10) + ":" + strconv.FormatUint(seq, 10)
}

type manual struct {
	mu sync.Mutex
	d  []time.Duration
	ch []chan time.Time
}

func (m *manual) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	m.mu.Lock()
	m.d = append(m.d, d)
	m.ch = append(m.ch, ch)
	m.mu.Unlock()
	return ch
}

func (m *manual) waits() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.d...)
}

func (m *manual) fire(i int) {
	m.mu.Lock()
	ch := m.ch[i]
	m.mu.Unlock()
	ch <- time.Unix(0, 0)
}

type orderRec struct {
	api int64
	req *tdv1.OrderRequest
}

type fake struct {
	t        *testing.T
	nc       *nats.Conn
	mu       sync.Mutex
	onOrder  func(int64, *tdv1.OrderRequest) *tdv1.OrderReply
	onView   func(int64, *tdv1.ViewRequest) *tdv1.ViewReply
	orderV   []orderRec
	viewN    int
	ordersOn bool
	viewsOn  bool
}

func (f *fake) listenOrders() { f.listen("td.order.>") }
func (f *fake) listenViews()  { f.listen("td.view.>") }

func (f *fake) listen(subject string) {
	f.mu.Lock()
	if subject == "td.order.>" && f.ordersOn {
		f.mu.Unlock()
		return
	}
	if subject == "td.view.>" && f.viewsOn {
		f.mu.Unlock()
		return
	}
	if subject == "td.order.>" {
		f.ordersOn = true
	} else {
		f.viewsOn = true
	}
	f.mu.Unlock()
	sub, err := f.nc.Subscribe(subject, f.handle)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := f.nc.FlushTimeout(2 * time.Second); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fake) handle(m *nats.Msg) {
	if m.Reply == "" {
		return
	}
	kind, api, ok := parseTD(m.Subject)
	if !ok {
		return
	}
	switch kind {
	case "order":
		var req tdv1.OrderRequest
		if err := proto.Unmarshal(m.Data, &req); err != nil {
			f.t.Errorf("order: %v", err)
			return
		}
		f.mu.Lock()
		f.orderV = append(f.orderV, orderRec{api: api, req: proto.Clone(&req).(*tdv1.OrderRequest)})
		fn := f.onOrder
		f.mu.Unlock()
		if fn == nil {
			return
		}
		rep := fn(api, &req)
		if rep == nil {
			return
		}
		b, err := proto.Marshal(rep)
		if err != nil {
			f.t.Errorf("order reply: %v", err)
			return
		}
		_ = m.Respond(b)
	case "view":
		var req tdv1.ViewRequest
		if err := proto.Unmarshal(m.Data, &req); err != nil {
			f.t.Errorf("view: %v", err)
			return
		}
		f.mu.Lock()
		f.viewN++
		fn := f.onView
		f.mu.Unlock()
		if fn == nil {
			return
		}
		rep := fn(api, &req)
		if rep == nil {
			return
		}
		b, err := proto.Marshal(rep)
		if err != nil {
			f.t.Errorf("view reply: %v", err)
			return
		}
		_ = m.Respond(b)
	}
}

func (f *fake) orders() []orderRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]orderRec(nil), f.orderV...)
}

func (f *fake) orderCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.orderV)
}

func (f *fake) viewCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.viewN
}

func accept(int64, *tdv1.OrderRequest) *tdv1.OrderReply {
	return &tdv1.OrderReply{Accepted: true}
}

func session(t *testing.T, recv time.Duration, accs ...*sessionv1.AccountDecl) (*fake, *Path) {
	t.Helper()
	f, p := sessionHooks(t, recv, Hooks{}, accs...)
	f.serve()
	return f, p
}

func sessionHooks(t *testing.T, recv time.Duration, hooks Hooks, accs ...*sessionv1.AccountDecl) (*fake, *Path) {
	t.Helper()
	url := startNATS(t)
	tdc := connect(t, url)
	sc := connect(t, url)
	f := &fake{t: t, nc: tdc}
	p := openOn(t, sc, recv, hooks, accs...)
	return f, p
}

func (f *fake) serve() { f.listenOrders(); f.listenViews() }

func openPath(t *testing.T, recv time.Duration, hooks Hooks, accs ...*sessionv1.AccountDecl) *Path {
	t.Helper()
	url := startNATS(t)
	return openOn(t, connect(t, url), recv, hooks, accs...)
}

func openOn(t *testing.T, nc *nats.Conn, recv time.Duration, hooks Hooks, accs ...*sessionv1.AccountDecl) *Path {
	t.Helper()
	p, err := Open(Config{
		SessionID:  "c0ffee",
		Accounts:   accs,
		RecvWindow: recv,
		NC:         nc,
		Hooks:      hooks,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func listenRecon(t *testing.T, f *fake, p *Path) {
	t.Helper()
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	if err := p.Recon(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return f.viewCount() >= len(p.accounts) })
	// The reply's hook has run once the ask returns to the account loop. Poll the
	// watermark so a following publish is live.
	waitFor(t, 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, acc := range p.accounts {
			if !acc.have || acc.holding {
				return false
			}
		}
		return true
	})
}

func decl(api int64, algos ...uint32) *sessionv1.AccountDecl {
	return &sessionv1.AccountDecl{ApiId: api, AlgoIds: algos}
}

func mustCloid(algo, seq uint32) string {
	s, err := cloid.Encode(algo, cloid.EpochMs, seq)
	if err != nil {
		panic(err)
	}
	return s
}

func snap(inc uint32, seq uint64, batch uint32, orders []*tdv1.Order) *tdv1.ViewReply {
	return &tdv1.ViewReply{
		Incarnation: inc,
		Seq:         seq,
		CancelBatch: batch,
		Reply:       &tdv1.ViewReply_Recon{Recon: &tdv1.Recon{OpenOrders: orders}},
	}
}

func orderEv(inc uint32, seq uint64, id string, st tdv1.OrderStatus) *tdv1.AccountEvent {
	return &tdv1.AccountEvent{
		Seq:    seq,
		Source: &commonv1.WorkerRef{Incarnation: inc},
		Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{Order: &tdv1.Order{
			ClientOrderId: id, Status: st, Uid: 1001,
		}}},
	}
}

func fillEv(inc uint32, seq uint64, id string) *tdv1.AccountEvent {
	return &tdv1.AccountEvent{
		Seq:    seq,
		Source: &commonv1.WorkerRef{Incarnation: inc},
		Event:  &tdv1.AccountEvent_Fill{Fill: &tdv1.Fill{ClientOrderId: id, FillId: id, Qty: 1}},
	}
}

func publish(t *testing.T, nc *nats.Conn, api int64, ev *tdv1.AccountEvent) {
	t.Helper()
	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(evtSubject(api), b); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func held(p *Path, api int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[api]
	if acc == nil {
		return 0
	}
	return len(acc.hold)
}

func parseTD(subject string) (string, int64, bool) {
	rest, ok := strings.CutPrefix(subject, "td.")
	if !ok {
		return "", 0, false
	}
	kind, num, ok := strings.Cut(rest, ".")
	if !ok {
		return "", 0, false
	}
	api, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return kind, api, true
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	if until(d, cond) {
		return
	}
	t.Fatalf("condition not met within %s", d)
}

func until(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return cond()
		}
		time.Sleep(time.Millisecond)
	}
}

func connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Name("tdpath-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func startNATS(t *testing.T) string {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: false}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("nats-server did not accept connections")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	addr := ns.Addr()
	if addr == nil {
		t.Fatal("nats-server has no address")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return "nats://" + host + ":" + port
}
