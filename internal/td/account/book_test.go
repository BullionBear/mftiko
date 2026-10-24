package account

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

type bookRig struct {
	t   *testing.T
	ctx context.Context
	b   *book
	bus *fakeBus
	ins *fakeInstruments
}

func newRig(t *testing.T) *bookRig {
	r := &bookRig{t: t, ctx: t.Context(), bus: newFakeBus(), ins: instruments()}
	r.b = newBook(r.ins, r.bus, 42, &commonv1.WorkerRef{Key: "k", Incarnation: 3}, zerolog.Nop(), time.Now)
	go r.b.run(r.ctx)
	return r
}

// sync waits until the book has handled everything sent so far: it answers in order.
func (r *bookRig) sync() *tdv1.ViewReply {
	r.t.Helper()
	v := r.b.view(r.ctx, &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Recon{Recon: &tdv1.ReconRequest{}}})
	if v == nil {
		r.t.Fatal("no answer")
	}
	return v
}

func (r *bookRig) viewRecon() *tdv1.Recon {
	r.t.Helper()
	return r.sync().GetRecon()
}

func (r *bookRig) recon(s *exchange.AccountSnapshot) {
	r.b.beginRecon(r.ctx)
	r.b.applyRecon(r.ctx, s)
}

func ord(oid, cid string, st exchange.OrderStatus, filled float64) exchange.OrderUpdate {
	return exchange.OrderUpdate{Exch: btc, OrderID: oid, ClientOrderID: cid, Side: exchange.SideBuy, Type: exchange.OrderTypeLimit,
		Status: st, Qty: 1, Price: 10, FilledQty: filled, QuoteQty: decimal.Absent(), AvgPrice: decimal.Absent()}
}

// kinds are the events after the first n, as short strings: what changed and to what.
func kinds(evs []*tdv1.AccountEvent) []string {
	var out []string
	for _, e := range evs {
		switch x := e.GetEvent().(type) {
		case *tdv1.AccountEvent_Recon:
			s := "recon:" + x.Recon.GetReason().String()
			for _, o := range x.Recon.GetOpenOrders() {
				s += " " + o.GetOrderId() + "=" + o.GetStatus().String()
			}
			out = append(out, s)
		case *tdv1.AccountEvent_OrderUpdate:
			o := x.OrderUpdate.GetOrder()
			out = append(out, "order:"+o.GetOrderId()+"="+o.GetStatus().String()+"@"+strconv.FormatFloat(o.GetFilledQty(), 'f', -1, 64))
		case *tdv1.AccountEvent_Fill:
			out = append(out, "fill:"+x.Fill.GetFillId())
		case *tdv1.AccountEvent_BalanceUpdate:
			out = append(out, "balance")
		case *tdv1.AccountEvent_PositionUpdate:
			out = append(out, "position")
		}
	}
	return out
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func seqsFrom1(t *testing.T, evs []*tdv1.AccountEvent) {
	t.Helper()
	for i, e := range evs {
		if e.GetSeq() != uint64(i+1) || e.GetSource().GetIncarnation() != 3 {
			t.Fatalf("event %d has seq %d", i, e.GetSeq())
		}
	}
}

// Events that arrive while a recon runs are held; after the snapshot they are replayed in order.
// One older than the snapshot is dropped; the ones after it apply.
func TestBookHoldsEventsDuringRecon(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{OpenOrders: []exchange.OrderUpdate{ord("1", "a", exchange.OrderStatusAccepted, 0)}, AsOfNs: 1})

	r.b.beginRecon(r.ctx)
	e1 := ord("1", "a", exchange.OrderStatusPartiallyFilled, 0.3) // older than the next snapshot
	e2 := ord("1", "a", exchange.OrderStatusPartiallyFilled, 0.8) // newer
	e3 := ord("2", "b", exchange.OrderStatusAccepted, 0)          // placed after the snapshot
	r.b.order(r.ctx, &e1)
	r.b.order(r.ctx, &e2)
	r.b.order(r.ctx, &e3)
	r.b.fill(r.ctx, &exchange.Fill{Exch: btc, FillID: "f1", OrderID: "1", Qty: 0.5, Price: 10})
	r.sync()
	if n := len(r.bus.all()); n != 1 {
		t.Fatalf("%d events published while the recon ran", n)
	}
	r.b.applyRecon(r.ctx, &exchange.AccountSnapshot{OpenOrders: []exchange.OrderUpdate{
		ord("1", "a", exchange.OrderStatusPartiallyFilled, 0.5), ord("3", "c", exchange.OrderStatusAccepted, 0),
	}, AsOfNs: 2})
	r.sync()
	evs := r.bus.all()
	seqsFrom1(t, evs)
	eq(t, kinds(evs),
		"recon:RECON_REASON_INCARNATION 1=ORDER_STATUS_ACCEPTED",
		"recon:RECON_REASON_RECONNECT 1=ORDER_STATUS_PARTIALLY_FILLED 3=ORDER_STATUS_ACCEPTED",
		"order:1=ORDER_STATUS_PARTIALLY_FILLED@0.8",
		"order:2=ORDER_STATUS_ACCEPTED@0",
		"fill:f1")
}

// A live order the snapshot does not have is UNKNOWN until the venue is asked (E06-D015); one the venue
// has not answered yet stays PENDING_NEW.
func TestBookVanishedOrders(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{OpenOrders: []exchange.OrderUpdate{ord("1", "a", exchange.OrderStatusAccepted, 0)}, AsOfNs: 1})
	p := ord("", "p", exchange.OrderStatusPendingNew, 0)
	r.b.order(r.ctx, &p)
	r.recon(&exchange.AccountSnapshot{AsOfNs: 2})
	r.sync()
	rc := r.bus.recons()
	if len(rc) != 2 {
		t.Fatalf("%d recons", len(rc))
	}
	got := map[string]tdv1.OrderStatus{}
	for _, o := range rc[1].GetOpenOrders() {
		got[o.GetClientOrderId()] = o.GetStatus()
	}
	if got["a"] != tdv1.OrderStatus_ORDER_STATUS_UNKNOWN || got["p"] != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW || len(got) != 2 {
		t.Fatalf("open orders %v", got)
	}
	// The venue's later word on it is news.
	f := ord("1", "a", exchange.OrderStatusCancelled, 0)
	r.b.order(r.ctx, &f)
	if v := r.sync(); len(v.GetRecon().GetOpenOrders()) != 1 {
		t.Fatalf("open orders %v", v.GetRecon().GetOpenOrders())
	}
}

// A finished order leaves the OMS at once (E06-D015). Its repeats, and a duplicate of any change, are
// not published again.
func TestBookFinishedOrdersLeave(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{AsOfNs: 1})
	for _, e := range []exchange.OrderUpdate{
		ord("1", "a", exchange.OrderStatusAccepted, 0),
		ord("1", "a", exchange.OrderStatusAccepted, 0),          // a duplicate
		ord("1", "a", exchange.OrderStatusPartiallyFilled, 1),   // filled, status not yet final
		ord("1", "a", exchange.OrderStatusFilled, 1),            // the end
		ord("1", "a", exchange.OrderStatusFilled, 1),            // a repeat of the end
		ord("1", "a", exchange.OrderStatusPartiallyFilled, 0.5), // older than the end
	} {
		r.b.order(r.ctx, &e)
	}
	v := r.sync()
	if len(v.GetRecon().GetOpenOrders()) != 0 {
		t.Fatalf("open orders %v", v.GetRecon().GetOpenOrders())
	}
	evs := r.bus.all()
	eq(t, kinds(evs[1:]), "order:1=ORDER_STATUS_ACCEPTED@0", "order:1=ORDER_STATUS_PARTIALLY_FILLED@1", "order:1=ORDER_STATUS_FILLED@1")
	if c := evs[3].GetOrderUpdate().GetCause(); c != tdv1.OrderCause_ORDER_CAUSE_FILLED {
		t.Fatalf("cause %v", c)
	}
	if v.GetSeq() != 4 {
		t.Fatalf("view seq %d", v.GetSeq())
	}
}

func TestBookFills(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{AsOfNs: 1})
	for _, f := range []exchange.Fill{
		{Exch: btc, FillID: "f1", Qty: 0.5},
		{Exch: btc, FillID: "f1", Qty: 0.5},                                    // the same fill twice (two connections, or the answer and the event)
		{Exch: symbol.ExchKey{ExchTicker: "BTC-SWAP"}, FillID: "f2", Qty: 300}, // 300 contracts of 0.01
		{Exch: symbol.ExchKey{ExchTicker: "OLD"}, FillID: "f3", Qty: 1},        // delisted since: SYM's terminal table
		{Exch: symbol.ExchKey{ExchTicker: "NOPE"}, FillID: "f4", Qty: 1},       // SYM never saw it: dropped
	} {
		r.b.fill(r.ctx, &f)
	}
	r.sync()
	evs := r.bus.all()[1:]
	eq(t, kinds(evs), "fill:f1", "fill:f2", "fill:f3")
	if q := evs[1].GetFill().GetQty(); math.Abs(q-3) > 1e-12 || evs[1].GetFill().GetUid() != 8 {
		t.Fatalf("contracts: %v", evs[1].GetFill())
	}
	if evs[2].GetFill().GetUid() != 9 || r.ins.asked.Load() != 2 {
		t.Fatalf("inactive: %v, asked %d", evs[2].GetFill(), r.ins.asked.Load())
	}
}

// Positions and balances follow the events; a zero size closes a position; leverage stays cached
// after it closes. A Recon returns every position and every balance (E06-D033).
func TestBookLedger(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{
		Positions: []exchange.Position{{Exch: btc, Size: 1, EntryPrice: 10, MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: 5}},
		Balances:  []exchange.Balance{{Asset: "USDT", Free: 100}},
		AsOfNs:    1,
	})
	r.b.positionEvent(r.ctx, []exchange.Position{
		{Exch: symbol.ExchKey{ExchTicker: "ETHUSDT"}, Size: -2, EntryPrice: decimal.Absent(), MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()},
	})
	r.b.balanceEvent(r.ctx, []exchange.Balance{{Asset: "USDT", Free: 90, Locked: 10}, {Asset: "ETH", Free: 1}})
	l := r.viewRecon()
	if l.GetReason() != tdv1.ReconReason_RECON_REASON_REQUESTED || len(l.GetPositions()) != 2 || len(l.GetBalances()) != 2 || l.GetBalances()[1].GetFree() != 90 {
		t.Fatalf("recon %v", l)
	}
	sizes := map[uint32]float64{}
	for _, p := range l.GetPositions() {
		sizes[p.GetUid()] = p.GetSize()
		if p.GetUid() == 10 && (p.Leverage != nil || p.EntryPrice != nil) {
			t.Fatalf("eth position %v", p)
		}
	}
	if sizes[7] != 1 || sizes[10] != -2 {
		t.Fatalf("positions %v", l.GetPositions())
	}
	r.b.positionEvent(r.ctx, []exchange.Position{{Exch: btc, Size: 0, EntryPrice: decimal.Absent(), MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()}})
	left := r.viewRecon().GetPositions()
	if len(left) != 1 || left[0].GetUid() != 10 {
		t.Fatalf("closed position %v", left)
	}
	lev := func(uid uint32) *tdv1.LeverageInfo {
		return r.b.view(r.ctx, &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Leverage{Leverage: &tdv1.LeverageRequest{Uid: uid}}}).GetLeverage()
	}
	if x := lev(7); x.Leverage == nil || *x.Leverage != 5 {
		t.Fatalf("leverage after close %v", x)
	}
	if x := lev(10); x.Leverage != nil {
		t.Fatalf("leverage never reported %v", x)
	}
}

// A flat row for a position not held (Bybit sends one whenever an order touches a symbol) is not
// published; its leverage is still cached. Closing a held position is published.
func TestBookFlatRowNotHeld(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{AsOfNs: 1})
	flat := exchange.Position{Exch: btc, Size: 0, EntryPrice: decimal.Absent(), MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: 10}
	r.b.positionEvent(r.ctx, []exchange.Position{flat})
	r.sync()
	eq(t, kinds(r.bus.all()), "recon:RECON_REASON_INCARNATION")
	if x := r.b.view(r.ctx, &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Leverage{Leverage: &tdv1.LeverageRequest{Uid: 7}}}).GetLeverage(); x.Leverage == nil || *x.Leverage != 10 {
		t.Fatalf("leverage %v", x)
	}
	open := flat
	open.Size = 0.001
	r.b.positionEvent(r.ctx, []exchange.Position{open})
	r.b.positionEvent(r.ctx, []exchange.Position{flat})
	r.sync()
	eq(t, kinds(r.bus.all()), "recon:RECON_REASON_INCARNATION", "position", "position")
	if p := r.viewRecon().GetPositions(); len(p) != 0 {
		t.Fatalf("closed position %v", p)
	}
}

// A Recon returns every live order. TD does not filter by the algo in the client order id, and an
// order placed by hand is in the same list (E06-D033).
func TestBookReconReturnsEveryOrder(t *testing.T) {
	r := newRig(t)
	c5, _ := cloid.Encode(5, cloid.EpochMs+1, 0)
	c6, _ := cloid.Encode(6, cloid.EpochMs+1, 0)
	r.recon(&exchange.AccountSnapshot{OpenOrders: []exchange.OrderUpdate{
		ord("1", c5, exchange.OrderStatusAccepted, 0), ord("2", c6, exchange.OrderStatusAccepted, 0),
		ord("3", "by-hand", exchange.OrderStatusAccepted, 0), ord("4", "", exchange.OrderStatusAccepted, 0),
	}, AsOfNs: 1})
	var got []string
	for _, o := range r.viewRecon().GetOpenOrders() {
		got = append(got, o.GetOrderId())
	}
	eq(t, got, "4", "1", "2", "3")
}

// A ReconRequest's orders, positions and balances are the book at ViewReply.seq. Events before the
// reply are in it; the next event is the next seq (E06-D033).
func TestBookReconSeqCutsBetweenEvents(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{
		OpenOrders: []exchange.OrderUpdate{ord("1", "a", exchange.OrderStatusAccepted, 0)},
		Positions:  []exchange.Position{{Exch: btc, Size: 1, EntryPrice: 10, MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()}},
		Balances:   []exchange.Balance{{Asset: "USDT", Free: 100}},
		AsOfNs:     1,
	})
	upd := ord("1", "a", exchange.OrderStatusPartiallyFilled, 0.4)
	r.b.order(r.ctx, &upd)
	r.b.positionEvent(r.ctx, []exchange.Position{
		{Exch: symbol.ExchKey{ExchTicker: "ETHUSDT"}, Size: -2, EntryPrice: decimal.Absent(), MarkPrice: decimal.Absent(), UnrealizedPnl: decimal.Absent(), Leverage: decimal.Absent()},
	})
	r.b.balanceEvent(r.ctx, []exchange.Balance{{Asset: "USDT", Free: 90, Locked: 10}})
	v := r.sync()
	before := r.bus.all()
	if v.GetSeq() != before[len(before)-1].GetSeq() || v.GetSeq() != 4 {
		t.Fatalf("view seq %d, events %d", v.GetSeq(), len(before))
	}
	rec := v.GetRecon()
	if rec.GetReason() != tdv1.ReconReason_RECON_REASON_REQUESTED || rec.GetIncarnation() != v.GetIncarnation() {
		t.Fatalf("recon %v", rec)
	}
	if len(rec.GetOpenOrders()) != 1 || rec.GetOpenOrders()[0].GetOrderId() != "1" || rec.GetOpenOrders()[0].GetFilledQty() != 0.4 {
		t.Fatalf("orders %v", rec.GetOpenOrders())
	}
	sizes := map[uint32]float64{}
	for _, p := range rec.GetPositions() {
		sizes[p.GetUid()] = p.GetSize()
	}
	if len(sizes) != 2 || sizes[7] != 1 || sizes[10] != -2 {
		t.Fatalf("positions %v", rec.GetPositions())
	}
	if len(rec.GetBalances()) != 1 || rec.GetBalances()[0].GetFree() != 90 || rec.GetBalances()[0].GetLocked() != 10 {
		t.Fatalf("balances %v", rec.GetBalances())
	}
	for _, e := range before {
		if e.GetRecon().GetReason() == tdv1.ReconReason_RECON_REASON_REQUESTED {
			t.Fatal("a requested recon was published on td.evt")
		}
	}
	later := ord("2", "b", exchange.OrderStatusAccepted, 0)
	r.b.order(r.ctx, &later)
	after := r.sync()
	evs := r.bus.all()
	if evs[len(before)].GetSeq() != v.GetSeq()+1 || after.GetSeq() != v.GetSeq()+1 {
		t.Fatalf("seq cut: view %d, next event %d, later view %d", v.GetSeq(), evs[len(before)].GetSeq(), after.GetSeq())
	}
	ids := map[string]bool{}
	for _, o := range after.GetRecon().GetOpenOrders() {
		ids[o.GetOrderId()] = true
	}
	if !ids["1"] || !ids["2"] || len(ids) != 2 {
		t.Fatalf("later orders %v", after.GetRecon().GetOpenOrders())
	}
}

// An order with an empty client id is one order, joined by order id on a later event and on recon.
func TestBookEmptyClientIDMatchesByOrderID(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{OpenOrders: []exchange.OrderUpdate{
		ord("9", "", exchange.OrderStatusAccepted, 0),
	}, AsOfNs: 1})
	e := ord("9", "", exchange.OrderStatusPartiallyFilled, 0.4)
	r.b.order(r.ctx, &e)
	got := r.sync().GetRecon().GetOpenOrders()
	if len(got) != 1 || got[0].GetOrderId() != "9" || got[0].GetClientOrderId() != "" || got[0].GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED {
		t.Fatalf("event did not join the empty client id: %v", got)
	}
	r.recon(&exchange.AccountSnapshot{OpenOrders: []exchange.OrderUpdate{
		ord("9", "", exchange.OrderStatusPartiallyFilled, 0.4),
	}, AsOfNs: 2})
	got = r.sync().GetRecon().GetOpenOrders()
	if len(got) != 1 || got[0].GetOrderId() != "9" || got[0].GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED {
		t.Fatalf("recon duplicated the empty client id: %v", got)
	}
}

// The first venue message that numbers an order joins the pending row by client id.
func TestBookFirstVenueIDJoinsByClientID(t *testing.T) {
	r := newRig(t)
	r.recon(&exchange.AccountSnapshot{AsOfNs: 1})
	p := ord("", "pend", exchange.OrderStatusPendingNew, 0)
	r.b.order(r.ctx, &p)
	a := ord("42", "pend", exchange.OrderStatusAccepted, 0)
	r.b.order(r.ctx, &a)
	got := r.sync().GetRecon().GetOpenOrders()
	if len(got) != 1 || got[0].GetOrderId() != "42" || got[0].GetClientOrderId() != "pend" {
		t.Fatalf("first order id did not join: %v", got)
	}
}

// A view carries how far the book is: state, reconciled_at, incarnation and seq.
func TestBookViewHeader(t *testing.T) {
	r := newRig(t)
	if v := r.sync(); v.GetSeq() != 0 || v.GetReconciledAtNs() != 0 || v.GetIncarnation() != 3 {
		t.Fatalf("before the first recon: %v", v)
	}
	r.recon(&exchange.AccountSnapshot{AsOfNs: 77})
	r.b.setState(r.ctx, tdv1.AccountState_ACCOUNT_STATE_READY, "")
	if v := r.sync(); v.GetSeq() != 1 || v.GetReconciledAtNs() != 77 || v.GetState() != tdv1.AccountState_ACCOUNT_STATE_READY {
		t.Fatalf("after: %v", v)
	}
	if v := r.b.view(r.ctx, &tdv1.ViewRequest{}); v.GetError().GetCode() != "invalid" {
		t.Fatalf("empty request: %v", v)
	}
}
