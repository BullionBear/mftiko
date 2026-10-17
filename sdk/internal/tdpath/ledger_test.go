package tdpath

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

func TestMergePendingNew(t *testing.T) {
	const (
		id = "local"
	)
	pending := &tdv1.Order{ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW, Qty: 1}
	accepted := &tdv1.Order{ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Qty: 9}
	filled := &tdv1.Order{ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_FILLED, Qty: 1}
	unknown := &tdv1.Order{ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_UNKNOWN, Qty: 1}

	cases := []struct {
		name        string
		local       *tdv1.Order
		gen         uint64
		stamped     bool
		snap        []*tdv1.Order
		requestGen  uint64
		fromRequest bool
		wantOK      bool
		wantStatus  tdv1.OrderStatus
		wantQty     float64
	}{
		{name: "after request and absent", local: pending, gen: 2, stamped: true, requestGen: 2, fromRequest: true, wantOK: true, wantStatus: tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW, wantQty: 1},
		{name: "before request and absent", local: pending, gen: 1, stamped: true, requestGen: 2, fromRequest: true},
		{name: "snapshot wins", local: pending, gen: 2, stamped: true, snap: []*tdv1.Order{accepted}, requestGen: 2, fromRequest: true, wantOK: true, wantStatus: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, wantQty: 9},
		{name: "pushed recon drops absent pending", local: pending, gen: 2, stamped: true, requestGen: 2, fromRequest: false},
		{name: "accepted absent is dropped", local: accepted, gen: 2, stamped: true, requestGen: 2, fromRequest: true},
		{name: "terminal snapshot drops local pending", local: pending, gen: 2, stamped: true, snap: []*tdv1.Order{filled}, requestGen: 2, fromRequest: true},
		{name: "not a local submit", local: pending, stamped: false, requestGen: 2, fromRequest: true},
		{name: "unknown absent is dropped", local: unknown, gen: 2, stamped: true, requestGen: 2, fromRequest: true},
		{name: "snapshot order with no local row", snap: []*tdv1.Order{accepted}, requestGen: 2, fromRequest: true, wantOK: true, wantStatus: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, wantQty: 9},
		{name: "nil and empty id are ignored", snap: []*tdv1.Order{nil, {Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED}}, requestGen: 1, fromRequest: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := &account{orders: map[string]*tdv1.Order{}, submitGen: map[string]uint64{}}
			if c.local != nil {
				a.orders[id] = proto.Clone(c.local).(*tdv1.Order)
				if c.stamped {
					a.submitGen[id] = c.gen
				}
			}
			a.merge(c.snap, c.requestGen, c.fromRequest)
			got, ok := a.orders[id]
			if ok != c.wantOK {
				t.Fatalf("present %v, want %v", ok, c.wantOK)
			}
			if !ok {
				if _, still := a.submitGen[id]; still {
					t.Fatal("submit generation survived a dropped order")
				}
				return
			}
			if got.GetStatus() != c.wantStatus || got.GetQty() != c.wantQty {
				t.Fatalf("status %v qty %v", got.GetStatus(), got.GetQty())
			}
			if c.stamped {
				if g, ok := a.submitGen[id]; !ok || g != c.gen {
					t.Fatalf("submit generation %v %v", g, ok)
				}
			}
		})
	}
}

func TestLedgerSequence(t *testing.T) {
	id := mustCloid(1, 1)
	other := mustCloid(2, 1)
	p := &Path{byAPI: map[int64]*account{
		7: newBook(map[uint32]bool{1: true}),
		8: newBook(map[uint32]bool{2: true}),
	}}
	var hook string
	var hookOrder *tdv1.Order
	var hookOpen int
	var hookPos float64
	var hookPosOK bool
	var hookBal *tdv1.Balance
	p.hooks = Hooks{
		Order: func(ev OrderEvent) {
			hook = "order"
			o, ok := p.Order(ev.APIID, ev.Order.GetClientOrderId())
			if !ok {
				hookOrder = nil
				return
			}
			hookOrder = o
			hookOpen = 0
			for _, row := range p.OpenOrders(ev.APIID, 0) {
				if row.GetClientOrderId() == ev.Order.GetClientOrderId() {
					hookOpen++
				}
			}
		},
		Fill: func(FillEvent) {
			hook = "fill"
			if o, ok := p.Order(7, id); ok {
				hookOrder = o
			}
			hookPos, hookPosOK = sizeOf(p, 7, 3)
			hookBal, _ = p.Balance(7, "USDT")
		},
		Position: func(PositionEvent) {
			hook = "position"
			hookPos, hookPosOK = sizeOf(p, 7, 3)
		},
		Balance: func(ev BalanceEvent) {
			hook = "balance"
			hookBal = nil
			if n := len(ev.Balances); n > 0 {
				hookBal, _ = p.Balance(7, ev.Balances[n-1].GetAsset())
			}
			if hookBal != nil {
				hookBal.Free = -1
			}
		},
		Recon: func(ReconEvent) {
			hook = "recon"
			hookPos, hookPosOK = sizeOf(p, 7, 3)
			hookBal, _ = p.Balance(7, "USDT")
			hookOrder, _ = p.Order(7, id)
		},
	}

	price := 10.0
	steps := []struct {
		name string
		run  func()
		want func(t *testing.T)
	}{
		{
			name: "order update replaces the whole row",
			run: func() {
				p.apply(7, orderUpdate(&tdv1.Order{
					ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_NEW,
					Qty: 1, Price: &price, Uid: 1001, OrderId: "old",
				}))
			},
			want: func(t *testing.T) {
				if hook != "order" || hookOrder.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_NEW || hookOrder.GetQty() != 1 || hookOpen != 1 {
					t.Fatalf("during hook %s %+v open %d", hook, hookOrder, hookOpen)
				}
				o, ok := p.Order(7, id)
				if !ok || o.GetQty() != 1 || o.GetPrice() != 10 || o.GetOrderId() != "old" {
					t.Fatalf("stored %+v %v", o, ok)
				}
				o.Qty = -1
				again, _ := p.Order(7, id)
				if again.GetQty() != 1 {
					t.Fatal("order copy wrote through")
				}
			},
		},
		{
			name: "later update does not keep fields the event omitted",
			run: func() {
				p.apply(7, orderUpdate(&tdv1.Order{
					ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED,
					Qty: 2, Uid: 1001, OrderId: "ex-1", FilledQty: 0.5, CancelSentNs: 5,
				}))
			},
			want: func(t *testing.T) {
				if hookOrder.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || hookOrder.GetQty() != 2 || hookOpen != 1 {
					t.Fatalf("during %+v open %d", hookOrder, hookOpen)
				}
				o, ok := p.Order(7, id)
				if !ok || o.Price != nil || o.GetQty() != 2 || o.GetFilledQty() != 0.5 || o.GetCancelSentNs() != 5 || o.GetOrderId() != "ex-1" {
					t.Fatalf("stored %+v %v", o, ok)
				}
			},
		},
		{
			name: "another algo is not stored",
			run: func() {
				hook = ""
				p.apply(7, orderUpdate(&tdv1.Order{ClientOrderId: other, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Qty: 4}))
			},
			want: func(t *testing.T) {
				if hook != "" {
					t.Fatalf("hook %s", hook)
				}
				if _, ok := p.Order(7, other); ok {
					t.Fatal("stored another algo")
				}
				if _, ok := p.Order(7, id); !ok {
					t.Fatal("own order was dropped")
				}
			},
		},
		{
			name: "partial fill stays live",
			run: func() {
				p.apply(7, orderUpdate(&tdv1.Order{
					ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
					Qty: 2, FilledQty: 0.5, Uid: 1001,
				}))
			},
			want: func(t *testing.T) {
				if hookOrder.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED || hookOpen != 1 {
					t.Fatalf("during %+v open %d", hookOrder, hookOpen)
				}
			},
		},
		{
			name: "position update is absolute and visible inside the hook",
			run: func() {
				p.apply(7, &tdv1.AccountEvent{Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{
					Positions: []*tdv1.Position{nil, {Uid: 3, Size: 1}, {Uid: 3, Size: 5}, {Uid: 9, Size: 0}},
				}}})
			},
			want: func(t *testing.T) {
				if hook != "position" || !hookPosOK || hookPos != 5 {
					t.Fatalf("during %s size %v %v", hook, hookPos, hookPosOK)
				}
				if s, ok := sizeOf(p, 7, 3); !ok || s != 5 {
					t.Fatalf("uid 3 %v %v", s, ok)
				}
				if s, ok := sizeOf(p, 7, 9); !ok || s != 0 {
					t.Fatalf("flat position %v %v", s, ok)
				}
				if _, ok := p.Position(8, 3); ok {
					t.Fatal("position leaked to the other account")
				}
			},
		},
		{
			name: "balance update stores the three fields and leaves other assets",
			run: func() {
				p.apply(7, balanceUpdate(&tdv1.Balance{Asset: "USDT", Free: 1, Locked: 2, Prelock: 3}))
				p.apply(7, balanceUpdate(&tdv1.Balance{Asset: "BTC", Free: 4, Locked: 5, Prelock: 6}))
			},
			want: func(t *testing.T) {
				if hook != "balance" || hookBal == nil || hookBal.GetAsset() != "BTC" {
					t.Fatalf("during %+v", hookBal)
				}
				u, ok := p.Balance(7, "USDT")
				if !ok || u.GetFree() != 1 || u.GetLocked() != 2 || u.GetPrelock() != 3 {
					t.Fatalf("usdt %+v %v", u, ok)
				}
				b, ok := p.Balance(7, "BTC")
				if !ok || b.GetFree() != 4 || b.GetLocked() != 5 || b.GetPrelock() != 6 {
					t.Fatalf("btc %+v %v", b, ok)
				}
				b.Free = 0
				b.Locked = 0
				b.Prelock = 0
				b, ok = p.Balance(7, "BTC")
				if !ok || b.GetFree() != 4 || b.GetLocked() != 5 || b.GetPrelock() != 6 {
					t.Fatalf("copy wrote through %+v", b)
				}
			},
		},
		{
			name: "fill does not move the order or the ledger",
			run: func() {
				p.apply(7, &tdv1.AccountEvent{Event: &tdv1.AccountEvent_Fill{Fill: &tdv1.Fill{
					ClientOrderId: id, FillId: "f", Qty: 1, Price: 10,
				}}})
			},
			want: func(t *testing.T) {
				if hook != "fill" || hookOrder.GetFilledQty() != 0.5 || !hookPosOK || hookPos != 5 {
					t.Fatalf("during order %+v pos %v %v", hookOrder, hookPos, hookPosOK)
				}
				if hookBal.GetFree() != 1 || hookBal.GetLocked() != 2 || hookBal.GetPrelock() != 3 {
					t.Fatalf("during balance %+v", hookBal)
				}
				o, _ := p.Order(7, id)
				if o.GetFilledQty() != 0.5 || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED {
					t.Fatalf("order changed %+v", o)
				}
				u, _ := p.Balance(7, "USDT")
				if u.GetFree() != 1 || u.GetPrelock() != 3 {
					t.Fatalf("balance changed %+v", u)
				}
			},
		},
		{
			name: "other account is independent",
			run: func() {
				p.apply(8, &tdv1.AccountEvent{Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{
					Positions: []*tdv1.Position{{Uid: 3, Size: 7}},
				}}})
			},
			want: func(t *testing.T) {
				if s, ok := sizeOf(p, 8, 3); !ok || s != 7 {
					t.Fatalf("account 8 %v %v", s, ok)
				}
				if s, ok := sizeOf(p, 7, 3); !ok || s != 5 {
					t.Fatalf("account 7 changed %v %v", s, ok)
				}
			},
		},
		{
			name: "terminal is visible in the hook and gone after it",
			run: func() {
				p.apply(7, orderUpdate(&tdv1.Order{
					ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_FILLED, Qty: 2, FilledQty: 2, Uid: 1001,
				}))
			},
			want: func(t *testing.T) {
				if hook != "order" || hookOrder.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_FILLED || hookOpen != 0 {
					t.Fatalf("during %+v open %d", hookOrder, hookOpen)
				}
				if _, ok := p.Order(7, id); ok {
					t.Fatal("filled order stayed")
				}
				if len(p.OpenOrders(7, 0)) != 0 {
					t.Fatal(p.OpenOrders(7, 0))
				}
			},
		},
		{
			name: "recon replaces both books and clears the other account's nothing",
			run: func() {
				p.deliverRecon(7, TD{APIID: 7}, &tdv1.Recon{
					OpenOrders: []*tdv1.Order{{ClientOrderId: id, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Qty: 4, Uid: 1001}},
					Positions:  []*tdv1.Position{{Uid: 2, Size: 8}},
					Balances:   []*tdv1.Balance{{Asset: "ETH", Free: 9, Locked: 1, Prelock: 0}},
				}, 0, false, 0)
			},
			want: func(t *testing.T) {
				if hook != "recon" || hookPosOK || hookBal != nil || hookOrder == nil {
					t.Fatalf("during hook %s pos %v %v bal %+v order %+v", hook, hookPos, hookPosOK, hookBal, hookOrder)
				}
				if hookOrder.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || hookOrder.GetQty() != 4 {
					t.Fatalf("during order %+v", hookOrder)
				}
				if _, ok := p.Position(7, 3); ok || func() bool { _, ok := p.Position(7, 9); return ok }() {
					t.Fatal("old positions survived recon")
				}
				if s, ok := sizeOf(p, 7, 2); !ok || s != 8 {
					t.Fatalf("new position %v %v", s, ok)
				}
				if _, ok := p.Balance(7, "USDT"); ok {
					t.Fatal("old balance survived recon")
				}
				eth, ok := p.Balance(7, "ETH")
				if !ok || eth.GetFree() != 9 || eth.GetLocked() != 1 || eth.GetPrelock() != 0 {
					t.Fatalf("eth %+v %v", eth, ok)
				}
				if s, ok := sizeOf(p, 8, 3); !ok || s != 7 {
					t.Fatalf("other account %v %v", s, ok)
				}
			},
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.run()
			step.want(t)
		})
	}

	for _, st := range []tdv1.OrderStatus{
		tdv1.OrderStatus_ORDER_STATUS_CANCELLED,
		tdv1.OrderStatus_ORDER_STATUS_REJECTED,
		tdv1.OrderStatus_ORDER_STATUS_EXPIRED,
	} {
		fresh := mustCloid(1, uint32(st))
		p.apply(7, orderUpdate(&tdv1.Order{ClientOrderId: fresh, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Qty: 1}))
		if hookOpen != 1 {
			t.Fatalf("%s was not live during the hook", st)
		}
		p.apply(7, orderUpdate(&tdv1.Order{ClientOrderId: fresh, Status: st, Qty: 1}))
		if hookOrder.GetStatus() != st || hookOpen != 0 {
			t.Fatalf("%s during %+v open %d", st, hookOrder, hookOpen)
		}
		if _, ok := p.Order(7, fresh); ok {
			t.Fatalf("%s stayed", st)
		}
	}

	unk := mustCloid(1, 9)
	p.apply(7, orderUpdate(&tdv1.Order{ClientOrderId: unk, Status: tdv1.OrderStatus_ORDER_STATUS_UNKNOWN, Qty: 1}))
	o, ok := p.Order(7, unk)
	if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_UNKNOWN || hookOpen != 1 {
		t.Fatalf("unknown %+v %v open %d", o, ok, hookOpen)
	}
}

func TestLedgerWithoutHandler(t *testing.T) {
	p := &Path{byAPI: map[int64]*account{7: newBook(map[uint32]bool{1: true})}}
	p.apply(7, &tdv1.AccountEvent{Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{
		Positions: []*tdv1.Position{{Uid: 3, Size: 1.5}},
	}}})
	p.apply(7, balanceUpdate(&tdv1.Balance{Asset: "USDT", Free: 1, Locked: 2, Prelock: 3}))
	pos, ok := p.Position(7, 3)
	if !ok || pos.GetSize() != 1.5 {
		t.Fatalf("position %+v %v", pos, ok)
	}
	bal, ok := p.Balance(7, "USDT")
	if !ok || bal.GetFree() != 1 || bal.GetLocked() != 2 || bal.GetPrelock() != 3 {
		t.Fatalf("balance %+v %v", bal, ok)
	}
	if _, ok := p.Position(99, 3); ok {
		t.Fatal("unknown account")
	}
	if _, ok := p.Balance(7, "ETH"); ok {
		t.Fatal("missing asset")
	}
	a, _ := p.Position(7, 3)
	b, _ := p.Position(7, 3)
	if a == b {
		t.Fatal("two reads share a pointer")
	}
}

func TestReconKeepsOnlyPostRequestPending(t *testing.T) {
	rec := &tape{}
	f, p := sessionHooks(t, time.Second, rec.hooks(), decl(7, 1))
	f.serve()
	f.onOrder = accept
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 1, nil) }
	listenRecon(t, f, p)

	before, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() >= 2 })
	p.apply(7, balanceUpdate(&tdv1.Balance{Asset: "OLD", Free: 1, Locked: 2, Prelock: 3}))
	p.apply(7, &tdv1.AccountEvent{Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{
		Positions: []*tdv1.Position{{Uid: 9, Size: 4}},
	}}})

	var after atomic.Value
	var mu sync.Mutex
	type shot struct {
		open      []string
		event     []string
		listedQty float64
		listedSt  tdv1.OrderStatus
		oldBal    bool
		usdtOK    bool
		free      float64
		locked    float64
		prelock   float64
		oldPos    bool
	}
	var shots []shot
	prev := p.hooks.Recon
	p.hooks.Recon = func(ev ReconEvent) {
		s := shot{event: idsOf(ev.Recon.GetOpenOrders())}
		for _, o := range p.OpenOrders(7, 0) {
			s.open = append(s.open, o.GetClientOrderId())
		}
		if o, ok := p.Order(7, listed); ok {
			s.listedQty = o.GetQty()
			s.listedSt = o.GetStatus()
			o.Qty = -1
		}
		_, s.oldBal = p.Balance(7, "OLD")
		if b, ok := p.Balance(7, "USDT"); ok {
			s.usdtOK = true
			s.free, s.locked, s.prelock = b.GetFree(), b.GetLocked(), b.GetPrelock()
			b.Prelock = -1
		}
		_, s.oldPos = p.Position(7, 9)
		mu.Lock()
		shots = append(shots, s)
		mu.Unlock()
		if prev != nil {
			prev(ev)
		}
	}
	f.mu.Lock()
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply {
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 3})
		if err != nil {
			t.Errorf("submit after request: %v", err)
			return snap(1, 1, 1, nil)
		}
		after.Store(id)
		return &tdv1.ViewReply{
			Incarnation: 1, Seq: 1, CancelBatch: 1,
			Reply: &tdv1.ViewReply_Recon{Recon: &tdv1.Recon{
				OpenOrders: []*tdv1.Order{{
					ClientOrderId: listed, Status: tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, Qty: 9, Uid: 1001,
				}},
				Balances: []*tdv1.Balance{{Asset: "USDT", Free: 1, Locked: 2, Prelock: 3}},
			}},
		}
	}
	f.mu.Unlock()
	if err := p.RequestRecon(7); err != nil {
		t.Fatal(err)
	}
	var late string
	waitFor(t, 5*time.Second, func() bool {
		id, _ := after.Load().(string)
		late = id
		return id != "" && len(rec.recons()) >= 2
	})
	mu.Lock()
	if len(shots) != 1 {
		mu.Unlock()
		t.Fatalf("shots %d", len(shots))
	}
	s := shots[0]
	mu.Unlock()
	if s.listedSt != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || s.listedQty != 9 {
		t.Fatalf("listed during hook %+v", s)
	}
	if containsID(s.open, before) || !containsID(s.open, listed) || !containsID(s.open, late) {
		t.Fatalf("open during hook %v before %s late %s", s.open, before, late)
	}
	if containsID(s.event, late) || containsID(s.event, before) || !containsID(s.event, listed) {
		t.Fatalf("event orders %v", s.event)
	}
	if s.oldBal || s.oldPos || !s.usdtOK || s.free != 1 || s.locked != 2 || s.prelock != 3 {
		t.Fatalf("ledger during hook %+v", s)
	}
	if _, ok := p.Order(7, before); ok {
		t.Fatal("pre-request pending survived")
	}
	got, ok := p.Order(7, listed)
	if !ok || got.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_ACCEPTED || got.GetQty() != 9 {
		t.Fatalf("listed after hook %+v %v", got, ok)
	}
	got, ok = p.Order(7, late)
	if !ok || got.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
		t.Fatalf("post-request pending %+v %v", got, ok)
	}
	usdt, ok := p.Balance(7, "USDT")
	if !ok || usdt.GetPrelock() != 3 {
		t.Fatalf("balance copy wrote through %+v %v", usdt, ok)
	}
	if _, ok := p.Balance(7, "OLD"); ok {
		t.Fatal("old balance survived")
	}
	if _, ok := p.Position(7, 9); ok {
		t.Fatal("old position survived")
	}

	publish(t, f.nc, 7, &tdv1.AccountEvent{
		Seq: 2, Source: &commonv1.WorkerRef{Incarnation: 1},
		Event: &tdv1.AccountEvent_Recon{Recon: &tdv1.Recon{
			Positions: []*tdv1.Position{{Uid: 4, Size: 8}},
		}},
	})
	waitFor(t, 5*time.Second, func() bool {
		s, ok := sizeOf(p, 7, 4)
		_, lateOK := p.Order(7, late)
		return ok && s == 8 && !lateOK && len(rec.recons()) >= 3
	})
	if _, ok := p.Order(7, listed); ok {
		t.Fatal("pushed recon kept an order the snapshot omitted")
	}
}

func newBook(algos map[uint32]bool) *account {
	return &account{
		algos:     algos,
		orders:    map[string]*tdv1.Order{},
		submitGen: map[string]uint64{},
		positions: map[uint32]*tdv1.Position{},
		balances:  map[string]*tdv1.Balance{},
	}
}

func orderUpdate(o *tdv1.Order) *tdv1.AccountEvent {
	return &tdv1.AccountEvent{
		Seq: 1, Source: &commonv1.WorkerRef{Incarnation: 1},
		Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{Order: o}},
	}
}

func balanceUpdate(b *tdv1.Balance) *tdv1.AccountEvent {
	return &tdv1.AccountEvent{Event: &tdv1.AccountEvent_BalanceUpdate{BalanceUpdate: &tdv1.BalanceUpdate{
		Balances: []*tdv1.Balance{b},
	}}}
}

func sizeOf(p *Path, api int64, uid uint32) (float64, bool) {
	pos, ok := p.Position(api, uid)
	if !ok {
		return 0, false
	}
	return pos.GetSize(), true
}

func idsOf(orders []*tdv1.Order) []string {
	out := make([]string, 0, len(orders))
	for _, o := range orders {
		if o != nil {
			out = append(out, o.GetClientOrderId())
		}
	}
	return out
}

func containsID(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}
