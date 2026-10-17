package bookpath

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/book"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
)

func TestOpenRejects(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("nil snapshot")
	}
	snap := func(uint32, func(*mdv1.BookSnapshot, error)) error { return nil }
	if _, err := Open(Config{Snapshot: snap}); err == nil {
		t.Fatal("nil info")
	}
	info := func(uint32) (symbol.Info, error) { return symbol.Info{}, nil }
	if _, err := Open(Config{Snapshot: snap, Info: info}); err == nil {
		t.Fatal("nil schedule")
	}
	sched := func(time.Duration, func()) {}
	if _, err := Open(Config{Snapshot: snap, Info: info, Schedule: sched}); err == nil {
		t.Fatal("zero window")
	}
}

func TestCursorAndBackoff(t *testing.T) {
	if !newer(1, 2, 1, 1) || newer(1, 1, 1, 1) || newer(1, 1, 1, 2) {
		t.Fatal("seq")
	}
	if !newer(2, 1, 1, 99) || newer(1, 99, 2, 1) {
		t.Fatal("epoch")
	}
	if !atLeast(1, 5, 1, 5) || atLeast(1, 4, 1, 5) || !atLeast(2, 0, 1, 9) {
		t.Fatal("atLeast")
	}
	d := time.Duration(0)
	var got []time.Duration
	for range 7 {
		d = grow(d)
		got = append(got, d)
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

func TestSnapshotThenDelta(t *testing.T) {
	h := newH(t)
	h.send(42, 1, 1, h.fresh(), delta(100, 1, 110, 2))
	if _, ok := h.p.View(42); ok {
		t.Fatal("visible before the snapshot")
	}
	if len(h.events) != 0 || len(h.reqs) != 1 {
		t.Fatalf("events %d reqs %d", len(h.events), len(h.reqs))
	}
	h.reply(0, shot(42, 1, 1, 100, 1, 110, 2), nil)
	if len(h.events) != 0 {
		t.Fatal("snapshot is not a hook")
	}
	ob, ok := h.p.View(42)
	if !ok || ob.Window() != book.DefaultN || ob.TickSize() != 0.1 {
		t.Fatalf("view %v window %d tick %v", ok, windowOf(ob), tickOf(ob))
	}
	if q, ok := ob.Qty(true, 100); !ok || q != 1 {
		t.Fatalf("bid %v %v", q, ok)
	}
	if q, ok := ob.Qty(false, 110); !ok || q != 2 {
		t.Fatalf("ask %v %v", q, ok)
	}
	// The delta that waited out the snapshot is not a hook. The next one is, and the
	// book inside the hook already contains it.
	h.send(42, 1, 2, h.fresh(), delta(100, 4, 0, 0))
	if len(h.events) != 1 || h.events[0].Seq != 2 || h.events[0].ExchTsNs != 99 {
		t.Fatalf("hook %+v", h.events)
	}
	if len(h.hookQty) != 1 || !h.hookOK[0] || h.hookQty[0] != 4 {
		t.Fatalf("inside hook qty %v ok %v", h.hookQty, h.hookOK)
	}
	if _, ok := h.p.View(7); ok {
		t.Fatal("other uid")
	}
}

func TestBufferDropsAtOrBelowSnapshot(t *testing.T) {
	h := newH(t)
	h.send(42, 1, 1, h.fresh(), delta(100, 9, 0, 0))
	h.send(42, 1, 2, h.fresh(), delta(100, 4, 0, 0))
	h.reply(0, shot(42, 1, 1, 100, 1, 0, 0), nil)
	if len(h.events) != 0 {
		t.Fatal("buffered deltas are not hooks")
	}
	q, ok := h.qty(42, 100)
	if !ok || q != 4 {
		t.Fatalf("folded %v %v", q, ok)
	}
	if len(h.reqs) != 1 {
		t.Fatalf("reqs %d", len(h.reqs))
	}
}

func TestGapResyncs(t *testing.T) {
	h := newH(t)
	h.sync(1, 5, 100, 1)
	h.send(42, 1, 7, h.fresh(), delta(100, 8, 0, 0))
	if _, ok := h.p.View(42); ok || len(h.events) != 0 {
		t.Fatal("gap stayed visible or hooked")
	}
	if len(h.reqs) != 2 {
		t.Fatalf("reqs %d", len(h.reqs))
	}
	h.reply(1, shot(42, 1, 7, 100, 8, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 8 || len(h.events) != 0 {
		t.Fatalf("after resync qty %v %v events %d", q, ok, len(h.events))
	}
	h.send(42, 1, 8, h.fresh(), delta(100, 3, 0, 0))
	if len(h.events) != 1 || h.events[0].ConnEpoch != 1 || h.events[0].Seq != 8 || h.hookQty[0] != 3 {
		t.Fatalf("next %+v qty %v", h.events, h.hookQty)
	}
}

func TestEpochChangeResyncs(t *testing.T) {
	h := newH(t)
	h.sync(1, 5, 100, 1)
	h.send(42, 2, 1, h.fresh(), delta(100, 6, 0, 0))
	if _, ok := h.p.View(42); ok {
		t.Fatal("old epoch still visible")
	}
	h.reply(1, shot(42, 2, 1, 100, 6, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 6 {
		t.Fatalf("new epoch %v %v", q, ok)
	}
	h.send(42, 2, 2, h.fresh(), delta(100, 7, 0, 0))
	if len(h.events) != 1 || h.events[0].ConnEpoch != 2 || h.hookQty[0] != 7 {
		t.Fatalf("%+v %v", h.events, h.hookQty)
	}
}

func TestNotSyncedBackoff(t *testing.T) {
	h := newH(t)
	h.send(42, 1, 1, h.fresh(), delta(100, 1, 0, 0))
	h.reply(0, nil, errors.New("mdpath: md.view: not_synced"))
	if _, ok := h.p.View(42); ok {
		t.Fatal("not_synced installed a book")
	}
	if d := h.pendingDelay(time.Second); d != time.Second {
		t.Fatalf("first wait %s", d)
	}
	// Another delta while the wait is outstanding does not start a second read.
	h.send(42, 1, 2, h.fresh(), delta(100, 2, 0, 0))
	if len(h.reqs) != 1 {
		t.Fatalf("reqs during backoff %d", len(h.reqs))
	}
	h.fire(t, time.Second)
	if len(h.reqs) != 2 {
		t.Fatalf("retry reqs %d", len(h.reqs))
	}
	h.reply(1, nil, errors.New("mdpath: md.view: not_synced"))
	if d := h.pendingDelay(2 * time.Second); d != 2*time.Second {
		t.Fatalf("second wait %s", d)
	}
	h.fire(t, 2*time.Second)
	h.reply(2, shot(42, 1, 2, 100, 2, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 2 || len(h.events) != 0 {
		t.Fatalf("synced from retry %v %v events %d", q, ok, len(h.events))
	}
	// A success clears the window. The next failure waits one second again.
	h.send(42, 1, 4, h.fresh(), delta(100, 4, 0, 0))
	h.reply(len(h.reqs)-1, nil, errors.New("mdpath: md.view: not_synced"))
	if d := h.pendingDelay(time.Second); d != time.Second {
		t.Fatalf("window after success %s", d)
	}
}

func TestStaleDeltaAppliesWithoutHook(t *testing.T) {
	h := newH(t)
	h.sync(1, 1, 100, 1)
	h.send(42, 1, 2, h.stale(), delta(100, 2, 0, 0))
	if len(h.events) != 0 {
		t.Fatal("stale delta called the hook")
	}
	q, ok := h.qty(42, 100)
	if !ok || q != 2 {
		t.Fatalf("stale was not applied %v %v", q, ok)
	}
	// Exactly recv_window_ms old is still delivered.
	h.send(42, 1, 3, h.edge(), delta(100, 3, 0, 0))
	if len(h.events) != 1 || h.hookQty[0] != 3 {
		t.Fatalf("edge %+v %v", h.events, h.hookQty)
	}
	h.send(42, 1, 4, h.fresh(), delta(100, 5, 0, 0))
	if len(h.events) != 2 || h.events[1].Seq != 4 || h.hookQty[1] != 5 {
		t.Fatalf("fresh hook does not see the stale apply %+v %v", h.events, h.hookQty)
	}
}

func TestResetClearsAndRefetches(t *testing.T) {
	h := newH(t)
	h.sync(1, 1, 100, 1)
	h.send(42, 1, 2, h.fresh(), &mdv1.OrderBookDelta{
		Meta:   &mdv1.Meta{ExchTsNs: 99},
		Bids:   []*mdv1.Level{{Price: 100, Qty: 9}},
		Reset_: true,
	})
	if _, ok := h.p.View(42); ok || len(h.events) != 0 {
		t.Fatal("reset stayed visible or hooked")
	}
	if len(h.reqs) != 2 {
		t.Fatalf("reqs %d", len(h.reqs))
	}
	// A snapshot from before the reset is not installed.
	h.reply(1, shot(42, 1, 1, 100, 1, 0, 0), nil)
	if _, ok := h.p.View(42); ok {
		t.Fatal("stale snapshot")
	}
	if d := h.pendingDelay(time.Second); d != time.Second {
		t.Fatalf("wait %s", d)
	}
	h.fire(t, time.Second)
	h.send(42, 1, 3, h.fresh(), delta(100, 3, 0, 0))
	h.reply(len(h.reqs)-1, shot(42, 1, 2, 100, 2, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 3 || len(h.events) != 0 {
		t.Fatalf("reset book %v %v events %d", q, ok, len(h.events))
	}
}

func TestBufferCapRefetches(t *testing.T) {
	h := newH(t)
	for seq := uint64(1); seq <= bufferMax; seq++ {
		h.send(42, 1, seq, h.fresh(), delta(100, float64(seq), 0, 0))
	}
	if len(h.reqs) != 2 {
		t.Fatalf("reqs %d", len(h.reqs))
	}
	if _, ok := h.p.View(42); ok {
		t.Fatal("capped book was visible")
	}
	// The snapshot that was in flight when the cap tripped is discarded.
	h.reply(0, shot(42, 1, 1, 100, 1, 0, 0), nil)
	if _, ok := h.p.View(42); ok {
		t.Fatal("discarded snapshot installed")
	}
	h.reply(1, shot(42, 1, bufferMax, 100, 4, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 4 || len(h.events) != 0 {
		t.Fatalf("after cap %v %v events %d", q, ok, len(h.events))
	}
}

func TestBufferAgeRefetches(t *testing.T) {
	h := newH(t)
	h.send(42, 1, 1, h.fresh(), delta(100, 1, 0, 0))
	if len(h.reqs) != 1 {
		t.Fatalf("reqs %d", len(h.reqs))
	}
	h.now = h.now.Add(bufferAge)
	h.fire(t, bufferAge)
	if len(h.reqs) != 2 {
		t.Fatalf("age reqs %d", len(h.reqs))
	}
	h.reply(0, shot(42, 1, 1, 100, 9, 0, 0), nil)
	if _, ok := h.p.View(42); ok {
		t.Fatal("old snapshot")
	}
	h.reply(1, shot(42, 1, 1, 100, 2, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 2 {
		t.Fatalf("age snapshot %v %v", q, ok)
	}
}

func TestUnresolvedDoesNotQuery(t *testing.T) {
	h := newH(t)
	h.send(9, 1, 1, h.fresh(), delta(100, 1, 0, 0))
	h.send(9, 1, 2, h.fresh(), delta(100, 2, 0, 0))
	if len(h.reqs) != 0 {
		t.Fatalf("queried %d", len(h.reqs))
	}
	if _, ok := h.p.View(9); ok {
		t.Fatal("unresolved book")
	}
	n := 0
	for _, line := range h.logs {
		if line == "book: uid 9 is not resolved" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("logs %q", h.logs)
	}
	h.send(8, 1, 1, h.fresh(), delta(100, 1, 0, 0))
	if len(h.reqs) != 0 {
		t.Fatal("bad tick queried")
	}
	found := false
	for _, line := range h.logs {
		if line == "book: uid 8 has no price tick" {
			found = true
		}
	}
	if !found {
		t.Fatalf("logs %q", h.logs)
	}
}

func TestBufferHoleWaitsForALaterSnapshot(t *testing.T) {
	h := newH(t)
	h.send(42, 1, 1, h.fresh(), delta(100, 1, 0, 0))
	h.send(42, 1, 3, h.fresh(), delta(100, 3, 0, 0))
	h.reply(0, shot(42, 1, 1, 100, 1, 0, 0), nil)
	if _, ok := h.p.View(42); ok {
		t.Fatal("hole was installed")
	}
	h.fire(t, time.Second)
	h.reply(len(h.reqs)-1, shot(42, 1, 3, 100, 3, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 3 || len(h.events) != 0 {
		t.Fatalf("caught up %v %v events %d", q, ok, len(h.events))
	}
}

func TestDuplicateDeltaIsIgnored(t *testing.T) {
	h := newH(t)
	h.sync(1, 1, 100, 1)
	h.send(42, 1, 1, h.fresh(), delta(100, 9, 0, 0))
	q, ok := h.qty(42, 100)
	if !ok || q != 1 || len(h.events) != 0 || len(h.reqs) != 1 {
		t.Fatalf("dup qty %v %v events %d reqs %d", q, ok, len(h.events), len(h.reqs))
	}
}

func TestTwoInstruments(t *testing.T) {
	h := newH(t)
	h.tick[43] = 0.1
	h.send(42, 1, 1, h.fresh(), delta(100, 1, 0, 0))
	h.send(43, 4, 2, h.fresh(), delta(50, 5, 0, 0))
	h.reply(0, shot(42, 1, 1, 100, 1, 0, 0), nil)
	h.reply(1, shot(43, 4, 2, 50, 5, 0, 0), nil)
	q, ok := h.qty(42, 100)
	if !ok || q != 1 {
		t.Fatalf("42 %v %v", q, ok)
	}
	ob, ok := h.p.View(43)
	if !ok {
		t.Fatal("43")
	}
	if q, ok := ob.Qty(true, 50); !ok || q != 5 {
		t.Fatalf("43 qty %v %v", q, ok)
	}
}

func TestDispatcherAppliesSnapshotThenDelta(t *testing.T) {
	now := time.Unix(0, int64(30*time.Second))
	var d *dispatch.Disp
	var p *Path
	hooked := make(chan float64, 1)
	var err error
	p, err = Open(Config{
		Snapshot: func(uint32, func(*mdv1.BookSnapshot, error)) error {
			// Queued from inside the first delta, so it runs before the next one.
			return d.PushTD(func() {})
		},
		Info: func(uint32) (symbol.Info, error) {
			return symbol.Info{UID: 42, Filters: symbol.Filters{symbol.PriceTick: 0.1}}, nil
		},
		Hook: func(ev Update) {
			ob, ok := p.View(ev.UID)
			if !ok || ev.Seq != 2 {
				hooked <- -1
				return
			}
			q, _ := ob.Qty(true, 100)
			hooked <- q
		},
		RecvWindow: time.Second,
		Now:        func() time.Time { return now },
		Schedule:   func(time.Duration, func()) {},
		Log:        func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.snapshot = func(_ uint32, fn func(*mdv1.BookSnapshot, error)) error {
		return d.PushTD(func() { fn(shot(42, 1, 1, 100, 1, 0, 0), nil) })
	}
	d = dispatch.New(dispatch.Config{
		RecvWindow: time.Second,
		Now:        func() time.Time { return now },
		Book:       p.OnMessage,
		Log:        func(string) {},
	})
	ts := now.UnixNano()
	if err := d.PushBook(hmsg(t, 42, 1, 1, ts, delta(100, 1, 0, 0))); err != nil {
		t.Fatal(err)
	}
	if err := d.PushBook(hmsg(t, 42, 1, 2, ts, delta(100, 6, 0, 0))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	select {
	case q := <-hooked:
		if q != 6 {
			t.Fatalf("hook qty %v", q)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hook did not run")
	}
	cancel()
	select {
	case <-errc:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher did not return")
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := d.OnStop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

type harness struct {
	t       *testing.T
	p       *Path
	reqs    []snapReq
	timers  []timer
	events  []Update
	hookQty []float64
	hookOK  []bool
	logs    []string
	now     time.Time
	tick    map[uint32]float64
}

type snapReq struct {
	uid uint32
	fn  func(*mdv1.BookSnapshot, error)
}

type timer struct {
	d    time.Duration
	fn   func()
	done bool
}

func newH(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:    t,
		now:  time.Unix(0, int64(30*time.Second)),
		tick: map[uint32]float64{42: 0.1, 8: 0},
	}
	p, err := Open(Config{
		Snapshot: func(uid uint32, fn func(*mdv1.BookSnapshot, error)) error {
			h.reqs = append(h.reqs, snapReq{uid: uid, fn: fn})
			return nil
		},
		Info: func(uid uint32) (symbol.Info, error) {
			tick, ok := h.tick[uid]
			if !ok {
				return symbol.Info{}, errors.New("unknown")
			}
			return symbol.Info{UID: uid, Filters: symbol.Filters{symbol.PriceTick: tick}}, nil
		},
		Hook: func(ev Update) {
			h.events = append(h.events, ev)
			ob, ok := h.p.View(ev.UID)
			if !ok {
				h.hookOK = append(h.hookOK, false)
				h.hookQty = append(h.hookQty, 0)
				return
			}
			q, qok := ob.Qty(true, 100)
			h.hookOK = append(h.hookOK, qok)
			h.hookQty = append(h.hookQty, q)
		},
		RecvWindow: time.Second,
		Now:        func() time.Time { return h.now },
		Schedule: func(d time.Duration, fn func()) {
			h.timers = append(h.timers, timer{d: d, fn: fn})
		},
		Log: func(s string) { h.logs = append(h.logs, s) },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.p = p
	return h
}

func (h *harness) send(uid uint32, epoch, seq uint64, mdTs int64, d *mdv1.OrderBookDelta) {
	h.t.Helper()
	h.p.OnMessage(hmsg(h.t, uid, epoch, seq, mdTs, d))
}

func (h *harness) reply(i int, snap *mdv1.BookSnapshot, err error) {
	h.t.Helper()
	if i < 0 || i >= len(h.reqs) {
		h.t.Fatalf("no request %d of %d", i, len(h.reqs))
	}
	h.reqs[i].fn(snap, err)
}

func (h *harness) fire(t *testing.T, d time.Duration) {
	t.Helper()
	for i := range h.timers {
		if h.timers[i].done || h.timers[i].d != d {
			continue
		}
		h.timers[i].done = true
		h.timers[i].fn()
		return
	}
	t.Fatalf("no %s timer", d)
}

func (h *harness) pendingDelay(d time.Duration) time.Duration {
	for i := range h.timers {
		if !h.timers[i].done && h.timers[i].d == d {
			return h.timers[i].d
		}
	}
	return -1
}

func (h *harness) fresh() int64 { return h.now.UnixNano() }

func (h *harness) stale() int64 { return h.now.Add(-2 * time.Second).UnixNano() }

func (h *harness) edge() int64 { return h.now.Add(-time.Second).UnixNano() }

func (h *harness) qty(uid uint32, price float64) (float64, bool) {
	ob, ok := h.p.View(uid)
	if !ok {
		return 0, false
	}
	return ob.Qty(true, price)
}

// sync installs a snapshot at (epoch, seq) and consumes the request it took.
func (h *harness) sync(epoch, seq uint64, price, qty float64) {
	h.t.Helper()
	n := len(h.reqs)
	h.send(42, epoch, seq, h.fresh(), delta(price, qty, 0, 0))
	if len(h.reqs) != n+1 {
		h.t.Fatalf("reqs %d", len(h.reqs))
	}
	h.reply(n, shot(42, epoch, seq, price, qty, 0, 0), nil)
	h.events = nil
	h.hookQty = nil
	h.hookOK = nil
}

func delta(bidPx, bidQty, askPx, askQty float64) *mdv1.OrderBookDelta {
	d := &mdv1.OrderBookDelta{Meta: &mdv1.Meta{ExchTsNs: 99}}
	if bidPx != 0 || bidQty != 0 {
		d.Bids = []*mdv1.Level{{Price: bidPx, Qty: bidQty}}
	}
	if askPx != 0 || askQty != 0 {
		d.Asks = []*mdv1.Level{{Price: askPx, Qty: askQty}}
	}
	return d
}

func shot(uid uint32, epoch, seq uint64, bidPx, bidQty, askPx, askQty float64) *mdv1.BookSnapshot {
	s := &mdv1.BookSnapshot{Uid: uid, ConnEpoch: epoch, Seq: seq, MdTsNs: 1}
	if bidPx != 0 || bidQty != 0 {
		s.Bids = []*mdv1.Level{{Price: bidPx, Qty: bidQty}}
	}
	if askPx != 0 || askQty != 0 {
		s.Asks = []*mdv1.Level{{Price: askPx, Qty: askQty}}
	}
	return s
}

func hmsg(t *testing.T, uid uint32, epoch, seq uint64, mdTs int64, d *mdv1.OrderBookDelta) []byte {
	t.Helper()
	body, err := proto.Marshal(&mdv1.FeedEvent{
		Uid:   uid,
		Event: &mdv1.FeedEvent_OrderBookDelta{OrderBookDelta: d},
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope.Append(nil, mdTs, epoch, seq, body)
}

func windowOf(ob *book.OrderBook) int {
	if ob == nil {
		return 0
	}
	return ob.Window()
}

func tickOf(ob *book.OrderBook) float64 {
	if ob == nil {
		return 0
	}
	return ob.TickSize()
}
