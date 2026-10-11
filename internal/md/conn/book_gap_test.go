package conn

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/bybit"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

// TestBookBybitGapRecovers is the regression from the PR comment. The recording has one
// snapshot, so replaying it after the gap does not rebuild the book: Bybit only sends a new
// full book after subscribe. The comment's loop, with no resubscribe, failed before the fix:
//
//	frames=150 deltas=18 resets=1 book_present=false depth_calls=0
func TestBookBybitGapRecovers(t *testing.T) {
	const ch = "orderbook.50.BTCUSDT"
	const other = "publicTrade.ETHUSDT"
	recs := readFixture(t, "testdata/book/bybit.jsonl")
	reg, err := exchange.NewRegistry(bybit.Declare())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reg.Venue(bybit.Name)
	if !ok || v.Market == nil {
		t.Fatal("no bybit market")
	}
	var snap []byte
	e := startFixtureBook(t, bybit.Name, "linear", ch, "BTCUSDT", 0.1, v.Market, nil, 20, nil)
	clk := newFakeClock(time.Unix(1_000_000, 0).UTC())
	e.clk = clk
	e.c.bookNow = clk.Now
	e.c.bookAfter = clk.After
	e.c.mu.Lock()
	e.c.live.observed[ch] = struct{}{}
	e.c.wanted[other] = []feed{{topic: "trade", ticker: "ETHUSDT"}}
	e.c.live.observed[other] = struct{}{}
	e.c.mu.Unlock()
	e.c.wakeBook()

	n := 0
	for _, r := range recs {
		if r.Kind != conformance.KindIn {
			continue
		}
		b, err := r.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		n++
		if n == 1 {
			snap = append([]byte(nil), b...)
		}
		if n == 20 { // drop one delta
			continue
		}
		e.feed(t, b)
	}
	if _, _, ok := e.maps(); ok {
		t.Fatal("book still present after the dropped delta")
	}
	if e.c.live == nil || e.c.epoch != 4 {
		t.Fatal("gap dropped the connection")
	}

	// Nothing is sent until this book's backoff elapses. The other channel stays up.
	e.waitUntil(t, func() bool {
		next, ok := e.clk.Next()
		return ok && next.Sub(e.clk.Now()) == time.Second
	})
	if frames := e.c.plan(); len(frames) != 0 {
		t.Fatalf("resubscribe before backoff: %d frames", len(frames))
	}
	e.clk.Advance(time.Second)
	e.waitUntil(t, func() bool {
		e.c.mu.Lock()
		defer e.c.mu.Unlock()
		_, ok := e.c.live.resub[ch]
		return ok
	})
	unsub := e.c.plan()
	if op, args := subFrame(t, unsub); op != "unsubscribe" || len(args) != 1 || args[0] != ch {
		t.Fatalf("unsubscribe op=%s args=%v", op, args)
	}
	ackInflight(t, e.c, false)
	sub := e.c.plan()
	if op, args := subFrame(t, sub); op != "subscribe" || len(args) != 1 || args[0] != ch {
		t.Fatalf("subscribe op=%s args=%v", op, args)
	}
	ackInflight(t, e.c, true)
	e.c.mu.Lock()
	_, otherUp := e.c.live.observed[other]
	e.c.mu.Unlock()
	if !otherUp {
		t.Fatal("resubscribe dropped the other channel")
	}

	// The venue's next full book is what Bybit sends on subscribe. The recording's snapshot
	// stands in for it; the deltas after the gap belong to the old sequence.
	e.feed(t, snap)
	d := deltasOf(e.open(t))
	resets := 0
	for _, x := range d {
		if x.ev.GetOrderBookDelta().GetReset_() {
			resets++
		}
	}
	_, _, present := e.maps()
	t.Logf("frames=%d deltas=%d resets=%d book_present=%v depth_calls=%d", n, len(d), resets, present, e.doer.calls())
	if !present {
		t.Fatal("book never recovered after a dropped delta")
	}
	if resets != 2 {
		t.Fatalf("resets %d, want the initial snapshot and the one after resubscribe", resets)
	}
	if calls := e.doer.calls(); calls != 0 {
		t.Fatalf("stream called depth %d", calls)
	}
}

// streamChecksum is the OKX shape: SnapshotStream sequencing, plus a checksum the adapter
// does not implement yet (E4-11). A non-zero checksum fails.
type streamChecksum struct{}

func (streamChecksum) Snapshot() exchange.SnapshotSource { return exchange.SnapshotStream }
func (streamChecksum) Depth() int                        { return 0 }
func (streamChecksum) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if u != nil && u.Snapshot {
		return exchange.BookApply
	}
	if !last.Valid || u == nil {
		return exchange.BookResync
	}
	if u.Last == last.Last+1 {
		return exchange.BookApply
	}
	if u.Last <= last.Last {
		return exchange.BookSkip
	}
	return exchange.BookResync
}
func (streamChecksum) Verify(_, _ []exchange.Level, u *exchange.BookUpdate) bool {
	return u == nil || u.Checksum == 0
}

func TestBookOKXChecksumResubscribes(t *testing.T) {
	const other = "other"
	e := newBookEnv(t, streamChecksum{}, 5, nil, nil)
	e.c.mu.Lock()
	e.c.live.observed[bookCh] = struct{}{}
	e.c.wanted[other] = []feed{{topic: "trade", ticker: "T"}}
	e.c.live.observed[other] = struct{}{}
	e.c.mu.Unlock()

	e.push(exchange.BookUpdate{
		Snapshot: true, Last: 10,
		Bids: []exchange.Level{{Price: 100, Qty: 1}},
		Asks: []exchange.Level{{Price: 110, Qty: 1}},
	})
	e.waitPubs(t, 2)
	e.push(exchange.BookUpdate{
		Last: 11, Checksum: 1,
		Bids: []exchange.Level{{Price: 100, Qty: 9}},
	})
	e.c.mu.Lock()
	sh := e.c.live.books[bookCh]
	down := sh == nil || sh.book == nil
	e.c.mu.Unlock()
	if !down {
		t.Fatal("checksum failure kept the book")
	}
	if frames := e.c.plan(); len(frames) != 0 {
		t.Fatalf("resubscribe before backoff: %d", len(frames))
	}
	dueResub(t, e)
	s := e.c.live.session.(*memSession)
	unsub := e.c.plan()
	if len(unsub) != 1 || len(s.unsub) != 1 || len(s.unsub[0]) != 1 || s.unsub[0][0] != bookCh {
		t.Fatalf("unsubscribe %v", s.unsub)
	}
	ackInflight(t, e.c, false)
	sub := e.c.plan()
	if len(sub) != 1 || len(s.sub) != 1 || len(s.sub[0]) != 1 || s.sub[0][0] != bookCh {
		t.Fatalf("subscribe %v", s.sub)
	}
	ackInflight(t, e.c, true)
	e.c.mu.Lock()
	_, otherUp := e.c.live.observed[other]
	live := e.c.live != nil && e.c.epoch == 4
	e.c.mu.Unlock()
	if !otherUp || !live {
		t.Fatal("checksum resubscribe dropped the connection")
	}

	e.push(exchange.BookUpdate{
		Snapshot: true, Last: 30,
		Bids: []exchange.Level{{Price: 100, Qty: 3}},
		Asks: []exchange.Level{{Price: 110, Qty: 1}},
	})
	e.waitUntil(t, func() bool {
		d := deltasOf(e.open(t))
		n := 0
		for _, x := range d {
			if x.ev.GetOrderBookDelta().GetReset_() {
				n++
			}
		}
		return n == 2
	})
	for _, o := range e.open(t) {
		if hasLevel(levelsOf(o), 100, 9) {
			t.Fatal("checksum failure was published")
		}
	}
	if e.doer.calls() != 0 {
		t.Fatalf("checksum called depth %d", e.doer.calls())
	}

	// A recovered book that gaps again waits 1s, not another immediate cycle.
	e.push(exchange.BookUpdate{
		Last: 31, Checksum: 2,
		Bids: []exchange.Level{{Price: 100, Qty: 4}},
	})
	if frames := e.c.plan(); len(frames) != 0 {
		t.Fatalf("second resubscribe skipped the backoff: %d", len(frames))
	}
	dueResub(t, e)
	again := e.c.plan()
	if len(again) != 1 || len(s.unsub) != 2 || s.unsub[1][0] != bookCh {
		t.Fatalf("second unsubscribe %v", s.unsub)
	}
}

func dueResub(t *testing.T, e *bookEnv) {
	t.Helper()
	e.waitUntil(t, func() bool {
		next, ok := e.clk.Next()
		return ok && next.Sub(e.clk.Now()) == time.Second
	})
	e.clk.Advance(time.Second)
	e.waitUntil(t, func() bool {
		e.c.mu.Lock()
		defer e.c.mu.Unlock()
		_, ok := e.c.live.resub[e.ch]
		return ok
	})
}

func ackInflight(t *testing.T, c *Conn, sub bool) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	src := c.live.unsubInflight
	if sub {
		src = c.live.subInflight
	}
	if len(src) != 1 {
		t.Fatalf("inflight %d", len(src))
	}
	var id uint64
	for k := range src {
		id = k
	}
	c.Ack(id, nil)
}

func subFrame(t *testing.T, frames [][]byte) (string, []string) {
	t.Helper()
	if len(frames) != 1 {
		t.Fatalf("frames %d", len(frames))
	}
	var frame struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}
	if err := json.Unmarshal(frames[0], &frame); err != nil {
		t.Fatal(err)
	}
	return frame.Op, frame.Args
}
