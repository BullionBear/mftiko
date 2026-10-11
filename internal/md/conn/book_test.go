package conn

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/bybit"
	"github.com/coder/websocket"
)

const bookCh = "btcusdt@depth@100ms"

func TestBookFirstSnapshotResetsFullBook(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 1, nil, []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}),
	}})
	e.doer.wait(t, 1)
	e.waitPubs(t, 2)
	all := e.open(t)
	d := deltasOf(all)
	b := booksOf(all)
	if len(d) != 1 || !d[0].ev.GetOrderBookDelta().GetReset_() {
		t.Fatalf("reset %+v", d)
	}
	if d[0].epoch != 4 || d[0].seq != 1 || d[0].subject != "md.f.md.orderbook_delta.T" {
		t.Fatalf("delta header %+v", d[0])
	}
	if got := gotLevels(d[0].ev.GetOrderBookDelta().GetBids()); !sameLevels(got, []exchange.Level{{Price: 100, Qty: 1}, {Price: 90, Qty: 1}}) {
		t.Fatalf("reset bids %v", got)
	}
	if got := gotLevels(d[0].ev.GetOrderBookDelta().GetAsks()); !sameLevels(got, []exchange.Level{{Price: 110, Qty: 1}, {Price: 200, Qty: 1}}) {
		t.Fatalf("reset asks %v", got)
	}
	if len(b) != 1 || b[0].seq != 1 || b[0].ev.GetOrderBook().GetDepth() != 1 {
		t.Fatalf("top %+v", b)
	}
	if got := gotLevels(b[0].ev.GetOrderBook().GetBids()); !sameLevels(got, []exchange.Level{{Price: 100, Qty: 1}}) {
		t.Fatalf("top-N is the full book: %v", got)
	}
	if _, ok := e.level(t, true, 90); !ok {
		t.Fatal("deeper bid dropped")
	}
}

func TestBookTopNAndDelta(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 1, nil, []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}),
	}})
	e.doer.wait(t, 1)
	e.waitPubs(t, 2)

	// A level behind the top changes: a delta, and no second orderbook.
	e.push(exchange.BookUpdate{First: 101, Last: 101, Prev: 100, Bids: []exchange.Level{{Price: 90, Qty: 2}}})
	e.waitPubs(t, 3)
	all := e.open(t)
	if len(booksOf(all)) != 1 {
		t.Fatalf("top republished: %d", len(booksOf(all)))
	}
	d := deltasOf(all)
	if len(d) != 2 || d[1].ev.GetOrderBookDelta().GetReset_() || d[1].seq != 2 {
		t.Fatalf("delta %+v", d)
	}
	if got := gotLevels(d[1].ev.GetOrderBookDelta().GetBids()); !sameLevels(got, []exchange.Level{{Price: 90, Qty: 2}}) {
		t.Fatalf("normalized delta %v", got)
	}

	// Deleting the level is qty 0. The top is still the best bid.
	e.push(exchange.BookUpdate{First: 102, Last: 102, Prev: 101, Bids: []exchange.Level{{Price: 90, Qty: 0}}})
	e.waitPubs(t, 4)
	all = e.open(t)
	d = deltasOf(all)
	if got := gotLevels(d[len(d)-1].ev.GetOrderBookDelta().GetBids()); !sameLevels(got, []exchange.Level{{Price: 90, Qty: 0}}) {
		t.Fatalf("delete %v", got)
	}
	if len(booksOf(all)) != 1 {
		t.Fatal("delete behind the top republished orderbook")
	}
	if _, ok := e.level(t, true, 90); ok {
		t.Fatal("deleted level kept")
	}

	// The best qty changes, so the top goes out again.
	e.push(exchange.BookUpdate{First: 103, Last: 103, Prev: 102, Bids: []exchange.Level{{Price: 100, Qty: 3}}})
	e.waitPubs(t, 6)
	all = e.open(t)
	b := booksOf(all)
	if len(b) != 2 || b[1].seq != 2 {
		t.Fatalf("second top %+v", b)
	}
	if got := gotLevels(b[1].ev.GetOrderBook().GetBids()); !sameLevels(got, []exchange.Level{{Price: 100, Qty: 3}}) {
		t.Fatalf("new top %v", got)
	}
}

func TestBookGapResetsWithoutNewEpoch(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 1, nil, []doerStep{
		{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		{resp: bookHTTP(102, 2, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
	})
	e.doer.wait(t, 1)
	e.waitPubs(t, 2)
	e.push(exchange.BookUpdate{First: 101, Last: 101, Prev: 100, Bids: []exchange.Level{{Price: 90, Qty: 2}}})
	e.waitPubs(t, 3)
	// pu skips 101. The update is not published; the next snapshot bridges it.
	e.push(exchange.BookUpdate{First: 103, Last: 103, Prev: 102, Bids: []exchange.Level{{Price: 90, Qty: 4}, {Price: 77, Qty: 7}}})
	e.doer.wait(t, 2)
	e.waitUntil(t, func() bool {
		d := deltasOf(e.open(t))
		return len(d) >= 3 && d[len(d)-1].ev.GetOrderBookDelta().GetReset_()
	})
	all := e.open(t)
	d := deltasOf(all)
	if seqs(d)[0] != 1 || seqs(d)[1] != 2 || seqs(d)[2] != 3 {
		t.Fatalf("delta seq %v", seqs(d))
	}
	for _, o := range all {
		if o.epoch != 4 {
			t.Fatalf("epoch %d", o.epoch)
		}
	}
	if e.c.Epoch() != 4 {
		t.Fatalf("conn_epoch %d", e.c.Epoch())
	}
	reset := d[len(d)-1].ev.GetOrderBookDelta()
	if !reset.GetReset_() {
		t.Fatal("resync did not reset")
	}
	// The gap itself is not published. The snapshot bridges it, and the level past the
	// original range is part of that reset (D426).
	saw77 := 0
	for _, o := range all {
		if !hasPrice(o, 77) {
			continue
		}
		saw77++
		if o.ev.GetOrderBookDelta() == nil || !o.ev.GetOrderBookDelta().GetReset_() {
			t.Fatal("gapped update was published before the reset")
		}
	}
	if saw77 != 1 {
		t.Fatalf("bridged level published %d times", saw77)
	}
	if got := gotLevels(reset.GetBids()); !sameLevels(got, []exchange.Level{{Price: 100, Qty: 1}, {Price: 90, Qty: 4}, {Price: 77, Qty: 7}}) {
		t.Fatalf("bridged book %v", got)
	}
	if len(booksOf(all)) != 1 {
		t.Fatal("top changed across a resync that left it alone")
	}
}

func TestBookPublishesPastTheSnapshot(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}),
	}})
	e.doer.wait(t, 1)
	e.waitPubs(t, 2)
	e.push(exchange.BookUpdate{First: 101, Last: 101, Prev: 100, Bids: []exchange.Level{{Price: 80, Qty: 5}}})
	e.waitPubs(t, 3)
	if e.doer.calls() != 1 {
		t.Fatalf("depth calls %d, want 1", e.doer.calls())
	}
	d := deltasOf(e.open(t))
	last := d[len(d)-1].ev.GetOrderBookDelta()
	if last.GetReset_() {
		t.Fatal("level past the snapshot was a reset")
	}
	if got := gotLevels(last.GetBids()); !sameLevels(got, []exchange.Level{{Price: 80, Qty: 5}}) {
		t.Fatalf("delta bids %v", got)
	}
	if _, ok := e.level(t, true, 80); !ok {
		t.Fatal("level past the snapshot was not kept")
	}
}

func TestBookDepthBackoff(t *testing.T) {
	t.Run("429 without retry or reset holds a minute", func(t *testing.T) {
		e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{
			{resp: &exchange.HTTPResponse{Status: 429, Body: []byte(`{"code":-1003}`)}},
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		})
		e.doer.wait(t, 1)
		e.waitNext(t, time.Minute)
		e.clk.Advance(time.Minute - time.Millisecond)
		if e.doer.calls() != 1 {
			t.Fatalf("sent during the hold: %d", e.doer.calls())
		}
		e.clk.Advance(time.Millisecond)
		e.doer.wait(t, 2)
	})
	t.Run("418 uses the same hold", func(t *testing.T) {
		e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{
			{resp: &exchange.HTTPResponse{Status: 418, Body: []byte(`{"code":-1003}`)}},
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		})
		e.doer.wait(t, 1)
		e.waitNext(t, time.Minute)
		e.clk.Advance(time.Minute - time.Millisecond)
		if e.doer.calls() != 1 {
			t.Fatalf("sent during 418: %d", e.doer.calls())
		}
		e.clk.Advance(time.Millisecond)
		e.doer.wait(t, 2)
	})
	t.Run("Retry-After wins", func(t *testing.T) {
		e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{
			{resp: &exchange.HTTPResponse{Status: 429, Header: []exchange.Header{{Name: "Retry-After-Ms", Value: "5000"}}, Body: []byte(`{}`)}},
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		})
		e.doer.wait(t, 1)
		e.waitNext(t, 5*time.Second)
		e.clk.Advance(5*time.Second - time.Millisecond)
		if e.doer.calls() != 1 {
			t.Fatal("sent inside Retry-After")
		}
		e.clk.Advance(time.Millisecond)
		e.doer.wait(t, 2)
	})
	t.Run("ResetAt when Retry-After is absent", func(t *testing.T) {
		start := time.Unix(1_000_000, 0)
		reset := start.Add(30 * time.Second)
		e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{
			{resp: &exchange.HTTPResponse{
				Status: 429,
				Header: []exchange.Header{{Name: "Reset-At-Ms", Value: strconv.FormatInt(reset.UnixMilli(), 10)}},
				Body:   []byte(`{}`),
			}},
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		})
		e.doer.wait(t, 1)
		e.waitNext(t, 30*time.Second)
		e.clk.Advance(30*time.Second - time.Millisecond)
		if e.doer.calls() != 1 {
			t.Fatal("sent before ResetAt")
		}
		e.clk.Advance(time.Millisecond)
		e.doer.wait(t, 2)
	})
	t.Run("other failures double from one second and reset after success", func(t *testing.T) {
		e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{
			{resp: &exchange.HTTPResponse{Status: 500, Body: []byte(`no`)}},
			{resp: &exchange.HTTPResponse{Status: 500, Body: []byte(`no`)}},
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
			{resp: &exchange.HTTPResponse{Status: 500, Body: []byte(`no`)}},
			{resp: bookHTTP(101, 2, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		})
		e.doer.wait(t, 1)
		e.waitNext(t, time.Second)
		e.clk.Advance(time.Second)
		e.doer.wait(t, 2)
		e.waitNext(t, 2*time.Second)
		e.clk.Advance(time.Second)
		if e.doer.calls() != 2 {
			t.Fatalf("third call before 2s: %d", e.doer.calls())
		}
		e.clk.Advance(time.Second)
		e.doer.wait(t, 3)
		e.waitPubs(t, 1)
		// The success cleared the window, so the next failure waits 1s again.
		e.push(exchange.BookUpdate{First: 110, Last: 110, Prev: 109}) // gap, immediate refetch, which fails
		e.doer.wait(t, 4)
		e.waitNext(t, time.Second)
		e.clk.Advance(time.Second - time.Millisecond)
		if e.doer.calls() != 4 {
			t.Fatalf("window did not reset: %d", e.doer.calls())
		}
		e.clk.Advance(time.Millisecond)
		e.doer.wait(t, 5)
	})
}

func TestBookBufferCaps(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		hold := make(chan struct{})
		e := newBookEnv(t, umBookSync(t), 5, &bookTune{max: 3}, []doerStep{
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}), hold: hold},
			{resp: bookHTTP(300, 2, [][2]float64{{80, 1}, {70, 1}}, [][2]float64{{120, 1}, {220, 1}})},
		})
		e.doer.wait(t, 1)
		for i := 1; i <= 3; i++ {
			e.push(exchange.BookUpdate{First: uint64(i), Last: uint64(i), Prev: uint64(i - 1), Bids: []exchange.Level{{Price: 100, Qty: float64(i)}}})
		}
		e.doer.wait(t, 2)
		e.waitUntil(t, func() bool {
			_, ok := e.levelOK(true, 80)
			return ok
		})
		close(hold)
		dead := time.Now().Add(150 * time.Millisecond)
		for time.Now().Before(dead) {
			if _, ok := e.levelOK(true, 100); ok {
				t.Fatal("stale snapshot applied")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if _, ok := e.levelOK(true, 80); !ok {
			t.Fatal("replacement snapshot missing")
		}
	})
	t.Run("age", func(t *testing.T) {
		hold := make(chan struct{})
		e := newBookEnv(t, umBookSync(t), 5, &bookTune{age: 10 * time.Second}, []doerStep{
			{resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}), hold: hold},
			{resp: bookHTTP(300, 2, [][2]float64{{80, 1}, {70, 1}}, [][2]float64{{120, 1}, {220, 1}})},
		})
		e.doer.wait(t, 1)
		e.push(exchange.BookUpdate{First: 1, Last: 1, Bids: []exchange.Level{{Price: 100, Qty: 2}}})
		e.waitNext(t, 10*time.Second)
		e.clk.Advance(10 * time.Second)
		e.doer.wait(t, 2)
		e.waitUntil(t, func() bool {
			_, ok := e.levelOK(true, 80)
			return ok
		})
		close(hold)
		dead := time.Now().Add(150 * time.Millisecond)
		for time.Now().Before(dead) {
			if _, ok := e.levelOK(true, 100); ok {
				t.Fatal("aged snapshot applied")
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
}

func TestBookChecksumDropsTheUpdate(t *testing.T) {
	e := newBookEnv(t, checksumSync{}, 5, nil, []doerStep{
		{resp: bookHTTP(1, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
		{resp: bookHTTP(2, 2, [][2]float64{{100, 3}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}})},
	})
	e.doer.wait(t, 1)
	e.waitPubs(t, 2)
	e.push(exchange.BookUpdate{Last: 2, Checksum: 1, Bids: []exchange.Level{{Price: 100, Qty: 9}}})
	e.doer.wait(t, 2)
	e.waitUntil(t, func() bool {
		d := deltasOf(e.open(t))
		return len(d) >= 2 && d[len(d)-1].ev.GetOrderBookDelta().GetReset_()
	})
	for _, o := range e.open(t) {
		if hasLevel(levelsOf(o), 100, 9) {
			t.Fatal("checksum failure was published or replayed")
		}
	}
	reset := deltasOf(e.open(t))
	if got := gotLevels(reset[len(reset)-1].ev.GetOrderBookDelta().GetBids()); !sameLevels(got, []exchange.Level{{Price: 100, Qty: 3}, {Price: 90, Qty: 1}}) {
		t.Fatalf("book after checksum %v", got)
	}
}

func TestBookForwardsDroppedLevel(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 5, nil, []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}),
	}})
	e.doer.wait(t, 1)
	e.waitPubs(t, 2)
	// N ticks under the best. The ring drops it; the delta still carries it.
	const far = 100 - float64(2048)
	e.push(exchange.BookUpdate{First: 101, Last: 101, Prev: 100, Bids: []exchange.Level{{Price: far, Qty: 5}}})
	e.waitPubs(t, 3)
	if e.doer.calls() != 1 {
		t.Fatalf("depth calls %d, want 1", e.doer.calls())
	}
	d := deltasOf(e.open(t))
	last := d[len(d)-1].ev.GetOrderBookDelta()
	if last.GetReset_() {
		t.Fatal("dropped level was a reset")
	}
	if got := gotLevels(last.GetBids()); !sameLevels(got, []exchange.Level{{Price: far, Qty: 5}}) {
		t.Fatalf("delta bids %v", got)
	}
	if _, ok := e.level(t, true, far); ok {
		t.Fatal("out-of-window level was stored")
	}
	bids, _, ok := e.c.BookWindow(bookCh)
	if !ok {
		t.Fatal("no window")
	}
	for _, l := range bids {
		if l.Price == far {
			t.Fatal("md.view window contains the dropped level")
		}
	}
	if len(booksOf(e.open(t))) != 1 {
		t.Fatal("dropped level rebuilt top-N")
	}
}

func TestBookBadPriceTickDoesNotFetch(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 5, &bookTune{zeroTick: true}, []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}}, [][2]float64{{110, 1}}),
	}})
	time.Sleep(40 * time.Millisecond)
	if e.doer.calls() != 0 {
		t.Fatalf("fetched without a price tick: %d", e.doer.calls())
	}
}

func TestBookEmptyExchTickerDoesNotFetch(t *testing.T) {
	e := newBookEnv(t, umBookSync(t), 5, &bookTune{bare: true}, []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}}, [][2]float64{{110, 1}}),
	}})
	time.Sleep(40 * time.Millisecond)
	if e.doer.calls() != 0 {
		t.Fatalf("fetched without an exch ticker: %d", e.doer.calls())
	}
}

func TestBookStreamDoesNotCallREST(t *testing.T) {
	bs := bybitBookSync(t)
	if bs.Snapshot() != exchange.SnapshotStream || bs.Depth() != 0 {
		t.Fatalf("bybit book %+v", bs)
	}
	e := newBookEnv(t, bs, 50, nil, nil)
	e.push(exchange.BookUpdate{
		Snapshot: true, Last: 10,
		Bids: []exchange.Level{{Price: 16493.5, Qty: 0.006}},
		Asks: []exchange.Level{{Price: 16611, Qty: 0.029}},
	})
	e.waitPubs(t, 2)
	d := deltasOf(e.open(t))
	if len(d) != 1 || !d[0].ev.GetOrderBookDelta().GetReset_() {
		t.Fatalf("stream reset %+v", d)
	}
	e.push(exchange.BookUpdate{
		Last: 11,
		Bids: []exchange.Level{{Price: 16493.5, Qty: 0}},
		Asks: []exchange.Level{{Price: 16611, Qty: 1}},
	})
	e.waitPubs(t, 3)
	delta := deltasOf(e.open(t))[1].ev.GetOrderBookDelta()
	if delta.GetReset_() || !hasLevel(delta.GetBids(), 16493.5, 0) || !hasLevel(delta.GetAsks(), 16611, 1) {
		t.Fatalf("stream delta %+v", delta)
	}
	e.push(exchange.BookUpdate{Last: 13, Bids: []exchange.Level{{Price: 1, Qty: 1}}})
	e.c.mu.Lock()
	sh := e.c.live.books[e.ch]
	gapped := sh == nil || sh.book == nil
	e.c.mu.Unlock()
	if !gapped {
		t.Fatal("gap update applied")
	}
	e.push(exchange.BookUpdate{
		Snapshot: true, Last: 20,
		Bids: []exchange.Level{{Price: 10, Qty: 1}},
		Asks: []exchange.Level{{Price: 11, Qty: 1}},
	})
	e.waitUntil(t, func() bool {
		dd := deltasOf(e.open(t))
		return len(dd) >= 3 && dd[len(dd)-1].ev.GetOrderBookDelta().GetReset_()
	})
	if e.doer.calls() != 0 {
		t.Fatalf("stream called depth %d", e.doer.calls())
	}
	if _, ok := e.levelOK(true, 10); !ok {
		t.Fatal("stream snapshot missing")
	}
}

func TestBookSyncStartsOnConnect(t *testing.T) {
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	})
	doer := &seqDoer{hit: make(chan int, 4), steps: []doerStep{{
		resp: bookHTTP(100, 1, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}}),
	}}}
	rec := newRecorder()
	c := New(Config{
		Venue: "V", Route: "r", Instance: "md", URL: url,
		Market:     &bookMarket{stubMarket: stubMarket{s: &memSession{lim: exchange.SessionLimits{ReadLimit: 1 << 20}}}, bs: umBookSync(t)},
		Publish:    rec.publish,
		BookDepth:  2,
		REST:       scriptREST{},
		Doer:       doer,
		RetryAfter: -1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	c.SetDesired(bookDesired("BTCUSDT"))
	rec.wait(t, 1)
	all := openAll(t, rec.snapshot())
	d := deltasOf(all)
	if len(d) == 0 || !d[0].ev.GetOrderBookDelta().GetReset_() || d[0].epoch != 1 || d[0].seq != 1 {
		t.Fatalf("first reset %+v", d)
	}
	if c.Epoch() != 1 {
		t.Fatalf("epoch %d", c.Epoch())
	}
	cancel()
	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestBookDepthAboveVenueFails(t *testing.T) {
	c := New(Config{
		Venue: "BinanceUM", Route: "public", Instance: "md", URL: "ws://127.0.0.1:1",
		Market: umMarket(t), Publish: func(string, []byte) error { return nil },
		BookDepth: 1001,
	})
	c.SetDesired(&mdctlv1.Desired{Atoms: []*mdctlv1.AtomAssignment{{
		AtomId: "BinanceUM:public:btcusdt@depth@100ms", Feeds: []string{"orderbook.T"}, ExchTicker: "BTCUSDT",
	}}})
	err := c.Run(context.Background())
	if err == nil || !contains(err.Error(), "above") {
		t.Fatal(err)
	}
	if c.Epoch() != 0 {
		t.Fatalf("dialed, epoch %d", c.Epoch())
	}
}

func TestBookStreamSkipsDepthCompare(t *testing.T) {
	bs := bybitBookSync(t)
	c := New(Config{
		Venue: "Bybit", Route: "linear", Instance: "md", URL: "ws://127.0.0.1:1",
		Market: &bookMarket{bs: bs}, Publish: func(string, []byte) error { return nil },
		BookDepth: 50,
	})
	c.SetDesired(&mdctlv1.Desired{Atoms: []*mdctlv1.AtomAssignment{{
		AtomId: "Bybit:linear:orderbook.50.BTCUSDT", Feeds: []string{"orderbook.T"}, ExchTicker: "BTCUSDT",
	}}})
	if c.fatal != nil {
		t.Fatal(c.fatal)
	}
}

func TestBookDepthRequired(t *testing.T) {
	c := New(Config{
		Venue: "V", Route: "r", Instance: "md", URL: "ws://127.0.0.1:1",
		Market: &bookMarket{bs: umBookSync(t)}, Publish: func(string, []byte) error { return nil },
	})
	c.SetDesired(bookDesired("BTCUSDT"))
	err := c.Run(context.Background())
	if err == nil || !contains(err.Error(), "required") {
		t.Fatal(err)
	}
}

func TestInitRequiresBookDepth(t *testing.T) {
	code, done := serveInit(t, &mdctlv1.InitPayload{Venue: "V", Route: "r", MdInstance: "md", Url: "ws://127.0.0.1:1"}, 0)
	if done.GetOk() || !contains(done.GetError(), "book depth") {
		t.Fatalf("done %+v", done)
	}
	if got := <-code; got != exitInit {
		t.Fatalf("exit %d", got)
	}
}

func TestInitRejectsNegativeRateHold(t *testing.T) {
	code, done := serveInit(t, &mdctlv1.InitPayload{Venue: "V", Route: "r", MdInstance: "md", BookDepth: 1}, -time.Second)
	if done.GetOk() || !contains(done.GetError(), "rate hold") {
		t.Fatalf("done %+v", done)
	}
	if got := <-code; got != exitInit {
		t.Fatalf("exit %d", got)
	}
}

func serveInit(t *testing.T, p *mdctlv1.InitPayload, hold time.Duration) (<-chan int, *workerv1.Done) {
	t.Helper()
	peer, workerEnd := net.Pipe()
	t.Cleanup(func() { peer.Close(); workerEnd.Close() })
	code := make(chan int, 1)
	go func() {
		code <- Serve(context.Background(), workerEnd, ServeConfig{
			Lookup:   func(string) (exchange.Market, bool) { return &stubMarket{}, true },
			Publish:  func(string, []byte) error { return nil },
			RateHold: hold,
		})
	}()
	ups := newUpFeed()
	go func() {
		r := frame.NewReader(bufio.NewReader(peer))
		for {
			m := &workerv1.WorkerUp{}
			if err := r.Read(m); err != nil {
				return
			}
			ups.add(m)
		}
	}()
	ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetHello() != nil })
	raw, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := frame.NewWriter(peer).Write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{
		Contract: Contract, Payload: raw,
	}}}); err != nil {
		t.Fatal(err)
	}
	done := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetDone().GetVerb() == workerv1.Verb_VERB_INIT })
	return code, done.GetDone()
}

// checksumSync applies consecutive sequence numbers and rejects a non-zero checksum.
type checksumSync struct{}

func (checksumSync) Snapshot() exchange.SnapshotSource { return exchange.SnapshotREST }
func (checksumSync) Depth() int                        { return 100 }
func (checksumSync) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if !last.Valid || u == nil {
		return exchange.BookResync
	}
	if u.Last <= last.Last {
		return exchange.BookSkip
	}
	if u.Last != last.Last+1 {
		return exchange.BookResync
	}
	return exchange.BookApply
}
func (checksumSync) Verify(_ []exchange.Level, _ []exchange.Level, u *exchange.BookUpdate) bool {
	return u == nil || u.Checksum == 0
}

type bookMarket struct {
	stubMarket
	bs exchange.BookSync
}

func (m *bookMarket) Book(string) (exchange.BookSync, bool) {
	if m == nil || m.bs == nil {
		return nil, false
	}
	return m.bs, true
}

type bookTune struct {
	max      int
	age      time.Duration
	bare     bool // SnapshotREST assignment with no exch ticker
	zeroTick bool // price_tick left at 0
}

type bookEnv struct {
	t    *testing.T
	c    *Conn
	rec  *recorder
	clk  *fakeClock
	doer *seqDoer
	ch   string
}

func newBookEnv(t *testing.T, bs exchange.BookSync, depth uint32, tune *bookTune, steps []doerStep) *bookEnv {
	t.Helper()
	if tune == nil {
		tune = &bookTune{}
	}
	exch := "BTCUSDT"
	if tune.bare {
		exch = ""
	}
	clk := newFakeClock(time.Unix(1_000_000, 0).UTC())
	doer := &seqDoer{hit: make(chan int, 8), steps: steps}
	rec := newRecorder()
	c := New(Config{
		Venue: "V", Route: "r", Instance: "md", URL: "ws://127.0.0.1:9",
		Market: &bookMarket{bs: bs}, Publish: rec.publish,
		BookDepth: depth, REST: scriptREST{}, Doer: doer,
		BookNow: clk.Now, BookAfter: clk.After, RetryAfter: -1,
		BufferMax: tune.max, BufferAge: tune.age,
	})
	c.mu.Lock()
	c.epoch = 4
	c.live = newLink(&memSession{})
	c.mu.Unlock()
	tick := 1.0
	if tune.zeroTick {
		tick = 0
	}
	d := bookDesiredTick(exch, tick)
	c.SetDesired(d)
	if c.fatal != nil {
		t.Fatal(c.fatal)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.syncBooks(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		c.fetchWG.Wait()
	})
	return &bookEnv{t: t, c: c, rec: rec, clk: clk, doer: doer, ch: bookCh}
}

func bookDesired(exch string) *mdctlv1.Desired {
	return bookDesiredTick(exch, 1)
}

func bookDesiredTick(exch string, tick float64) *mdctlv1.Desired {
	return &mdctlv1.Desired{
		Version: &commonv1.Version{Seq: 1},
		Atoms: []*mdctlv1.AtomAssignment{{
			AtomId: "V:r:" + bookCh, Feeds: []string{"orderbook.T", "orderbook_delta.T"},
			ExchTicker: exch, PriceTick: tick,
		}},
	}
}

func (e *bookEnv) push(u exchange.BookUpdate) {
	e.c.mu.Lock()
	e.c.Book(e.ch, &u)
	e.c.mu.Unlock()
}

func (e *bookEnv) wake() { e.c.wakeBook() }

func (e *bookEnv) waitPubs(t *testing.T, n int) {
	t.Helper()
	e.rec.wait(t, n)
}

func (e *bookEnv) open(t *testing.T) []opened {
	t.Helper()
	return openAll(t, e.rec.snapshot())
}

func (e *bookEnv) waitUntil(t *testing.T, ready func() bool) {
	t.Helper()
	dead := time.NewTimer(5 * time.Second)
	defer dead.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-dead.C:
			t.Fatal("timeout")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// waitNext blocks until the book loop is waiting about d on the fake clock.
func (e *bookEnv) waitNext(t *testing.T, d time.Duration) {
	t.Helper()
	e.waitUntil(t, func() bool {
		next, ok := e.clk.Next()
		if !ok {
			return false
		}
		left := next.Sub(e.clk.Now())
		return left > d-time.Second && left <= d
	})
}

func (e *bookEnv) level(t *testing.T, bid bool, price float64) (float64, bool) {
	t.Helper()
	q, ok := e.levelOK(bid, price)
	return q, ok
}

func (e *bookEnv) levelOK(bid bool, price float64) (float64, bool) {
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	if e.c.live == nil {
		return 0, false
	}
	sh := e.c.live.books[e.ch]
	if sh == nil || sh.book == nil {
		return 0, false
	}
	return sh.book.Qty(bid, price)
}

type opened struct {
	subject    string
	epoch, seq uint64
	ev         *mdv1.FeedEvent
}

func openAll(t *testing.T, pubs []publication) []opened {
	t.Helper()
	out := make([]opened, len(pubs))
	for i, p := range pubs {
		_, epoch, seq, ev := openPub(t, p)
		out[i] = opened{subject: p.subject, epoch: epoch, seq: seq, ev: ev}
	}
	return out
}

func deltasOf(all []opened) []opened {
	var out []opened
	for _, o := range all {
		if o.ev.GetOrderBookDelta() != nil {
			out = append(out, o)
		}
	}
	return out
}

func booksOf(all []opened) []opened {
	var out []opened
	for _, o := range all {
		if o.ev.GetOrderBook() != nil {
			out = append(out, o)
		}
	}
	return out
}

func seqs(os []opened) []uint64 {
	s := make([]uint64, len(os))
	for i, o := range os {
		s[i] = o.seq
	}
	return s
}

func gotLevels(ls []*mdv1.Level) []exchange.Level {
	out := make([]exchange.Level, len(ls))
	for i, l := range ls {
		out[i] = exchange.Level{Price: l.GetPrice(), Qty: l.GetQty()}
	}
	return out
}

func hasLevel(ls []*mdv1.Level, price, qty float64) bool {
	for _, l := range ls {
		if l.GetPrice() == price && l.GetQty() == qty {
			return true
		}
	}
	return false
}

func hasPrice(o opened, price float64) bool {
	var ls []*mdv1.Level
	if d := o.ev.GetOrderBookDelta(); d != nil {
		ls = append(d.GetBids(), d.GetAsks()...)
	}
	if b := o.ev.GetOrderBook(); b != nil {
		ls = append(b.GetBids(), b.GetAsks()...)
	}
	for _, l := range ls {
		if l.GetPrice() == price {
			return true
		}
	}
	return false
}

func levelsOf(o opened) []*mdv1.Level {
	if d := o.ev.GetOrderBookDelta(); d != nil {
		return append(append([]*mdv1.Level{}, d.GetBids()...), d.GetAsks()...)
	}
	if b := o.ev.GetOrderBook(); b != nil {
		return append(append([]*mdv1.Level{}, b.GetBids()...), b.GetAsks()...)
	}
	return nil
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

type doerStep struct {
	resp *exchange.HTTPResponse
	hold <-chan struct{}
}

type seqDoer struct {
	mu    sync.Mutex
	steps []doerStep
	n     int
	hit   chan int
}

func (d *seqDoer) Do(ctx context.Context, _ *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	d.mu.Lock()
	i := d.n
	d.n++
	var step doerStep
	if i < len(d.steps) {
		step = d.steps[i]
	}
	d.mu.Unlock()
	select {
	case d.hit <- i + 1:
	default:
	}
	if step.hold != nil {
		select {
		case <-step.hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if step.resp == nil {
		return nil, errors.New("no depth response")
	}
	return step.resp, nil
}

func (d *seqDoer) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}

func (d *seqDoer) wait(t *testing.T, n int) {
	t.Helper()
	dead := time.NewTimer(5 * time.Second)
	defer dead.Stop()
	for {
		if d.calls() >= n {
			return
		}
		select {
		case <-d.hit:
		case <-dead.C:
			t.Fatalf("depth calls %d, want %d", d.calls(), n)
		}
	}
}

type scriptREST struct{}

func (scriptREST) Listing(symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return exchange.Call[exchange.Page[symbol.Listing]]{}, exchange.ErrNotImplemented
}

func (scriptREST) Depth(*symbol.Info, int) (exchange.Call[*exchange.BookUpdate], error) {
	return exchange.Call[*exchange.BookUpdate]{
		Req:   exchange.HTTPRequest{Method: "GET", Path: "/depth"},
		Parse: parseScriptBook,
	}, nil
}

func (scriptREST) Klines(*symbol.Info, string, int64, int) (exchange.Call[[]exchange.Kline], error) {
	return exchange.Call[[]exchange.Kline]{}, exchange.ErrNotImplemented
}

func (scriptREST) KlinePage() int { return 0 }

func (scriptREST) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrNotImplemented
}

func (scriptREST) Usage(resp *exchange.HTTPResponse, _ time.Time) []exchange.Used {
	if resp == nil {
		return nil
	}
	v := resp.Get("Reset-At-Ms")
	if v == "" {
		return nil
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	return []exchange.Used{{Bucket: "weight", N: 1, ResetAt: time.UnixMilli(ms).UTC()}}
}

func parseScriptBook(resp *exchange.HTTPResponse) (*exchange.BookUpdate, error) {
	if resp.Status == 429 || resp.Status == 418 {
		var ms int64
		if v := resp.Get("Retry-After-Ms"); v != "" {
			ms, _ = strconv.ParseInt(v, 10, 64)
		}
		return nil, &exchange.RateLimited{RetryAfterMs: ms}
	}
	if resp.Status != 200 {
		return nil, errors.New("depth status")
	}
	var raw struct {
		Last uint64       `json:"last"`
		Ts   int64        `json:"ts"`
		Bids [][2]float64 `json:"bids"`
		Asks [][2]float64 `json:"asks"`
	}
	if err := json.Unmarshal(resp.Body, &raw); err != nil {
		return nil, err
	}
	u := &exchange.BookUpdate{ExchTsNs: raw.Ts, Last: raw.Last, Snapshot: true}
	for _, l := range raw.Bids {
		u.Bids = append(u.Bids, exchange.Level{Price: l[0], Qty: l[1]})
	}
	for _, l := range raw.Asks {
		u.Asks = append(u.Asks, exchange.Level{Price: l[0], Qty: l[1]})
	}
	return u, nil
}

func bookHTTP(last uint64, ts int64, bids, asks [][2]float64) *exchange.HTTPResponse {
	raw := struct {
		Last uint64       `json:"last"`
		Ts   int64        `json:"ts"`
		Bids [][2]float64 `json:"bids"`
		Asks [][2]float64 `json:"asks"`
	}{Last: last, Ts: ts, Bids: bids, Asks: asks}
	body, err := json.Marshal(raw)
	if err != nil {
		panic(err)
	}
	return &exchange.HTTPResponse{Status: 200, Body: body}
}

func umBookSync(t *testing.T) exchange.BookSync {
	t.Helper()
	bs, ok := umMarket(t).Book(bookCh)
	if !ok {
		t.Fatal("no um book")
	}
	return bs
}

func bybitBookSync(t *testing.T) exchange.BookSync {
	t.Helper()
	reg, err := exchange.NewRegistry(bybit.Declare())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reg.Venue(bybit.Name)
	if !ok || v.Market == nil {
		t.Fatal("no bybit market")
	}
	bs, ok := v.Market.Book("orderbook.50.BTCUSDT")
	if !ok {
		t.Fatal("no bybit book")
	}
	return bs
}

type fakeClock struct {
	mu      sync.Mutex
	t       time.Time
	waiters []fakeWait
	gen     int
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

func (f *fakeClock) Gen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gen
}

func (f *fakeClock) Next() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var next time.Time
	ok := false
	for _, w := range f.waiters {
		if !ok || w.until.Before(next) {
			next = w.until
			ok = true
		}
	}
	return next, ok
}

func (f *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	f.mu.Lock()
	f.gen++
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
