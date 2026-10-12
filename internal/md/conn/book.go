package conn

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	obook "github.com/BullionBear/mftiko/sdk/book"
)

const (
	// DefaultBufferMax and DefaultBufferAge bound the updates kept while a snapshot is outstanding.
	// Either one drops the buffer and makes that snapshot stale (D424).
	DefaultBufferMax = 10000
	DefaultBufferAge = 10 * time.Second
	// DefaultRateHold is how long a 429 or 418 with neither Retry-After nor ResetAt keeps depth
	// quiet (D424). It is the same default as md.fetch's -rate-hold.
	DefaultRateHold = time.Minute
)

// depthGate is this connection's snapshot clock. One gate for the worker: the venue counts by IP,
// and every book on the connection shares it. wait is the exponential failure window. notBefore
// is the next time a Depth may be sent. A rate-limit hold moves notBefore without touching wait.
type depthGate struct {
	notBefore time.Time
	wait      time.Duration
}

func (g *depthGate) ready(now time.Time) bool {
	return g.notBefore.IsZero() || !now.Before(g.notBefore)
}

// success is a snapshot the book kept. The failure window starts over.
func (g *depthGate) success() {
	g.wait = 0
	g.notBefore = time.Time{}
}

// backoff is D424's exponential wait after an HTTP failure: 1s, then double, cap 30s. The first
// call after a success waits the base, because success clears wait. A gap or a checksum failure
// does not wait; only the HTTP error that follows does.
func (g *depthGate) backoff(now time.Time) {
	g.wait = nextDelay(g.wait, false, 0, 0)
	until := now.Add(g.wait)
	if until.After(g.notBefore) {
		g.notBefore = until
	}
}

// hold is a 429 or 418. It does not advance the exponential window.
func (g *depthGate) hold(until time.Time) {
	if until.After(g.notBefore) {
		g.notBefore = until
	}
}

// shelf is one channel's book on the connection that is up. Reconnect replaces the link, and the
// shelf with it. token and pending tell an in-flight Depth from one whose buffer was dropped:
// pending equals token only while that request is still the one we will apply.
type shelf struct {
	bs      exchange.BookSync
	exch    string
	tick    float64
	token   uint64
	pending uint64
	buf     []exchange.BookUpdate
	bufAt   time.Time // the first buffered update of this attempt
	book    *obook.OrderBook
	seq     exchange.BookSeq
	topB    []exchange.Level
	topA    []exchange.Level
	haveTop bool
	stalled bool // missing price tick, or SnapshotREST with no exch ticker
	// SnapshotStream resubscribe (D434). holdResub is set from the gap until the replacement
	// subscribe is acknowledged. resubWait is the last backoff; an installed snapshot zeroes it.
	// resubAt is the earliest moment plan may cycle the channel.
	holdResub bool
	resubWait time.Duration
	resubAt   time.Time
}

func (sh *shelf) inflight() bool { return sh.pending != 0 && sh.pending == sh.token }

// depthJob is one Depth to send. The token is the shelf's pending value at the start; a buffer
// cap bumps the shelf token so this response is ignored.
type depthJob struct {
	ch    string
	token uint64
	exch  string
	limit int
}

// syncBooks fetches SnapshotREST depth off the read goroutine. The read path only buffers or
// applies, and a ping is answered while Depth is in flight. It stops when ctx ends, which is
// when the connection does.
func (c *Conn) syncBooks(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		c.refreshSymbols()
		jobs, wait := c.planBook()
		if c.symbols != nil && wait > time.Second {
			wait = time.Second
		}
		for _, j := range jobs {
			c.fetchWG.Add(1)
			go func(j depthJob) {
				defer c.fetchWG.Done()
				c.fetch(ctx, j)
			}(j)
		}
		if wait <= 0 {
			wait = time.Hour
		}
		select {
		case <-ctx.Done():
			return
		case <-c.bookWake:
		case <-c.after(wait):
		}
	}
}

// planBook adopts the desired books and starts at most one Depth per shelf. The wait is the
// sooner of the rate-limit hold and the buffer-age cap. Called with no lock held; it takes c.mu.
func (c *Conn) planBook() ([]depthJob, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wait := time.Hour
	if c.live == nil {
		return nil, wait
	}
	l := c.live
	c.adopt()
	now := c.bookNow()
	var jobs []depthJob
	for ch, sh := range c.live.books {
		if c.overCap(sh, now) {
			c.invalidate(sh)
		}
		if d := c.ageWait(sh, now); d < wait {
			wait = d
		}
		if sh.holdResub {
			if now.Before(sh.resubAt) {
				if d := sh.resubAt.Sub(now); d < wait {
					wait = d
				}
			} else {
				l.resub[ch] = struct{}{}
				c.poke()
			}
		}
		if sh.book != nil || sh.stalled || sh.bs.Snapshot() != exchange.SnapshotREST {
			continue
		}
		if sh.inflight() {
			continue
		}
		if !c.gate.ready(now) {
			if d := c.gate.notBefore.Sub(now); d > 0 && d < wait {
				wait = d
			}
			continue
		}
		sh.token++
		sh.pending = sh.token
		jobs = append(jobs, depthJob{ch: ch, token: sh.pending, exch: sh.exch, limit: sh.bs.Depth()})
	}
	return jobs, wait
}

// adopt opens a shelf for every wanted book channel and drops one that is no longer wanted.
// An exch ticker or price tick that changed drops the book: the in-flight snapshot was for the
// previous instrument or the previous grid.
func (c *Conn) adopt() {
	l := c.live
	if l == nil {
		return
	}
	for ch, sh := range l.books {
		if c.wantBook(ch) {
			continue
		}
		sh.token++
		delete(l.books, ch)
		delete(l.resub, ch)
	}
	for ch := range c.wanted {
		if !c.wantBook(ch) {
			continue
		}
		want := c.exch[ch]
		wantTick := c.tick[ch]
		if sh := l.books[ch]; sh != nil {
			if sh.exch != want || sh.tick != wantTick {
				sh.exch = want
				sh.tick = wantTick
				sh.token++
				sh.book = nil
				sh.seq = exchange.BookSeq{}
				sh.haveTop = false
				sh.topB, sh.topA = nil, nil
				sh.buf = nil
				sh.bufAt = time.Time{}
				c.clearBook(ch)
			}
			c.markStall(sh, ch)
			continue
		}
		bs, ok := c.market.Book(ch)
		if !ok {
			continue
		}
		sh := &shelf{bs: bs, exch: want, tick: wantTick}
		c.markStall(sh, ch)
		l.books[ch] = sh
	}
}

func (c *Conn) markStall(sh *shelf, ch string) {
	badTick := !(sh.tick > 0) || math.IsNaN(sh.tick)
	badExch := sh.bs.Snapshot() == exchange.SnapshotREST && sh.exch == ""
	if !badTick && !badExch {
		sh.stalled = false
		return
	}
	if sh.stalled {
		return
	}
	sh.stalled = true
	c.clearBook(ch)
	c.log.Warn().Str("channel", ch).Str("exch", sh.exch).Float64("price_tick", sh.tick).
		Bool("bad_tick", badTick).Bool("bad_exch", badExch).Msg("book stalled")
}

func (c *Conn) wantBook(ch string) bool {
	for _, f := range c.wanted[ch] {
		if f.topic == string(exchange.TopicOrderBook) || f.topic == string(exchange.TopicOrderBookDelta) {
			return true
		}
	}
	return false
}

func (c *Conn) ensureShelf(ch string) *shelf {
	if c.live == nil || !c.wantBook(ch) {
		return nil
	}
	if sh := c.live.books[ch]; sh != nil {
		return sh
	}
	bs, ok := c.market.Book(ch)
	if !ok {
		return nil
	}
	sh := &shelf{bs: bs, exch: c.exch[ch], tick: c.tick[ch]}
	c.markStall(sh, ch)
	c.live.books[ch] = sh
	return sh
}

// overCap is either buffer bound, inclusive. An empty buffer has no age.
func (c *Conn) overCap(sh *shelf, now time.Time) bool {
	if sh == nil || len(sh.buf) == 0 {
		return false
	}
	if len(sh.buf) >= c.bufferMax {
		return true
	}
	return !sh.bufAt.IsZero() && !now.Before(sh.bufAt.Add(c.bufferAge))
}

// ageWait is how long until the age cap. An empty buffer does not wake the loop.
func (c *Conn) ageWait(sh *shelf, now time.Time) time.Duration {
	if len(sh.buf) == 0 || sh.bufAt.IsZero() {
		return time.Hour
	}
	d := sh.bufAt.Add(c.bufferAge).Sub(now)
	if d < 0 {
		return 0
	}
	return d
}

// invalidate drops the buffer and bumps the token so the Depth already in flight is stale.
func (c *Conn) invalidate(sh *shelf) {
	sh.token++
	sh.buf = nil
	sh.bufAt = time.Time{}
	c.log.Debug().Msg("book buffer cap; snapshot restarted")
}

func (c *Conn) bufferUpdate(sh *shelf, u exchange.BookUpdate) {
	now := c.bookNow()
	if len(sh.buf) == 0 {
		sh.bufAt = now
	}
	sh.buf = append(sh.buf, u)
	if c.overCap(sh, now) {
		c.invalidate(sh)
	}
}

// onBook is Book. Decode calls it with c.mu held. It returns before any HTTP: a snapshot is the
// sync loop's job, and the read goroutine is what answers a venue ping.
func (c *Conn) onBook(ch string, u *exchange.BookUpdate) {
	if u == nil || c.live == nil || !c.wantBook(ch) {
		return
	}
	sh := c.ensureShelf(ch)
	if sh == nil || sh.stalled {
		return
	}
	cp := cloneUpdate(u)
	if sh.bs.Snapshot() == exchange.SnapshotStream && cp.Snapshot {
		c.onStream(ch, sh, cp)
		return
	}
	if sh.book == nil {
		c.bufferUpdate(sh, cp)
		c.wakeBook()
		return
	}
	c.onLive(ch, sh, cp)
}

// onStream installs a full book from the venue and publishes one reset. Deltas buffered before
// it are from earlier in the same stream and are dropped; the snapshot replaces them.
func (c *Conn) onStream(ch string, sh *shelf, u exchange.BookUpdate) {
	sh.buf = nil
	sh.bufAt = time.Time{}
	b, err := obook.New(obook.DefaultN, sh.tick)
	if err != nil {
		c.log.Warn().Err(err).Str("channel", ch).Msg("book")
		return
	}
	b.Apply(bookLevels(u.Bids), bookLevels(u.Asks))
	sh.book = b
	sh.seq = exchange.BookSeq{Last: u.Last, Valid: true}
	// The venue already sent a full book. Drop a cycle that has not been sent yet, and start
	// the next gap at 1s again (D434). A cycle already on the wire still finishes.
	sh.holdResub = false
	sh.resubWait = 0
	sh.resubAt = time.Time{}
	if c.live != nil {
		delete(c.live.resub, ch)
	}
	c.publishReset(ch, sh, u.ExchTsNs)
}

// onLive applies one update to a book that is already in sync. A gap or a checksum failure
// discards the book without publishing that update. The gap update is kept so the next snapshot
// can bridge it; a checksum failure is not applied again. A synced book is not fetched again
// on its own (D426). The delta is the increment itself; the ring decides what it keeps (D432).
func (c *Conn) onLive(ch string, sh *shelf, u exchange.BookUpdate) {
	switch sh.bs.Check(sh.seq, &u) {
	case exchange.BookSkip:
		return
	case exchange.BookResync:
		c.beginResync(ch, sh, &u)
		return
	case exchange.BookApply:
		c.publishIncremental(ch, sh, u.ExchTsNs, u)
	default:
		c.beginResync(ch, sh, &u)
	}
}

// beginResync drops the local book. keep is the update that did not apply; nil drops the buffer,
// which is what a checksum failure does so that update is not replayed. The last published top
// is kept, so a resync that leaves it unchanged does not take another orderbook seq (D427).
// SnapshotStream then cycles that one channel after its own backoff (D434). SnapshotREST
// refetches Depth; it does not unsubscribe.
func (c *Conn) beginResync(ch string, sh *shelf, keep *exchange.BookUpdate) {
	sh.book = nil
	c.clearBook(ch)
	sh.seq = exchange.BookSeq{}
	if keep == nil {
		sh.buf = nil
		sh.bufAt = time.Time{}
	} else {
		sh.buf = []exchange.BookUpdate{*keep}
		sh.bufAt = c.bookNow()
	}
	if sh.bs.Snapshot() == exchange.SnapshotStream {
		c.scheduleResub(ch, sh)
	}
	c.wakeBook()
}

// scheduleResub arms one unsubscribe-plus-subscribe for this book. The first attempt waits 1s,
// then twice the previous wait, up to 30s, the same curve as a failed Depth (D424, D434).
// A cycle already waiting or in flight is not restarted, so a stream of gaps cannot hot-loop.
func (c *Conn) scheduleResub(ch string, sh *shelf) {
	if sh.holdResub || sh.stalled {
		return
	}
	now := c.bookNow()
	delay := nextDelay(sh.resubWait, false, 0, 0)
	sh.resubWait = delay
	sh.resubAt = now.Add(delay)
	sh.holdResub = true
	c.log.Warn().Str("channel", ch).Dur("retry_in", delay).Msg("book resubscribe")
}

func (c *Conn) fetch(ctx context.Context, j depthJob) {
	snap, err, used := c.doDepth(ctx, j)
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		c.abandon(j)
		return
	}
	c.finishDepth(j, snap, err, used)
}

func (c *Conn) abandon(j depthJob) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.live == nil {
		return
	}
	sh := c.live.books[j.ch]
	if sh != nil && sh.pending == j.token {
		sh.pending = 0
	}
}

// doDepth sends one Depth. Usage is taken from this response before Parse, including when Parse
// fails, and only this response decides a rate-limit hold (D424). There is no shared counter with
// md.fetch and no pre-check against a weight threshold.
func (c *Conn) doDepth(ctx context.Context, j depthJob) (*exchange.BookUpdate, error, []exchange.Used) {
	if c.rest == nil || c.doer == nil {
		return nil, errors.New("conn: depth transport is missing"), nil
	}
	call, err := c.rest.Depth(&symbol.Info{
		ExchTicker: j.exch,
		Ticker:     symbol.UniversalTicker{Venue: c.venue},
	}, j.limit)
	if err != nil {
		return nil, err, nil
	}
	if call.Parse == nil {
		return nil, errors.New("conn: depth parse is missing"), nil
	}
	parse := call.Parse
	var used []exchange.Used
	call.Parse = func(resp *exchange.HTTPResponse) (*exchange.BookUpdate, error) {
		used = append([]exchange.Used(nil), c.rest.Usage(resp, c.bookNow())...)
		return parse(resp)
	}
	out, err := exchange.Run(ctx, c.doer, call)
	return out, err, used
}

// finishDepth applies a snapshot that is still the one this shelf is waiting for. A mismatched
// token is a buffer cap that already started a newer snapshot.
func (c *Conn) finishDepth(j depthJob, snap *exchange.BookUpdate, err error, used []exchange.Used) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.live == nil {
		return
	}
	sh := c.live.books[j.ch]
	// token moves on when the buffer is dropped, so a snapshot that was in flight for the
	// previous attempt no longer matches even though pending still names it.
	if sh == nil || sh.pending != j.token || sh.token != j.token {
		return
	}
	sh.pending = 0
	now := c.bookNow()
	if err != nil || snap == nil {
		if err == nil {
			err = errors.New("conn: depth returned no book")
		}
		if until, ok := rateDeadline(now, c.rateHold, err, used); ok {
			c.gate.hold(until)
			c.log.Warn().Err(err).Time("until", until).Str("channel", j.ch).Msg("depth limited")
		} else {
			c.gate.backoff(now)
			c.log.Warn().Err(err).Dur("retry_in", c.gate.wait).Str("channel", j.ch).Msg("depth")
		}
		c.wakeBook()
		return
	}
	// The venue answered, including a snapshot the buffer cap throws away. The failure window
	// starts over; the cap restarts the snapshot immediately.
	c.gate.success()
	if c.overCap(sh, now) {
		c.invalidate(sh)
		c.wakeBook()
		return
	}
	c.installREST(j.ch, sh, cloneUpdate(snap))
	c.wakeBook()
}

// installREST folds the snapshot, replays the buffer through Check, and publishes one reset of
// the window after that replay. A gap or a checksum failure in the buffer publishes nothing and
// keeps the tail (the checksum failure itself is not kept). The shelf then has no book, so the
// next plan fetches again. A kept book is not fetched again on its own (D426).
func (c *Conn) installREST(ch string, sh *shelf, snap exchange.BookUpdate) {
	b, err := obook.New(obook.DefaultN, sh.tick)
	if err != nil {
		c.log.Warn().Err(err).Str("channel", ch).Msg("book")
		return
	}
	b.Apply(bookLevels(snap.Bids), bookLevels(snap.Asks))
	seq := exchange.BookSeq{Last: snap.Last, Valid: true}
	ts := snap.ExchTsNs
	for i := 0; i < len(sh.buf); i++ {
		u := sh.buf[i]
		switch sh.bs.Check(seq, &u) {
		case exchange.BookSkip:
			continue
		case exchange.BookApply:
			b.Apply(bookLevels(u.Bids), bookLevels(u.Asks))
			if !c.checksumOK(b, sh.bs, &u) {
				c.keepTail(ch, sh, sh.buf[i+1:])
				return
			}
			seq.Last = u.Last
			seq.Valid = true
			ts = u.ExchTsNs
		default:
			c.keepTail(ch, sh, sh.buf[i:])
			return
		}
	}
	sh.book = b
	sh.seq = seq
	sh.buf = nil
	sh.bufAt = time.Time{}
	c.publishReset(ch, sh, ts)
}

func (c *Conn) keepTail(ch string, sh *shelf, tail []exchange.BookUpdate) {
	sh.book = nil
	c.clearBook(ch)
	sh.seq = exchange.BookSeq{}
	sh.buf = append([]exchange.BookUpdate(nil), tail...)
	if len(sh.buf) == 0 {
		sh.bufAt = time.Time{}
	}
}

// rateDeadline is how long a 429 or 418 stays quiet. Retry-After wins. Otherwise the latest
// ResetAt on this response. With neither, the configured hold. A non-limit error is not a hold:
// the caller uses the exponential window instead.
func rateDeadline(now time.Time, hold time.Duration, err error, used []exchange.Used) (time.Time, bool) {
	var rl *exchange.RateLimited
	if !errors.As(err, &rl) || rl == nil {
		return time.Time{}, false
	}
	if rl.RetryAfterMs > 0 {
		return now.Add(time.Duration(rl.RetryAfterMs) * time.Millisecond), true
	}
	var until time.Time
	saw := false
	for _, u := range used {
		if u.ResetAt.IsZero() {
			continue
		}
		if !saw || u.ResetAt.After(until) {
			until = u.ResetAt
			saw = true
		}
	}
	if !saw {
		if hold <= 0 {
			hold = DefaultRateHold
		}
		return now.Add(hold), true
	}
	return until, true
}

func (c *Conn) checksumOK(b *obook.OrderBook, bs exchange.BookSync, u *exchange.BookUpdate) bool {
	cs, ok := bs.(exchange.BookChecksum)
	if !ok || b == nil {
		return true
	}
	n := b.Window()
	return cs.Verify(sideLevels(b, true, n), sideLevels(b, false, n), u)
}

// publishIncremental forwards the increment unchanged, then rebuilds top-N only when the
// increment touches it. The ring drops out-of-window levels while applying; that filter is not
// applied to the delta (D432).
func (c *Conn) publishIncremental(ch string, sh *shelf, ts int64, u exchange.BookUpdate) {
	bids := bookLevels(u.Bids)
	asks := bookLevels(u.Asks)
	touch := sh.book.AffectsTop(bids, asks, int(c.bookDepth))
	sh.book.Apply(bids, asks)
	if !c.checksumOK(sh.book, sh.bs, &u) {
		c.beginResync(ch, sh, nil)
		return
	}
	sh.seq.Last = u.Last
	sh.seq.Valid = true
	if len(u.Bids)+len(u.Asks) > 0 {
		c.emitDelta(ch, ts, false, u.Bids, u.Asks)
	}
	if touch {
		c.emitTop(ch, sh, ts)
	}
	c.noteBook(ch, sh, ts)
}

// publishReset is the full in-window book, then the top-N when that top changed (D425, D430).
func (c *Conn) publishReset(ch string, sh *shelf, ts int64) {
	if sh.book == nil {
		return
	}
	n := sh.book.Window()
	c.emitDelta(ch, ts, true, sideLevels(sh.book, true, n), sideLevels(sh.book, false, n))
	c.emitTop(ch, sh, ts)
	c.noteBook(ch, sh, ts)
}

// BookWindow is the in-window book. md.view reads a snapshot published beside it (D430, D441),
// not this method: this method takes the connection lock. ok is false when this connection is
// not holding the channel's book.
func (c *Conn) BookWindow(ch string) (bids, asks []exchange.Level, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.live == nil {
		return nil, nil, false
	}
	sh := c.live.books[ch]
	if sh == nil || sh.book == nil {
		return nil, nil, false
	}
	n := sh.book.Window()
	return sideLevels(sh.book, true, n), sideLevels(sh.book, false, n), true
}

func (c *Conn) emitDelta(ch string, ts int64, reset bool, bids, asks []exchange.Level) {
	c.publishTopic(ch, string(exchange.TopicOrderBookDelta), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_OrderBookDelta{OrderBookDelta: &mdv1.OrderBookDelta{
			Meta: meta(ts), Bids: protoLevels(bids), Asks: protoLevels(asks), Reset_: reset,
		}},
	})
}

func (c *Conn) emitTop(ch string, sh *shelf, ts int64) {
	if sh.book == nil {
		return
	}
	b := sideLevels(sh.book, true, int(c.bookDepth))
	a := sideLevels(sh.book, false, int(c.bookDepth))
	if sh.haveTop && sameLevels(sh.topB, b) && sameLevels(sh.topA, a) {
		return
	}
	sh.topB = b
	sh.topA = a
	sh.haveTop = true
	c.publishTopic(ch, string(exchange.TopicOrderBook), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_OrderBook{OrderBook: &mdv1.OrderBook{
			Meta: meta(ts), Bids: protoLevels(sh.topB), Asks: protoLevels(sh.topA), Depth: c.bookDepth,
		}},
	})
}

func sideLevels(b *obook.OrderBook, bid bool, n int) []exchange.Level {
	if b == nil || n <= 0 {
		return nil
	}
	it := b.Bids(n)
	if !bid {
		it = b.Asks(n)
	}
	var out []exchange.Level
	for {
		tick, sz, ok := it.Next()
		if !ok {
			return out
		}
		out = append(out, exchange.Level{Price: b.Price(tick), Qty: sz})
	}
}

func bookLevels(ls []exchange.Level) []obook.Level {
	if len(ls) == 0 {
		return nil
	}
	out := make([]obook.Level, len(ls))
	for i, l := range ls {
		out[i] = obook.Level{Price: l.Price, Size: l.Qty}
	}
	return out
}

func sameLevels(a, b []exchange.Level) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Price != b[i].Price || a[i].Qty != b[i].Qty {
			return false
		}
	}
	return true
}

func protoLevels(ls []exchange.Level) []*mdv1.Level {
	if len(ls) == 0 {
		return nil
	}
	out := make([]*mdv1.Level, len(ls))
	for i, l := range ls {
		out[i] = &mdv1.Level{Price: l.Price, Qty: l.Qty}
	}
	return out
}

func cloneUpdate(u *exchange.BookUpdate) exchange.BookUpdate {
	out := *u
	out.Bids = slices.Clone(u.Bids)
	out.Asks = slices.Clone(u.Asks)
	return out
}

func (c *Conn) wakeBook() {
	select {
	case c.bookWake <- struct{}{}:
	default:
	}
}

func (c *Conn) after(d time.Duration) <-chan time.Time {
	if c.bookAfter != nil {
		return c.bookAfter(d)
	}
	return time.After(d)
}

// vetBooks is the startup check that needs a channel. Missing N fails here as well as at Init.
// SnapshotREST with N above Depth fails before the first snapshot. SnapshotStream is not compared
// with Depth: Bybit's is zero. The worker exits with this error and does not dial again.
func (c *Conn) vetBooks(wanted map[string][]feed) error {
	if c.market == nil {
		return nil
	}
	for ch, feeds := range wanted {
		if !bookFeed(feeds) {
			continue
		}
		if c.bookDepth == 0 {
			return errors.New("conn: book depth is required")
		}
		bs, ok := c.market.Book(ch)
		if !ok {
			return errors.New("conn: " + ch + " is not a book")
		}
		if bs.Snapshot() != exchange.SnapshotREST {
			continue
		}
		if d := bs.Depth(); int(c.bookDepth) > d {
			return fmt.Errorf("conn: book depth %d is above %s depth %d", c.bookDepth, ch, d)
		}
	}
	return nil
}

func bookFeed(feeds []feed) bool {
	for _, f := range feeds {
		if f.topic == string(exchange.TopicOrderBook) || f.topic == string(exchange.TopicOrderBookDelta) {
			return true
		}
	}
	return false
}
