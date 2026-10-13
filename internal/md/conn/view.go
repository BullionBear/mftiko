package conn

import (
	"strings"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/exchange"
)

// viewFeed is one md.view responder. ring is set for tape topics, book for book
// topics, and neither for a topic this worker answers with "unsupported".
// The handler closes over this value. Its ring and book pointers do not change.
type viewFeed struct {
	key, topic, ticker, channel string
	ring                        *tapeRing
	book                        *atomic.Pointer[bookSnap]
}

// viewSub is one live md.view subscription and the feed it captured.
type viewSub struct {
	stop func()
	feed *viewFeed
}

// bookSnap is the in-window book of the last applied cursor (D441). The writer
// stores a new value; readers only Load it.
type bookSnap struct {
	bids, asks   []*mdv1.Level
	epoch, seq   uint64
	mdTs, exchTs int64
}

func bookTopic(topic string) bool {
	return topic == string(exchange.TopicOrderBookDelta)
}

func viewSubject(inst, topic, ticker string) string {
	return viewPrefix + strings.TrimPrefix(subject(inst, topic, ticker), feedPrefix)
}

// syncViewsLocked creates a ring when a feed enters wanted, keeps a feed that
// has gone PENDING, and drops one that was removed or REJECTED (D448, D449).
// The caller holds c.mu. Subscriptions are rebound after the lock is released.
func (c *Conn) syncViewsLocked() {
	if c.views == nil {
		c.views = map[string]*viewFeed{}
	}
	keep := make(map[string]struct{}, len(c.wanted))
	for ch, feeds := range c.wanted {
		for _, f := range feeds {
			key := f.topic + "." + f.ticker
			keep[key] = struct{}{}
			if _, ok := c.views[key]; ok {
				continue
			}
			c.views[key] = c.newViewLocked(key, f, ch)
		}
	}
	for key, v := range c.views {
		if _, ok := keep[key]; ok {
			continue
		}
		st, listed := c.status[key]
		if listed && st.state != mdctlv1.FeedState_FEED_STATE_REJECTED {
			continue
		}
		if v.ring != nil {
			c.tapeBytes -= int64(v.ring.bytes)
			if c.tapeBytes < 0 {
				c.tapeBytes = 0
			}
		}
		delete(c.views, key)
	}
}

func (c *Conn) newViewLocked(key string, f feed, ch string) *viewFeed {
	vf := &viewFeed{key: key, topic: f.topic, ticker: f.ticker, channel: ch}
	switch {
	case tapeTopic(f.topic):
		vf.ring = newTapeRing(c.tapeMax)
		if !c.wasDown {
			vf.ring.setEmptyHead(c.epoch)
		}
	case bookTopic(f.topic):
		vf.book = c.bookPtrLocked(ch)
		if vf.book.Load() == nil && c.live != nil {
			if sh := c.live.books[ch]; sh != nil && sh.book != nil && !sh.stalled {
				c.noteBook(ch, sh, 0)
			}
		}
	}
	return vf
}

// rebindViews makes the md.view subscriptions match c.views. It does not hold
// c.mu across Subscribe: a NATS flush must not stall the websocket read (D448).
func (c *Conn) rebindViews() {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	if c.subscribe == nil {
		return
	}
	c.mu.Lock()
	want := make(map[string]*viewFeed, len(c.views))
	for k, v := range c.views {
		want[k] = v
	}
	c.mu.Unlock()
	if c.viewStop == nil {
		c.viewStop = map[string]*viewSub{}
	}
	for key, sub := range c.viewStop {
		if want[key] == sub.feed {
			continue
		}
		sub.stop()
		delete(c.viewStop, key)
	}
	for key, vf := range want {
		if _, ok := c.viewStop[key]; ok {
			continue
		}
		subject := viewSubject(c.inst, vf.topic, vf.ticker)
		stop, err := c.subscribe(subject, func(reply string, req []byte) {
			c.serveView(vf, reply, req)
		})
		if err != nil {
			c.log.Warn().Err(err).Str("subject", subject).Msg("md.view")
			continue
		}
		c.viewStop[key] = &viewSub{stop: stop, feed: vf}
	}
}

func (c *Conn) closeViews() {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	for key, sub := range c.viewStop {
		if sub != nil && sub.stop != nil {
			sub.stop()
		}
		delete(c.viewStop, key)
	}
}

// onUpLocked records a gap for rings that were up when the previous connection
// ended, then sets an empty ring's head to this epoch (D437, D440).
func (c *Conn) onUpLocked() {
	wasDown := c.wasDown
	c.wasDown = false
	now := c.now().UnixNano()
	for _, v := range c.views {
		if v.ring == nil {
			continue
		}
		before := v.ring.bytes
		if wasDown && v.ring.pendingDown {
			v.ring.appendGap(c.downNs, now, c.epoch, c.tapeWindow)
		}
		v.ring.setEmptyHead(c.epoch)
		c.tapeBytes += int64(v.ring.bytes - before)
	}
	c.trimBudget()
}

// onDownLocked remembers each ring's head and drops book snapshots. The book
// dies with the connection (D425). Tape stays.
func (c *Conn) onDownLocked() {
	if c.wasDown {
		return
	}
	c.wasDown = true
	c.downNs = c.now().UnixNano()
	for _, v := range c.views {
		if v.ring != nil {
			v.ring.markDown()
		}
	}
	for _, p := range c.books {
		if p != nil {
			p.Store(nil)
		}
	}
}

func (c *Conn) recordTape(key string, epoch, seq uint64, mdTs int64, raw []byte) {
	v := c.views[key]
	if v == nil || v.ring == nil {
		return
	}
	before := v.ring.bytes
	v.ring.appendEvent(epoch, seq, mdTs, raw, c.tapeWindow)
	c.tapeBytes += int64(v.ring.bytes - before)
	c.trimBudget()
}

func (c *Conn) trimBudget() {
	for c.tapeBytes > c.tapeBudget {
		var (
			best    *tapeRing
			bestTs  int64
			bestKey string
		)
		for key, v := range c.views {
			if v.ring == nil || v.ring.events == 0 {
				continue
			}
			ts := v.ring.oldest
			if best == nil || ts < bestTs || (ts == bestTs && key < bestKey) {
				best, bestTs, bestKey = v.ring, ts, key
			}
		}
		if best == nil {
			return
		}
		before := best.bytes
		if !best.dropOneEvent(c.epoch) {
			return
		}
		freed := int64(before - best.bytes)
		c.tapeBytes -= freed
		if freed <= 0 {
			return
		}
	}
}

func (c *Conn) serveView(vf *viewFeed, reply string, req []byte) {
	if vf == nil || reply == "" || c.pub == nil {
		return
	}
	var payload []byte
	switch {
	case vf.ring != nil:
		payload = tapeReply(vf.ring, req)
	case vf.book != nil:
		payload = bookReply(vf.book)
	default:
		payload = viewError("unsupported")
	}
	if err := c.pub(reply, payload); err != nil {
		c.log.Warn().Err(err).Str("subject", reply).Msg("md.view")
	}
}

func tapeReply(r *tapeRing, req []byte) []byte {
	in := &mdv1.ViewRequest{}
	if err := proto.Unmarshal(req, in); err != nil {
		return viewError("invalid")
	}
	limit := in.GetLimit()
	if limit == 0 {
		limit = defaultTapeLimit
	} else if limit > maxTapeLimit {
		return viewError("invalid")
	}
	var since int64
	has := in.SinceNs != nil
	if has {
		since = in.GetSinceNs()
	}
	return r.page(in.GetBefore(), since, has, int(limit))
}

func bookReply(p *atomic.Pointer[bookSnap]) []byte {
	if p == nil {
		return viewError("not_synced")
	}
	snap := p.Load()
	if snap == nil {
		return viewError("not_synced")
	}
	b, err := proto.Marshal(&mdv1.ViewReply{Result: &mdv1.ViewReply_Book{Book: &mdv1.BookSnapshot{
		Meta:      &mdv1.Meta{ExchTsNs: snap.exchTs},
		Bids:      snap.bids,
		Asks:      snap.asks,
		ConnEpoch: snap.epoch,
		Seq:       snap.seq,
		MdTsNs:    snap.mdTs,
	}}})
	if err != nil {
		return viewError("invalid")
	}
	return b
}

func viewError(code string) []byte {
	b, err := proto.Marshal(&mdv1.ViewReply{Error: code})
	if err != nil {
		return nil
	}
	return b
}

func (c *Conn) bookPtrLocked(ch string) *atomic.Pointer[bookSnap] {
	if c.books == nil {
		c.books = map[string]*atomic.Pointer[bookSnap]{}
	}
	p := c.books[ch]
	if p == nil {
		p = &atomic.Pointer[bookSnap]{}
		c.books[ch] = p
	}
	return p
}

func (c *Conn) clearBook(ch string) {
	if p := c.books[ch]; p != nil {
		p.Store(nil)
	}
}

// noteBook publishes the in-window book. The cursor is the orderbook_delta seq,
// because that is the increment a subscriber folds (D441, D453).
func (c *Conn) noteBook(ch string, sh *shelf, exchTs int64) {
	if sh == nil || sh.book == nil {
		return
	}
	p := c.books[ch]
	if p == nil {
		return
	}
	epoch, seq, mdTs, ok := c.seamLocked(ch)
	if !ok {
		return
	}
	n := sh.book.Window()
	p.Store(&bookSnap{
		bids:   protoLevels(sideLevels(sh.book, true, n)),
		asks:   protoLevels(sideLevels(sh.book, false, n)),
		epoch:  epoch,
		seq:    seq,
		mdTs:   mdTs,
		exchTs: exchTs,
	})
}

func (c *Conn) seamLocked(ch string) (epoch, seq uint64, mdTs int64, ok bool) {
	l := c.live
	if l == nil {
		return 0, 0, 0, false
	}
	for _, topic := range []string{string(exchange.TopicOrderBookDelta)} {
		for _, f := range c.wanted[ch] {
			if f.topic != topic {
				continue
			}
			key := f.topic + "." + f.ticker
			seq = l.seq[key]
			if seq == 0 {
				continue
			}
			if l.mdTs != nil {
				mdTs = l.mdTs[key]
			}
			return c.epoch, seq, mdTs, true
		}
	}
	return 0, 0, 0, false
}
