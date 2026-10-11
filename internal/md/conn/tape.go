package conn

import (
	"bytes"
	"math"
	"sort"
	"time"

	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
)

const (
	// DefaultTapeWindow is how long one atom keeps tape when Init leaves the window at 0 (D438).
	DefaultTapeWindow = 10 * time.Minute
	// DefaultTapeEvents is how many events one atom keeps when Init leaves the count at 0 (D438).
	DefaultTapeEvents = 100_000
	// DefaultTapeBytes is the worker-wide cap when Init leaves it at 0: 256 MiB (D438).
	DefaultTapeBytes int64 = 256 << 20

	// tapePageDefault is the page size when ViewRequest.limit is 0 (D439).
	tapePageDefault = 1000
	// tapePageLimit is the largest accepted limit. Above it the reply is invalid (D439).
	tapePageLimit = 5000
	// tapeReplyBudget is where a tape page stops. 900 KiB leaves margin under NATS 1 MiB (D439).
	tapeReplyBudget = 900 * 1024
)

// feedOwner is one feed this worker is holding. The ring is keyed by the atom, because the
// limits are per atom; the feed key is what md.view and seq are counted on (D401, D409, D438).
type feedOwner struct {
	atomID string
	ch     string
	record bool
}

// pubMark is the latest md.f cursor of one feed, including a publish that was not stored
// (FeedEnd, a book, a failed publish that still consumed seq). An empty ring does not clear it.
type pubMark struct {
	epoch uint64
	seq   uint64
	mdTs  int64
	set   bool
}

// cursor is (conn_epoch, seq) in lexicographic order. A higher epoch is newer.
type cursor struct {
	epoch uint64
	seq   uint64
}

func cursorLess(a, b cursor) bool {
	if a.epoch != b.epoch {
		return a.epoch < b.epoch
	}
	return a.seq < b.seq
}

// tapeEvent is one stored md.f message. raw is the envelope plus payload; the bytes are never
// overwritten, only dropped from the front of the ring (D435).
type tapeEvent struct {
	feed       string
	epoch, seq uint64
	mdTs       int64
	raw        []byte
	ord        uint64
}

// tapeGap is one reconnect of one feed. Seq is per feed, so the gap lives on the atom ring
// tagged by that feed (D409, D440). first is unset until the next stored event of the feed.
type tapeGap struct {
	feed                 string
	startNs, endNs       int64
	lastEpoch, lastSeq   uint64
	lastSet              bool
	firstEpoch, firstSeq uint64
	firstSet             bool
}

// tapeRing is one atom. events are oldest at index 0. bytes counts the stored messages.
type tapeRing struct {
	events []tapeEvent
	gaps   []tapeGap
	bytes  int64
}

// tapeFromInit converts the three InitPayload fields. Zero means "use the default", which New
// applies when the corresponding Config field is still zero (D438).
func tapeFromInit(windowMs, events uint32, maxBytes uint64) (time.Duration, int, int64) {
	var window time.Duration
	if windowMs > 0 {
		window = time.Duration(windowMs) * time.Millisecond
	}
	var n int
	if events > 0 {
		n = int(events)
	}
	var b int64
	if maxBytes > 0 {
		if maxBytes > uint64(math.MaxInt64) {
			b = math.MaxInt64
		} else {
			b = int64(maxBytes)
		}
	}
	return window, n, b
}

func tapeTopic(topic string) bool {
	switch exchange.Topic(topic) {
	case exchange.TopicTrade, exchange.TopicAggTrade, exchange.TopicLiquidation:
		return true
	default:
		return false
	}
}

func bookTopic(topic string) bool {
	switch exchange.Topic(topic) {
	case exchange.TopicOrderBook, exchange.TopicOrderBookDelta:
		return true
	default:
		return false
	}
}

func (c *Conn) ensureRing(atomID string) *tapeRing {
	r := c.rings[atomID]
	if r == nil {
		r = &tapeRing{}
		c.rings[atomID] = r
	}
	return r
}

// syncTape installs the feed map from one Desired and drops rings whose atom is gone (D443).
func (c *Conn) syncTape(owners map[string]feedOwner) {
	atoms := make(map[string]struct{}, len(owners))
	for _, ow := range owners {
		atoms[ow.atomID] = struct{}{}
	}
	c.owners = owners
	for id, r := range c.rings {
		if _, ok := atoms[id]; ok {
			continue
		}
		c.tapeBytes -= r.bytes
		delete(c.rings, id)
	}
	if c.tapeBytes < 0 {
		c.tapeBytes = 0
	}
	for key := range c.marks {
		if _, ok := owners[key]; !ok {
			delete(c.marks, key)
		}
	}
}

// notePublish records the cursor md.f just handed out. The call is in the same critical section
// as the publish, so a view that runs after it sees this cursor as head (D437).
func (c *Conn) notePublish(key string, epoch, seq uint64, mdTs int64) {
	c.marks[key] = pubMark{epoch: epoch, seq: seq, mdTs: mdTs, set: true}
}

func (c *Conn) headOf(feed string) cursor {
	if m := c.marks[feed]; m.set {
		return cursor{epoch: m.epoch, seq: m.seq}
	}
	return cursor{epoch: c.epoch, seq: 0}
}

// recordTape stores one published message when this feed's topic is recorded. The worker
// decides that from the topic and ignores AtomAssignment.record (D447). Book topics and
// FeedEnd never get here (D435, D442, D446).
func (c *Conn) recordTape(key, topic string, mdTs int64, epoch, seq uint64, msg []byte) {
	if !tapeTopic(topic) || len(msg) < envelope.Size {
		return
	}
	ow, ok := c.owners[key]
	if !ok || !ow.record {
		return
	}
	raw := bytes.Clone(msg)
	r := c.ensureRing(ow.atomID)
	c.noteFirst(r, key, epoch, seq)
	c.tapeOrd++
	n := int64(len(raw))
	if n < 0 {
		n = math.MaxInt64
	}
	r.events = append(r.events, tapeEvent{
		feed: key, epoch: epoch, seq: seq, mdTs: mdTs, raw: raw, ord: c.tapeOrd,
	})
	r.bytes += n
	c.tapeBytes += n
	c.trimAtom(r, mdTs)
	c.trimBytes()
}

// noteFirst fills the latest open gap of this feed. Older gaps that never saw an event stay open.
func (c *Conn) noteFirst(r *tapeRing, feed string, epoch, seq uint64) {
	for i := len(r.gaps) - 1; i >= 0; i-- {
		if r.gaps[i].feed != feed {
			continue
		}
		if r.gaps[i].firstSet {
			return
		}
		r.gaps[i].firstEpoch = epoch
		r.gaps[i].firstSeq = seq
		r.gaps[i].firstSet = true
		return
	}
}

// markDown snapshots the tape feeds that were recording when a connection that had come up
// goes away. The first dial of the process has not been up, so it records the time and no feeds.
// A failed dial never calls this. The clock is the md_ts clock (D440).
func (c *Conn) markDown() {
	c.downNs = c.now().UnixNano()
	c.downFeeds = nil
	if !c.seenUp {
		return
	}
	for key, ow := range c.owners {
		topic, _, ok := splitFeed(key)
		if !ok || !ow.record || !tapeTopic(topic) {
			continue
		}
		c.downFeeds = append(c.downFeeds, key)
	}
	sort.Strings(c.downFeeds)
}

// openGaps appends one gap per feed that was recording at the previous disconnect and is still
// a recorded tape feed. last is that feed's last md.f cursor, or (prevEpoch, 0) when it never
// published. first is filled by the next stored event (D440).
func (c *Conn) openGaps(feeds []string, prevEpoch uint64, start, end int64) {
	for _, key := range feeds {
		ow, ok := c.owners[key]
		if !ok || !ow.record {
			continue
		}
		topic, _, ok := splitFeed(key)
		if !ok || !tapeTopic(topic) {
			continue
		}
		g := tapeGap{feed: key, startNs: start, endNs: end, lastSet: true}
		if m := c.marks[key]; m.set {
			g.lastEpoch, g.lastSeq = m.epoch, m.seq
		} else {
			g.lastEpoch, g.lastSeq = prevEpoch, 0
		}
		c.ensureRing(ow.atomID).gaps = append(c.rings[ow.atomID].gaps, g)
	}
}

func (c *Conn) trimAtom(r *tapeRing, now int64) {
	if r == nil {
		return
	}
	cutoff := now - int64(c.tapeWindow)
	for len(r.events) > 0 && (r.events[0].mdTs < cutoff || len(r.events) > c.tapeMax) {
		c.dropFront(r)
	}
	c.trimGaps(r, cutoff)
}

func (c *Conn) trimGaps(r *tapeRing, cutoff int64) {
	kept := r.gaps[:0]
	for _, g := range r.gaps {
		if g.endNs < cutoff {
			continue
		}
		kept = append(kept, g)
	}
	if len(kept) == 0 {
		r.gaps = nil
		return
	}
	r.gaps = kept
}

// trimAll expires quiet atoms before a page is copied. Inserts already trim the atom they
// touch; a view is the other time a ring that has gone silent is cut back to the window.
func (c *Conn) trimAll(now int64) {
	for id, r := range c.rings {
		c.trimAtom(r, now)
		if len(r.events) == 0 && len(r.gaps) == 0 {
			delete(c.rings, id)
		}
	}
	c.trimBytes()
}

// trimBytes drops the oldest event on the worker until the stored messages fit (D438).
// Oldest is the front of a ring: lowest md_ts, then the earlier insert.
func (c *Conn) trimBytes() {
	for c.tapeBytes > c.tapeMaxBytes {
		id, r := c.oldestRing()
		if r == nil {
			return
		}
		c.dropFront(r)
		if len(r.events) == 0 && len(r.gaps) == 0 {
			delete(c.rings, id)
		}
	}
}

func (c *Conn) oldestRing() (string, *tapeRing) {
	var (
		bestID string
		best   *tapeRing
	)
	for id, r := range c.rings {
		if len(r.events) == 0 {
			continue
		}
		if best == nil || r.events[0].mdTs < best.events[0].mdTs ||
			(r.events[0].mdTs == best.events[0].mdTs && r.events[0].ord < best.events[0].ord) {
			best = r
			bestID = id
		}
	}
	return bestID, best
}

func (c *Conn) dropFront(r *tapeRing) {
	if len(r.events) == 0 {
		return
	}
	n := int64(len(r.events[0].raw))
	r.bytes -= n
	c.tapeBytes -= n
	r.events[0] = tapeEvent{}
	r.events = r.events[1:]
	if len(r.events) == 0 {
		r.events = nil
		r.bytes = 0
	}
	if r.bytes < 0 {
		r.bytes = 0
	}
	if c.tapeBytes < 0 {
		c.tapeBytes = 0
	}
}

// continuousSince is the latest gap end for the feed, otherwise the oldest retained event,
// otherwise 0. From that time forward the retained tape has no gap.
func continuousSince(r *tapeRing, feed string) int64 {
	if r == nil {
		return 0
	}
	var latest int64
	var gap bool
	for _, g := range r.gaps {
		if g.feed != feed {
			continue
		}
		if !gap || g.endNs >= latest {
			latest = g.endNs
			gap = true
		}
	}
	if gap {
		return latest
	}
	for _, e := range r.events {
		if e.feed == feed {
			return e.mdTs
		}
	}
	return 0
}
