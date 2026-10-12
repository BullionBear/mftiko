package conn

import (
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
)

// Tape limits (D438, D439, D449). Zero in InitPayload selects these.
const (
	defaultTapeWindow    = int64(10 * time.Minute)
	defaultTapeMaxEvents = 100_000
	defaultTapeBudget    = 256 << 20
	tapePageMax          = 900 * 1024
	defaultTapeLimit     = 1000
	maxTapeLimit         = 5000
	tapeReadAttempts     = 8
)

const (
	kindEvent uint8 = 1
	kindGap   uint8 = 2
)

// tapeRec is one immutable ring entry. The writer publishes it and never mutates it.
// raw is the md.f message for an event. A gap orders as (epoch, 0) — the new
// conn_epoch — and mdTs is its end_ns, so the time index follows append order.
type tapeRec struct {
	kind               uint8
	epoch, seqNo       uint64
	mdTs               int64
	start, end         int64
	lastEpoch, lastSeq uint64
	hasLast            bool
	raw                []byte
}

// tapeSnap is one published index. Readers load it, then the slots. The writer
// stores a new value; it does not mutate a snap a reader might hold.
type tapeSnap struct {
	tail, written      uint64
	headEpoch, headSeq uint64
	continuous         int64
}

// tapeSlot is a seqlock. seq is (index+1)<<1 while the pointer is stable, and odd
// while the writer replaces it. A mismatch means the slot was overwritten (D450).
type tapeSlot struct {
	seq atomic.Uint64
	ptr atomic.Pointer[tapeRec]
}

// tapeRing is one feed's append-only log. The conn worker is the only writer.
// page reads the atomics below and takes no lock (D450, D451).
type tapeRing struct {
	slots []tapeSlot
	max   int
	pub   atomic.Pointer[tapeSnap]

	// Writer fields. The read path does not touch them.
	tail, written uint64
	events        int
	bytes         int
	gapCount      int
	oldest        int64
	newestGapEnd  int64
	headEpoch     uint64
	headSeq       uint64
	pendingDown   bool
	savedOK       bool
	savedEpoch    uint64
	savedSeq      uint64
}

func newTapeRing(max int) *tapeRing {
	if max < 1 {
		max = 1
	}
	r := &tapeRing{slots: make([]tapeSlot, max), max: max}
	r.publish()
	return r
}

func tapeTopic(topic string) bool {
	switch topic {
	case string(exchange.TopicTrade), string(exchange.TopicAggTrade), string(exchange.TopicLiquidation):
		return true
	default:
		return false
	}
}

func (r *tapeRing) publish() {
	cont := r.oldest
	if r.gapCount > 0 {
		cont = r.newestGapEnd
	}
	if r.events == 0 && r.gapCount == 0 {
		cont = 0
	}
	r.pub.Store(&tapeSnap{
		tail: r.tail, written: r.written,
		headEpoch: r.headEpoch, headSeq: r.headSeq,
		continuous: cont,
	})
}

func (r *tapeRing) write(abs uint64, rec *tapeRec) {
	s := &r.slots[abs%uint64(len(r.slots))]
	s.seq.Store(((abs + 1) << 1) | 1)
	s.ptr.Store(rec)
	s.seq.Store((abs + 1) << 1)
}

func (r *tapeRing) load(abs uint64) (*tapeRec, bool) {
	s := &r.slots[abs%uint64(len(r.slots))]
	s1 := s.seq.Load()
	if s1&1 == 1 {
		return nil, false
	}
	p := s.ptr.Load()
	s2 := s.seq.Load()
	if s1 != s2 || s1>>1 != abs+1 || p == nil {
		return nil, false
	}
	return p, true
}

func (r *tapeRing) loadWriter(abs uint64) *tapeRec {
	return r.slots[abs%uint64(len(r.slots))].ptr.Load()
}

func (r *tapeRing) removeTail() {
	if r.tail >= r.written {
		return
	}
	rec := r.loadWriter(r.tail)
	if rec != nil {
		if rec.kind == kindEvent {
			r.events--
			r.bytes -= len(rec.raw)
		} else {
			r.gapCount--
		}
	}
	r.tail++
	r.noteOldest()
}

func (r *tapeRing) noteOldest() {
	r.oldest = 0
	for i := r.tail; i < r.written; i++ {
		rec := r.loadWriter(i)
		if rec != nil && rec.kind == kindEvent {
			r.oldest = rec.mdTs
			return
		}
	}
}

// trim drops the tail until the event count and the time window both hold.
// The caller publishes the new tail before a slot is reused.
func (r *tapeRing) trim(cutoff int64) {
	for r.tail < r.written {
		rec := r.loadWriter(r.tail)
		if rec == nil {
			r.tail++
			continue
		}
		if r.events <= r.max && rec.mdTs >= cutoff {
			break
		}
		r.removeTail()
	}
}

func (r *tapeRing) ensureCap() {
	capn := uint64(len(r.slots))
	for r.written-r.tail >= capn && r.tail < r.written {
		r.removeTail()
	}
}

func (r *tapeRing) appendEvent(epoch, seq uint64, mdTs int64, raw []byte, windowNs int64) {
	r.trim(windowCutoff(mdTs, windowNs))
	r.ensureCap()
	// An empty ring's head is this epoch, seq 0, including the moment after the
	// last retained event was just dropped (D437).
	r.noteEmpty(epoch)
	r.publish()
	rec := &tapeRec{
		kind: kindEvent, epoch: epoch, seqNo: seq, mdTs: mdTs,
		raw: append([]byte(nil), raw...),
	}
	r.write(r.written, rec)
	r.written++
	r.events++
	r.bytes += len(rec.raw)
	if r.events == 1 {
		r.oldest = mdTs
	}
	r.headEpoch, r.headSeq = epoch, seq
	r.publish()
}

func (r *tapeRing) appendGap(start, end int64, epoch uint64, windowNs int64) {
	r.trim(windowCutoff(end, windowNs))
	r.ensureCap()
	r.noteEmpty(epoch)
	r.publish()
	rec := &tapeRec{
		kind: kindGap, epoch: epoch, seqNo: 0, mdTs: end, start: start, end: end,
		hasLast: r.savedOK, lastEpoch: r.savedEpoch, lastSeq: r.savedSeq,
	}
	r.write(r.written, rec)
	r.written++
	r.gapCount++
	r.newestGapEnd = end
	r.pendingDown = false
	r.publish()
}

func windowCutoff(now, windowNs int64) int64 {
	if windowNs <= 0 || now < windowNs {
		return 0
	}
	return now - windowNs
}

// dropOneEvent removes the oldest event, and any gaps sitting in front of it.
// epoch is the connection's current conn_epoch, used when the ring becomes empty.
func (r *tapeRing) dropOneEvent(epoch uint64) bool {
	if r.events == 0 {
		return false
	}
	for r.tail < r.written && r.events > 0 {
		rec := r.loadWriter(r.tail)
		event := rec != nil && rec.kind == kindEvent
		r.removeTail()
		if event {
			r.noteEmpty(epoch)
			r.publish()
			return true
		}
	}
	r.noteEmpty(epoch)
	r.publish()
	return false
}

func (r *tapeRing) markDown() {
	r.pendingDown = true
	r.savedOK = r.events > 0
	r.savedEpoch, r.savedSeq = r.headEpoch, r.headSeq
}

// noteEmpty sets head to (epoch, 0) when no event is retained (D437).
func (r *tapeRing) noteEmpty(epoch uint64) {
	if r.events > 0 || epoch == 0 {
		return
	}
	r.headEpoch, r.headSeq = epoch, 0
}

func (r *tapeRing) setEmptyHead(epoch uint64) {
	if r.events > 0 {
		return
	}
	r.noteEmpty(epoch)
	r.publish()
}

func cursorLess(epoch, seq, otherEpoch, otherSeq uint64) bool {
	if epoch != otherEpoch {
		return epoch < otherEpoch
	}
	return seq < otherSeq
}

// page builds one tape reply. It takes no locks. before and since_ns locate the
// range by binary search; the walk then reads at most limit events (D451).
func (r *tapeRing) page(before *mdv1.Cursor, since int64, hasSince bool, limit int) []byte {
	if limit <= 0 {
		limit = defaultTapeLimit
	}
	var last *tapeSnap
	for range tapeReadAttempts {
		snap := r.pub.Load()
		last = snap
		out, torn := r.build(snap, before, since, hasSince, limit, false)
		if !torn {
			return out
		}
	}
	out, _ := r.build(last, before, since, hasSince, limit, true)
	return out
}

func (r *tapeRing) build(snap *tapeSnap, before *mdv1.Cursor, since int64, hasSince bool, limit int, shrink bool) ([]byte, bool) {
	if snap == nil || snap.tail >= snap.written {
		return encodeTape(nil, nil, false, nil, snap), false
	}
	hi, found, ok := r.findBefore(snap, before)
	if !ok {
		if shrink {
			return encodeTape(nil, nil, false, nil, snap), false
		}
		return nil, true
	}
	if !found {
		return encodeTape(nil, nil, false, nil, snap), false
	}
	lo, ok := r.findSince(snap, since, hasSince)
	if !ok {
		if shrink {
			return encodeTape(nil, nil, false, nil, snap), false
		}
		return nil, true
	}
	if hi < lo {
		return encodeTape(nil, nil, false, nil, snap), false
	}

	var (
		events    []*tapeRec
		evSize    int
		gaps      []*mdv1.Gap
		oldestRec *tapeRec
		hitSince  bool
		torn      bool
	)
	for i := hi; ; {
		rec, good := r.load(i)
		if !good {
			torn = true
			break
		}
		if rec.kind == kindEvent && hasSince && rec.mdTs < since {
			hitSince = true
			break
		}
		if rec.kind == kindEvent && len(events) >= limit {
			break
		}
		var gap *mdv1.Gap
		if rec.kind == kindGap {
			var gapTorn bool
			gap, gapTorn = r.gapProto(snap, i, rec)
			if gapTorn {
				torn = true
				break
			}
		}
		field := 0
		if rec.kind == kindEvent {
			field = tapeEventFieldSize(rec)
		}
		nextGaps := gaps
		if gap != nil {
			nextGaps = append(append([]*mdv1.Gap{}, gaps...), gap)
		}
		// more is false only when this entry is the last qualifying one.
		// Otherwise the reply also carries next, so the fit check includes it.
		nextCur := &mdv1.Cursor{ConnEpoch: rec.epoch, Seq: rec.seqNo}
		size := pageBytes(evSize+field, nextGaps, true, nextCur, snap)
		if i == lo {
			size = pageBytes(evSize+field, nextGaps, false, nil, snap)
		}
		if size > tapePageMax {
			break
		}
		if rec.kind == kindEvent {
			events = append(events, rec)
			evSize += field
		} else {
			gaps = nextGaps
		}
		oldestRec = rec
		if i == lo {
			break
		}
		i--
	}

	more := false
	var next *mdv1.Cursor
	kept := len(events) + len(gaps)
	if !hitSince && !torn && kept > 0 && oldestRec != nil {
		oldestIdx := hi + 1 - uint64(kept)
		if oldestIdx > lo {
			more = true
			next = &mdv1.Cursor{ConnEpoch: oldestRec.epoch, Seq: oldestRec.seqNo}
		}
	}
	return encodeTape(events, gaps, more, next, snap), torn && !shrink
}

// findBefore is the rightmost entry strictly older than before, or the newest
// entry when before is unset. ok is false when a slot was overwritten.
func (r *tapeRing) findBefore(snap *tapeSnap, before *mdv1.Cursor) (idx uint64, found, ok bool) {
	lo, hi := snap.tail, snap.written
	if lo >= hi {
		return 0, false, true
	}
	if before == nil {
		return hi - 1, true, true
	}
	be, bs := before.GetConnEpoch(), before.GetSeq()
	for lo < hi {
		mid := lo + (hi-lo)/2
		rec, good := r.load(mid)
		if !good {
			return 0, false, false
		}
		if cursorLess(rec.epoch, rec.seqNo, be, bs) {
			found = true
			idx = mid
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return idx, found, true
}

// findSince is the leftmost entry whose time is >= since. The writer clock does
// not go backwards, so time follows the index (D451).
func (r *tapeRing) findSince(snap *tapeSnap, since int64, has bool) (uint64, bool) {
	lo, hi := snap.tail, snap.written
	if !has || lo >= hi {
		return snap.tail, true
	}
	for lo < hi {
		mid := lo + (hi-lo)/2
		rec, good := r.load(mid)
		if !good {
			return 0, false
		}
		if rec.mdTs >= since {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, true
}

func (r *tapeRing) gapProto(snap *tapeSnap, idx uint64, rec *tapeRec) (*mdv1.Gap, bool) {
	g := &mdv1.Gap{StartNs: rec.start, EndNs: rec.end}
	if rec.hasLast {
		g.Last = &mdv1.Cursor{ConnEpoch: rec.lastEpoch, Seq: rec.lastSeq}
	}
	if idx+1 < snap.written {
		next, ok := r.load(idx + 1)
		if !ok {
			return nil, true
		}
		if next != nil && next.kind == kindEvent {
			g.First = &mdv1.Cursor{ConnEpoch: next.epoch, Seq: next.seqNo}
		}
	}
	return g, false
}

func pageBytes(evSize int, gaps []*mdv1.Gap, more bool, next *mdv1.Cursor, snap *tapeSnap) int {
	shell := &mdv1.TapePage{More: more, Gaps: gaps}
	if more {
		shell.Next = next
	}
	if snap != nil {
		shell.ContinuousSinceNs = snap.continuous
		shell.Head = &mdv1.Cursor{ConnEpoch: snap.headEpoch, Seq: snap.headSeq}
	}
	body := proto.Size(shell) + evSize
	return protowire.SizeTag(10) + protowire.SizeVarint(uint64(body)) + body
}

func encodeTape(events []*tapeRec, gaps []*mdv1.Gap, more bool, next *mdv1.Cursor, snap *tapeSnap) []byte {
	shell := &mdv1.TapePage{More: more, Gaps: gaps}
	if more {
		shell.Next = next
	}
	if snap != nil {
		shell.ContinuousSinceNs = snap.continuous
		shell.Head = &mdv1.Cursor{ConnEpoch: snap.headEpoch, Seq: snap.headSeq}
	}
	shellBytes, err := proto.Marshal(shell)
	if err != nil {
		return viewError("invalid")
	}
	var evBuf []byte
	for _, rec := range events {
		evBuf = protowire.AppendTag(evBuf, 1, protowire.BytesType)
		evBuf = protowire.AppendBytes(evBuf, encodeTapeEvent(rec))
	}
	page := make([]byte, 0, len(evBuf)+len(shellBytes))
	page = append(page, evBuf...)
	page = append(page, shellBytes...)
	out := protowire.AppendTag(nil, 10, protowire.BytesType)
	return protowire.AppendBytes(out, page)
}

func payloadOf(raw []byte) []byte {
	if len(raw) < envelope.Size {
		return nil
	}
	return raw[envelope.Size:]
}

func encodeTapeEvent(rec *tapeRec) []byte {
	payload := payloadOf(rec.raw)
	var b []byte
	b = appendVarintField(b, 1, rec.epoch)
	b = appendVarintField(b, 2, rec.seqNo)
	if rec.mdTs != 0 {
		b = appendVarintField(b, 3, uint64(rec.mdTs))
	}
	b = protowire.AppendTag(b, 4, protowire.BytesType)
	return protowire.AppendBytes(b, payload)
}

func tapeEventFieldSize(rec *tapeRec) int {
	payload := payloadOf(rec.raw)
	n := sizeVarintField(1, rec.epoch) + sizeVarintField(2, rec.seqNo)
	if rec.mdTs != 0 {
		n += sizeVarintField(3, uint64(rec.mdTs))
	}
	n += protowire.SizeTag(4) + protowire.SizeVarint(uint64(len(payload))) + len(payload)
	return protowire.SizeTag(1) + protowire.SizeVarint(uint64(n)) + n
}

func appendVarintField(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func sizeVarintField(num protowire.Number, v uint64) int {
	if v == 0 {
		return 0
	}
	return protowire.SizeTag(num) + protowire.SizeVarint(v)
}
