package conn

import (
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/protobuf/encoding/protowire"

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
	// wire is the encoded size of this event as one TapePage.events field:
	// the FeedEvent length plus the protobuf tags around it. The writer fills
	// it once; the page walk only adds the integer (D439).
	wire int
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

// tapeRingPad keeps the slots slice header off the cache line, and off the
// adjacent 128-byte prefetcher pair, that holds pub and the writer index.
const tapeRingPad = 128

// tapeRingAlign is the alignment of a tapeRing allocation.
const tapeRingAlign = 128

// tapeRing is one feed's append-only log. The conn worker is the only writer.
// page reads the atomics below and takes no lock (D450, D451).
//
// slots is the header every load touches. pub, tail, and the rest of the
// writer index move on every append. 128 bytes between them, and a
// 128-aligned base, keep those on different lines. tailPad exists so
// sizeof(tapeRing) % 128 == 8: the arena in newAlignedRing can then always
// place one element on a 128-byte boundary. Add a field and this pad has to
// change with it.
type tapeRing struct {
	slots []tapeSlot
	max   int
	_     [tapeRingPad]byte
	pub   atomic.Pointer[tapeSnap]

	// Writer fields. The read path does not touch them, apart from one load of pub per page.
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
	keep          []tapeRing
	tailPad       [104]byte
}

func newTapeRing(max int) *tapeRing {
	if max < 1 {
		max = 1
	}
	r := newAlignedRing()
	r.slots = make([]tapeSlot, max)
	r.max = max
	r.publish()
	return r
}

// newAlignedRing returns a tapeRing at a 128-byte-aligned address. The ring
// lives inside keep, which is what the GC scans, so the slice header and pub
// stay real pointers.
func newAlignedRing() *tapeRing {
	buf := make([]tapeRing, 16)
	for i := range buf {
		if uintptr(unsafe.Pointer(&buf[i]))%tapeRingAlign == 0 {
			r := &buf[i]
			r.keep = buf
			return r
		}
	}
	buf[0].keep = buf
	return &buf[0]
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
	rec.wire = tapeEventFieldSize(rec)
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

// pageCut is the range a page will encode. stop is the oldest kept index.
// Sizes are protobuf bytes, summed from the wire length stored at append.
type pageCut struct {
	hi, stop              uint64
	kept, evSize, gapSize int
	nextEpoch, nextSeq    uint64
	more, hitSince, torn  bool
}

func (r *tapeRing) build(snap *tapeSnap, before *mdv1.Cursor, since int64, hasSince bool, limit int, shrink bool) ([]byte, bool) {
	if snap == nil || snap.tail >= snap.written {
		return r.finishPage(snap, pageCut{}, shrink)
	}
	hi, found, ok := r.findBefore(snap, before)
	if !ok {
		return r.finishPage(snap, pageCut{torn: true}, shrink)
	}
	if !found {
		return r.finishPage(snap, pageCut{}, shrink)
	}
	lo, ok := r.findSince(snap, since, hasSince)
	if !ok {
		return r.finishPage(snap, pageCut{torn: true}, shrink)
	}
	if hi < lo {
		return r.finishPage(snap, pageCut{}, shrink)
	}
	return r.finishPage(snap, r.measure(snap, hi, lo, since, hasSince, limit), shrink)
}

// measure walks newest to oldest and sums stored event sizes. It allocates nothing.
func (r *tapeRing) measure(snap *tapeSnap, hi, lo uint64, since int64, hasSince bool, limit int) pageCut {
	cut := pageCut{hi: hi}
	evSize, gapSize, events, kept := 0, 0, 0, 0
	var nextEpoch, nextSeq, stop uint64
	hitSince, torn := false, false
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
		if rec.kind == kindEvent && events >= limit {
			break
		}
		field, addGap := 0, 0
		if rec.kind == kindEvent {
			field = rec.wire
		} else {
			var ok bool
			addGap, ok = r.gapEncodedSize(snap, i, rec)
			if !ok {
				torn = true
				break
			}
		}
		// Stopping here publishes next, unless this entry is the last one that qualifies.
		size := replySize(evSize+field, gapSize+addGap, i != lo, rec.epoch, rec.seqNo, snap)
		if size > tapePageMax {
			break
		}
		evSize += field
		gapSize += addGap
		if rec.kind == kindEvent {
			events++
		}
		kept++
		nextEpoch, nextSeq = rec.epoch, rec.seqNo
		stop = i
		if i == lo {
			break
		}
		i--
	}
	cut.stop = stop
	cut.kept = kept
	cut.evSize = evSize
	cut.gapSize = gapSize
	cut.nextEpoch, cut.nextSeq = nextEpoch, nextSeq
	cut.hitSince = hitSince
	cut.torn = torn
	if !hitSince && !torn && kept > 0 && stop > lo {
		cut.more = true
	}
	return cut
}

func (r *tapeRing) finishPage(snap *tapeSnap, cut pageCut, shrink bool) ([]byte, bool) {
	if cut.torn && !shrink {
		return nil, true
	}
	out, torn := r.writePage(snap, cut)
	if torn {
		if !shrink {
			return nil, true
		}
		out, _ = r.writePage(snap, pageCut{})
	}
	return out, false
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

// writePage encodes the cut into one buffer. torn means a slot moved and the
// bytes were discarded. The only allocation is the reply itself.
func (r *tapeRing) writePage(snap *tapeSnap, cut pageCut) ([]byte, bool) {
	epoch, seq := uint64(0), uint64(0)
	if cut.more {
		epoch, seq = cut.nextEpoch, cut.nextSeq
	}
	body := cut.evSize + shellSize(cut.gapSize, cut.more, epoch, seq, snap)
	total := protowire.SizeTag(10) + protowire.SizeVarint(uint64(body)) + body
	buf := make([]byte, 0, total)
	buf = protowire.AppendTag(buf, 10, protowire.BytesType)
	buf = protowire.AppendVarint(buf, uint64(body))
	if cut.kept > 0 {
		var torn bool
		buf, torn = r.appendEvents(buf, cut.hi, cut.stop)
		if torn {
			return nil, true
		}
	}
	buf = appendShellPrefix(buf, cut.more, epoch, seq, snap)
	if cut.gapSize > 0 {
		var torn bool
		buf, torn = r.appendGaps(buf, snap, cut.hi, cut.stop)
		if torn {
			return nil, true
		}
	}
	buf = appendHead(buf, snap)
	if len(buf) != total {
		return nil, true
	}
	return buf, false
}

func (r *tapeRing) appendEvents(buf []byte, hi, stop uint64) ([]byte, bool) {
	for i := hi; ; {
		rec, ok := r.load(i)
		if !ok {
			return nil, true
		}
		if rec.kind == kindEvent {
			buf = appendTapeEvent(buf, rec)
		}
		if i == stop {
			return buf, false
		}
		i--
	}
}

func (r *tapeRing) appendGaps(buf []byte, snap *tapeSnap, hi, stop uint64) ([]byte, bool) {
	for i := hi; ; {
		rec, ok := r.load(i)
		if !ok {
			return nil, true
		}
		if rec.kind == kindGap {
			has, epoch, seq, good := r.gapFirst(snap, i)
			if !good {
				return nil, true
			}
			buf = appendGap(buf, rec, has, epoch, seq)
		}
		if i == stop {
			return buf, false
		}
		i--
	}
}

func (r *tapeRing) gapEncodedSize(snap *tapeSnap, idx uint64, rec *tapeRec) (int, bool) {
	has, epoch, seq, ok := r.gapFirst(snap, idx)
	if !ok {
		return 0, false
	}
	return gapFieldSize(rec, has, epoch, seq), true
}

// gapFirst is the event published just after the gap, when that slot is still there.
func (r *tapeRing) gapFirst(snap *tapeSnap, idx uint64) (has bool, epoch, seq uint64, ok bool) {
	if idx+1 >= snap.written {
		return false, 0, 0, true
	}
	next, good := r.load(idx + 1)
	if !good {
		return false, 0, 0, false
	}
	if next != nil && next.kind == kindEvent {
		return true, next.epoch, next.seqNo, true
	}
	return false, 0, 0, true
}

func payloadOf(raw []byte) []byte {
	if len(raw) < envelope.Size {
		return nil
	}
	return raw[envelope.Size:]
}

func encodeTapeEvent(rec *tapeRec) []byte {
	return appendTapeEventInner(nil, rec)
}

func appendTapeEvent(b []byte, rec *tapeRec) []byte {
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(tapeEventInnerSize(rec)))
	return appendTapeEventInner(b, rec)
}

func appendTapeEventInner(b []byte, rec *tapeRec) []byte {
	b = appendVarintField(b, 1, rec.epoch)
	b = appendVarintField(b, 2, rec.seqNo)
	b = appendInt64Field(b, 3, rec.mdTs)
	payload := payloadOf(rec.raw)
	b = protowire.AppendTag(b, 4, protowire.BytesType)
	return protowire.AppendBytes(b, payload)
}

func tapeEventFieldSize(rec *tapeRec) int {
	n := tapeEventInnerSize(rec)
	return protowire.SizeTag(1) + protowire.SizeVarint(uint64(n)) + n
}

func tapeEventInnerSize(rec *tapeRec) int {
	payload := payloadOf(rec.raw)
	n := sizeVarintField(1, rec.epoch) + sizeVarintField(2, rec.seqNo) + sizeInt64Field(3, rec.mdTs)
	n += protowire.SizeTag(4) + protowire.SizeVarint(uint64(len(payload))) + len(payload)
	return n
}

func appendGap(b []byte, rec *tapeRec, hasFirst bool, epoch, seq uint64) []byte {
	b = protowire.AppendTag(b, 5, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(gapInnerSize(rec, hasFirst, epoch, seq)))
	b = appendInt64Field(b, 1, rec.start)
	b = appendInt64Field(b, 2, rec.end)
	if rec.hasLast {
		b = appendCursor(b, 3, rec.lastEpoch, rec.lastSeq)
	}
	if hasFirst {
		b = appendCursor(b, 4, epoch, seq)
	}
	return b
}

func gapFieldSize(rec *tapeRec, hasFirst bool, epoch, seq uint64) int {
	n := gapInnerSize(rec, hasFirst, epoch, seq)
	return protowire.SizeTag(5) + protowire.SizeVarint(uint64(n)) + n
}

func gapInnerSize(rec *tapeRec, hasFirst bool, epoch, seq uint64) int {
	n := sizeInt64Field(1, rec.start) + sizeInt64Field(2, rec.end)
	if rec.hasLast {
		n += cursorFieldSize(3, rec.lastEpoch, rec.lastSeq)
	}
	if hasFirst {
		n += cursorFieldSize(4, epoch, seq)
	}
	return n
}

func appendShellPrefix(b []byte, more bool, epoch, seq uint64, snap *tapeSnap) []byte {
	if more {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
		b = appendCursor(b, 3, epoch, seq)
	}
	if snap != nil {
		b = appendInt64Field(b, 4, snap.continuous)
	}
	return b
}

func appendHead(b []byte, snap *tapeSnap) []byte {
	if snap == nil {
		return b
	}
	return appendCursor(b, 6, snap.headEpoch, snap.headSeq)
}

func appendCursor(b []byte, num protowire.Number, epoch, seq uint64) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(cursorInnerSize(epoch, seq)))
	b = appendVarintField(b, 1, epoch)
	return appendVarintField(b, 2, seq)
}

func shellSize(gapSize int, more bool, epoch, seq uint64, snap *tapeSnap) int {
	return shellPrefixSize(more, epoch, seq, snap) + gapSize + headSize(snap)
}

func shellPrefixSize(more bool, epoch, seq uint64, snap *tapeSnap) int {
	n := 0
	if more {
		n += protowire.SizeTag(2) + 1
		n += cursorFieldSize(3, epoch, seq)
	}
	if snap != nil {
		n += sizeInt64Field(4, snap.continuous)
	}
	return n
}

func headSize(snap *tapeSnap) int {
	if snap == nil {
		return 0
	}
	return cursorFieldSize(6, snap.headEpoch, snap.headSeq)
}

func replySize(evSize, gapSize int, more bool, epoch, seq uint64, snap *tapeSnap) int {
	body := evSize + shellSize(gapSize, more, epoch, seq, snap)
	return protowire.SizeTag(10) + protowire.SizeVarint(uint64(body)) + body
}

func cursorFieldSize(num protowire.Number, epoch, seq uint64) int {
	n := cursorInnerSize(epoch, seq)
	return protowire.SizeTag(num) + protowire.SizeVarint(uint64(n)) + n
}

func cursorInnerSize(epoch, seq uint64) int {
	return sizeVarintField(1, epoch) + sizeVarintField(2, seq)
}

func appendInt64Field(b []byte, num protowire.Number, v int64) []byte {
	if v == 0 {
		return b
	}
	return appendVarintField(b, num, uint64(v))
}

func sizeInt64Field(num protowire.Number, v int64) int {
	if v == 0 {
		return 0
	}
	return sizeVarintField(num, uint64(v))
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
