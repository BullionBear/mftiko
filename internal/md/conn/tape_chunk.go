package conn

import (
	"sync/atomic"
	"unsafe"
)

// Chunk geometry (D458). A chunk is one 64 KiB object: a scanned header plus
// the bytes events and slots share. The size is not configurable. An event
// larger than tapeEventMax is not stored; the ring records a gap instead.
const (
	tapeChunkSize   = 64 << 10
	tapeChunkHeader = 128
	tapeEventMax    = 32 << 10
	tapeSlotSize    = 72 // 9 little-endian uint64 words, in-memory only
)

// Compile-time: the header is exactly one cache-line pair and the chunk
// object is exactly 64 KiB, so the allocator does not round it up.
// A duplicate map key means the sizes diverged.
var _ = map[bool]struct{}{
	false: {},
	unsafe.Sizeof(tapeChunkHead{}) == tapeChunkHeader && unsafe.Sizeof(tapeChunk{}) == tapeChunkSize: {},
}

// tapeChunkHead is the scanned prefix of a chunk. gen, base, count and seq
// are read by the lock-free path and so are atomic. The writer is the only
// goroutine that touches the rest. 128 bytes, verified above.
type tapeChunkHead struct {
	gen   atomic.Uint64
	base  atomic.Uint64
	count atomic.Uint32
	_     uint32

	newest int64
	oldest int64
	dir    int32
	pay    int32
	heap   int32
	_      int32
	seq    atomic.Uint64
	ring   *tapeRing
	_      [56]byte
}

// tapeChunk is one fixed block. Readers copy out of buf with atomic loads and
// then re-read gen. A mismatch means the block was reclaimed (D459).
type tapeChunk struct {
	tapeChunkHead
	buf [tapeChunkSize - tapeChunkHeader]byte
}

// tapeArena is the conn worker's tape memory. used counts every chunk and
// every ring index it has allocated. Chunks go back to free, not to the
// garbage collector, so the cap does not depend on a collection.
type tapeArena struct {
	budget int64
	used   int64
	free   []*tapeChunk
	heap   []*tapeChunk
}

func newTapeArena(budget int64) *tapeArena {
	if budget < 0 {
		budget = 0
	}
	return &tapeArena{budget: budget}
}

// chunkPointerLen is the fixed ring of chunk pointers (D458). One record
// never spans a chunk, so the event cap is the longest the ring could need.
// The budget cannot fund more chunks than budget/chunk, and that bound is
// what keeps an idle ring near a chunk instead of a 100000-slot table.
func chunkPointerLen(maxEvents int, budget int64) int {
	if maxEvents < 1 {
		maxEvents = 1
	}
	sz := int64(unsafe.Sizeof(tapeChunk{}))
	n := budget / sz
	if n < 1 {
		n = 1
	}
	if n > int64(maxEvents) {
		n = int64(maxEvents)
	}
	return int(n)
}

func (a *tapeArena) newRing(max int, key string) *tapeRing {
	if max < 1 {
		max = 1
	}
	r := newAlignedRing()
	r.max = max
	r.key = key
	r.arena = a
	keep := int64(len(r.keep)) * int64(unsafe.Sizeof(tapeRing{}))
	// The alignment slab is already allocated. Count it even when it does
	// not fit, and do not spend a chunk on top of a blown budget.
	a.used += keep
	r.held = keep
	n := chunkPointerLen(max, a.budget)
	idx := int64(n) * int64(unsafe.Sizeof(atomic.Pointer[tapeChunk]{}))
	if a.reserve(idx) {
		r.slots = make([]atomic.Pointer[tapeChunk], n)
		r.held += idx
	}
	r.publish()
	return r
}

// reserve charges n bytes of index memory. A reclaimed chunk stays on the
// free list, so used does not shrink and an index that does not fit is
// refused. The ring then has no chunk pointers and cannot store.
func (a *tapeArena) reserve(n int64) bool {
	if n <= 0 {
		return true
	}
	if a.used+n > a.budget {
		return false
	}
	a.used += n
	return true
}

// release drops a ring's chunks back onto the free list and forgets its
// index bytes. The pointer array stays reachable until the ring is collected,
// so an in-flight reader can still retry; chunks themselves are not collected.
func (a *tapeArena) release(r *tapeRing) {
	if r == nil {
		return
	}
	r.tail = r.written
	r.events = 0
	r.gapCount = 0
	r.bytes = 0
	r.oldest = 0
	r.noteEmpty(r.connEpoch)
	r.publish()
	if len(r.slots) > 0 {
		for seq := r.chunkTail; seq < r.chunkWritten; seq++ {
			ch := r.slots[seq%uint64(len(r.slots))].Load()
			if ch == nil {
				continue
			}
			if r.open == ch {
				r.open = nil
			}
			a.retire(ch)
		}
	}
	r.chunkTail = r.chunkWritten
	r.open = nil
	a.used -= r.held
	if a.used < 0 {
		a.used = 0
	}
	r.held = 0
}

func (a *tapeArena) allocChunk() *tapeChunk {
	sz := int64(unsafe.Sizeof(tapeChunk{}))
	for {
		if ch := a.popFree(); ch != nil {
			return ch
		}
		if a.used+sz <= a.budget {
			a.used += sz
			ch := new(tapeChunk)
			ch.heap = -1
			ch.gen.Store(1) // odd until installed
			return ch
		}
		if !a.reclaimOldest() {
			return nil
		}
	}
}

func (a *tapeArena) popFree() *tapeChunk {
	n := len(a.free)
	if n == 0 {
		return nil
	}
	ch := a.free[n-1]
	a.free[n-1] = nil
	a.free = a.free[:n-1]
	return ch
}

// reclaimOldest returns the sealed chunk whose newest event is the oldest
// to the free list. Only a ring's oldest chunk is dropped, so the log does
// not grow a hole. The open chunk stays out of the heap: each ring keeps
// one chunk (D458). False means nothing was freed.
func (a *tapeArena) reclaimOldest() bool {
	for len(a.heap) > 0 {
		ch := a.heap[0]
		if ch == nil || ch.ring == nil || len(ch.ring.slots) == 0 || ch.ring.chunkTail >= ch.ring.chunkWritten {
			a.heapPop()
			continue
		}
		ring := ch.ring
		oldest := ring.slots[ring.chunkTail%uint64(len(ring.slots))].Load()
		if oldest == nil {
			ring.chunkTail++
			continue
		}
		// Not this ring's prefix. Dropping it would open a hole.
		if oldest != ch || oldest == ring.open {
			a.heapRemove(ch)
			continue
		}
		ring.dropChunk(ch)
		return true
	}
	return false
}

func (a *tapeArena) seal(ch *tapeChunk) {
	if ch == nil || ch.heap >= 0 || ch.count.Load() == 0 {
		return
	}
	a.heapPush(ch)
}

func (a *tapeArena) retire(ch *tapeChunk) {
	if ch == nil {
		return
	}
	a.heapRemove(ch)
	g := ch.gen.Load()
	if g&1 == 0 {
		ch.gen.Store(g + 1)
	}
	ch.ring = nil
	a.free = append(a.free, ch)
}

func (a *tapeArena) heapPush(ch *tapeChunk) {
	ch.heap = int32(len(a.heap))
	a.heap = append(a.heap, ch)
	a.siftUp(int(ch.heap))
}

func (a *tapeArena) heapPop() *tapeChunk {
	if len(a.heap) == 0 {
		return nil
	}
	ch := a.heap[0]
	a.heapRemoveAt(0)
	return ch
}

func (a *tapeArena) heapRemove(ch *tapeChunk) {
	i := int(ch.heap)
	if i < 0 || i >= len(a.heap) || a.heap[i] != ch {
		ch.heap = -1
		return
	}
	a.heapRemoveAt(i)
}

func (a *tapeArena) heapRemoveAt(i int) {
	n := len(a.heap) - 1
	if i < n {
		a.heapSwap(i, n)
	}
	removed := a.heap[n]
	removed.heap = -1
	a.heap[n] = nil
	a.heap = a.heap[:n]
	if i < len(a.heap) {
		a.siftDown(i)
		a.siftUp(i)
	}
}

func (a *tapeArena) heapSwap(i, j int) {
	a.heap[i], a.heap[j] = a.heap[j], a.heap[i]
	a.heap[i].heap = int32(i)
	a.heap[j].heap = int32(j)
}

func (a *tapeArena) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !chunkBefore(a.heap[i], a.heap[p]) {
			return
		}
		a.heapSwap(i, p)
		i = p
	}
}

func (a *tapeArena) siftDown(i int) {
	n := len(a.heap)
	for {
		l := i*2 + 1
		if l >= n {
			return
		}
		s := l
		if r := l + 1; r < n && chunkBefore(a.heap[r], a.heap[l]) {
			s = r
		}
		if !chunkBefore(a.heap[s], a.heap[i]) {
			return
		}
		a.heapSwap(i, s)
		i = s
	}
}

// chunkBefore orders sealed chunks by the time of the last record, then by
// feed key, then by chunk sequence. The minimum is the oldest chunk.
func chunkBefore(a, b *tapeChunk) bool {
	if a.newest != b.newest {
		return a.newest < b.newest
	}
	ak, bk := "", ""
	if a.ring != nil {
		ak = a.ring.key
	}
	if b.ring != nil {
		bk = b.ring.key
	}
	if ak != bk {
		return ak < bk
	}
	return a.seq.Load() < b.seq.Load()
}

func fitBytes(payload int) int {
	if payload < 0 {
		payload = 0
	}
	pad := (8 - payload%8) % 8
	return tapeSlotSize + payload + pad
}

func (ch *tapeChunk) canFit(payload int) bool {
	if payload < 0 || ch == nil {
		return false
	}
	return int(ch.pay)-int(ch.dir) >= fitBytes(payload)
}

func (ch *tapeChunk) appendSlot(rec tapeRec, raw []byte) bool {
	if !ch.canFit(len(raw)) {
		return false
	}
	padded := (len(raw) + 7) &^ 7
	off := int(ch.pay) - padded
	storeBytes(ch.buf[:], off, raw)
	base := int(ch.dir)
	var word [9]uint64
	flags := uint64(0)
	if rec.hasLast {
		flags = 1
	}
	word[0] = uint64(rec.kind) | flags<<8 | uint64(uint32(rec.wire))<<32
	word[1] = uint64(uint32(off)) | uint64(uint32(len(raw)))<<32
	word[2] = rec.epoch
	word[3] = rec.seqNo
	word[4] = uint64(rec.mdTs)
	word[5] = uint64(rec.start)
	word[6] = uint64(rec.end)
	word[7] = rec.lastEpoch
	word[8] = rec.lastSeq
	for i := range word {
		atomic.StoreUint64(u64at(ch.buf[:], base+i*8), word[i])
	}
	ch.dir = int32(base + tapeSlotSize)
	ch.pay = int32(off)
	if ch.count.Load() == 0 {
		ch.oldest = rec.mdTs
	}
	ch.newest = rec.mdTs
	ch.count.Store(ch.count.Load() + 1)
	return true
}

func (r *tapeRing) appendEvent(epoch, seq uint64, mdTs int64, raw []byte, windowNs int64) {
	r.connEpoch = epoch
	r.trim(windowCutoff(mdTs, windowNs))
	if len(raw) > tapeEventMax {
		r.appendOversize(epoch, seq, mdTs)
		return
	}
	rec := tapeRec{kind: kindEvent, epoch: epoch, seqNo: seq, mdTs: mdTs, raw: raw}
	rec.wire = tapeEventFieldSize(&rec)
	rec.raw = nil
	if !r.commit(rec, raw, true) {
		r.finishTrim()
	}
}

// appendOversize records a gap in place of an event the chunk will not hold.
// The gap takes that event's cursor, so before/since stay aligned with md.f.
// head moves with it: tape has accounted for the publish (D459).
func (r *tapeRing) appendOversize(epoch, seq uint64, mdTs int64) {
	rec := tapeRec{
		kind: kindGap, epoch: epoch, seqNo: seq, mdTs: mdTs,
		start: mdTs, end: mdTs,
	}
	if r.events > 0 {
		rec.hasLast = true
		rec.lastEpoch, rec.lastSeq = r.headEpoch, r.headSeq
	}
	if !r.commit(rec, nil, true) {
		r.finishTrim()
	}
}

func (r *tapeRing) appendGap(start, end int64, epoch uint64, windowNs int64) {
	r.connEpoch = epoch
	r.trim(windowCutoff(end, windowNs))
	r.noteEmpty(epoch)
	rec := tapeRec{
		kind: kindGap, epoch: epoch, seqNo: 0, mdTs: end,
		start: start, end: end,
		hasLast: r.savedOK, lastEpoch: r.savedEpoch, lastSeq: r.savedSeq,
	}
	if !r.commit(rec, nil, false) {
		r.finishTrim()
		return
	}
	r.pendingDown = false
}

// commit appends one record. advanceHead is set for a real event and for an
// oversize gap, and clear for a reconnect gap (D440): a reconnect does not
// move head until the next event.
func (r *tapeRing) commit(rec tapeRec, raw []byte, advanceHead bool) bool {
	if !r.ensure(len(raw)) || r.open == nil || !r.open.appendSlot(rec, raw) {
		return false
	}
	if rec.kind == kindEvent {
		r.events++
		r.bytes += len(raw)
		if r.events == 1 {
			r.oldest = rec.mdTs
		}
		r.headEpoch, r.headSeq = rec.epoch, rec.seqNo
	} else {
		r.gapCount++
		r.newestGapEnd = rec.end
		if advanceHead {
			r.headEpoch, r.headSeq = rec.epoch, rec.seqNo
		}
	}
	r.written++
	r.publish()
	return true
}

func (r *tapeRing) finishTrim() {
	r.noteOldest()
	r.noteEmpty(r.connEpoch)
	r.publish()
}

// trim applies the per-feed window and count from the oldest record. Whole
// chunks return to the arena only once every record in them is gone.
func (r *tapeRing) trim(cutoff int64) {
	changed := false
	for r.tail < r.written {
		rec, _, ok := r.peek(r.tail)
		if !ok {
			r.tail++
			changed = true
			continue
		}
		// The record being appended still has to fit, so a full ring
		// sheds one before it. Gaps do not count toward max (D449).
		if r.events < r.max && rec.mdTs >= cutoff {
			break
		}
		r.removeOne()
		changed = true
	}
	if !changed {
		return
	}
	r.noteOldest()
	r.noteEmpty(r.connEpoch)
	if r.chunkExpired() {
		// Publish the new tail before a chunk is reused (D450).
		r.publish()
		r.collectChunks()
	}
}

func (r *tapeRing) removeOne() {
	rec, n, ok := r.peek(r.tail)
	if ok {
		if rec.kind == kindEvent {
			r.events--
			r.bytes -= int(n)
			if r.events < 0 {
				r.events = 0
			}
			if r.bytes < 0 {
				r.bytes = 0
			}
		} else if r.gapCount > 0 {
			r.gapCount--
		}
	}
	if r.tail < r.written {
		r.tail++
	}
}

func (r *tapeRing) noteOldest() {
	r.oldest = 0
	for i := r.tail; i < r.written; i++ {
		rec, _, ok := r.peek(i)
		if ok && rec.kind == kindEvent {
			r.oldest = rec.mdTs
			return
		}
	}
}

func (r *tapeRing) chunkExpired() bool {
	if len(r.slots) == 0 || r.chunkTail >= r.chunkWritten {
		return false
	}
	ch := r.slots[r.chunkTail%uint64(len(r.slots))].Load()
	if ch == nil {
		return true
	}
	end := ch.base.Load() + uint64(ch.count.Load())
	return r.tail >= end
}

func (r *tapeRing) collectChunks() {
	for r.chunkTail < r.chunkWritten && len(r.slots) > 0 {
		ch := r.slots[r.chunkTail%uint64(len(r.slots))].Load()
		if ch == nil {
			r.chunkTail++
			continue
		}
		end := ch.base.Load() + uint64(ch.count.Load())
		if r.tail < end {
			return
		}
		if ch == r.open {
			r.reopen(ch, r.written)
			return
		}
		r.chunkTail++
		r.arena.retire(ch)
	}
}

func (r *tapeRing) openHasLive() bool {
	if r.open == nil || r.tail >= r.written {
		return false
	}
	end := r.open.base.Load() + uint64(r.open.count.Load())
	return r.tail < end
}

func (r *tapeRing) ensure(payload int) bool {
	if r.open != nil && r.open.canFit(payload) {
		return true
	}
	if r.tryNewChunk() && r.open != nil && r.open.canFit(payload) {
		return true
	}
	return r.open != nil && r.compactFor(payload) && r.open.canFit(payload)
}

func (r *tapeRing) tryNewChunk() bool {
	if r.arena == nil || len(r.slots) == 0 {
		return false
	}
	if r.open != nil && !r.openHasLive() {
		r.reopen(r.open, r.written)
		return true
	}
	if r.chunkWritten-r.chunkTail >= uint64(len(r.slots)) {
		// The only chunk is the open one. Another feed cannot take it,
		// and dropping it here would throw away the floor. compactFor
		// rewrites it instead.
		if r.chunkWritten-r.chunkTail <= 1 {
			return false
		}
		if !r.dropOldestSlot() {
			return false
		}
	}
	ch := r.arena.allocChunk()
	if ch == nil {
		return false
	}
	if r.open != nil && r.openHasLive() {
		r.arena.seal(r.open)
	}
	r.install(ch)
	return true
}

func (r *tapeRing) dropOldestSlot() bool {
	if len(r.slots) == 0 || r.chunkTail >= r.chunkWritten {
		return false
	}
	ch := r.slots[r.chunkTail%uint64(len(r.slots))].Load()
	if ch == nil {
		r.chunkTail++
		return true
	}
	r.dropChunk(ch)
	return true
}

// dropChunk discards every record in ch, publishes the new tail, then parks
// the chunk. ch is the oldest chunk of this ring.
func (r *tapeRing) dropChunk(ch *tapeChunk) {
	if ch == nil {
		return
	}
	end := ch.base.Load() + uint64(ch.count.Load())
	r.discardThrough(end)
	if len(r.slots) > 0 && r.chunkTail < r.chunkWritten {
		cur := r.slots[r.chunkTail%uint64(len(r.slots))].Load()
		if cur == ch {
			r.chunkTail++
		}
	}
	if r.open == ch {
		r.open = nil
	}
	r.noteOldest()
	r.noteEmpty(r.connEpoch)
	r.publish()
	if r.arena != nil {
		r.arena.retire(ch)
	}
}

func (r *tapeRing) discardThrough(end uint64) {
	if end > r.written {
		end = r.written
	}
	for r.tail < end {
		r.removeOne()
	}
}

func (r *tapeRing) install(ch *tapeChunk) {
	r.prep(ch, r.written, r.chunkWritten)
	r.slots[r.chunkWritten%uint64(len(r.slots))].Store(ch)
	r.chunkWritten++
	r.open = ch
}

func (r *tapeRing) prep(ch *tapeChunk, base, seq uint64) {
	g := ch.gen.Load()
	if g&1 == 0 {
		ch.gen.Store(g + 1)
		g++
	}
	ch.ring = r
	ch.base.Store(base)
	ch.count.Store(0)
	ch.seq.Store(seq)
	ch.dir = 0
	ch.pay = int32(len(ch.buf))
	ch.newest = 0
	ch.oldest = 0
	ch.heap = -1
	ch.gen.Store(g + 1)
}

func (r *tapeRing) reopen(ch *tapeChunk, base uint64) {
	r.prep(ch, base, ch.seq.Load())
	r.open = ch
}

// compactFor rewrites the open chunk so payload fits, keeping the newest
// suffix. Used when every other ring is already down to its last chunk and
// the arena cannot hand out another block.
func (r *tapeRing) compactFor(payload int) bool {
	ch := r.open
	if ch == nil || r.arena == nil {
		return false
	}
	need := fitBytes(payload)
	if need > len(ch.buf) {
		return false
	}
	if ch.canFit(payload) {
		return true
	}
	room := len(ch.buf) - need
	base := ch.base.Load()
	cnt := ch.count.Load()
	gen := ch.gen.Load()
	if gen&1 == 1 {
		return false
	}
	liveLo := r.tail
	if liveLo < base {
		liveLo = base
	}
	liveHi := base + uint64(cnt)
	if liveHi > r.written {
		liveHi = r.written
	}
	// The open chunk is the newest one, so its records run up to written.
	if liveHi != r.written {
		return false
	}
	keep := 0
	used := 0
	for abs := liveHi; abs > liveLo; abs-- {
		_, _, n, ok := readSlot(ch, gen, uint32(abs-1-base))
		if !ok {
			return false
		}
		sz := fitBytes(int(n))
		if used+sz > room {
			break
		}
		used += sz
		keep++
	}
	first := liveHi - uint64(keep)
	type keptRec struct {
		rec tapeRec
		raw []byte
	}
	kept := make([]keptRec, 0, keep)
	for abs := first; abs < liveHi; abs++ {
		rec, off, n, ok := readSlot(ch, gen, uint32(abs-base))
		if !ok {
			return false
		}
		var raw []byte
		if n > 0 {
			raw = make([]byte, n)
			copyAtomic(raw, ch.buf[:], int(off))
			if ch.gen.Load() != gen {
				return false
			}
		}
		kept = append(kept, keptRec{rec: rec, raw: raw})
	}
	r.discardThrough(first)
	r.noteOldest()
	r.noteEmpty(r.connEpoch)
	// Publish the trimmed tail before the bytes are rewritten (D450).
	r.publish()
	g := ch.gen.Load()
	if g&1 == 0 {
		ch.gen.Store(g + 1)
		g++
	}
	ch.base.Store(first)
	ch.count.Store(0)
	ch.dir = 0
	ch.pay = int32(len(ch.buf))
	ch.newest = 0
	ch.oldest = 0
	ch.ring = r
	for _, item := range kept {
		if !ch.appendSlot(item.rec, item.raw) {
			ch.gen.Store(g + 1)
			return false
		}
	}
	ch.gen.Store(g + 1)
	return ch.canFit(payload)
}

func (r *tapeRing) peek(abs uint64) (tapeRec, uint32, bool) {
	if len(r.slots) == 0 || abs < r.tail || abs >= r.written {
		return tapeRec{}, 0, false
	}
	lo, hi := r.chunkTail, r.chunkWritten
	for lo < hi {
		mid := lo + (hi-lo)/2
		ch := r.slots[mid%uint64(len(r.slots))].Load()
		if ch == nil {
			return tapeRec{}, 0, false
		}
		base := ch.base.Load()
		cnt := uint64(ch.count.Load())
		if abs < base {
			hi = mid
			continue
		}
		if abs >= base+cnt {
			lo = mid + 1
			continue
		}
		gen := ch.gen.Load()
		rec, _, n, ok := readSlot(ch, gen, uint32(abs-base))
		return rec, n, ok
	}
	return tapeRec{}, 0, false
}

// loc is a chunk a reader has checked against gen.
type loc struct {
	ch    *tapeChunk
	gen   uint64
	base  uint64
	count uint32
}

func (l loc) covers(abs uint64) bool {
	return l.ch != nil && l.gen&1 == 0 && abs >= l.base && abs < l.base+uint64(l.count) && l.ch.gen.Load() == l.gen
}

// slotMeta is one record copied off a chunk. raw is not copied; off/n locate
// the bytes for the page writer.
type slotMeta struct {
	rec    tapeRec
	ch     *tapeChunk
	gen    uint64
	off, n uint32
}

func (r *tapeRing) metaAt(snap *tapeSnap, abs uint64, hint *loc) (slotMeta, loc, bool) {
	if snap == nil || abs < snap.tail || abs >= snap.written {
		return slotMeta{}, loc{}, false
	}
	var l loc
	var ok bool
	if hint != nil && hint.covers(abs) {
		l = *hint
		ok = true
	} else {
		l, ok = r.locate(snap, abs)
	}
	if !ok {
		return slotMeta{}, loc{}, false
	}
	rec, off, n, good := readSlot(l.ch, l.gen, uint32(abs-l.base))
	if !good {
		return slotMeta{}, loc{}, false
	}
	return slotMeta{rec: rec, ch: l.ch, gen: l.gen, off: off, n: n}, l, true
}

func (r *tapeRing) locate(snap *tapeSnap, abs uint64) (loc, bool) {
	if len(r.slots) == 0 || snap.chunkTail >= snap.chunkWritten {
		return loc{}, false
	}
	lo, hi := snap.chunkTail, snap.chunkWritten
	for lo+1 < hi {
		mid := lo + (hi-lo)/2
		_, _, base, _, ok := r.chunkMeta(mid)
		if !ok {
			return loc{}, false
		}
		if abs < base {
			hi = mid
		} else {
			lo = mid
		}
	}
	ch, gen, base, cnt, ok := r.chunkMeta(lo)
	if !ok || abs < base || abs >= base+uint64(cnt) {
		return loc{}, false
	}
	return loc{ch: ch, gen: gen, base: base, count: cnt}, true
}

func (r *tapeRing) chunkMeta(seq uint64) (ch *tapeChunk, gen, base uint64, count uint32, ok bool) {
	if len(r.slots) == 0 {
		return nil, 0, 0, 0, false
	}
	ch = r.slots[seq%uint64(len(r.slots))].Load()
	if ch == nil {
		return nil, 0, 0, 0, false
	}
	g1 := ch.gen.Load()
	if g1&1 == 1 {
		return nil, 0, 0, 0, false
	}
	base = ch.base.Load()
	count = ch.count.Load()
	got := ch.seq.Load()
	g2 := ch.gen.Load()
	if g1 != g2 || got != seq {
		return nil, 0, 0, 0, false
	}
	return ch, g1, base, count, true
}

// load copies one published record. Page building does not use it; tests do.
func (r *tapeRing) load(abs uint64) (*tapeRec, bool) {
	m, _, ok := r.metaAt(r.pub.Load(), abs, nil)
	if !ok {
		return nil, false
	}
	if m.n > 0 {
		raw := make([]byte, m.n)
		copyAtomic(raw, m.ch.buf[:], int(m.off))
		if m.ch.gen.Load() != m.gen {
			return nil, false
		}
		m.rec.raw = raw
	}
	rec := m.rec
	return &rec, true
}

func readSlot(ch *tapeChunk, gen uint64, slot uint32) (tapeRec, uint32, uint32, bool) {
	if ch == nil || gen&1 == 1 || ch.gen.Load() != gen {
		return tapeRec{}, 0, 0, false
	}
	base := int(slot) * tapeSlotSize
	if base < 0 || base+tapeSlotSize > len(ch.buf) {
		return tapeRec{}, 0, 0, false
	}
	var word [9]uint64
	for i := range word {
		word[i] = atomic.LoadUint64(u64at(ch.buf[:], base+i*8))
	}
	if ch.gen.Load() != gen {
		return tapeRec{}, 0, 0, false
	}
	rec := tapeRec{
		kind:      uint8(word[0]),
		hasLast:   uint8(word[0]>>8)&1 == 1,
		wire:      int(uint32(word[0] >> 32)),
		epoch:     word[2],
		seqNo:     word[3],
		mdTs:      int64(word[4]),
		start:     int64(word[5]),
		end:       int64(word[6]),
		lastEpoch: word[7],
		lastSeq:   word[8],
	}
	return rec, uint32(word[1]), uint32(word[1] >> 32), true
}

func u64at(b []byte, off int) *uint64 {
	return (*uint64)(unsafe.Add(unsafe.Pointer(&b[0]), off))
}

func storeBytes(dst []byte, off int, src []byte) {
	padded := (len(src) + 7) &^ 7
	for i := 0; i < padded; i += 8 {
		var u uint64
		for j := 0; j < 8; j++ {
			if i+j < len(src) {
				u |= uint64(src[i+j]) << (8 * uint(j))
			}
		}
		atomic.StoreUint64(u64at(dst, off+i), u)
	}
}

// copyAtomic reads a chunk with atomic words. A plain copy races with reuse
// under the race detector; the generation check around this copy closes it.
func copyAtomic(dst, src []byte, off int) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		u := atomic.LoadUint64(u64at(src, off+i))
		dst[i] = byte(u)
		dst[i+1] = byte(u >> 8)
		dst[i+2] = byte(u >> 16)
		dst[i+3] = byte(u >> 24)
		dst[i+4] = byte(u >> 32)
		dst[i+5] = byte(u >> 40)
		dst[i+6] = byte(u >> 48)
		dst[i+7] = byte(u >> 56)
	}
	if i < len(dst) {
		u := atomic.LoadUint64(u64at(src, off+i))
		for j := 0; j < len(dst)-i; j++ {
			dst[i+j] = byte(u >> (8 * uint(j)))
		}
	}
}

// appendAtomic copies n payload bytes from a chunk onto a private buffer and
// reports whether gen survived the copy.
func appendAtomic(b []byte, ch *tapeChunk, gen uint64, off, n int) ([]byte, bool) {
	if n <= 0 {
		return b, ch == nil || ch.gen.Load() == gen
	}
	if ch == nil || ch.gen.Load() != gen {
		return nil, false
	}
	if off < 0 || off+((n+7)&^7) > len(ch.buf) {
		return nil, false
	}
	i0 := len(b)
	if cap(b) < i0+n {
		nb := make([]byte, i0+n)
		copy(nb, b)
		b = nb
	} else {
		b = b[:i0+n]
	}
	copyAtomic(b[i0:i0+n], ch.buf[:], off)
	if ch.gen.Load() != gen {
		return nil, false
	}
	return b, true
}
