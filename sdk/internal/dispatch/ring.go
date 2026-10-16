package dispatch

// ring is the MD buffer: one preallocated []byte holding variable-length messages (D816).
// The bytes contain no pointers, so the garbage collector does not scan them (D803).
//
// Live messages are packed at the front. The message a hook is reading is pinned at buf[:pinN]
// until the hook returns; those bytes are not moved and not overwritten. A push that does not fit
// drops the oldest messages that are not pinned, sliding the rest down, until it fits. If the
// pinned message still leaves too little room, or the new message is larger than the whole ring,
// the new message is dropped and counted. Dropping never blocks the caller: a hook must not stall
// the receive path (D803).
type ring struct {
	buf  []byte
	msgs []mdMsg // oldest at index 0; msgs[0] is the pinned message when pinned

	pinned bool
	pinN   int // byte length of the pinned message; valid when pinned, including 0
	used   int // bytes occupied at buf[:used]

	dropped uint64
}

// mdMsg is one message's length and whether it is orderbook_delta.
// The bytes themselves live in ring.buf, in the same order.
type mdMsg struct {
	n    int
	book bool
}

func newRing(n int) *ring {
	if n <= 0 {
		n = MDBytes
	}
	return &ring{buf: make([]byte, n)}
}

// push copies msg into the ring. false means msg was dropped (and counted).
func (r *ring) push(msg []byte, book bool) bool {
	if len(msg) > len(r.buf) {
		r.dropped++
		return false
	}
	for r.free() < len(msg) {
		if !r.dropUnpinned() {
			// The pinned message is the only thing left, and it occupies the bytes msg needs.
			r.dropped++
			return false
		}
	}
	copy(r.buf[r.used:], msg)
	r.used += len(msg)
	r.msgs = append(r.msgs, mdMsg{n: len(msg), book: book})
	return true
}

func (r *ring) free() int { return len(r.buf) - r.used }

// dropUnpinned drops the oldest message that is not pinned and slides the later bytes down.
// The pinned prefix is left where it is. false means there is no such message.
func (r *ring) dropUnpinned() bool {
	idx := 0
	off := 0
	if r.pinned {
		if len(r.msgs) < 2 {
			return false
		}
		idx = 1
		off = r.pinN
	} else if len(r.msgs) == 0 {
		return false
	}
	n := r.msgs[idx].n
	copy(r.msgs[idx:], r.msgs[idx+1:])
	r.msgs = r.msgs[:len(r.msgs)-1]
	from := off + n
	copy(r.buf[off:], r.buf[from:r.used])
	r.used -= n
	r.dropped++
	return true
}

// peek pins the oldest message and returns its bytes. The slice aliases the ring and stays valid
// until release. ok is false when the ring is empty or a message is already pinned.
func (r *ring) peek() (msg []byte, book bool, ok bool) {
	if r.pinned || len(r.msgs) == 0 {
		return nil, false, false
	}
	r.pinned = true
	r.pinN = r.msgs[0].n
	return r.buf[:r.pinN], r.msgs[0].book, true
}

// release drops the pinned message and slides whatever follows to the front.
func (r *ring) release() {
	if !r.pinned {
		return
	}
	n := r.pinN
	copy(r.msgs, r.msgs[1:])
	r.msgs = r.msgs[:len(r.msgs)-1]
	copy(r.buf, r.buf[n:r.used])
	r.used -= n
	r.pinned = false
	r.pinN = 0
}

// discard drops every message, including a pinned one. The caller must already be done with the slice.
func (r *ring) discard() {
	r.msgs = r.msgs[:0]
	r.used = 0
	r.pinned = false
	r.pinN = 0
}

// waiting is the number of messages not currently pinned.
func (r *ring) waiting() int {
	n := len(r.msgs)
	if r.pinned && n > 0 {
		n--
	}
	return n
}
