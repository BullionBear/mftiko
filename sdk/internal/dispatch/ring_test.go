package dispatch

import (
	"bytes"
	"testing"
)

func TestRingDropsOldest(t *testing.T) {
	r := newRing(50)
	a := bytes.Repeat([]byte("a"), 20)
	b := bytes.Repeat([]byte("b"), 20)
	c := bytes.Repeat([]byte("c"), 20)
	if !r.push(a, false, 0) || !r.push(b, false, 0) {
		t.Fatal("push")
	}
	// 40 bytes used, 10 free. c needs 20, so the oldest message is dropped.
	if !r.push(c, false, 0) {
		t.Fatal("push c")
	}
	if r.dropped != 1 || r.waiting() != 2 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
	msg, _, _, ok := r.peek()
	if !ok || !bytes.Equal(msg, b) {
		t.Fatalf("oldest now %q", msg)
	}
	r.release()
	msg, _, _, ok = r.peek()
	if !ok || !bytes.Equal(msg, c) {
		t.Fatalf("next %q", msg)
	}
}

func TestRingDropsSeveral(t *testing.T) {
	r := newRing(50)
	if !r.push(bytes.Repeat([]byte("a"), 20), false, 0) ||
		!r.push(bytes.Repeat([]byte("b"), 20), false, 0) ||
		!r.push(bytes.Repeat([]byte("c"), 10), false, 0) {
		t.Fatal("push")
	}
	// 50 used. 30 needs the two oldest messages dropped; c stays.
	if !r.push(bytes.Repeat([]byte("d"), 30), false, 0) {
		t.Fatal("push d")
	}
	if r.dropped != 2 || r.waiting() != 2 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
	msg, _, _, ok := r.peek()
	if !ok || !bytes.Equal(msg, bytes.Repeat([]byte("c"), 10)) {
		t.Fatalf("oldest %q", msg)
	}
	r.release()
	msg, _, _, ok = r.peek()
	if !ok || !bytes.Equal(msg, bytes.Repeat([]byte("d"), 30)) {
		t.Fatalf("next %q", msg)
	}
}

func TestRingSkipsPinned(t *testing.T) {
	r := newRing(50)
	a := bytes.Repeat([]byte("a"), 20)
	b := bytes.Repeat([]byte("b"), 20)
	c := bytes.Repeat([]byte("c"), 20)
	if !r.push(a, false, 0) || !r.push(b, false, 0) {
		t.Fatal("push")
	}
	msg, _, _, ok := r.peek()
	if !ok {
		t.Fatal("peek")
	}
	held := append([]byte(nil), msg...)
	// free is 10. c needs 20, so b (the oldest message that is not pinned) is dropped.
	if !r.push(c, false, 0) {
		t.Fatal("push c")
	}
	if r.dropped != 1 || !bytes.Equal(msg, held) || !bytes.Equal(msg, a) {
		t.Fatalf("pinned moved or dropped %d", r.dropped)
	}
	r.release()
	msg, _, _, ok = r.peek()
	if !ok || !bytes.Equal(msg, c) {
		t.Fatalf("after pin %q ok %v", msg, ok)
	}
}

func TestRingPinnedLeavesNoRoom(t *testing.T) {
	r := newRing(50)
	if !r.push(bytes.Repeat([]byte("a"), 40), false, 0) {
		t.Fatal("push")
	}
	if _, _, _, ok := r.peek(); !ok {
		t.Fatal("peek")
	}
	// 40 pinned, 10 free, nothing unpinned to drop. The new message does not fit.
	if r.push(bytes.Repeat([]byte("b"), 20), false, 0) {
		t.Fatal("accepted")
	}
	if r.dropped != 1 || r.waiting() != 0 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
}

func TestRingKeepsReceiveTimes(t *testing.T) {
	r := newRing(50)
	r.timed = true
	if !r.push(bytes.Repeat([]byte("a"), 20), false, 100) || !r.push(bytes.Repeat([]byte("b"), 20), true, 200) {
		t.Fatal("push")
	}
	msg, book, at, ok := r.peek()
	if !ok || msg[0] != 'a' || book || at != 100 {
		t.Fatalf("peek %q book %v at %d", msg, book, at)
	}
	// a is pinned. c needs 20 of the 10 free bytes, so b is dropped, and its time with it.
	if !r.push(bytes.Repeat([]byte("c"), 20), false, 300) {
		t.Fatal("push c")
	}
	r.release()
	msg, book, at, ok = r.peek()
	if !ok || msg[0] != 'c' || book || at != 300 {
		t.Fatalf("after drop %q book %v at %d", msg, book, at)
	}
	r.release()
	if !r.push([]byte("d"), false, 400) {
		t.Fatal("push d")
	}
	r.discard()
	if len(r.at) != 0 || r.waiting() != 0 {
		t.Fatalf("discard left %d times", len(r.at))
	}
}

func TestRingUntimedKeepsNoTimes(t *testing.T) {
	r := newRing(50)
	if !r.push([]byte("a"), false, 100) {
		t.Fatal("push")
	}
	if _, _, at, _ := r.peek(); at != 0 || r.at != nil {
		t.Fatalf("an untimed ring kept %d (%v)", at, r.at)
	}
}

func TestRingOversize(t *testing.T) {
	r := newRing(50)
	if r.push(make([]byte, 51), false, 0) {
		t.Fatal("larger than the ring was accepted")
	}
	if !r.push(make([]byte, 50), false, 0) {
		t.Fatal("a message the size of the ring should fit when the ring is empty")
	}
	if r.dropped != 1 || r.waiting() != 1 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
}
