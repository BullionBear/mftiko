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
	if !r.push(a, false) || !r.push(b, false) {
		t.Fatal("push")
	}
	// 40 bytes used, 10 free. c needs 20, so the oldest message is dropped.
	if !r.push(c, false) {
		t.Fatal("push c")
	}
	if r.dropped != 1 || r.waiting() != 2 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
	msg, _, ok := r.peek()
	if !ok || !bytes.Equal(msg, b) {
		t.Fatalf("oldest now %q", msg)
	}
	r.release()
	msg, _, ok = r.peek()
	if !ok || !bytes.Equal(msg, c) {
		t.Fatalf("next %q", msg)
	}
}

func TestRingDropsSeveral(t *testing.T) {
	r := newRing(50)
	if !r.push(bytes.Repeat([]byte("a"), 20), false) ||
		!r.push(bytes.Repeat([]byte("b"), 20), false) ||
		!r.push(bytes.Repeat([]byte("c"), 10), false) {
		t.Fatal("push")
	}
	// 50 used. 30 needs the two oldest messages dropped; c stays.
	if !r.push(bytes.Repeat([]byte("d"), 30), false) {
		t.Fatal("push d")
	}
	if r.dropped != 2 || r.waiting() != 2 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
	msg, _, ok := r.peek()
	if !ok || !bytes.Equal(msg, bytes.Repeat([]byte("c"), 10)) {
		t.Fatalf("oldest %q", msg)
	}
	r.release()
	msg, _, ok = r.peek()
	if !ok || !bytes.Equal(msg, bytes.Repeat([]byte("d"), 30)) {
		t.Fatalf("next %q", msg)
	}
}

func TestRingSkipsPinned(t *testing.T) {
	r := newRing(50)
	a := bytes.Repeat([]byte("a"), 20)
	b := bytes.Repeat([]byte("b"), 20)
	c := bytes.Repeat([]byte("c"), 20)
	if !r.push(a, false) || !r.push(b, false) {
		t.Fatal("push")
	}
	msg, _, ok := r.peek()
	if !ok {
		t.Fatal("peek")
	}
	held := append([]byte(nil), msg...)
	// free is 10. c needs 20, so b (the oldest message that is not pinned) is dropped.
	if !r.push(c, false) {
		t.Fatal("push c")
	}
	if r.dropped != 1 || !bytes.Equal(msg, held) || !bytes.Equal(msg, a) {
		t.Fatalf("pinned moved or dropped %d", r.dropped)
	}
	r.release()
	msg, _, ok = r.peek()
	if !ok || !bytes.Equal(msg, c) {
		t.Fatalf("after pin %q ok %v", msg, ok)
	}
}

func TestRingPinnedLeavesNoRoom(t *testing.T) {
	r := newRing(50)
	if !r.push(bytes.Repeat([]byte("a"), 40), false) {
		t.Fatal("push")
	}
	if _, _, ok := r.peek(); !ok {
		t.Fatal("peek")
	}
	// 40 pinned, 10 free, nothing unpinned to drop. The new message does not fit.
	if r.push(bytes.Repeat([]byte("b"), 20), false) {
		t.Fatal("accepted")
	}
	if r.dropped != 1 || r.waiting() != 0 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
}

func TestRingOversize(t *testing.T) {
	r := newRing(50)
	if r.push(make([]byte, 51), false) {
		t.Fatal("larger than the ring was accepted")
	}
	if !r.push(make([]byte, 50), false) {
		t.Fatal("a message the size of the ring should fit when the ring is empty")
	}
	if r.dropped != 1 || r.waiting() != 1 {
		t.Fatalf("dropped %d waiting %d", r.dropped, r.waiting())
	}
}
