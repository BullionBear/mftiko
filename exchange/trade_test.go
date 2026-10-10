package exchange

import (
	"errors"
	"testing"
	"time"
)

func TestInflight(t *testing.T) {
	t0 := time.Unix(1000, 0)
	var f Inflight[string]
	a := f.Add("a", t0.Add(time.Second))
	b := f.Add("b", t0.Add(3*time.Second))
	c := f.Add("c", t0.Add(2*time.Second))
	if a != 1 || b != 2 || c != 3 || f.Waiting() != 3 {
		t.Fatalf("ids %d %d %d, waiting %d", a, b, c, f.Waiting())
	}
	if !f.Next().Equal(t0.Add(time.Second)) {
		t.Fatalf("Next = %v", f.Next())
	}

	// Answers in any order.
	if v, ok := f.Answer(c); !ok || v != "c" {
		t.Fatalf("Answer(c) = %q, %v", v, ok)
	}
	if _, ok := f.Answer(c); ok {
		t.Fatal("an id answered twice")
	}
	if _, ok := f.Answer(99); ok {
		t.Fatal("an id never sent was answered")
	}

	// a's deadline passes: an unknown outcome, and its late answer is not taken.
	exp := f.Expire(t0.Add(time.Second))
	if len(exp) != 1 || exp[0].ID != a || exp[0].V != "a" || !errors.Is(exp[0].Err, ErrUnknownOutcome) {
		t.Fatalf("Expire = %+v", exp)
	}
	if _, ok := f.Answer(a); ok {
		t.Fatal("an answer after the deadline was taken as live")
	}

	// The connection drops: what waits is lost, as an unknown outcome.
	d := f.Add("d", t0.Add(time.Hour))
	lost := f.Lost()
	if len(lost) != 2 || lost[0].ID != b || lost[1].ID != d || !errors.Is(lost[1].Err, ErrUnknownOutcome) {
		t.Fatalf("Lost = %+v", lost)
	}
	if f.Waiting() != 0 || !f.Next().IsZero() {
		t.Fatalf("after Lost: waiting %d, next %v", f.Waiting(), f.Next())
	}
	if e := f.Add("e", t0); e != 5 {
		t.Fatalf("ids are reused: %d", e)
	}
}
