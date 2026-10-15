package main

import "testing"

func TestStep(t *testing.T) {
	s := &seqs{}
	for _, c := range []struct {
		inc  uint32
		seq  uint64
		want string
	}{
		{1, 4, noteFirst}, // the watch joined late
		{1, 5, ""},
		{1, 5, noteDup},
		{1, 7, noteGap},
		{1, 8, ""},
		{2, 1, noteInc}, // a restarted worker numbers from 1
		{2, 2, ""},
		{3, 2, noteGap}, // the next incarnation's first event was missed
		{2, 9, noteGap}, // an older incarnation
	} {
		if got := s.step(c.inc, c.seq); got != c.want {
			t.Fatalf("(%d,%d): %q, want %q", c.inc, c.seq, got, c.want)
		}
	}
	if s.gaps != 3 || s.dups != 1 {
		t.Fatalf("gaps %d dups %d", s.gaps, s.dups)
	}
}
