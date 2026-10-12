package main

import "testing"

func TestStep(t *testing.T) {
	type in struct {
		epoch, seq uint64
		note       string
	}
	cases := map[string][]in{
		"contiguous":         {{1, 1, noteFirst}, {1, 2, ""}, {1, 3, ""}},
		"late start":         {{1, 40, noteFirst}, {1, 41, ""}},
		"gap":                {{1, 1, noteFirst}, {1, 3, noteGap}, {1, 4, ""}},
		"dup":                {{1, 1, noteFirst}, {1, 2, ""}, {1, 2, noteDup}, {1, 3, ""}},
		"reconnect":          {{1, 1, noteFirst}, {1, 2, ""}, {2, 1, noteNewEpoch}, {2, 2, ""}},
		"epoch not at seq 1": {{1, 1, noteFirst}, {2, 5, noteEpochSeq}},
		"epoch back":         {{2, 1, noteFirst}, {1, 9, noteEpochBack}, {2, 2, ""}},
	}
	for name, steps := range cases {
		st := &feedState{}
		for i, s := range steps {
			if got := st.step(s.epoch, s.seq); got != s.note {
				t.Errorf("%s step %d (%d,%d): note %q, want %q", name, i, s.epoch, s.seq, got, s.note)
			}
		}
	}
}

func TestCursorNext(t *testing.T) {
	for _, c := range []struct {
		a, b cursor
		want bool
	}{
		{cursor{1, 5}, cursor{1, 6}, true},
		{cursor{1, 5}, cursor{1, 7}, false},
		{cursor{1, 5}, cursor{1, 5}, false},
		{cursor{1, 5}, cursor{2, 1}, true},
		{cursor{1, 5}, cursor{2, 2}, false},
	} {
		if got := c.a.next(c.b); got != c.want {
			t.Errorf("%s then %s: %v", c.a, c.b, got)
		}
	}
	if !(cursor{1, 9}).atOrBefore(cursor{2, 1}) || (cursor{2, 1}).atOrBefore(cursor{1, 9}) {
		t.Error("epoch orders before seq")
	}
}
