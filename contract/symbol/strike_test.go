package symbol_test

import (
	"errors"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestSpellStrike(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		bad      bool
	}{
		{in: "70000.0", want: "70000"},
		{in: "7E4", want: "70000"},
		{in: "7e4", want: "70000"},
		{in: "7E-4", want: "0D0007"},
		{in: "10_000", want: "10000"},
		{in: "6.4", want: "6D4"},
		{in: "6.40", want: "6D4"},
		{in: "6d4", want: "6D4"},
		{in: "6D4", want: "6D4"},
		{in: "6400.0", want: "6400"},
		{in: "6400", want: "6400"},
		{in: "6400.00", want: "6400"},
		{in: "0.10", want: "0D1"},
		{in: ".5", want: "0D5"},
		{in: "6.", want: "6"},
		{in: "00.50", want: "0D5"},
		{in: "6.4E1", want: "64"},
		{in: "+6.4", want: "6D4"},
		{in: "", bad: true},
		{in: "   ", bad: true},
		{in: "10 000", bad: true},
		{in: "10/000", bad: true},
		{in: "10__000", bad: true},
		{in: "_1000", bad: true},
		{in: "nope", bad: true},
	} {
		got, err := symbol.SpellStrike(tc.in)
		if tc.bad {
			if !errors.Is(err, symbol.ErrInvalidTicker) {
				t.Errorf("SpellStrike(%q) err = %v", tc.in, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("SpellStrike(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
		again, err := symbol.SpellStrike(got)
		if err != nil || again != got {
			t.Errorf("SpellStrike(%q) did not stick: %q, %v", got, again, err)
		}
	}
}

func FuzzSpellStrike(f *testing.F) {
	for _, s := range []string{"6.4", "6D4", "6400.0", "7E4", "7E-4", "10_000", ".5", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, err := symbol.SpellStrike(s)
		if err != nil {
			return
		}
		again, err := symbol.SpellStrike(got)
		if err != nil || again != got {
			t.Fatalf("SpellStrike(%q) = %q, then %q, %v", s, got, again, err)
		}
	})
}
