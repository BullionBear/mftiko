package cloid

import (
	"errors"
	"strconv"
	"testing"
)

func TestEncodeLayout(t *testing.T) {
	// ver 1, algo 1, ts_ms at the epoch, seq 0: 2^60 + 2^50.
	s, err := Encode(1, EpochMs, 0)
	if err != nil || s != "1154047404513689600" {
		t.Fatalf("%q %v", s, err)
	}
	s, err = Encode(AlgoMax, EpochMs+1<<TsBits-1, SeqMax)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := strconv.ParseInt(s, 10, 64); err != nil || v != 1<<61-1 {
		t.Fatalf("all fields full: %q %d %v", s, v, err)
	}
	if len(s) > MaxLen {
		t.Fatalf("%q is longer than %d", s, MaxLen)
	}
	id, err := Decode(s)
	if err != nil || id != (ID{Ver: Version, Algo: AlgoMax, TsMs: EpochMs + 1<<TsBits - 1, Seq: SeqMax}) {
		t.Fatalf("%+v %v", id, err)
	}
}

func TestEncodeRejectsWhatDoesNotFit(t *testing.T) {
	for _, c := range []struct {
		algo uint32
		ts   int64
		seq  uint32
	}{
		{0, EpochMs, 0},
		{AlgoMax + 1, EpochMs, 0},
		{1, EpochMs - 1, 0},
		{1, EpochMs + 1<<TsBits, 0},
		{1, EpochMs, SeqMax + 1},
	} {
		if s, err := Encode(c.algo, c.ts, c.seq); err == nil {
			t.Errorf("Encode(%d, %d, %d) = %q", c.algo, c.ts, c.seq, s)
		}
	}
}

func TestNotPlatform(t *testing.T) {
	valid, _ := Encode(5, EpochMs+12345, 7)
	for _, s := range []string{
		"",
		"0",
		"0" + valid,                   // leading zero
		"+" + valid,                   // sign
		valid + " ",                   // space
		"x" + valid[1:],               // not a digit
		"abc-123",                     // another system's id
		"42",                          // ver 0
		"18446744073709551615",        // uint64 max, too long
		"1152921504606846976",         // ver 1, algo 0
		strconv.FormatUint(8<<60, 10), // ver 8
	} {
		if algo, ok := AlgoOf(s); ok {
			t.Errorf("AlgoOf(%q) = %d, want no algo", s, algo)
		}
		if _, err := Decode(s); !errors.Is(err, ErrNotPlatform) {
			t.Errorf("Decode(%q): %v", s, err)
		}
	}
	if algo, ok := AlgoOf(valid); !ok || algo != 5 {
		t.Fatalf("AlgoOf(%q) = %d %v", valid, algo, ok)
	}
	// Versions 2..7 have the same layout and are platform ids.
	if id, err := Decode("9223372036854775807"); err != nil || id.Ver != 7 || id.Algo != AlgoMax {
		t.Fatalf("ver 7: %+v %v", id, err)
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Add(uint32(1), uint64(0), uint32(0))
	f.Add(uint32(AlgoMax), uint64(1<<TsBits-1), uint32(SeqMax))
	f.Add(uint32(321), uint64(23_000_000_000), uint32(512))
	f.Fuzz(func(t *testing.T, algo uint32, ts uint64, seq uint32) {
		algo = algo%AlgoMax + 1
		tsMs := EpochMs + int64(ts%(1<<TsBits))
		seq %= SeqMax + 1
		s, err := Encode(algo, tsMs, seq)
		if err != nil {
			t.Fatal(err)
		}
		if len(s) > MaxLen {
			t.Fatalf("%q longer than %d", s, MaxLen)
		}
		id, err := Decode(s)
		want := ID{Ver: Version, Algo: algo, TsMs: tsMs, Seq: seq}
		if err != nil || id != want {
			t.Fatalf("Decode(Encode(%+v)) = %+v %v", want, id, err)
		}
		if got, ok := AlgoOf(s); !ok || got != algo {
			t.Fatalf("AlgoOf(%q) = %d %v", s, got, ok)
		}
	})
}

// FuzzDecode: whatever Decode accepts, it accepts in the one spelling String makes.
func FuzzDecode(f *testing.F) {
	for _, s := range []string{"1154047404513689600", "0", "", "+1", "01154047404513689600", "9223372036854775807", "abc"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		id, err := Decode(s)
		if err != nil {
			if !errors.Is(err, ErrNotPlatform) {
				t.Fatalf("Decode(%q): %v", s, err)
			}
			if _, ok := AlgoOf(s); ok {
				t.Fatalf("AlgoOf(%q) has an algo", s)
			}
			return
		}
		if id.String() != s {
			t.Fatalf("Decode(%q) = %+v, which spells %q", s, id, id.String())
		}
	})
}
