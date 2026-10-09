package decimal

import (
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for _, s := range []string{"0", "0.1", "-12.5", "+3", "1e-8", "6.4E2", "65000.00", ".5", "5."} {
		v, err := Parse(s)
		want, _ := strconv.ParseFloat(s, 64)
		if err != nil || v != want {
			t.Errorf("Parse(%q) = %v, %v; want %v", s, v, err, want)
		}
	}
	for _, s := range []string{"", "NaN", "nan", "Inf", "-Infinity", "0x1p3", "1_000", " 1", "1e400", "abc", "1,5"} {
		if v, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) = %v; want an error", s, v)
		}
	}
	if v, err := ParseBytes([]byte("42.25")); err != nil || v != 42.25 {
		t.Errorf("ParseBytes = %v, %v", v, err)
	}
}

func TestParseBytesNoAlloc(t *testing.T) {
	b := []byte("65123.45")
	if n := testing.AllocsPerRun(100, func() { _, _ = ParseBytes(b) }); n != 0 {
		t.Errorf("ParseBytes allocates %v times", n)
	}
}

func TestPlaces(t *testing.T) {
	for step, want := range map[float64]int{0.001: 3, 0.5: 1, 5: 0, 1: 0, 1e-8: 8, 0.00025: 5, 0: 0, -1: 0} {
		if got := Places(step); got != want {
			t.Errorf("Places(%v) = %d, want %d", step, got, want)
		}
	}
}

func TestAppend(t *testing.T) {
	for _, c := range []struct {
		v, step float64
		want    string
	}{
		{0.1 + 0.2, 0.1, "0.3"},
		{0.00001, 0.00001, "0.00001"}, // never 1e-05
		{65000, 0.01, "65000.00"},
		{65000.004, 0.01, "65000.00"},
		{65000.006, 0.01, "65000.01"},
		{3 * 0.1, 0.1, "0.3"},
		{12.5, 5, "15"}, // nearest; half rounds away from zero: 2.5 steps is 3
		{-0.0000000001, 0.01, "0.00"},
		{0.1, 0, "0.1"},
		{1e-7, 0, "0.0000001"},
	} {
		if got := Format(c.v, c.step); got != c.want {
			t.Errorf("Format(%v, %v) = %q, want %q", c.v, c.step, got, c.want)
		}
	}
}

func TestSnapDirection(t *testing.T) {
	for _, c := range []struct{ v, step, down, up float64 }{
		{0.3, 0.1, 0.3, 0.3}, // 0.3/0.1 is 2.9999999999999996: on the boundary, not below it
		{0.35, 0.1, 0.3, 0.4},
		{1.0000000001, 0.5, 1, 1}, // within tolerance of a boundary
		{1.01, 0.5, 1, 1.5},
		{65000.004, 0.01, 65000, 65000.01},
	} {
		if d := Format(SnapDown(c.v, c.step), c.step); d != Format(c.down, c.step) {
			t.Errorf("SnapDown(%v, %v) = %s, want %v", c.v, c.step, d, c.down)
		}
		if u := Format(SnapUp(c.v, c.step), c.step); u != Format(c.up, c.step) {
			t.Errorf("SnapUp(%v, %v) = %s, want %v", c.v, c.step, u, c.up)
		}
	}
}

// TestEqualAfterSums is the fill case: ten fills of 0.1 add up to 0.9999999999999999, which is the order's 1.
func TestEqualAfterSums(t *testing.T) {
	var sum float64
	for range 10 {
		sum += 0.1
	}
	if sum == 1 {
		t.Fatal("the premise no longer holds")
	}
	if !Equal(sum, 1, 0.001) {
		t.Errorf("Equal(%v, 1) = false", sum)
	}
	if Equal(0.999, 1, 0.001) {
		t.Error("Equal(0.999, 1, 0.001) = true")
	}
}

func TestAbsent(t *testing.T) {
	if Has(Absent()) || !Has(0) {
		t.Error("Has")
	}
}

// TestRoundTrip: text of at most 15 significant digits, as a venue sends it, comes back from Parse and Format
// with a step of the same number of decimals, digit for digit.
func TestRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 200000 {
		digits := 1 + r.IntN(15)
		places := r.IntN(digits + 1)
		if places > 12 {
			places = 12
		}
		var b strings.Builder
		for i := range digits {
			if i == digits-places && places > 0 {
				if i == 0 {
					b.WriteByte('0')
				}
				b.WriteByte('.')
			}
			d := byte('0' + r.IntN(10))
			if i == 0 && digits-places > 1 && d == '0' {
				d = '1'
			}
			b.WriteByte(d)
		}
		s := b.String()
		v, err := Parse(s)
		if err != nil {
			t.Fatalf("Parse(%q): %v", s, err)
		}
		step := math.Pow10(-places)
		if got := Format(v, step); got != s {
			t.Fatalf("Format(Parse(%q), %v) = %q", s, step, got)
		}
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{"0", "0.1", "1e-8", "NaN", "65000.00", "-3"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		v, err := Parse(s)
		if err != nil {
			return
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("Parse(%q) = %v", s, v)
		}
		w, err := Parse(strconv.FormatFloat(v, 'f', -1, 64))
		if err != nil || w != v {
			t.Fatalf("%q: %v does not come back from its own text: %v, %v", s, v, w, err)
		}
	})
}
