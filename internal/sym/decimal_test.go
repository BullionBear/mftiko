package sym

import "testing"

func TestCanonicalDecimal(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{in: "", want: ""},
		{in: "  ", want: ""},
		{in: "1", want: "1"},
		{in: "1.0", want: "1"},
		{in: "1.10", want: "1.1"},
		{in: "0.50", want: "0.5"},
		{in: "1e-8", want: "0.00000001"},
		{in: "7E4", want: "70000"},
		{in: "-0.50", want: "-0.5"},
		{in: "+1.20", want: "1.2"},
		{in: "0", want: "0"},
		{in: "0.0", want: "0"},
	} {
		got, err := canonicalDecimal(tc.in)
		if err != nil {
			t.Fatalf("canonicalDecimal(%q) = %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("canonicalDecimal(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if _, err := canonicalDecimal("nope"); err == nil {
		t.Fatal("expected a rejected decimal")
	}
}
