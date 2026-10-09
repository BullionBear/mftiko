package sym

import "testing"

func TestRandomSourceStaysInInterval(t *testing.T) {
	src := RandomSource()
	for range 64 {
		uid, err := src.NextUID()
		if err != nil {
			t.Fatal(err)
		}
		if uid < uidMin || uid > uidMax {
			t.Fatalf("uid %d outside %d..%d", uid, uidMin, uidMax)
		}
	}
}
