package sym

import (
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestRandomSourceStaysInInterval(t *testing.T) {
	src := RandomSource()
	for range 64 {
		uid, err := src.NextUID()
		if err != nil {
			t.Fatal(err)
		}
		if !symbol.ValidUID(uid) {
			t.Fatalf("uid %d is not a valid UID", uid)
		}
	}
}
