package symbol_test

import (
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestStatus(t *testing.T) {
	if symbol.Unspecified.Valid() || symbol.Unspecified.IsTradable() || symbol.Unspecified.Active() || symbol.Unspecified.Terminal() {
		t.Fatal("unspecified is stored or tradable")
	}
	if symbol.Status("UNSPECIFIED").Valid() || symbol.Status("NOPE").Valid() {
		t.Fatal("unknown spelling is valid")
	}
	for _, s := range []symbol.Status{symbol.Prelisting, symbol.Halted, symbol.Settling, symbol.Unknown} {
		if !s.Valid() || !s.Active() || s.Terminal() || s.IsTradable() {
			t.Fatalf("%s active=%v terminal=%v tradable=%v", s, s.Active(), s.Terminal(), s.IsTradable())
		}
	}
	if !symbol.Tradable.Valid() || !symbol.Tradable.Active() || !symbol.Tradable.IsTradable() || symbol.Tradable.Terminal() {
		t.Fatal("TRADABLE")
	}
	for _, s := range []symbol.Status{symbol.Expired, symbol.Delisted} {
		if !s.Valid() || s.Active() || !s.Terminal() || s.IsTradable() {
			t.Fatalf("%s active=%v terminal=%v tradable=%v", s, s.Active(), s.Terminal(), s.IsTradable())
		}
	}
}
