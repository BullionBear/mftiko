package symbol_test

import (
	"errors"
	"math"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestCanonicalAsset(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		bad      bool
	}{
		{in: "btc", want: "BTC"},
		{in: "BTC", want: "BTC"},
		{in: "xbt", want: "BTC"},
		{in: "XBT", want: "BTC"},
		{in: "  usdt ", want: "USDT"},
		{in: "USD", want: "USD"},
		{in: "USDC", want: "USDC"},
		{in: "龙虾", want: "龙虾"},
		{in: "1000SATS", want: "1000SATS"},
		{in: "", bad: true},
		{in: "BTC/USDT", bad: true},
		{in: "BTC-USDT", bad: true},
	} {
		got, err := symbol.CanonicalAsset(tc.in)
		if tc.bad {
			if !errors.Is(err, symbol.ErrAsset) {
				t.Errorf("CanonicalAsset(%q) err = %v", tc.in, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("CanonicalAsset(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	if symbol.IsCanonicalAsset("XBT") || symbol.IsCanonicalAsset("btc") || symbol.IsCanonicalAsset(" BTC") {
		t.Fatal("non-canonical asset reported canonical")
	}
	if !symbol.IsCanonicalAsset("BTC") || !symbol.IsCanonicalAsset("USDT") || !symbol.IsCanonicalAsset("龙虾") {
		t.Fatal("canonical asset rejected")
	}
	for _, code := range []string{"USD", "USDC", "USDT"} {
		if _, ok := symbol.AssetAliases()[code]; ok {
			t.Fatalf("%s is an alias", code)
		}
	}
	copied := symbol.AssetAliases()
	copied["XBT"] = "ETH"
	if symbol.AssetAliases()["XBT"] != "BTC" {
		t.Fatal("AssetAliases returned the live map")
	}
	for from, to := range symbol.AssetAliases() {
		if !symbol.IsCanonicalAsset(to) {
			t.Errorf("alias %s → %s, target is not canonical", from, to)
		}
		if _, ok := symbol.AssetAliases()[to]; ok {
			t.Errorf("alias target %s is itself an alias", to)
		}
	}
}

func TestValidUID(t *testing.T) {
	if symbol.ValidUID(0) {
		t.Fatal("0 is a UID")
	}
	if !symbol.ValidUID(1) || !symbol.ValidUID(math.MaxUint64) {
		t.Fatal("range is applied before it is decided")
	}
}
