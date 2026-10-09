package all

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

var (
	// A venue name is the first part of a UniversalTicker: CamelCase, no separator (D201).
	camel = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	// A route goes into the worker key md/<venue>/<route>/<n> and the atom id <venue>:<route>:<channel>.
	word = regexp.MustCompile(`^[a-z][a-z0-9]*$`)
)

// Both enums are part of the contract (D201, D403, D301).
func TestRegistry(t *testing.T) {
	r := Registry()
	if got, want := strings.Join(r.Venues(), ","), "Binance,BinanceCM,BinanceEO,BinanceUM,Bybit,Okx"; got != want {
		t.Errorf("venues = %s, want %s", got, want)
	}
	if got, want := strings.Join(r.AccountKinds(), ","), "Binance,BinanceCM,BinanceEO,BinancePM,BinanceUM,Bybit,Okx"; got != want {
		t.Errorf("account kinds = %s, want %s", got, want)
	}
	for _, name := range r.Venues() {
		v, _ := r.Venue(name)
		if !camel.MatchString(name) {
			t.Errorf("venue %q is not a UniversalTicker venue token", name)
		}
		if v.Market == nil {
			continue
		}
		for _, route := range v.Market.Routes() {
			if !word.MatchString(route) {
				t.Errorf("%s: route %q is not one lowercase word", name, route)
			}
		}
	}

	// BinancePM is an account kind only: it trades three venues and is not one (D301).
	if _, ok := r.Lookup("BinancePM"); ok {
		t.Error("BinancePM resolves as a venue")
	}
	pm, _ := r.AccountKind("BinancePM")
	for _, v := range []string{"Binance", "BinanceUM", "BinanceCM"} {
		if !pm.CanTrade(v) {
			t.Errorf("BinancePM cannot trade %s", v)
		}
	}
	if pm.CanTrade("BinanceEO") {
		t.Error("BinancePM trades BinanceEO")
	}

	// Resolve folds case against the registry (D201). Binance is spot only.
	got, err := symbol.Resolve("binanceum_future_btcusdt250926", r)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "BinanceUM_Future_BTCUSDT-250926" {
		t.Fatalf("got %s", got.String())
	}
	if _, err := symbol.Resolve("bybit_perp_btc/usdt", r); err != nil {
		t.Fatal(err)
	}
	if _, err := symbol.Resolve("binance_perp_btcusdt", r); !errors.Is(err, symbol.ErrCategory) {
		t.Fatalf("binance perp: %v", err)
	}
}
