package all

import (
	"regexp"
	"strings"
	"testing"
)

var (
	// A venue name is the first part of a UniversalTicker: CamelCase, no separator (D11).
	camel = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	// A route goes into the worker key md/<venue>/<route>/<n> and the atom id <venue>:<route>:<channel>.
	word = regexp.MustCompile(`^[a-z][a-z0-9]*$`)
)

// Both enums are part of the contract (D11, D26, D32).
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

	// BinancePM is an account kind only: it trades three venues and is not one (D32).
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
}
