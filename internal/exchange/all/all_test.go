package all

import (
	"regexp"
	"strings"
	"testing"
)

// A venue name is the first part of a UniversalTicker: CamelCase, no separator (D11).
var venueName = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

// The venue set is part of the contract: venue names are UniversalTicker tokens (D11, D26).
func TestVenues(t *testing.T) {
	r := Registry()
	want := "Binance,BinanceCM,BinanceEO,BinancePM,BinanceUM,Bybit,Okx"
	if got := strings.Join(r.Names(), ","); got != want {
		t.Fatalf("venues = %s, want %s", got, want)
	}
	for _, name := range r.Names() {
		v, _ := r.Venue(name)
		if !venueName.MatchString(name) {
			t.Errorf("venue %q is not a UniversalTicker venue token", name)
		}
		if v.Market == nil {
			continue
		}
		routes := v.Market.Routes()
		if len(routes) == 0 {
			t.Errorf("%s: market without routes", name)
		}
		for _, route := range routes {
			if !regexp.MustCompile(`^[a-z][a-z0-9]*$`).MatchString(route) {
				t.Errorf("%s: route %q is not one lowercase word (worker key md/<venue>/<route>/<n>, atom id <venue>:<route>:<channel>)", name, route)
			}
		}
	}
	if pm, _ := r.Venue("BinancePM"); pm.Market != nil || pm.Account == nil {
		t.Error("BinancePM is an account without market data")
	}
}
