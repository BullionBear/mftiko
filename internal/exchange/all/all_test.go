package all

import (
	"strings"
	"testing"
)

// The venue set is part of the contract: venue names are UniversalTicker tokens (D11, D26).
func TestVenues(t *testing.T) {
	r := Registry()
	want := "Binance,BinanceCM,BinanceEO,BinancePM,BinanceUM,Bybit,OKX"
	if got := strings.Join(r.Names(), ","); got != want {
		t.Fatalf("venues = %s, want %s", got, want)
	}
	for _, name := range r.Names() {
		v, _ := r.Venue(name)
		if v.Market == nil {
			continue
		}
		routes := v.Market.Routes()
		if len(routes) == 0 {
			t.Errorf("%s: market without routes", name)
		}
		for _, route := range routes {
			if route == "" || strings.ContainsAny(route, "/.: ") {
				t.Errorf("%s: route %q cannot go into the worker key md/<venue>/<route>/<n>", name, route)
			}
		}
	}
	if pm, _ := r.Venue("BinancePM"); pm.Market != nil || pm.Account == nil {
		t.Error("BinancePM is an account without market data")
	}
}
