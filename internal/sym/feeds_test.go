package sym

import (
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

func TestSelectFeeds(t *testing.T) {
	src, err := exchangetest.New("Bybit")
	if err != nil {
		t.Fatal(err)
	}
	reg, err := exchange.NewRegistry(exchange.Declaration{
		Exchange: "test",
		Venues: []exchange.Venue{
			{Venue: symbol.Venue{Name: "Bybit", Categories: []symbol.Category{symbol.Spot, symbol.Perp}}, REST: src},
			{Venue: symbol.Venue{Name: "Okx", Categories: []symbol.Category{symbol.Spot}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	all, err := SelectFeeds(reg, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := feedNames(all); got != "Bybit/Spot,Bybit/Perp" {
		t.Fatalf("feeds %s", got)
	}
	if len(all) != 2 {
		t.Fatalf("feeds %d, want the venue that has a reader", len(all))
	}

	one, err := SelectFeeds(reg, "Bybit/Perp")
	if err != nil {
		t.Fatal(err)
	}
	if feedNames(one) != "Bybit/Perp" {
		t.Fatalf("feeds %s", feedNames(one))
	}

	for _, spec := range []string{"Bybit", "Bybit/Option", "Okx/Spot", "Nope/Spot", "Bybit/Spot,Bybit/Spot", "Bybit/Spot,", "/Spot"} {
		if _, err := SelectFeeds(reg, spec); err == nil {
			t.Fatalf("spec %q was accepted", spec)
		}
	}
}

func feedNames(feeds []Feed) string {
	parts := make([]string, len(feeds))
	for i, f := range feeds {
		parts[i] = feedKey(f.Venue, f.Category)
	}
	return strings.Join(parts, ",")
}
