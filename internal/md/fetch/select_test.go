package fetch

import (
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

func TestSelectVenues(t *testing.T) {
	reg, err := exchange.NewRegistry(exchange.Declaration{
		Exchange: "fake",
		Venues: []exchange.Venue{
			{Venue: symbol.Venue{Name: "BinanceUM", Categories: []symbol.Category{symbol.Perp, symbol.Future}}, REST: stubREST{}},
			{Venue: symbol.Venue{Name: "Okx", Categories: []symbol.Category{symbol.Spot}}, REST: stubREST{}},
			{Venue: symbol.Venue{Name: "Bare", Categories: []symbol.Category{symbol.Spot}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := SelectVenues(reg, "BinanceUM, Okx")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "BinanceUM" || len(got[0].Categories) != 2 || got[1].Name != "Okx" || got[1].Categories[0] != symbol.Spot {
		t.Fatalf("%+v", got)
	}
	for _, spec := range []string{"", " ", "BinanceUM,", ",Okx", "BinanceUM,BinanceUM", "Nope", "Bare", "Binance.UM"} {
		if _, err := SelectVenues(reg, spec); err == nil {
			t.Fatalf("spec %q was accepted", spec)
		}
	}
}

type stubREST struct{}

func (stubREST) Listing(symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return exchange.Call[exchange.Page[symbol.Listing]]{}, exchange.ErrUnsupported
}

func (stubREST) Depth(*symbol.Info, int) (exchange.Call[*exchange.BookUpdate], error) {
	return exchange.Call[*exchange.BookUpdate]{}, exchange.ErrUnsupported
}

func (stubREST) Klines(*symbol.Info, string, int64, int) (exchange.Call[[]exchange.Kline], error) {
	return exchange.Call[[]exchange.Kline]{}, exchange.ErrUnsupported
}

func (stubREST) KlinePage() int { return 0 }

func (stubREST) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrUnsupported
}

func (stubREST) Usage(*exchange.HTTPResponse, time.Time) []exchange.Used { return nil }
