package binance

import "github.com/BullionBear/mftiko/internal/exchange"

const Exchange = "binance"

// product is what differs between Binance's venues; the codecs are shared.
type product struct {
	venue  string
	routes []string // public market-data routes
	budget int      // streams per connection
}

var (
	spot = product{venue: "Binance", routes: []string{exchange.RouteDefault}, budget: 1024}
	um   = product{venue: "BinanceUM", routes: []string{"public", "market"}, budget: 1024}
	// TODO(verify): CM and EO after Binance's 2026 changes (see doc.go).
	cm = product{venue: "BinanceCM", routes: []string{"public", "market"}, budget: 1024}
	eo = product{venue: "BinanceEO", routes: []string{"market"}, budget: 1024}
	pm = product{venue: "BinancePM"}
)

// Venues declares Binance's venues for exchange.NewRegistry.
func Venues() []exchange.Venue {
	out := make([]exchange.Venue, 0, 5)
	for _, p := range []product{spot, um, cm, eo} {
		out = append(out, exchange.Venue{
			Name:     p.venue,
			Exchange: Exchange,
			Market:   &market{p: p},
			REST:     &rest{p: p},
			Account:  accountFactory(p),
			History:  historyFactory(p),
		})
	}
	// Portfolio Margin is an account over UM, CM and margin: no public market data of its own.
	out = append(out, exchange.Venue{
		Name:     pm.venue,
		Exchange: Exchange,
		Account:  accountFactory(pm),
		History:  historyFactory(pm),
	})
	return out
}
