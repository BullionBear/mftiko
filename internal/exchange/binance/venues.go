package binance

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

const Exchange = "binance"

var keyTypes = []string{"hmac", "ed25519"}

// product is what differs between Binance's venues; the codecs are shared.
type product struct {
	venue  symbol.Venue
	routes []string // public market-data routes
	budget int      // streams per connection
}

func venue(name, label string, cats ...symbol.Category) symbol.Venue {
	return symbol.Venue{Name: name, Label: label, Categories: cats, KeyTypes: keyTypes}
}

var (
	spot = product{
		venue:  venue("Binance", "Binance Spot", symbol.Spot),
		routes: []string{exchange.RouteDefault}, budget: 1024,
	}
	um = product{
		venue:  venue("BinanceUM", "Binance USDⓈ-M", symbol.Perp, symbol.Future),
		routes: []string{"public", "market"}, budget: 1024,
	}
	// TODO(verify): CM and EO after Binance's 2026 changes (see doc.go, DECISIONS Q20).
	cm = product{
		venue:  venue("BinanceCM", "Binance COIN-M", symbol.Inverse, symbol.Future),
		routes: []string{"public", "market"}, budget: 1024,
	}
	eo = product{
		venue:  venue("BinanceEO", "Binance Options", symbol.Option),
		routes: []string{"market"}, budget: 1024,
	}
	// Portfolio Margin trades the UM and CM books through one account. How its tickers relate to
	// BinanceUM's and BinanceCM's is part of DECISIONS Q20.
	pm = product{venue: venue("BinancePM", "Binance Portfolio Margin", symbol.Perp, symbol.Future, symbol.Inverse)}
)

// Venues declares Binance's venues for exchange.NewRegistry.
func Venues() []exchange.Venue {
	out := make([]exchange.Venue, 0, 5)
	for _, p := range []product{spot, um, cm, eo} {
		out = append(out, exchange.Venue{
			Venue:    p.venue,
			Exchange: Exchange,
			Market:   &market{p: p},
			REST:     &rest{p: p},
			Account:  accountFactory(p),
			History:  historyFactory(p),
		})
	}
	// No public market data of its own: market data comes from BinanceUM and BinanceCM.
	out = append(out, exchange.Venue{
		Venue:    pm.venue,
		Exchange: Exchange,
		Account:  accountFactory(pm),
		History:  historyFactory(pm),
	})
	return out
}
