package binance

import (
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

const Exchange = "binance"

// Venue names. The account kinds below reuse four of them as their own names: two enums (E03-D001).
const (
	VenueSpot = "Binance"
	VenueUM   = "BinanceUM"
	VenueCM   = "BinanceCM"
	VenueEO   = "BinanceEO"
)

// product is what differs between Binance's market venues; the codecs are shared.
type product struct {
	venue  symbol.Venue
	routes []string // public market-data routes
	budget int      // streams per connection
}

var (
	spot = product{
		venue:  symbol.Venue{Name: VenueSpot, Label: "Binance Spot", Categories: []symbol.Category{symbol.Spot}},
		routes: []string{exchange.RouteDefault}, budget: 1024,
	}
	// fstream.binance.com, split by path since 2026-03.
	um = product{
		venue:  symbol.Venue{Name: VenueUM, Label: "Binance USDⓈ-M", Categories: []symbol.Category{symbol.Perp, symbol.Future}},
		routes: []string{"public", "market"}, budget: 1024,
	}
	// dstream.binance.com: no path split. TODO(verify): streams per connection.
	cm = product{
		venue:  symbol.Venue{Name: VenueCM, Label: "Binance COIN-M", Categories: []symbol.Category{symbol.Inverse, symbol.Future}},
		routes: []string{exchange.RouteDefault}, budget: 1024,
	}
	// fstream.binance.com/market for index, kline, mark price, open interest. TODO(verify): where
	// options depth and trades live, and streams per connection.
	eo = product{
		venue:  symbol.Venue{Name: VenueEO, Label: "Binance Options", Categories: []symbol.Category{symbol.Option}},
		routes: []string{"market"}, budget: 1024,
	}
)

// kind is one Binance account kind: an API key's product line and the venues it trades.
type kind struct {
	name   string
	label  string
	trades []string
}

var (
	kindSpot = kind{name: "Binance", label: "Binance Spot", trades: []string{VenueSpot}}
	kindUM   = kind{name: "BinanceUM", label: "Binance USDⓈ-M", trades: []string{VenueUM}}
	kindCM   = kind{name: "BinanceCM", label: "Binance COIN-M", trades: []string{VenueCM}}
	kindEO   = kind{name: "BinanceEO", label: "Binance Options", trades: []string{VenueEO}}
	// Portfolio Margin: one account over spot margin, USDⓈ-M and COIN-M, all through papi. Its
	// adapter routes each request by the ticker's venue (E03-D001).
	kindPM = kind{name: "BinancePM", label: "Binance Portfolio Margin", trades: []string{VenueSpot, VenueUM, VenueCM}}
)

// Declare is Binance's contribution to exchange.NewRegistry.
func Declare() exchange.Declaration {
	d := exchange.Declaration{Exchange: Exchange}
	for _, p := range []product{spot, um, cm, eo} {
		d.Venues = append(d.Venues, exchange.Venue{
			Venue:  p.venue,
			Market: &market{p: p},
			REST:   &rest{p: p},
		})
	}
	for _, k := range []kind{kindSpot, kindUM, kindCM, kindEO, kindPM} {
		d.AccountKinds = append(d.AccountKinds, exchange.AccountKind{
			Name:   k.name,
			Label:  k.label,
			Trades: k.trades,
			// The adapter signs with ed25519 only (E03-D003); an HMAC account would never connect (#385).
			KeyTypes: []exchange.KeyType{exchange.KeyEd25519},
			Account:  accountFactory(k),
			History:  historyFactory(k),
		})
	}
	return d
}
