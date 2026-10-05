package bybit

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

const Exchange = "bybit"

// Venue is Bybit's registry entry.
var Venue = symbol.Venue{
	Name:  "Bybit",
	Label: "Bybit",
	// Unified account: every book behind one credential, and the same ExchTicker can name two of
	// them (Bybit_Spot_BTCUSDT and Bybit_Perp_BTCUSDT are both "BTCUSDT").
	Categories: []symbol.Category{symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option},
	KeyTypes:   []string{"hmac"},
}

// Venues declares Bybit's venue for exchange.NewRegistry.
func Venues() []exchange.Venue {
	return []exchange.Venue{{
		Venue:    Venue,
		Exchange: Exchange,
		Market:   &market{},
		REST:     &rest{},
		Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
			return &account{cfg: cfg}, nil
		},
		History: func(cfg exchange.AccountConfig) (exchange.History, error) {
			return &history{cfg: cfg}, nil
		},
	}}
}
