package okx

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

const Exchange = "okx"

// Venue is OKX's registry entry. The token is "Okx": venue names are CamelCase (D11), so a ticker
// reads Okx_Perp_BTCUSDT.
var Venue = symbol.Venue{
	Name:  "Okx",
	Label: "OKX",
	// Unified account: every book behind one credential.
	Categories: []symbol.Category{symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option},
	KeyTypes:   []string{"hmac"},
	Passphrase: true,
}

// Venues declares OKX's venue for exchange.NewRegistry.
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
