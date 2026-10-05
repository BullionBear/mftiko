package bybit

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

const Exchange = "bybit"

// Name is both Bybit's venue and its account kind (D32).
const Name = "Bybit"

// Declare is Bybit's contribution to exchange.NewRegistry.
func Declare() exchange.Declaration {
	return exchange.Declaration{
		Exchange: Exchange,
		Venues: []exchange.Venue{{
			Venue: symbol.Venue{
				Name:  Name,
				Label: "Bybit",
				// Unified account: every book behind one credential, and the same ExchTicker can name
				// two of them (Bybit_Spot_BTCUSDT and Bybit_Perp_BTCUSDT are both "BTCUSDT").
				Categories: []symbol.Category{symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option},
			},
			Market: &market{},
			REST:   &rest{},
		}},
		AccountKinds: []exchange.AccountKind{{
			Name:     Name,
			Label:    "Bybit",
			Trades:   []string{Name},
			KeyTypes: []exchange.KeyType{exchange.KeyHMAC},
			Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
				return &account{cfg: cfg}, nil
			},
			History: func(cfg exchange.AccountConfig) (exchange.History, error) {
				return &history{cfg: cfg}, nil
			},
		}},
	}
}
