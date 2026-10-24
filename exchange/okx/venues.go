package okx

import (
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

const Exchange = "okx"

// Name is both OKX's venue and its account kind (E03-D001). The token is "Okx": venue names are
// CamelCase (E02-D001), so a ticker reads Okx_Perp_BTCUSDT.
const Name = "Okx"

// Declare is OKX's contribution to exchange.NewRegistry.
func Declare() exchange.Declaration {
	return exchange.Declaration{
		Exchange: Exchange,
		Venues: []exchange.Venue{{
			Venue: symbol.Venue{
				Name:  Name,
				Label: "OKX",
				// Unified account: every book behind one credential.
				Categories: []symbol.Category{symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option},
			},
			Market: &market{},
			REST:   &rest{},
		}},
		AccountKinds: []exchange.AccountKind{{
			Name:       Name,
			Label:      "OKX",
			Trades:     []string{Name},
			KeyTypes:   []exchange.KeyType{exchange.KeyHMAC},
			Passphrase: true,
			Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
				if _, err := cfg.Window(); err != nil {
					return nil, err
				}
				return &account{cfg: cfg}, nil
			},
			History: func(cfg exchange.AccountConfig) (exchange.History, error) {
				return &history{cfg: cfg}, nil
			},
		}},
	}
}
