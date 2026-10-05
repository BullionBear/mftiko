package okx

import "github.com/BullionBear/mftiko/internal/exchange"

const (
	Exchange = "okx"
	Venue    = "OKX"
)

// Venues declares OKX's venue for exchange.NewRegistry.
func Venues() []exchange.Venue {
	return []exchange.Venue{{
		Name:     Venue,
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
