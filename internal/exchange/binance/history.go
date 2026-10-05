package binance

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
)

func historyFactory(p product) exchange.HistoryFactory {
	return func(cfg exchange.AccountConfig) (exchange.History, error) {
		return &history{p: p, cfg: cfg}, nil
	}
}

// history reads myTrades (spot) / userTrades (futures) by fromId, ascending; the trade id is the
// same "id" the private stream reports as "t", which is what D20 needs for fill_id.
type history struct {
	p   product
	cfg exchange.AccountConfig
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits { return exchange.HistoryTraits{} }

func (h *history) Fills(context.Context, exchange.FillQuery) (*exchange.FillPage, error) {
	return nil, exchange.ErrNotImplemented
}

func (h *history) Order(context.Context, exchange.Symbol, string) (*exchange.OrderUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
