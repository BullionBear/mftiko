package binance

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

func historyFactory(k kind) exchange.HistoryFactory {
	return func(cfg exchange.AccountConfig) (exchange.History, error) {
		return &history{k: k, cfg: cfg}, nil
	}
}

// history reads myTrades (spot) / userTrades (futures; papi's um and cm userTrades for BinancePM) by
// fromId, ascending; the trade id is the same "id" the private stream reports as "t", which is
// what D20 needs for fill_id.
type history struct {
	k   kind
	cfg exchange.AccountConfig
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits { return exchange.HistoryTraits{} }

func (h *history) Fills(context.Context, exchange.FillQuery) (*exchange.FillPage, error) {
	return nil, exchange.ErrNotImplemented
}

func (h *history) Order(context.Context, *symbol.Info, string) (*exchange.OrderUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
