package okx

import (
	"context"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// history reads /api/v5/trade/fills-history, newest first: HistoryTraits.Descending (D20).
// fill_id is tradeId, the same field the orders channel pushes.
type history struct{ cfg exchange.AccountConfig }

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits { return exchange.HistoryTraits{} }

func (h *history) Fills(context.Context, exchange.FillQuery) (*exchange.FillPage, error) {
	return nil, exchange.ErrNotImplemented
}

func (h *history) Order(context.Context, *symbol.Info, string) (*exchange.OrderUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
