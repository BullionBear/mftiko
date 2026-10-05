package bybit

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

// history reads /v5/execution/list; fill_id is execId (D20).
type history struct{ cfg exchange.AccountConfig }

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits { return exchange.HistoryTraits{} }

func (h *history) Fills(context.Context, exchange.FillQuery) (*exchange.FillPage, error) {
	return nil, exchange.ErrNotImplemented
}

func (h *history) Order(context.Context, *symbol.Info, string) (*exchange.OrderUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
