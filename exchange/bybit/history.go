package bybit

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// history reads /v5/execution/list; fill_id is execId (D601).
type history struct{ cfg exchange.AccountConfig }

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits { return exchange.HistoryTraits{} }

func (h *history) Fills(time.Time, exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	return exchange.Call[*exchange.FillPage]{}, exchange.ErrNotImplemented
}

func (h *history) Order(time.Time, *symbol.Info, string) (exchange.Call[*exchange.OrderUpdate], error) {
	return exchange.Call[*exchange.OrderUpdate]{}, exchange.ErrNotImplemented
}

func (h *history) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
