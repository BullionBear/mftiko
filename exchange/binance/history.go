package binance

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

func historyFactory(k kind) exchange.HistoryFactory {
	return func(cfg exchange.AccountConfig) (exchange.History, error) {
		return &history{k: k, cfg: cfg}, nil
	}
}

// history reads myTrades (spot) / userTrades (futures; papi's um and cm userTrades for BinancePM) by
// fromId, ascending; the trade id is the same "id" the private stream reports as "t", which is
// what D601 needs for fill_id.
type history struct {
	k   kind
	cfg exchange.AccountConfig
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits { return exchange.HistoryTraits{} }

func (h *history) Fills(time.Time, exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	return exchange.Call[*exchange.FillPage]{}, exchange.ErrNotImplemented
}

func (h *history) Order(time.Time, *symbol.Info, string) (exchange.Call[*exchange.OrderUpdate], error) {
	return exchange.Call[*exchange.OrderUpdate]{}, exchange.ErrNotImplemented
}

func (h *history) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
