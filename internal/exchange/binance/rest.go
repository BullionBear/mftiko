package binance

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
)

// rest is the public REST reader: api (spot), fapi (UM), dapi (CM), eapi (EO).
type rest struct{ p product }

var _ exchange.PublicREST = (*rest)(nil)

// Instruments: exchangeInfo.
func (r *rest) Instruments(context.Context) ([]exchange.Instrument, error) {
	return nil, exchange.ErrNotImplemented
}

// Depth: GET depth; Last is lastUpdateId.
func (r *rest) Depth(context.Context, exchange.Symbol, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) Klines(context.Context, exchange.Symbol, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) KlinePage() int { return 0 }

// OpenInterest: UM and CM do not push open interest (D12), so it is query-only here.
func (r *rest) OpenInterest(context.Context, exchange.Symbol) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
