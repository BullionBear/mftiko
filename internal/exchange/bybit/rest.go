package bybit

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
)

type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

// Instruments: /v5/market/instruments-info, once per category, cursor paginated.
func (r *rest) Instruments(context.Context) ([]exchange.Instrument, error) {
	return nil, exchange.ErrNotImplemented
}

// Depth: the conn worker does not need it (SnapshotStream).
func (r *rest) Depth(context.Context, exchange.Symbol, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) Klines(context.Context, exchange.Symbol, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) KlinePage() int { return 0 }

// OpenInterest: the linear and inverse tickers stream carries open interest, so this is
// ErrUnsupported there once implemented (D12).
func (r *rest) OpenInterest(context.Context, exchange.Symbol) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
