package okx

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
)

type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

// Instruments: /api/v5/public/instruments, once per instType.
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

// OpenInterest: OKX pushes open-interest, so this is ErrUnsupported once implemented (D12).
func (r *rest) OpenInterest(context.Context, exchange.Symbol) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
