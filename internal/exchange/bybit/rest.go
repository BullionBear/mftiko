package bybit

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

// Listing: /v5/market/instruments-info, cursor paginated. Perp and Future both read category=linear
// and split by contractType.
func (r *rest) Listing(context.Context, symbol.Category) ([]symbol.Listing, error) {
	return nil, exchange.ErrNotImplemented
}

// Depth: the conn worker does not need it (SnapshotStream).
func (r *rest) Depth(context.Context, *symbol.Info, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) Klines(context.Context, *symbol.Info, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) KlinePage() int { return 0 }

// OpenInterest: the linear and inverse tickers stream carries open interest, so this is
// ErrUnsupported there once implemented (D12).
func (r *rest) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
