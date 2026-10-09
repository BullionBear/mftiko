package okx

import (
	"context"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

// Listing: /api/v5/public/instruments, instType from the category (SPOT, SWAP, FUTURES, OPTION;
// Perp and Inverse are both SWAP, split by settle currency).
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

// OpenInterest: OKX pushes open-interest, so this is ErrUnsupported once implemented (D401).
func (r *rest) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
