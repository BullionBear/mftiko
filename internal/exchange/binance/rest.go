package binance

import (
	"context"

	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

// rest is the public REST reader: api (spot), fapi (UM), dapi (CM), eapi (EO).
type rest struct{ p product }

var _ exchange.PublicREST = (*rest)(nil)

// Listing: exchangeInfo. UM and CM list perpetual and dated contracts together; the category picks
// them apart by contractType.
func (r *rest) Listing(context.Context, symbol.Category) ([]symbol.Listing, error) {
	return nil, exchange.ErrNotImplemented
}

// Depth: GET depth; Last is lastUpdateId.
func (r *rest) Depth(context.Context, *symbol.Info, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) Klines(context.Context, *symbol.Info, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}

func (r *rest) KlinePage() int { return 0 }

// OpenInterest: UM and CM do not push open interest (D12), so it is query-only here.
func (r *rest) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
