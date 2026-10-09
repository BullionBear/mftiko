package okx

import (
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

// Listing: /api/v5/public/instruments, instType from the category (SPOT, SWAP, FUTURES, OPTION;
// Perp and Inverse are both SWAP, split by settle currency).
func (r *rest) Listing(symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return exchange.Call[exchange.Page[symbol.Listing]]{}, exchange.ErrNotImplemented
}

// Depth: the conn worker does not need it (SnapshotStream).
func (r *rest) Depth(*symbol.Info, int) (exchange.Call[*exchange.BookUpdate], error) {
	return exchange.Call[*exchange.BookUpdate]{}, exchange.ErrNotImplemented
}

func (r *rest) Klines(*symbol.Info, string, int64, int) (exchange.Call[[]exchange.Kline], error) {
	return exchange.Call[[]exchange.Kline]{}, exchange.ErrNotImplemented
}

func (r *rest) KlinePage() int { return 0 }

// OpenInterest: OKX pushes open-interest, so this is ErrUnsupported once implemented (D401).
func (r *rest) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrNotImplemented
}

func (r *rest) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
