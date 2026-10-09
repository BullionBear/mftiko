package bybit

import (
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

// Listing: /v5/market/instruments-info, cursor paginated. Perp and Future both read category=linear
// and split by contractType.
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

// OpenInterest: the linear and inverse tickers stream carries open interest, so this is
// ErrUnsupported there once implemented (D401).
func (r *rest) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrNotImplemented
}

func (r *rest) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
