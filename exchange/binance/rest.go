package binance

import (
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// rest is the public REST reader: api (spot), fapi (UM), dapi (CM), eapi (EO).
type rest struct{ p product }

var _ exchange.PublicREST = (*rest)(nil)

// Listing: exchangeInfo. UM and CM list perpetual and dated contracts together; the category picks
// them apart by contractType.
func (r *rest) Listing(symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return exchange.Call[exchange.Page[symbol.Listing]]{}, exchange.ErrNotImplemented
}

// Depth: GET depth; Last is lastUpdateId.
func (r *rest) Depth(*symbol.Info, int) (exchange.Call[*exchange.BookUpdate], error) {
	return exchange.Call[*exchange.BookUpdate]{}, exchange.ErrNotImplemented
}

func (r *rest) Klines(*symbol.Info, string, int64, int) (exchange.Call[[]exchange.Kline], error) {
	return exchange.Call[[]exchange.Kline]{}, exchange.ErrNotImplemented
}

func (r *rest) KlinePage() int { return 0 }

// OpenInterest: UM and CM do not push open interest (D401), so it is query-only here.
func (r *rest) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrNotImplemented
}

func (r *rest) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
