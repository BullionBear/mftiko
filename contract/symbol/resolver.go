package symbol

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotImplemented = errors.New("symbol: not implemented")
	ErrInvalidTicker  = errors.New("symbol: invalid universal ticker")
	// ErrNotFound: SYM has no such instrument, or it is no longer listed. A lease item asking for
	// it is REJECTED (PROTOCOL §3.5); an order for it is rejected 1xx.
	ErrNotFound = errors.New("symbol: no such instrument")
)

// DefaultTTL is how long a Resolver trusts a cached answer. Listings are near-static.
const DefaultTTL = 10 * time.Minute

// ListPage is the most instruments one sym reply carries, so a venue with thousands of listings
// with every filter still fits in NATS's 1 MiB.
const ListPage = 500

// Resolver is the cached client of SYM, used by the MD agent, the TD agent, account workers, the
// API and md.fetch. Safe for concurrent use.
type Resolver interface {
	// Get is one instrument by exact ticker: a single request, never a whole venue table.
	// Inactive instruments are ErrNotFound unless includeInactive (MD's expiry watch needs them).
	Get(ctx context.Context, t UniversalTicker, includeInactive bool) (*Info, error)

	// Reverse turns a venue's spelling back into a ticker, from the (venue, category) table.
	Reverse(ctx context.Context, key ExchKey) (UniversalTicker, error)

	// List is every active instrument of one (venue, category), paged under the hood. Selectors
	// evaluate on it (D9).
	List(ctx context.Context, venue string, c Category) ([]Info, error)

	// Invalidate drops the cache for a venue, or all venues when venue is "".
	Invalidate(venue string)
}
