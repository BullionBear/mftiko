package symbol

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotImplemented = errors.New("symbol: not implemented")
	ErrInvalidTicker  = errors.New("symbol: invalid universal ticker")
	// ErrNotFound: the active table has no such instrument (D38). A terminal listing (EXPIRED,
	// DELISTED) is also ErrNotFound unless the caller asked for historical lookup. A lease item
	// asking for it is REJECTED (PROTOCOL §3.5); an order for it is rejected 1xx. Quanto is not
	// stored.
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
	// The Resolver reads the active table only (D38): PRELISTING, TRADABLE, HALTED, SETTLING,
	// UNKNOWN. A terminal listing (EXPIRED, DELISTED) is ErrNotFound unless includeInactive.
	// That flag is historical lookup (old fills and positions, and MD's expiry watch). The
	// two-table store is not implemented yet.
	Get(ctx context.Context, t UniversalTicker, includeInactive bool) (*Info, error)

	// Reverse turns a venue's spelling back into a ticker. The hot path reads the active table.
	// Historical reverse lookup of an EXPIRED or DELISTED listing reads the terminal table; the
	// ticker string is the one it had when it was active (D38). That split is not implemented yet.
	Reverse(ctx context.Context, key ExchKey) (UniversalTicker, error)

	// List is the active table of one (venue, category), paged under the hood: every non-terminal
	// status, not only TRADABLE. Selectors evaluate on it (D9). Only TRADABLE may trade.
	List(ctx context.Context, venue string, c Category) ([]Info, error)

	// Invalidate drops the cache for a venue, or all venues when venue is "".
	Invalidate(venue string)
}
