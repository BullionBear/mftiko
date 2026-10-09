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
	// Get resolves a UniversalTicker at the config and spec load boundary (D38): the only
	// place a human-written ticker becomes a UID. Planes that already hold a UID call
	// GetByUID instead. One row, never a whole venue table. The Resolver reads the active
	// table only: PRELISTING, TRADABLE, HALTED, SETTLING, UNKNOWN. A terminal listing
	// (EXPIRED, DELISTED) is ErrNotFound unless includeInactive, which is historical
	// lookup (old fills and positions, and MD's expiry watch). Not implemented yet.
	Get(ctx context.Context, t UniversalTicker, includeInactive bool) (*Info, error)

	// GetByUID is the internal lookup. STS, TD and MD pass this integer, not the ticker
	// string (D38). includeInactive has the same meaning as on Get. uid 0 is ErrNotFound.
	// Not implemented yet.
	GetByUID(ctx context.Context, uid uint64, includeInactive bool) (*Info, error)

	// Reverse turns a venue's spelling into the listing. Callers keep Info.UID (D38).
	// The hot path reads the active table. Historical reverse lookup of an EXPIRED or
	// DELISTED listing reads the terminal table; the UID is the one assigned when that
	// listing was created, not a new one. Not implemented yet.
	Reverse(ctx context.Context, key ExchKey) (*Info, error)

	// List is the active table of one (venue, category), paged under the hood: every non-terminal
	// status, not only TRADABLE. Selectors evaluate on it (D9). Only TRADABLE may trade.
	List(ctx context.Context, venue string, c Category) ([]Info, error)

	// Invalidate drops the cache for a venue, or all venues when venue is "".
	Invalidate(venue string)
}
