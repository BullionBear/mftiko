package symbol

import (
	"context"
	"errors"
)

var (
	ErrNotImplemented = errors.New("symbol: not implemented")
	ErrInvalidTicker  = errors.New("symbol: invalid universal ticker")
	ErrInvalidListing = errors.New("symbol: invalid listing")
	ErrAsset          = errors.New("symbol: asset code is not canonical")
	ErrQuanto         = errors.New("symbol: quanto is not listed")
	ErrUnknownVenue   = errors.New("symbol: unknown venue")
	ErrCategory       = errors.New("symbol: venue does not trade category")
	// ErrNotFound: the active table has no such instrument (D203). A terminal listing (EXPIRED,
	// DELISTED) is also ErrNotFound unless the caller asked for historical lookup. A lease item
	// asking for it is REJECTED (PROTOCOL §3.5); an order for it is rejected 1xx. Quanto is not
	// stored.
	ErrNotFound = errors.New("symbol: no such instrument")
)

// ListPage is the most instruments one sym reply carries, so a venue with thousands of listings
// with every filter still fits in NATS's 1 MiB.
const ListPage = 500

// Resolver is the in-memory active table used by STS, MD and TD. The API does not
// query SYM (D202). A process loads the active rows of each (venue, category) it
// needs at startup and reloads that table when SYM reports a change (D202, D203).
// There is no per-lookup TTL. Safe for concurrent use.
//
// The table it keeps is the active one: PRELISTING, TRADABLE, HALTED, SETTLING,
// UNKNOWN. EXPIRED and DELISTED stay on the terminal table and are ErrNotFound
// unless the caller passes includeInactive. Only TRADABLE may trade.
type Resolver interface {
	// Get is the config and spec load boundary (D203): a human-written
	// UniversalTicker becomes Info, and the caller keeps Info.UID. Planes that
	// already hold a UID call GetByUID. The active table is the default; a
	// terminal listing is ErrNotFound unless includeInactive, which is historical
	// lookup (old fills and positions, and MD's expiry watch).
	Get(ctx context.Context, t UniversalTicker, includeInactive bool) (*Info, error)

	// GetByUID is the internal lookup. STS, TD and MD pass this integer, not the ticker
	// string (D203). includeInactive has the same meaning as on Get. uid 0 is not a
	// valid UID (ValidUID); it is ErrNotFound. Width and range are open (PROTOCOL §10 P13).
	GetByUID(ctx context.Context, uid uint64, includeInactive bool) (*Info, error)

	// Reverse turns a venue's spelling into the listing. Callers keep Info.UID (D203).
	// The hot path reads the active table. Historical reverse lookup of an EXPIRED or
	// DELISTED listing reads the terminal table; the UID is the one assigned when that
	// listing was created, not a new one.
	Reverse(ctx context.Context, key ExchKey) (*Info, error)

	// List is every non-terminal row of one (venue, category): the active table, not
	// only TRADABLE. Selectors evaluate on it (D901). Only TRADABLE may trade.
	// Paging under the hood uses ListPage.
	List(ctx context.Context, venue string, c Category) ([]Info, error)

	// Invalidate drops the cached active table for a venue, or every venue when
	// venue is "". The next read reloads. A SYM change notification is what prompts
	// the reload (D202); the notification itself is PROTOCOL §10 P7.
	Invalidate(venue string)
}
