package exchange

import (
	"context"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// PublicREST is a venue's public REST reader. It is code, not a service (D402): the conn worker calls
// it in process for book snapshots, md.fetch calls it to answer FetchRequest, SYM calls it for the
// listing. Safe for concurrent use.
type PublicREST interface {
	// Listing is every instrument the venue lists in one category, canonicalized (D201). One
	// (venue, category) is the unit SYM refreshes and delists within: a spot refresh must not
	// deactivate perp rows just because they were not in a spot response. A paginated listing is
	// read to the end or fails; a partial one would make SYM delist the rest.
	Listing(ctx context.Context, c symbol.Category) ([]symbol.Listing, error)

	// Depth is a full book snapshot for SnapshotREST venues, with BookUpdate.Snapshot set and Last
	// set to the venue's update id (Binance lastUpdateId).
	Depth(ctx context.Context, inst *symbol.Info, limit int) (*BookUpdate, error)

	// Klines returns the latest limit closed klines, oldest first. limit may exceed the venue's
	// page size; md.fetch pages with KlinePage.
	Klines(ctx context.Context, inst *symbol.Info, interval string, limit int) ([]Kline, error)
	KlinePage() int

	// OpenInterest is a query-only value on venues that do not push it (D401). ErrUnsupported
	// elsewhere.
	OpenInterest(ctx context.Context, inst *symbol.Info) (*OpenInterest, error)
}

// InProcessUIDSource is a PublicREST whose listings are not a real venue.
// The in-process Resolver allocates symbol UIDs only for a source that implements
// this. exchangetest.Source is that source. Those UIDs exist only inside the
// process that allocated them: they are not written to a database and must not be
// sent to another process or plane. A real adapter must not implement this.
// Its instruments take the UIDs SYM assigns.
type InProcessUIDSource interface {
	PublicREST
	// InProcessUIDs marks the source. It has no behavior.
	InProcessUIDs()
}
