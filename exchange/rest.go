package exchange

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// PublicREST is a venue's public REST, sans-I/O (D302): each method builds a Call, and the caller sends
// it. It is code, not a service (D402): the conn worker runs Depth in process for book snapshots,
// md.fetch runs Klines and OpenInterest, SYM runs Listing (through ListingOver). Public requests are
// not signed, so those methods do not take the time. Usage takes when the response arrived, so it
// can fill ResetAt without reading a clock (D420). Safe for concurrent use.
type PublicREST interface {
	// Listing is every instrument the venue lists in one category, canonicalized (D201), one page at a
	// time. One (venue, category) is the unit SYM refreshes and delists within: a spot refresh must
	// not deactivate perp rows just because they were not in a spot response. The caller reads every
	// page (Collect); a partial listing would make SYM delist the rest.
	Listing(c symbol.Category) (Call[Page[symbol.Listing]], error)

	// Depth is a full book snapshot for SnapshotREST venues, with BookUpdate.Snapshot set and Last
	// set to the venue's update id (Binance lastUpdateId).
	Depth(inst *symbol.Info, limit int) (Call[*BookUpdate], error)

	// Klines is one page of closed klines ending at or before endNs (0 = the latest), oldest first,
	// at most min(limit, KlinePage()). md.fetch pages back from the earliest it got.
	Klines(inst *symbol.Info, interval string, endNs int64, limit int) (Call[[]Kline], error)
	KlinePage() int

	// OpenInterest is a query-only value on venues that do not push it (D401). ErrUnsupported
	// elsewhere.
	OpenInterest(inst *symbol.Info) (Call[*OpenInterest], error)

	// Usage reads the rate-limit usage the venue reports in a response; nil when it reports none.
	// now is when the response arrived. The adapter does not read a clock (D302). Each Used.ResetAt
	// is when that counter returns to zero; the zero time means the venue did not say (D420).
	Usage(resp *HTTPResponse, now time.Time) []Used
}

// InProcessUIDSource is a listing whose instruments are not a real venue. The in-process Resolver
// allocates symbol UIDs only for a source that implements this. exchangetest.Source is that source.
// Those UIDs exist only inside the process that allocated them: they are not written to a database
// and must not be sent to another process or plane. A real adapter must not implement this. Its
// instruments take the UIDs SYM assigns.
type InProcessUIDSource interface {
	Lister
	// InProcessUIDs marks the source. It has no behavior.
	InProcessUIDs()
}
