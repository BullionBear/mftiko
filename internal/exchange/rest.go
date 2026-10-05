package exchange

import "context"

// PublicREST is a venue's public REST reader. It is code, not a service (D21): the conn worker calls
// it in process for book snapshots, md.fetch calls it to answer FetchRequest, SYM calls it for the
// listing. Safe for concurrent use.
type PublicREST interface {
	// Instruments is the venue's full listing with filters; SYM normalizes it into the symbol model
	// and builds the SymbolResolver tables from it (D11).
	Instruments(ctx context.Context) ([]Instrument, error)

	// Depth is a full book snapshot for SnapshotREST venues, with BookUpdate.Snapshot set and Last
	// set to the venue's update id (Binance lastUpdateId).
	Depth(ctx context.Context, sym Symbol, limit int) (*BookUpdate, error)

	// Klines returns the latest limit closed klines, oldest first. limit may exceed the venue's
	// page size; md.fetch pages with KlinePage.
	Klines(ctx context.Context, sym Symbol, interval string, limit int) ([]Kline, error)
	KlinePage() int

	// OpenInterest is a query-only value on venues that do not push it (D12). ErrUnsupported
	// elsewhere.
	OpenInterest(ctx context.Context, sym Symbol) (*OpenInterest, error)
}

// Instrument is one row of a venue listing, before SYM normalizes it.
type Instrument struct {
	Symbol      Symbol
	Status      string // the venue's own value: TRADING, live, Trading, ...
	Base        string
	Quote       string
	Settle      string // derivatives only
	TickSize    string
	StepSize    string
	MinQty      string
	MinNotional string
	ContractVal string // contract size or multiplier; "" for spot
	ExpiryNs    int64  // 0 = no expiry
	Strike      string // options only
	OptionType  string // C | P
	Raw         []byte // the venue's JSON for this row, so SYM can read what this struct lacks
}
