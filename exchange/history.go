package exchange

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// HistoryFactory builds a History for one api_id. The backfill read is a stateless service,
// like md.fetch (E06-D001), and does not start an account worker. Who calls it is open and is not
// the TD agent (E06-D038, E06-D039).
type HistoryFactory func(cfg AccountConfig) (History, error)

// History is a venue's private trade history over REST, sans-I/O (E03-D002), read by the backfill
// service (E06-D001). Safe for concurrent use. The caller owns the cursor; History builds one page's
// request at a time.
type History interface {
	Traits() HistoryTraits

	// Fills is one page of the account's fills on one instrument. A backfill scope is
	// (api_id, UniversalTicker) (E06-D001); the job resolves it and passes the Info. Continue with
	// FillPage.Next while catching up; once caught up, the job re-reads from the confirmed line by
	// time (E06-D001). The cursor is a string the job stores, not a Page: it outlives the process.
	Fills(now time.Time, q FillQuery) (Call[*FillPage], error)

	// Order looks up one order by venue id. The job asks for every order_id that has fills but no
	// terminal row in orders yet, and stores it once terminal with filled_qty > 0 (E06-D001).
	Order(now time.Time, inst *symbol.Info, orderID string) (Call[*OrderUpdate], error)

	// Usage reads the rate-limit usage the venue reports in a response; nil when it reports none.
	// now is when the response arrived. The adapter does not read a clock (E03-D002). Each Used.ResetAt
	// is when that counter returns to zero; the zero time means the venue did not say (E04-D020).
	Usage(resp *HTTPResponse, now time.Time) []Used
}

// HistoryTraits tell the backfill job how to walk a venue's history.
type HistoryTraits struct {
	// Descending: pages come newest first. The confirmed line cannot move until a scan reaches its
	// end (E06-D001).
	Descending bool
	// ByID: Fills accepts FillQuery.FromID, so catching up continues by id instead of by time.
	ByID bool
	// MaxWindow is the widest [StartNs, EndNs) one request may span (Binance futures: 7 days).
	// 0 = unlimited.
	MaxWindow time.Duration
	// Retention is how far back the venue keeps fills. AccountSettings.backfill_interval_s must be
	// well below it (E06-D001).
	Retention time.Duration
	PageLimit int
}

// FillQuery is one page request: Cursor, or FromID, or StartNs and EndNs.
type FillQuery struct {
	Instrument *symbol.Info
	Cursor     string // FillPage.Next of the previous page
	FromID     string
	StartNs    int64
	EndNs      int64
	Limit      int
}

type FillPage struct {
	Fills []Fill
	// Next continues the scan in the venue's own order; "" = no more pages in this query.
	Next string
}
