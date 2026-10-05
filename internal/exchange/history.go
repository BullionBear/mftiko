package exchange

import (
	"context"
	"time"

	"github.com/BullionBear/mftiko/internal/symbol"
)

// HistoryFactory builds a History for one api_id. The TD agent calls it for backfill without
// starting an account worker (D20), so it takes the same AccountConfig.
type HistoryFactory func(cfg AccountConfig) (History, error)

// History is a venue's private trade history over REST, read by the TD agent's backfill jobs (D20).
// Safe for concurrent use. The job owns the cursor; History only reads one page at a time.
type History interface {
	Traits() HistoryTraits

	// Fills reads one page of the account's fills on one instrument. A backfill scope is
	// (api_id, UniversalTicker) (D20); the job resolves it and passes the Info. Continue with
	// FillPage.Next while catching up; once caught up, the job re-reads from the confirmed line by
	// time (D20).
	Fills(ctx context.Context, q FillQuery) (*FillPage, error)

	// Order looks up one order by venue id. The job asks for every order_id that has fills but no
	// terminal row in orders yet, and stores it once terminal with filled_qty > 0 (D20).
	Order(ctx context.Context, inst *symbol.Info, orderID string) (*OrderUpdate, error)
}

// HistoryTraits tell the backfill job how to walk a venue's history.
type HistoryTraits struct {
	// Descending: pages come newest first. The confirmed line cannot move until a scan reaches its
	// end (D20).
	Descending bool
	// ByID: Fills accepts FillQuery.FromID, so catching up continues by id instead of by time.
	ByID bool
	// MaxWindow is the widest [StartNs, EndNs) one request may span (Binance futures: 7 days).
	// 0 = unlimited.
	MaxWindow time.Duration
	// Retention is how far back the venue keeps fills. AccountSettings.backfill_interval_s must be
	// well below it (D20).
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
