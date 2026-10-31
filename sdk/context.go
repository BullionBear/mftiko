package sdk

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/rs/zerolog"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/sdk/book"
)

// Context is what a hook may do. Every method except Post must be called from a hook (E08-D003). Nothing
// here blocks except Resolve, which only OnInit and OnStart may call.
//
// Instruments are UIDs everywhere (DECISIONS E08-D004): a strategy resolves the tickers of its own config
// once, keeps the UIDs, and reads current fields with Info when it needs them.
type Context interface {
	SessionID() string
	// Params are the strategy's own parameters from the session spec; the platform does not read them.
	Params() []byte
	// Log is the session logger (E15-D019). It carries component=strategy, this binary's version and
	// commit, and session_id. Logs go to stderr. Outside a hook, use the package Log, which has the
	// same fields except session_id. The level and format come from MFTIKO_LOG_LEVEL and MFTIKO_LOG_FORMAT.
	// zerolog's level methods have pointer receivers, so keep the returned value:
	//
	//	log := ctx.Log()
	//	log.Info().Msg("in a hook")
	Log() zerolog.Logger
	// Now is when the running hook started, in UTC. It does not move inside a hook. A replay returns
	// the recorded start, so a strategy that reads the time through Now decides the same way again
	// (E20-D004).
	Now() time.Time
	// Rand is the session's random source. Its seed is drawn once per worker and written to the
	// event log; a replay seeds it the same, so the same draws in the same hooks repeat (E20-D004).
	// It is not safe for concurrent use: draw from hooks only.
	Rand() *rand.Rand

	// ---------- instruments ----------

	// Resolve looks a UniversalTicker up in SYM (GetRequest). Only OnInit and OnStart may call it;
	// any other hook returns ErrNotInInit without calling SYM. It may block. A ticker SYM does not
	// know is not one the platform supports (the error wraps symbol.ErrNotFound). The strategy
	// returning that error ends the session INIT_FAILED.
	Resolve(ticker string) (symbol.Info, error)
	// Info is the instrument's current row (E08-D025). It reads memory, so any hook may call it; keep the
	// UID, not the Info, since status and filters change. A UID this session has not resolved returns
	// ErrUnknownUID and does not call SYM (E08-D024). A resolved instrument that later delists is still
	// returned, with its terminal status (E08-D026). Info does not retry.
	Info(uid uint32) (symbol.Info, error)

	// ---------- orders ----------

	// Submit places an order and returns its client_order_id. The order is in OpenOrders as PENDING_NEW
	// before Submit returns; what happens to it comes as OrderEvents. An error means nothing was sent
	// and is a mistake in the call: an algo not declared for the account, a UID the SDK does not know.
	Submit(o Order) (clientOrderID string, err error)
	// Cancel asks TD to cancel one of the strategy's orders. A local refusal comes as a RejectEvent.
	Cancel(apiID int64, clientOrderID string) error
	// BatchCancel cancels every open order of algoID on the account (only those of uids, when given),
	// whichever session or incarnation placed them. The SDK picks the orders from OpenOrders and splits
	// them by the account's cancel_batch (DECISIONS E06-D002, E06-D031).
	BatchCancel(apiID int64, algoID uint32, uids ...uint32) error
	// RequestRecon asks td.view for a new snapshot of the account; it arrives as OnRecon.
	RequestRecon(apiID int64) error

	// ---------- local state (current as of the event being handled) ----------

	// Order is one of the strategy's orders. The pointer is a copy; the read does not block
	// (DECISIONS E08-D056). Inside OnOrder it includes that event, and a terminal order is still
	// visible there. After the hook returns, a terminal order is gone.
	Order(apiID int64, clientOrderID string) (*tdv1.Order, bool)
	// OpenOrders are the account's live orders of algoID; 0 means every algo the session declared.
	// Each order is a copy. The read does not block (DECISIONS E08-D056). Terminal orders are omitted.
	OpenOrders(apiID int64, algoID uint32) []*tdv1.Order
	// Position is the account's position for uid, as last written by PositionUpdate or Recon.
	// A size of zero stays until a Recon omits it. The pointer is a copy; the read does not block
	// (DECISIONS E08-D053, E08-D056). An unknown account or uid returns false.
	Position(apiID int64, uid uint32) (*tdv1.Position, bool)
	// Balance is the account's balance of asset. free, locked and prelock are the values TD sent;
	// the SDK does not combine them. The pointer is a copy; the read does not block
	// (DECISIONS E08-D053, E08-D056). An unknown account or asset returns false.
	Balance(apiID int64, asset string) (*tdv1.Balance, bool)
	// Book is the local book of an instrument once md.view's snapshot is in place (E08-D057).
	// The view is the sdk/book ring and is valid only inside the hook (E08-D003). An instrument
	// that is not synced returns (nil, false) (E08-D060).
	Book(uid uint32) (BookView, bool)

	// ---------- reads (the answer runs fn on the dispatcher) ----------
	// Each call waits InitPayload.recv_window_ms. A timeout or a missing responder is an
	// error delivered to fn (DECISIONS E08-D048).

	// Tape pages md.view (DECISIONS E04-D036). The SDK does not fetch tape on its own and does not
	// hold live events (E08-D047). page.Head is the feed's high water; the strategy treats a live
	// cursor at or below that head as already covered.
	Tape(uid uint32, topic Topic, q TapeQuery, fn func(ctx Context, page *mdv1.TapePage, err error)) error
	// BookSnapshot asks md.view for the windowed book. The local book needs no help from it; this is for
	// strategies that want the conn worker's view.
	BookSnapshot(uid uint32, fn func(ctx Context, snap *mdv1.BookSnapshot, err error)) error
	// FetchKlines asks md.fetch for the latest limit closed klines (DECISIONS E04-D019).
	FetchKlines(uid uint32, interval string, limit uint32, fn func(ctx Context, k *mdv1.Klines, err error)) error
	// FetchOpenInterest asks md.fetch for the open interest the exchange does not push.
	FetchOpenInterest(uid uint32, fn func(ctx Context, oi *mdv1.OpenInterest, err error)) error

	// ---------- control ----------

	// After schedules fn on the dispatcher. Repeats of one timer coalesce (E08-D003).
	After(d time.Duration, fn func(Context))
	// Post hands fn to the dispatcher from any goroutine. It is the only thread-safe entry. The event
	// log numbers Posts in call order, and a replay runs the Post with the recorded number. Posts made
	// from hooks get the same numbers again; Posts from the strategy's own goroutines may not (E20-D004).
	Post(fn func(Context))
	// Exit ends the session as DONE; Fail ends it as FAILED (PROTOCOL §5.3).
	Exit()
	Fail(reason string)

	// VolumeDir is the session's writable directory, kept across incarnations (E09-D004).
	VolumeDir() string
	// InputDir is where the named input from the spec was unpacked, read-only (E09-D004).
	InputDir(name string) string
}

// Order is a new order, in the units of td.Submit (DECISIONS E02-D003).
type Order struct {
	APIID  int64
	AlgoID uint32 // one declared for the account (E06-D002)
	UID    uint32
	Side   commonv1.Side
	Type   commonv1.OrderType
	TIF    commonv1.TimeInForce
	// Qty is in contract_value_ccy and QuoteQty in quote; set one, and leave the other decimal.Absent().
	Qty      float64
	QuoteQty float64
	Price    float64 // price_ccy; decimal.Absent() for a market order
	// RecvWindowMs is how long the request stays valid at the exchange; 0 is the TD instance's default
	// (E06-D020).
	RecvWindowMs int64
	ReduceOnly   bool
	// Params go to the exchange adapter as they are. Position-side keys are refused (E06-D007).
	Params map[string]string
}

// BookView is the read side of a local book.
type BookView interface {
	TickSize() float64
	Window() int
	Price(tick int64) float64
	BestBidTick() (tick int64, ok bool)
	BestAskTick() (tick int64, ok bool)
	Qty(bid bool, price float64) (float64, bool)
	Bids(limit int) book.Iter
	Asks(limit int) book.Iter
}

var _ BookView = (*book.OrderBook)(nil)

// Topic is a market data topic (PROTOCOL §2).
type Topic string

const (
	TopicBestQuote      Topic = "bestquote"
	TopicTrade          Topic = "trade"
	TopicAggTrade       Topic = "aggtrade"
	TopicOrderBookDelta Topic = "orderbook_delta"
	TopicTicker         Topic = "ticker"
	TopicLiquidation    Topic = "liquidation"
	TopicFundingRate    Topic = "funding_rate"
	TopicOpenInterest   Topic = "open_interest"
	TopicGreeks         Topic = "greeks"
	TopicMarkPrice      Topic = "mark_price"
)

// KlineTopic is the topic of klines of one interval, such as "1m" or "4h".
func KlineTopic(interval string) Topic { return Topic("kline_" + interval) }

// TapeQuery is one page of md.view's tape (read.v1 ViewRequest).
type TapeQuery struct {
	Limit   uint32       // 0 is 1000; at most 5000
	Before  *mdv1.Cursor // the previous page's next; nil for the newest page
	SinceNs int64        // only events with md_ts_ns ≥ SinceNs; 0 for no bound
}

// Errors a Context method returns synchronously. They are mistakes in the call, not outcomes: TD's
// answers come as events.
var (
	ErrNotInInit       = errors.New("sdk: only OnInit and OnStart may call this")
	ErrUnknownUID      = errors.New("sdk: the instrument is not known to this session")
	ErrUnknownAccount  = errors.New("sdk: the account is not bound to this session")
	ErrAlgoNotDeclared = errors.New("sdk: the algo_id is not declared for this account")
	ErrNoFeed          = errors.New("sdk: the session has no such feed")
	ErrStopping        = errors.New("sdk: the session is stopping")
)
