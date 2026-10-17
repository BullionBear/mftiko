package sdk

import (
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

// ---------- market data ----------

// MD is what every market data event carries besides its payload: the instrument and the envelope
// (PROTOCOL §7.1). The exchange's own time is in the payload's Meta (exch_ts_ns).
type MD struct {
	UID       uint32 // the instrument, from the payload (DECISIONS D461); look it up with Context.Info
	MdTsNs    int64  // when the conn worker published it, on the MD host's clock
	ConnEpoch uint64 // with Seq, the transport's continuity; a gap means events were dropped or lost
	Seq       uint64
}

// Feed is one market data event: the envelope and the decoded payload. The payload is the strategy's
// own copy (DECISIONS D803); it stays valid after the hook returns.
type Feed[T any] struct {
	MD
	Data T
}

// The market data events, one per topic (PROTOCOL §2). orderbook_delta has none: the SDK applies it to
// the local book and calls OnBook.
type (
	BestQuote    = Feed[*mdv1.BestQuote]
	Trade        = Feed[*mdv1.Trade]
	AggTrade     = Feed[*mdv1.AggTrade]
	Kline        = Feed[*mdv1.Kline] // every kline_<interval> topic; the interval is in Data
	Ticker       = Feed[*mdv1.Ticker]
	Liquidation  = Feed[*mdv1.Liquidation]
	FundingRate  = Feed[*mdv1.FundingRate]
	OpenInterest = Feed[*mdv1.OpenInterest]
	Greeks       = Feed[*mdv1.Greeks]
	MarkPrice    = Feed[*mdv1.MarkPrice]
)

// BookUpdate says the local book of UID now includes the delta at (ConnEpoch, Seq). Read the book with
// Context.Book; it is current when OnBook is called.
type BookUpdate struct {
	MD
	ExchTsNs int64
}

// ---------- trading ----------

// TD is what every account event carries. Seq and Incarnation are the account worker's td.evt numbering;
// both are 0 on an event the SDK made itself (a local reject, a timeout).
type TD struct {
	APIID       int64 // the account: td.* messages do not carry it, the subject does (DECISIONS D633)
	Incarnation uint32
	Seq         uint64
}

// Origin says where an order's change came from.
type Origin int

const (
	// FromTD is a td.evt OrderUpdate.
	FromTD Origin = iota + 1
	// LocalReject is td.order's 1xx, or no responder (101): nothing was sent, the order is REJECTED.
	LocalReject
	// Timeout is td.order without an answer: the order is UNKNOWN until an OrderUpdate or a Recon says
	// otherwise. The SDK does not ask td.view or send it again (DECISIONS D605).
	Timeout
)

// OrderEvent is one change of one of the strategy's orders. Order is the state after the change, the
// same as Context.Order returns from inside the hook.
type OrderEvent struct {
	TD
	Origin Origin
	Order  *tdv1.Order
	Cause  tdv1.OrderCause // FromTD only
	Code   int32           // REJECTED and CANCEL_REJECTED: 1xx, 2xx or the exchange's code
	Reason string
}

// FillEvent is one fill of one of the strategy's orders. The order's new state comes as its own
// OrderEvent.
type FillEvent struct {
	TD
	Fill *tdv1.Fill
}

// PositionEvent carries the account's positions that changed, as absolute values.
type PositionEvent struct {
	TD
	Positions []*tdv1.Position
}

// BalanceEvent carries the account's balances that changed, as absolute values.
type BalanceEvent struct {
	TD
	Balances []*tdv1.Balance
}

// ReconEvent replaces the account's state (DECISIONS D603, D854): Recon.open_orders holds only the
// strategy's own algos; positions and balances are the whole account's. By the time OnRecon runs, the
// ledger and the OMS have been replaced with it. A td.view reply keeps a local pending_new that
// Submit recorded after that ReconRequest and that the snapshot does not list. A pushed Recon does not.
type ReconEvent struct {
	TD
	Recon *tdv1.Recon
}

// RejectedRequest is the kind of request a RejectEvent answers.
type RejectedRequest int

const (
	RejectedCancel RejectedRequest = iota + 1
	RejectedBatchCancel
)

// RejectEvent is a cancel request that TD refused locally (1xx): nothing was sent and no order
// changed. A rejected Submit is an OrderEvent instead.
type RejectEvent struct {
	TD
	Request        RejectedRequest
	ClientOrderIDs []string
	Code           int32
	Reason         string
}
