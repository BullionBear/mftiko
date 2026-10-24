package tdpath

import tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"

// Origin says where an order's change came from. The values match sdk.Origin.
type Origin int

const (
	// FromTD is a td.evt OrderUpdate.
	FromTD Origin = iota + 1
	// LocalReject is td.order's 1xx, or no responder (101).
	LocalReject
	// Timeout is a Submit whose wait ended with no answer (E08-D043, E06-D005).
	Timeout
)

// TD is what every account event carries. Seq and Incarnation are 0 on an event this
// package made itself (a local reject, a timeout).
type TD struct {
	APIID       int64
	Incarnation uint32
	Seq         uint64
}

// OrderEvent is one change of one of the strategy's orders.
type OrderEvent struct {
	TD
	Origin Origin
	Order  *tdv1.Order
	Cause  tdv1.OrderCause
	Code   int32
	Reason string
}

// FillEvent is one fill of one of the strategy's orders.
type FillEvent struct {
	TD
	Fill *tdv1.Fill
}

// PositionEvent carries the account's positions that changed.
type PositionEvent struct {
	TD
	Positions []*tdv1.Position
}

// BalanceEvent carries the account's balances that changed.
type BalanceEvent struct {
	TD
	Balances []*tdv1.Balance
}

// ReconEvent is an account snapshot. OpenOrders have been filtered to this session's
// algos. Positions and balances are the whole account's, unchanged (E06-D033).
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

// RejectEvent is a cancel request TD refused locally. No order changed.
type RejectEvent struct {
	TD
	Request        RejectedRequest
	ClientOrderIDs []string
	Code           int32
	Reason         string
}

// Hooks are the strategy callbacks. A nil func is skipped. Each func runs on the
// dispatcher, and the values it is given stay valid after it returns.
type Hooks struct {
	Order    func(OrderEvent)
	Fill     func(FillEvent)
	Position func(PositionEvent)
	Balance  func(BalanceEvent)
	Recon    func(ReconEvent)
	Reject   func(RejectEvent)
}
