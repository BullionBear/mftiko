package sdk

import "time"

// Context is what a hook may do. Every method except Post must be called from a hook (D803).
type Context interface {
	SessionID() string
	// Params are the strategy's own parameters from the session spec; the platform does not read them.
	Params() []byte

	// Submit places an order. AlgoID must be one declared for the account (D602).
	Submit(o Order) (clientOrderID string, err error)
	Cancel(apiID int64, clientOrderID string) error
	// BatchCancel cancels every open order on the account whose client_order_id carries algoID,
	// whichever session or incarnation placed it (D602).
	BatchCancel(apiID int64, algoID uint32, tickers ...string) error

	// After schedules fn on the dispatcher. Repeats of one timer coalesce (D803).
	After(d time.Duration, fn func(Context))
	// Post hands fn to the dispatcher from any goroutine. It is the only thread-safe entry.
	Post(fn func(Context))

	// Exit ends the session as DONE; Fail ends it as FAILED (PROTOCOL §5.3).
	Exit()
	Fail(reason string)

	// VolumeDir is the session's writable directory, kept across incarnations (D904).
	VolumeDir() string
	// InputDir is where the named input from the spec was unpacked, read-only (D904).
	InputDir(name string) string
}

// Order is a new order.
type Order struct {
	APIID      int64
	AlgoID     uint32
	Ticker     string // UniversalTicker
	Side       Side
	Type       OrderType
	Qty        float64 // contract_value_ccy (D203); snapped to the instrument's qty_step by TD (D008)
	Price      float64 // price_ccy; decimal.Absent (NaN) for market orders
	ReduceOnly bool
}

type (
	Side      int
	OrderType int
)
