package exchange

import (
	"context"
	"time"
)

// AccountConfig is what the TD account worker (or the TD agent, for History) builds an adapter from.
type AccountConfig struct {
	APIID       int64
	Credentials Credentials
	// RESTEndpoint and StreamEndpoint override the venue's default hosts, the way
	// mdctl.InitPayload.url does for market data (D26). Empty means the default.
	RESTEndpoint   string
	StreamEndpoint string
}

// AccountFactory builds an Account for one api_id.
type AccountFactory func(cfg AccountConfig) (Account, error)

// Account is a venue's private trading interface for one api_id, owned by the TD account worker.
// Safe for concurrent use: the worker's HTTP pool calls it from several goroutines.
//
// It knows nothing about algo_id (D25). The worker checks algo ownership, and translates
// BatchCancel(algo_id) by filtering its OMS for open orders whose client_order_id carries that algo,
// then calling CancelBatch in chunks of Limits().CancelBatch.
type Account interface {
	Limits() AccountLimits

	// Submit sends one order. A venue refusal is a *Reject; any other error means the outcome is
	// unknown and the private stream or the next recon will tell.
	Submit(ctx context.Context, req *OrderRequest) (*OrderResult, error)

	Cancel(ctx context.Context, ref CancelRef) error

	// CancelBatch cancels up to Limits().CancelBatch orders in one venue request. The result has one
	// entry per ref, nil for success; the second return is for a failure of the whole request.
	CancelBatch(ctx context.Context, refs []CancelRef) ([]error, error)

	// Snapshot is the recon read (D27): open orders, positions, balances.
	Snapshot(ctx context.Context) (*AccountSnapshot, error)

	// Leverage answers td.view's EnsureLeverage.
	Leverage(ctx context.Context, sym Symbol) (string, error)

	// NewStream returns the private websocket codec for one connection.
	NewStream() (PrivateStream, error)
}

// AccountLimits are the venue's rules for one api_id.
type AccountLimits struct {
	ClientOrderIDMaxLen int         // the D25 client_order_id is at most 19 decimal digits
	CancelBatch         int         // orders per CancelBatch; 1 = the venue has no batch cancel
	OrderRate           []RateLimit // order requests, all must hold
}

// CancelOnDisconnect is implemented by venues with a dead-man switch, used for
// AccountSettings.cancel_on_disconnect_s. syms is ignored by venues whose switch is account-wide;
// Binance's is per symbol.
type CancelOnDisconnect interface {
	ArmCancelOnDisconnect(ctx context.Context, timeout time.Duration, syms []Symbol) error
}

type OrderRequest struct {
	Symbol        Symbol
	Side          Side
	Type          OrderType
	TIF           TimeInForce
	Qty           string // "" when QuoteQty is used
	QuoteQty      string
	Price         string // "" for market orders
	ReduceOnly    bool
	ClientOrderID string            // internal/cloid, decimal (D25)
	Params        map[string]string // td.v1.Submit.params, venue-specific, passed through
}

// OrderResult is the venue's synchronous answer to Submit. Later states arrive on the private stream.
type OrderResult struct {
	OrderID  string
	Status   OrderStatus
	ExchTsNs int64
}

// CancelRef names an order. Either id is enough; the platform always has ClientOrderID for its own
// orders (D25).
type CancelRef struct {
	Symbol        Symbol
	OrderID       string
	ClientOrderID string
}

// AccountSnapshot is the whole account at one moment (td.v1.Recon before the worker adds incarnation
// and reason).
type AccountSnapshot struct {
	OpenOrders []OrderUpdate
	Positions  []Position
	Balances   []Balance
	AsOfNs     int64
}

// OrderUpdate follows td.v1.Order.
type OrderUpdate struct {
	Symbol        Symbol
	OrderID       string
	ClientOrderID string
	Side          Side
	Type          OrderType
	Status        OrderStatus
	Qty           string
	QuoteQty      string
	Price         string
	FilledQty     string
	AvgPrice      string
	RejectReason  string
	ExchTsNs      int64
}

// Fill follows td.v1.Fill. FillID must be the same value here and in History (D20).
type Fill struct {
	Symbol        Symbol
	FillID        string
	OrderID       string
	ClientOrderID string // "" when the venue does not send it (REST history usually does not)
	Side          Side
	Price, Qty    string
	Fee, FeeAsset string
	Liquidity     string // maker | taker
	ExchTsNs      int64
}

// Balance follows td.v1.Balance without prelock, which the account worker computes.
type Balance struct {
	Asset  string
	Free   string
	Locked string
}

type Position struct {
	Symbol        Symbol
	Size          string // signed: negative is short
	EntryPrice    string
	MarkPrice     string
	UnrealizedPnl string
	Leverage      string
}

// PrivateStream is the codec of one private websocket connection. Not safe for concurrent use.
type PrivateStream interface {
	// Open does whatever must happen before connecting (Binance: create a listenKey over REST) and
	// returns the URL and the frames to send once connected (OKX: login then subscribe; Bybit: auth
	// then subscribe). The host is AccountConfig.StreamEndpoint or the venue's default.
	Open(ctx context.Context) (*Dial, error)

	// Refresh keeps the stream's credentials alive (Binance: extend the listenKey), called every
	// Limits().RefreshEvery. A no-op where nothing expires.
	Refresh(ctx context.Context) error

	Limits() StreamLimits
	Ping() []byte
	Decode(frame []byte, sink AccountSink) error
}

type Dial struct {
	URL    string
	Frames [][]byte
}

type StreamLimits struct {
	Keepalive    Keepalive
	RefreshEvery time.Duration // 0 = no Refresh needed
	MaxLifetime  time.Duration
}

// AccountSink receives what PrivateStream.Decode finds. Pointers are valid only during the call.
type AccountSink interface {
	// Ready: login and subscriptions acknowledged, the stream is live. The account worker recons
	// next and only then accepts new orders again (D27).
	Ready()
	// Expired: the venue ended the stream on purpose (listenKey expired). Reconnect.
	Expired(reason string)

	Order(e *OrderUpdate)
	Fill(e *Fill)
	Balances(e []Balance)
	Positions(e []Position)
}
