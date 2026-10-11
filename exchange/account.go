package exchange

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// AccountConfig is what the TD account worker (or the TD agent, for History) builds an adapter from.
type AccountConfig struct {
	APIID       int64
	Credentials Credentials
	// RESTEndpoint and StreamEndpoint override the venue's default hosts, the way
	// mdctl.InitPayload.url does for market data (D403). Empty means the default.
	RESTEndpoint   string
	StreamEndpoint string
	// Simulated selects a venue's demo trading mode when that mode is a flag rather than
	// another host. OKX adds the header x-simulated-trading: 1 and, unless StreamEndpoint
	// is set, dials the demo private socket. Other venues ignore it.
	Simulated bool
}

// AccountFactory builds an Account for one api_id of one AccountKind.
type AccountFactory func(cfg AccountConfig) (Account, error)

// Account is a venue's private trading interface for one api_id, sans-I/O (D302): each method builds a
// signed Call at now, and the TD account worker sends it over its HTTP pool. Safe for concurrent use.
// Where the adapter has a TradeStream (D303), orders and cancels go over it, and Submit and Cancel
// here are the REST fallback the venue may still offer.
//
// Instruments go in resolved (*symbol.Info, cached by the worker) and come out as the venue names
// them (symbol.ExchKey); the worker maps them back with symbol.Resolver.Reverse and keeps Info.UID
// (D202, D203). td.order still carries a UniversalTicker; switching that field to UID is later work.
// The worker has already checked AccountKind.CanTrade; a kind that trades several venues (BinancePM)
// routes each request by inst.Ticker.Venue (D301).
//
// It knows nothing about algo_id (D602). The worker checks algo ownership, and translates
// BatchCancel(algo_id) by filtering its OMS for open orders whose client_order_id carries that algo,
// then running CancelBatch in chunks of Limits().CancelBatch.
type Account interface {
	Limits() AccountLimits

	// Submit is one order. Its Parse returns a *Reject for a venue refusal. A request that went out
	// with no readable answer is ErrUnknownOutcome (the transport's to say): the private stream or the
	// next recon tells.
	Submit(now time.Time, req *OrderRequest) (Call[*OrderResult], error)

	Cancel(now time.Time, ref CancelRef) (Call[struct{}], error)

	// CancelBatch cancels up to Limits().CancelBatch orders in one venue request. The result has one
	// entry per ref, nil for success; Parse's error is for a failure of the whole request.
	CancelBatch(now time.Time, refs []CancelRef) (Call[[]error], error)

	// Snapshot is the recon read (D603): open orders, positions, balances. A venue answers them in
	// several requests (BinancePM: one per product), each of them paginated or not: the worker runs
	// every Call to its last page and merges what the pages carry. It stamps AsOfNs itself, before the
	// first request goes out.
	Snapshot(now time.Time) ([]Call[Page[AccountSnapshot]], error)

	// Leverage answers td.view's EnsureLeverage.
	Leverage(now time.Time, inst *symbol.Info) (Call[float64], error)

	// NewStream returns the private websocket codec for one connection. Not needed, and not opened,
	// when the trade stream carries the private events (TradeLimits.Events).
	NewStream() (PrivateStream, error)

	// NewTradeStream returns the codec of one trade connection (D303). ErrUnsupported when the venue
	// takes orders over REST only, or this adapter does not use its websocket orders: the worker then
	// sends Submit and Cancel as REST Calls.
	NewTradeStream() (TradeStream, error)

	// Usage reads the rate-limit usage the venue reports in a response; nil when it reports none.
	// now is when the response arrived. The adapter does not read a clock (D302). Each Used.ResetAt
	// is when that counter returns to zero; the zero time means the venue did not say (D420).
	Usage(resp *HTTPResponse, now time.Time) []Used
}

// AccountLimits are the venue's rules for one api_id.
type AccountLimits struct {
	ClientOrderIDMaxLen int         // the D602 client_order_id is at most 19 decimal digits
	CancelBatch         int         // orders per CancelBatch; 1 = the venue has no batch cancel
	OrderRate           []RateLimit // order requests, all must hold
}

// CancelOnDisconnect is implemented by venues with a dead-man switch, used for
// AccountSettings.cancel_on_disconnect_s. insts is ignored by venues whose switch is account-wide;
// Binance's is per symbol.
type CancelOnDisconnect interface {
	ArmCancelOnDisconnect(now time.Time, timeout time.Duration, insts []*symbol.Info) (Call[struct{}], error)
}

// OrderRequest carries the resolved instrument: ExchTicker for the wire, filters for the worker's
// checks before sending.
type OrderRequest struct {
	Instrument    *symbol.Info
	Side          Side
	Type          OrderType
	TIF           TimeInForce
	Qty           float64 // decimal.Absent when QuoteQty is used
	QuoteQty      float64 // decimal.Absent unless used
	Price         float64 // decimal.Absent for market orders
	ReduceOnly    bool
	ClientOrderID string            // contract/wire/cloid, decimal (D602)
	Params        map[string]string // td.v1.Submit.params, venue-specific, passed through
}

// OrderResult is the venue's synchronous answer to Submit. Later states arrive on the private stream.
type OrderResult struct {
	OrderID  string
	Status   OrderStatus
	ExchTsNs int64
}

// CancelRef names an order. Either id is enough; the platform always has ClientOrderID for its own
// orders (D602).
type CancelRef struct {
	Instrument    *symbol.Info
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

// OrderUpdate follows td.v1.Order, with the instrument as the venue names it.
type OrderUpdate struct {
	Exch          symbol.ExchKey
	OrderID       string
	ClientOrderID string
	Side          Side
	Type          OrderType
	Status        OrderStatus
	Qty           float64
	QuoteQty      float64 // optional
	Price         float64 // optional
	FilledQty     float64
	AvgPrice      float64 // optional
	RejectReason  string
	ExchTsNs      int64
}

// Fill follows td.v1.Fill. FillID must be the same value here and in History (D601).
type Fill struct {
	Exch          symbol.ExchKey
	FillID        string
	OrderID       string
	ClientOrderID string // "" when the venue does not send it (REST history usually does not)
	Side          Side
	Price, Qty    float64
	Fee           float64
	FeeAsset      string
	Liquidity     string // maker | taker
	ExchTsNs      int64
}

// Balance follows td.v1.Balance without prelock, which the account worker computes.
type Balance struct {
	Asset  string
	Free   float64
	Locked float64
}

type Position struct {
	Exch          symbol.ExchKey
	Size          float64 // signed: negative is short
	EntryPrice    float64 // each optional from here
	MarkPrice     float64
	UnrealizedPnl float64
	Leverage      float64
}

// PrivateStream is the codec of one private websocket connection. Not safe for concurrent use.
type PrivateStream interface {
	// Prepare is what must happen over REST before connecting (Binance: create a listenKey); its
	// result is the token Dial takes. nil when the venue needs nothing (OKX, Bybit).
	Prepare(now time.Time) (*Call[string], error)

	// Dial is the URL to connect to and the frames to send once connected (OKX: login then
	// subscribe; Bybit: auth then subscribe), signed at now. token is what Prepare's Call returned,
	// "" when there was none. The host is AccountConfig.StreamEndpoint or the venue's default.
	Dial(now time.Time, token string) (*Dial, error)

	// Refresh keeps the stream's credentials alive (Binance: extend the listenKey), run every
	// Limits().RefreshEvery. nil where nothing expires.
	Refresh(now time.Time) (*Call[struct{}], error)

	Limits() StreamLimits
	Ping() []byte
	Decode(frame []byte, sink AccountSink) error
}

type Dial struct {
	URL    string
	Frames [][]byte
}

// String is the dial as it may be logged: the URL by MaskURL, and how many frames but not what they
// say. A private stream's URL carries its listenKey, and OKX's and Bybit's login frames the API key,
// the passphrase and the signature. Format and MarshalJSON give the same.
func (d Dial) String() string { return MaskURL(d.URL) + ", " + strconv.Itoa(len(d.Frames)) + " frames" }

func (d Dial) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, d.String()) }

func (d Dial) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

type StreamLimits struct {
	Keepalive    Keepalive
	RefreshEvery time.Duration // 0 = no Refresh needed
	MaxLifetime  time.Duration
	ReadLimit    int64 // as SessionLimits.ReadLimit
}

// AccountSink receives what PrivateStream.Decode finds. Pointers, and the strings in what they point
// to, are valid only during the call (D302): the frame they came from is the worker's to reuse.
type AccountSink interface {
	// Ready: login and subscriptions acknowledged, the stream is live. The account worker recons
	// next and only then accepts new orders again (D603).
	Ready()
	// Expired: the venue ended the stream on purpose (listenKey expired). Reconnect.
	Expired(reason string)

	Order(e *OrderUpdate)
	Fill(e *Fill)
	Balances(e []Balance)
	Positions(e []Position)
}
