package exchangetest

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

// AccountVenue is a fake venue for one account (api_id), for the TD account worker's tests (E6-05). It
// holds the account side of a venue in memory: open orders, positions, balances, leverage, and whether
// the account is in hedge mode (D607). It has three ends:
//
//   - Account and Kind give the sans-I/O adapter (exchange.Account): REST Calls, and the private
//     stream's codec from NewStream.
//   - The venue itself is the exchange.Doer that answers those Calls.
//   - Handler serves the private websocket the codec dials. Run it with httptest.NewServer and put the
//     server's ws:// URL in AccountConfig.StreamEndpoint; the worker dials it with exchange/transport/ws.
//
// The test sets the account's state, scripts how the venue answers order requests (accept, refuse, no
// answer, or arrive late: OnSubmit, OnCancel, Arrive), throttles requests with 429, reports usage, drops stream connections, refuses logins, and
// pushes private events. Changing the state through the venue (PutOrder, Fill, SetBalance,
// SetPosition) also pushes the event a venue would send, so after a run the worker's view can be
// compared with the venue's.
//
// QueryOrder finds an order open or ended, by client order id or order id; one the venue never took
// is ErrNotFound. Every Submit and Cancel carries its deadline (AccountConfig.Validity after it was
// signed): a request held back with Outcome.Late takes effect when it Arrives before the deadline,
// and is refused after it, as on a real venue (D605).
//
// The venue does not match orders: an accepted order rests until the test fills or cancels it. Its
// wire format is its own JSON; it checks the API key but not signatures. Everything goes over
// loopback or no network at all.
type AccountVenue struct {
	opts AccountOptions
	base string

	mu       sync.Mutex
	key      string
	orders   map[string]*exchange.OrderUpdate // by order id; open orders only
	ended    map[string]*exchange.OrderUpdate // by order id; filled, cancelled, rejected, expired
	nextID   int
	fills    int
	balances map[string]exchange.Balance
	pos      map[symbol.ExchKey]exchange.Position
	lev      map[symbol.ExchKey]float64
	hedge    bool
	requests []string
	queries  []string   // GET /order keys, "id=…" when an order id was sent, otherwise "cid=…"
	batches  [][]string // client order ids of each CancelBatch, in arrival order

	submit      map[string]Outcome // by client order id, used once
	cancel      map[string]Outcome
	late        map[string]lateReq // by client order id: held until Arrive
	batch       []Outcome
	throttle    int
	retry       time.Duration
	used        []exchange.Used
	failSnap    map[snapKey]int
	failQueries int // GET /order failures left, each a 500
	arms        []Arm
	listenKey   string
	keys        int

	conns      map[*streamConn]struct{}
	hold       bool
	held       [][]byte
	refuse     int
	failDials  int
	connsTotal int
}

// AccountOptions configure an AccountVenue. The zero value is usable.
type AccountOptions struct {
	// Venue is the venue name on every ExchKey the adapter reports. Empty is "Fake".
	Venue string
	// Key is the API key the venue accepts, on REST and on the stream's login. Account builds its
	// requests with AccountConfig.Credentials.Key.
	Key string
	// Limits is what Account.Limits returns. A zero CancelBatch is 5, a zero ClientOrderIDMaxLen 19.
	Limits exchange.AccountLimits
	// PageSize is the rows per Snapshot page, for orders, positions and balances alike. 0 is one page.
	PageSize int
	// Ack is the status of Submit's synchronous answer: OrderStatusNew (the default: the venue has the
	// request, as OKX and Bybit answer) or OrderStatusAccepted (on the book, as Binance answers).
	Ack exchange.OrderStatus
	// ListenKey makes the stream need one, as Binance's does: Prepare creates it over REST, Refresh
	// extends it, and ExpireListenKey ends it.
	ListenKey bool
	// Stream is what the stream codec's Limits returns. A zero ReadLimit is 1 MiB.
	Stream exchange.StreamLimits
	// Quiet makes the stream acknowledge nothing, as Binance's listenKey stream: the codec does not
	// call Ready on the login's answer, and Dial is Live instead.
	Quiet bool
	// Trade, when set, is the venue behind NewTradeStream. Without it NewTradeStream is
	// ErrUnsupported and the worker sends orders over REST.
	Trade *TradeVenue
	// Now stamps the venue's events. nil is time.Now.
	Now func() time.Time
}

// Outcome is how the venue answers one order request.
type Outcome struct {
	// Code refuses the request with the venue's code: -2010 is insufficient balance (mapped to 201),
	// -2011 an unknown order. Anything else passes through as the native code.
	Code int
	// NoAnswer: the request reaches the venue and no answer comes back, so Do returns an error
	// wrapping exchange.ErrUnknownOutcome. Applied says whether the venue acted on it all the same.
	NoAnswer bool
	Applied  bool
	// Late: the request is delayed on its way. Do returns no answer (ErrUnknownOutcome) and the
	// venue does nothing until the test calls Arrive. Not for CancelBatch.
	Late bool
}

// lateReq is a Submit or Cancel held back by Outcome.Late.
type lateReq struct {
	submit *orderReq
	cancel *cancelReq
}

// SnapshotPart is one of the reads Snapshot makes.
type SnapshotPart uint8

const (
	SnapshotOrders SnapshotPart = iota
	SnapshotPositions
	SnapshotBalances
)

var snapshotPaths = map[SnapshotPart]string{SnapshotOrders: "/orders", SnapshotPositions: "/positions", SnapshotBalances: "/balances"}

type snapKey struct {
	part SnapshotPart
	page int
}

// Arm is one ArmCancelOnDisconnect the venue received.
type Arm struct {
	AtNs    int64
	Timeout time.Duration
	Tickers []string
}

// NewAccountVenue returns a venue with no orders, positions or balances, in one-way mode.
func NewAccountVenue(o AccountOptions) *AccountVenue {
	if o.Venue == "" {
		o.Venue = "Fake"
	}
	if o.Limits.CancelBatch == 0 {
		o.Limits.CancelBatch = 5
	}
	if o.Limits.ClientOrderIDMaxLen == 0 {
		o.Limits.ClientOrderIDMaxLen = 19
	}
	if o.Ack == exchange.OrderStatusUnspecified {
		o.Ack = exchange.OrderStatusNew
	}
	if o.Stream.ReadLimit == 0 {
		o.Stream.ReadLimit = 1 << 20
	}
	if o.ListenKey && o.Stream.RefreshEvery == 0 {
		o.Stream.RefreshEvery = 30 * time.Minute
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &AccountVenue{
		opts: o, base: "exchangetest://" + o.Venue + "-account", key: o.Key,
		orders: map[string]*exchange.OrderUpdate{}, ended: map[string]*exchange.OrderUpdate{}, balances: map[string]exchange.Balance{},
		pos: map[symbol.ExchKey]exchange.Position{}, lev: map[symbol.ExchKey]float64{},
		submit: map[string]Outcome{}, cancel: map[string]Outcome{}, late: map[string]lateReq{}, failSnap: map[snapKey]int{},
		conns: map[*streamConn]struct{}{},
	}
}

// Kind is an AccountKind whose factory builds this venue's adapter, for a registry: name is the
// account kind, trades the venues it trades (this venue's name when empty).
func (v *AccountVenue) Kind(name string, trades ...string) exchange.AccountKind {
	if len(trades) == 0 {
		trades = []string{v.opts.Venue}
	}
	return exchange.AccountKind{
		Name: name, Label: name, Trades: trades, KeyTypes: []exchange.KeyType{exchange.KeyHMAC, exchange.KeyEd25519},
		Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
			if _, err := cfg.Window(); err != nil {
				return nil, err
			}
			return v.Account(cfg), nil
		},
	}
}

// ---------- the account's state ----------

// PutOrder sets one order as the venue holds it and pushes it as an order event. A terminal status
// takes it off the open orders. An order without an id gets the next one.
func (v *AccountVenue) PutOrder(o exchange.OrderUpdate) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if o.OrderID == "" {
		v.nextID++
		o.OrderID = strconv.Itoa(v.nextID)
	}
	if o.Exch.Venue == "" {
		o.Exch.Venue = v.opts.Venue
	}
	if o.ExchTsNs == 0 {
		o.ExchTsNs = v.opts.Now().UnixNano()
	}
	v.setOrder(o)
	v.broadcast(orderFrame(&o))
}

// Fill fills qty of the open order with client order id cloid at price: the order becomes
// PARTIALLY_FILLED, or FILLED and leaves the open orders, and the venue pushes the order event and
// then the fill. It returns the fill's id.
func (v *AccountVenue) Fill(cloid string, qty, price float64) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	o := v.byCloid(cloid)
	if o == nil {
		return "", fmt.Errorf("exchangetest: no open order %q", cloid)
	}
	u := *o
	filled := u.FilledQty + qty
	if decimal.Has(u.AvgPrice) && u.FilledQty > 0 {
		u.AvgPrice = (u.AvgPrice*u.FilledQty + price*qty) / filled
	} else {
		u.AvgPrice = price
	}
	u.FilledQty = filled
	u.Status = exchange.OrderStatusPartiallyFilled
	if decimal.Has(u.Qty) && filled >= u.Qty*(1-1e-12) {
		u.Status = exchange.OrderStatusFilled
	}
	u.ExchTsNs = v.opts.Now().UnixNano()
	v.setOrder(u)
	v.fills++
	f := exchange.Fill{
		Exch: u.Exch, FillID: "f" + strconv.Itoa(v.fills), OrderID: u.OrderID, ClientOrderID: u.ClientOrderID,
		Side: u.Side, Price: price, Qty: qty, Fee: 0, FeeAsset: "", Liquidity: "maker", ExchTsNs: u.ExchTsNs,
	}
	v.broadcast(orderFrame(&u))
	v.broadcast(fillFrame(&f))
	return f.FillID, nil
}

// SetBalance sets one asset's balance and pushes a balance event.
func (v *AccountVenue) SetBalance(b exchange.Balance) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.balances[b.Asset] = b
	v.broadcast(mustJSON(streamFrame{Event: "balances", Balances: []wireBalance{toWireBalance(b)}}))
}

// SetPosition sets one instrument's position and pushes a position event. A zero size removes it.
func (v *AccountVenue) SetPosition(p exchange.Position) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if p.Exch.Venue == "" {
		p.Exch.Venue = v.opts.Venue
	}
	if p.Size == 0 {
		delete(v.pos, p.Exch)
	} else {
		v.pos[p.Exch] = p
	}
	v.broadcast(mustJSON(streamFrame{Event: "positions", Positions: []wirePosition{toWirePosition(p)}}))
}

// SetLeverage sets what Account.Leverage reads for one instrument.
func (v *AccountVenue) SetLeverage(k symbol.ExchKey, lev float64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if k.Venue == "" {
		k.Venue = v.opts.Venue
	}
	v.lev[k] = lev
}

// SetHedgeMode puts the account in hedge mode, or back in one-way mode. Venues push nothing when it
// changes; the next Snapshot shows it (AccountSnapshot.HedgeMode).
func (v *AccountVenue) SetHedgeMode(on bool) { v.mu.Lock(); v.hedge = on; v.mu.Unlock() }

// OpenOrders, Positions and Balances are the venue's state, sorted by order id, instrument and asset.
func (v *AccountVenue) OpenOrders() []exchange.OrderUpdate {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.openOrders()
}

func (v *AccountVenue) Positions() []exchange.Position {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.positions()
}

func (v *AccountVenue) Balances() []exchange.Balance {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.balanceList()
}

// ---------- how the venue answers ----------

// OnSubmit scripts the answer to the next Submit with client order id cloid.
func (v *AccountVenue) OnSubmit(cloid string, o Outcome) {
	v.mu.Lock()
	v.submit[cloid] = o
	v.mu.Unlock()
}

// OnCancel scripts the answer to the next Cancel of client order id cloid, and to its entry in the
// next CancelBatch that names it (Code only: one entry cannot go unanswered alone).
func (v *AccountVenue) OnCancel(cloid string, o Outcome) {
	v.mu.Lock()
	v.cancel[cloid] = o
	v.mu.Unlock()
}

// Arrive delivers the request held back for client order id cloid (Outcome.Late) at venue time at.
// Before or at its deadline the venue acts on it as it would on time: it places the order, or cancels
// it, and pushes the event. After the deadline the venue refuses it and nothing changes. took says
// which; the error is for a cloid with no request held.
func (v *AccountVenue) Arrive(cloid string, at time.Time) (took bool, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	r, ok := v.late[cloid]
	if !ok {
		return false, fmt.Errorf("exchangetest: no request held for %q", cloid)
	}
	delete(v.late, cloid)
	exp := int64(0)
	if r.submit != nil {
		exp = r.submit.Exp
	} else {
		exp = r.cancel.Exp
	}
	if exp != 0 && at.UnixNano() > exp {
		return false, nil
	}
	if r.submit != nil {
		if v.byCloid(r.submit.Cid) != nil {
			return false, nil
		}
		v.place(r.submit)
		return true, nil
	}
	return v.cancelOne(*r.cancel, true) == 0, nil
}

// Held is the client order ids of the requests held back by Outcome.Late, sorted.
func (v *AccountVenue) Held() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Sorted(maps.Keys(v.late))
}

// OnBatch scripts the answer to the next CancelBatch as a whole. Code refuses the whole request.
func (v *AccountVenue) OnBatch(o Outcome) { v.mu.Lock(); v.batch = append(v.batch, o); v.mu.Unlock() }

// Throttle answers the next n REST requests with 429. retryAfter goes in Retry-After, in whole
// seconds; 0 sends none.
func (v *AccountVenue) Throttle(n int, retryAfter time.Duration) {
	v.mu.Lock()
	v.throttle, v.retry = n, retryAfter
	v.mu.Unlock()
}

// ReportUsage makes every REST response report these counters, until it is called again.
func (v *AccountVenue) ReportUsage(u ...exchange.Used) {
	v.mu.Lock()
	v.used = slices.Clone(u)
	v.mu.Unlock()
}

// FailQueries makes the next n GET /order reads fail with 500. A query the worker cannot read is
// a failure it backs off, not ErrNotFound (D622).
func (v *AccountVenue) FailQueries(n int) {
	v.mu.Lock()
	v.failQueries = n
	v.mu.Unlock()
}

// FailSnapshot makes the next times reads of page (from 0) of part fail as a dropped connection
// would: no response.
func (v *AccountVenue) FailSnapshot(part SnapshotPart, page, times int) {
	v.mu.Lock()
	v.failSnap[snapKey{part, page}] = times
	v.mu.Unlock()
}

// SetKey changes the API key the venue accepts. Requests and logins with the old one get ErrAuth,
// as with a revoked key.
func (v *AccountVenue) SetKey(key string) { v.mu.Lock(); v.key = key; v.mu.Unlock() }

// Requests is every REST request the venue received, as "METHOD /path", in order.
func (v *AccountVenue) Requests() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.requests)
}

// Queries is every GET /order, as "id=…" when the request carried an order id and "cid=…" otherwise.
func (v *AccountVenue) Queries() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.queries)
}

// Batches is the client order ids of each CancelBatch, in arrival order.
func (v *AccountVenue) Batches() [][]string {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([][]string, len(v.batches))
	for i, b := range v.batches {
		out[i] = slices.Clone(b)
	}
	return out
}

// ResetRequests forgets the requests received so far.
func (v *AccountVenue) ResetRequests() {
	v.mu.Lock()
	v.requests, v.queries, v.batches = nil, nil, nil
	v.mu.Unlock()
}

// Arms is every ArmCancelOnDisconnect the venue received, in order.
func (v *AccountVenue) Arms() []Arm {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.arms)
}

// ---------- state helpers; v.mu held ----------

func (v *AccountVenue) setOrder(o exchange.OrderUpdate) {
	switch o.Status {
	case exchange.OrderStatusFilled, exchange.OrderStatusCancelled, exchange.OrderStatusRejected, exchange.OrderStatusExpired:
		delete(v.orders, o.OrderID)
		v.ended[o.OrderID] = &o
	default:
		delete(v.ended, o.OrderID)
		v.orders[o.OrderID] = &o
	}
}

func (v *AccountVenue) byCloid(cloid string) *exchange.OrderUpdate {
	if cloid == "" {
		return nil
	}
	for _, o := range v.orders {
		if o.ClientOrderID == cloid {
			return o
		}
	}
	return nil
}

// lookup finds an order open or ended: by order id, else the latest with client order id cid.
func (v *AccountVenue) lookup(id, cid string) *exchange.OrderUpdate {
	if id != "" {
		if o := v.orders[id]; o != nil {
			return o
		}
		return v.ended[id]
	}
	if o := v.byCloid(cid); o != nil {
		return o
	}
	var last *exchange.OrderUpdate
	for _, o := range v.ended {
		if o.ClientOrderID == cid && cid != "" && (last == nil || o.ExchTsNs > last.ExchTsNs) {
			last = o
		}
	}
	return last
}

func (v *AccountVenue) find(ref cancelReq) *exchange.OrderUpdate {
	if ref.ID != "" {
		return v.orders[ref.ID]
	}
	return v.byCloid(ref.Cid)
}

func (v *AccountVenue) openOrders() []exchange.OrderUpdate {
	out := make([]exchange.OrderUpdate, 0, len(v.orders))
	for _, o := range v.orders {
		out = append(out, *o)
	}
	slices.SortFunc(out, func(a, b exchange.OrderUpdate) int {
		x, _ := strconv.Atoi(a.OrderID)
		y, _ := strconv.Atoi(b.OrderID)
		return cmp.Or(cmp.Compare(x, y), cmp.Compare(a.OrderID, b.OrderID))
	})
	return out
}

func (v *AccountVenue) positions() []exchange.Position {
	keys := slices.SortedFunc(maps.Keys(v.pos), func(a, b symbol.ExchKey) int {
		return cmp.Or(cmp.Compare(a.Venue, b.Venue), cmp.Compare(a.Category, b.Category), cmp.Compare(a.ExchTicker, b.ExchTicker))
	})
	out := make([]exchange.Position, len(keys))
	for i, k := range keys {
		out[i] = v.pos[k]
	}
	return out
}

func (v *AccountVenue) balanceList() []exchange.Balance {
	out := make([]exchange.Balance, 0, len(v.balances))
	for _, a := range slices.Sorted(maps.Keys(v.balances)) {
		out = append(out, v.balances[a])
	}
	return out
}

// ---------- the Doer ----------

var _ exchange.Doer = (*AccountVenue)(nil)

// Do answers the Calls Account builds. A request for another base is a 404.
func (v *AccountVenue) Do(ctx context.Context, req *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.requests = append(v.requests, req.Method+" "+req.Path)
	if req.Base != v.base {
		return v.respond(404, nil), nil
	}
	if v.throttle > 0 {
		v.throttle--
		r := v.respond(429, nil)
		if s := int64(v.retry / time.Second); s > 0 {
			r.Header = append(r.Header, exchange.Header{Name: "Retry-After", Value: strconv.FormatInt(s, 10)})
		}
		return r, nil
	}
	if keyOf(req) != v.key {
		return v.respond(401, nil), nil
	}
	q, err := url.ParseQuery(string(req.Query))
	if err != nil {
		return v.respond(400, nil), nil
	}
	switch req.Method + " " + req.Path {
	case "POST /order":
		return v.doSubmit(req.Body)
	case "DELETE /order":
		return v.doCancel(req.Body)
	case "DELETE /batch":
		return v.doBatch(req.Body)
	case "GET /order":
		if id := q.Get("id"); id != "" {
			v.queries = append(v.queries, "id="+id)
		} else if cid := q.Get("cid"); cid != "" {
			v.queries = append(v.queries, "cid="+cid)
		}
		if v.failQueries > 0 {
			v.failQueries--
			return v.respond(500, []byte("query failed")), nil
		}
		o := v.lookup(q.Get("id"), q.Get("cid"))
		if o == nil {
			return v.refusal(-2013, "order does not exist"), nil
		}
		return v.respond(200, mustJSON(toWireOrder(o))), nil
	case "GET /orders", "GET /positions", "GET /balances":
		return v.doSnapshot(req.Path, q)
	case "GET /leverage":
		k := symbol.ExchKey{Venue: v.opts.Venue, Category: symbol.Category(q.Get("cat")), ExchTicker: q.Get("sym")}
		lev, ok := v.lev[k]
		if !ok {
			return v.refusal(-4003, "no leverage for "+k.ExchTicker), nil
		}
		return v.respond(200, mustJSON(map[string]string{"lev": num(lev)})), nil
	case "POST /listenKey":
		v.keys++
		v.listenKey = "lk" + strconv.Itoa(v.keys)
		return v.respond(200, mustJSON(map[string]string{"listenKey": v.listenKey})), nil
	case "PUT /listenKey":
		if v.listenKey == "" {
			return v.refusal(-1125, "this listenKey does not exist"), nil
		}
		return v.respond(200, []byte("{}")), nil
	case "POST /cod":
		var a codReq
		if err := json.Unmarshal(req.Body, &a); err != nil {
			return v.respond(400, nil), nil
		}
		v.arms = append(v.arms, Arm{AtNs: a.Ts, Timeout: time.Duration(a.TimeoutMs) * time.Millisecond, Tickers: a.Syms})
		return v.respond(200, []byte("{}")), nil
	}
	return v.respond(404, nil), nil
}

var errNoAnswer = fmt.Errorf("exchangetest: no answer: %w", exchange.ErrUnknownOutcome)

func (v *AccountVenue) doSubmit(body []byte) (*exchange.HTTPResponse, error) {
	var r orderReq
	if err := json.Unmarshal(body, &r); err != nil {
		return v.respond(400, nil), nil
	}
	out, scripted := v.submit[r.Cid]
	delete(v.submit, r.Cid)
	if scripted && out.Code != 0 {
		return v.refusal(out.Code, "refused"), nil
	}
	if scripted && out.Late {
		v.late[r.Cid] = lateReq{submit: &r}
		return nil, errNoAnswer
	}
	place := !scripted || !out.NoAnswer || out.Applied
	if place && r.Cid != "" && v.byCloid(r.Cid) != nil {
		if scripted && out.NoAnswer {
			return nil, errNoAnswer
		}
		return v.refusal(-4116, "client order id is duplicated"), nil
	}
	var o exchange.OrderUpdate
	if place {
		o = v.place(&r)
	}
	if scripted && out.NoAnswer {
		return nil, errNoAnswer
	}
	return v.respond(200, mustJSON(submitAns{ID: o.OrderID, Status: v.opts.Ack, Ts: r.Ts})), nil
}

// place puts the order of r on the book and pushes it.
func (v *AccountVenue) place(r *orderReq) exchange.OrderUpdate {
	v.nextID++
	o := exchange.OrderUpdate{
		Exch:    symbol.ExchKey{Venue: v.opts.Venue, Category: r.Cat, ExchTicker: r.Sym},
		OrderID: strconv.Itoa(v.nextID), ClientOrderID: r.Cid, Side: r.Side, Type: r.Type,
		Status: exchange.OrderStatusAccepted, Qty: parseNum(r.Qty), QuoteQty: parseNum(r.Quote),
		Price: parseNum(r.Price), FilledQty: 0, AvgPrice: decimal.Absent(), ExchTsNs: r.Ts,
	}
	v.setOrder(o)
	v.broadcast(orderFrame(&o))
	return o
}

// cancelOne applies one cancel: nil, or the venue's refusal code.
func (v *AccountVenue) cancelOne(r cancelReq, apply bool) (code int) {
	o := v.find(r)
	if o == nil {
		return -2011
	}
	if !apply {
		return 0
	}
	u := *o
	u.Status, u.ExchTsNs = exchange.OrderStatusCancelled, r.Ts
	v.setOrder(u)
	v.broadcast(orderFrame(&u))
	return 0
}

func (v *AccountVenue) doCancel(body []byte) (*exchange.HTTPResponse, error) {
	var r cancelReq
	if err := json.Unmarshal(body, &r); err != nil {
		return v.respond(400, nil), nil
	}
	out, scripted := v.cancel[r.Cid]
	delete(v.cancel, r.Cid)
	if scripted && out.Code != 0 {
		return v.refusal(out.Code, "refused"), nil
	}
	if scripted && out.Late {
		v.late[r.Cid] = lateReq{cancel: &r}
		return nil, errNoAnswer
	}
	if scripted && out.NoAnswer {
		if out.Applied {
			v.cancelOne(r, true)
		}
		return nil, errNoAnswer
	}
	if code := v.cancelOne(r, true); code != 0 {
		return v.refusal(code, "unknown order sent"), nil
	}
	return v.respond(200, []byte("{}")), nil
}

func (v *AccountVenue) doBatch(body []byte) (*exchange.HTTPResponse, error) {
	var refs []cancelReq
	if err := json.Unmarshal(body, &refs); err != nil {
		return v.respond(400, nil), nil
	}
	ids := make([]string, len(refs))
	for i, r := range refs {
		ids[i] = r.Cid
	}
	v.batches = append(v.batches, ids)
	var whole Outcome
	if len(v.batch) > 0 {
		whole, v.batch = v.batch[0], v.batch[1:]
	}
	if whole.Code != 0 {
		return v.refusal(whole.Code, "refused"), nil
	}
	if len(refs) > v.opts.Limits.CancelBatch {
		return v.refusal(-4000, "too many orders in one batch"), nil
	}
	apply := !whole.NoAnswer || whole.Applied
	res := make([]batchEntry, len(refs))
	for i, r := range refs {
		if out, ok := v.cancel[r.Cid]; ok && out.Code != 0 {
			delete(v.cancel, r.Cid)
			res[i] = batchEntry{Code: out.Code, Msg: "refused"}
			continue
		}
		if code := v.cancelOne(r, apply); code != 0 {
			res[i] = batchEntry{Code: code, Msg: "unknown order sent"}
		}
	}
	if whole.NoAnswer {
		return nil, errNoAnswer
	}
	return v.respond(200, mustJSON(batchAns{Results: res})), nil
}

func (v *AccountVenue) doSnapshot(path string, q url.Values) (*exchange.HTTPResponse, error) {
	page, err := strconv.Atoi(q.Get("page"))
	if err != nil || page < 0 {
		return v.respond(400, nil), nil
	}
	var part SnapshotPart
	for p, s := range snapshotPaths {
		if s == path {
			part = p
		}
	}
	k := snapKey{part, page}
	if v.failSnap[k] > 0 {
		v.failSnap[k]--
		return nil, errors.New("exchangetest: connection reset")
	}
	var ans snapAns
	switch part {
	case SnapshotOrders:
		rows := v.openOrders()
		lo, hi := pageOf(len(rows), v.opts.PageSize, page)
		for _, o := range rows[lo:hi] {
			ans.Orders = append(ans.Orders, toWireOrder(&o))
		}
		ans.More = hi < len(rows)
	case SnapshotPositions:
		rows := v.positions()
		lo, hi := pageOf(len(rows), v.opts.PageSize, page)
		for _, p := range rows[lo:hi] {
			ans.Positions = append(ans.Positions, toWirePosition(p))
		}
		ans.More = hi < len(rows)
		ans.Hedge = v.hedge
	case SnapshotBalances:
		rows := v.balanceList()
		lo, hi := pageOf(len(rows), v.opts.PageSize, page)
		for _, b := range rows[lo:hi] {
			ans.Balances = append(ans.Balances, toWireBalance(b))
		}
		ans.More = hi < len(rows)
	}
	return v.respond(200, mustJSON(ans)), nil
}

// pageOf is the row range of page n.
func pageOf(n, size, page int) (lo, hi int) {
	if size <= 0 {
		size = max(n, 1)
	}
	lo = min(page*size, n)
	return lo, min(lo+size, n)
}

// respond is a response carrying the usage the venue reports.
func (v *AccountVenue) respond(status int, body []byte) *exchange.HTTPResponse {
	r := &exchange.HTTPResponse{Status: status, Body: body}
	for _, u := range v.used {
		r.Header = append(r.Header, exchange.Header{Name: "X-Used-" + u.Bucket, Value: strconv.Itoa(u.N)})
		if !u.ResetAt.IsZero() {
			r.Header = append(r.Header, exchange.Header{Name: "X-Reset-" + u.Bucket, Value: strconv.FormatInt(u.ResetAt.UnixMilli(), 10)})
		}
	}
	return r
}

func (v *AccountVenue) refusal(code int, msg string) *exchange.HTTPResponse {
	return v.respond(400, mustJSON(batchEntry{Code: code, Msg: msg}))
}

// venueReject is the venue's refusal code as the adapter reports it: -2010 (insufficient balance)
// is mapped to the platform's 201, anything else passes through (PROTOCOL §7.2).
func venueReject(code int, msg string) *exchange.Reject {
	if code == -2010 {
		return &exchange.Reject{Code: 201, Native: "-2010", Reason: msg}
	}
	return &exchange.Reject{Code: int32(code), Native: strconv.Itoa(code), Reason: msg}
}
