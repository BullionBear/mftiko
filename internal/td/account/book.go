package account

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

// Instruments is the venue's symbol table as the worker reads it (D203): the active table in memory,
// and, with includeInactive, a question to SYM. internal/sym's Remote is one.
type Instruments interface {
	Reverse(ctx context.Context, key symbol.ExchKey, includeInactive bool) (*symbol.Info, error)
	GetByUID(ctx context.Context, uid uint32, includeInactive bool) (*symbol.Info, error)
}

// Bus is the account's NATS: it publishes td.evt and answers td.view (D611).
type Bus interface {
	Publish(subject string, data []byte) error
	// Respond answers requests on subject with h's reply, until stop.
	Respond(subject string, h func(req []byte) []byte) (stop func(), err error)
}

// Subjects of one account (PROTOCOL §7).
func evtSubject(apiID int64) string   { return "td.evt." + strconv.FormatInt(apiID, 10) }
func viewSubject(apiID int64) string  { return "td.view." + strconv.FormatInt(apiID, 10) }
func orderSubject(apiID int64) string { return "td.order." + strconv.FormatInt(apiID, 10) }

// inactiveWait bounds a question to SYM about an instrument that is not in the active table.
const inactiveWait = 2 * time.Second

// maxBuffered bounds the events held while a recon runs. Past it they are dropped: the recon that
// follows has the state, and fills reach the store through backfill (D601).
const maxBuffered = 100_000

// fillMemory is how many fill ids, and finished orders, the book remembers to drop a repeat of
// what it already published.
const fillMemory = 10_000

// book is the account's state: the OMS (live orders only) and the ledger (positions, balances,
// leverage). One goroutine owns it; everything else sends it messages. It publishes each change on
// td.evt, numbered per incarnation from 1, and answers td.view from memory (D606).
//
// While a recon runs it holds the events it gets. The recon's snapshot replaces the state, a Recon
// goes out, and the held events are replayed in the order they arrived: on one connection the venue
// sends a change before the changes after it, so a held event older than the snapshot is followed
// by the newer ones and the replay ends where the account is. An order's filled_qty never goes back
// and a finished order does not come back (D615).
type book struct {
	ins    Instruments
	bus    Bus
	subj   string
	source *commonv1.WorkerRef
	log    zerolog.Logger
	now    func() time.Time
	in     chan any

	// Owned by run.
	ctx          context.Context
	seq          uint64
	orders       map[string]*tdv1.Order // by orderKey
	byCloid      map[string]string      // client_order_id -> orderKey
	positions    map[uint32]*tdv1.Position
	balances     map[string]*tdv1.Balance
	leverage     map[uint32]float64
	fills        idSet
	finished     idSet // orders that reached a terminal status: their repeats are dropped
	reconciling  bool
	held         []any
	dropped      int
	recons       int
	state        tdv1.AccountState
	reason       string
	reconciledAt int64

	// Orders (E6-08). The book goroutine owns these; firstSubmits is also read when a report
	// is built, under mu.
	mu       sync.Mutex
	trades   []string
	limits   exchange.AccountLimits
	validity time.Duration
	skew     time.Duration
	hold     time.Duration
	rate     *limiter
	seen     *seenMem
	queries  map[string]*query
	eventsUp bool
	first    map[uint32]int64
	onState  func(tdv1.AccountState, string)
	reportFn func(*tdctlv1.Report) error
	qwake    chan struct{}
}

func newBook(ins Instruments, bus Bus, apiID int64, source *commonv1.WorkerRef, log zerolog.Logger, now func() time.Time) *book {
	return &book{
		ins: ins, bus: bus, subj: evtSubject(apiID), source: source, log: log, now: now,
		in:     make(chan any, 1024),
		orders: map[string]*tdv1.Order{}, byCloid: map[string]string{},
		positions: map[uint32]*tdv1.Position{}, balances: map[string]*tdv1.Balance{}, leverage: map[uint32]float64{},
		fills: newIDSet(fillMemory), finished: newIDSet(fillMemory),
	}
}

// Messages to the book.
type (
	msgOrder struct {
		e     exchange.OrderUpdate
		cause tdv1.OrderCause
		code  int32
	}
	msgFill      struct{ e exchange.Fill }
	msgBalances  struct{ e []exchange.Balance }
	msgPositions struct{ e []exchange.Position }
	msgBegin     struct{}
	msgRecon     struct {
		snap *exchange.AccountSnapshot
		done chan struct{}
	}
	msgState struct {
		state  tdv1.AccountState
		reason string
	}
	msgEvents struct{ up bool }
	msgAdmit  struct {
		req   *tdv1.OrderRequest
		reply chan admitOut
	}
	msgResult struct {
		kind     resultKind
		cloid    string
		orderID  string
		res      *exchange.OrderResult
		upd      *exchange.OrderUpdate
		err      error
		refs     []exchange.CancelRef // a CancelBatch, one entry per order
		perrs    []error              // one per ref; nil is success. Set only for resultBatch
		sent     time.Time
		window   time.Duration
		deadline time.Time
		used     []exchange.Used
	}
	msgCall struct {
		fn   func()
		done chan struct{}
	}
	msgUsage struct{ used []exchange.Used }
	msgView  struct {
		req   *tdv1.ViewRequest
		reply chan *tdv1.ViewReply
	}
)

func (b *book) run(ctx context.Context) {
	b.ctx = ctx
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-b.in:
			b.handle(m)
		}
	}
}

func (b *book) send(ctx context.Context, m any) bool {
	select {
	case b.in <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

// The calls below copy what they are handed: it is lent (D302).

func (b *book) order(ctx context.Context, e *exchange.OrderUpdate) {
	c := *e
	c.Exch = cloneKey(c.Exch)
	c.OrderID, c.ClientOrderID, c.RejectReason = strings.Clone(c.OrderID), strings.Clone(c.ClientOrderID), strings.Clone(c.RejectReason)
	b.send(ctx, msgOrder{e: c})
}

func (b *book) fill(ctx context.Context, e *exchange.Fill) {
	c := *e
	c.Exch = cloneKey(c.Exch)
	c.FillID, c.OrderID, c.ClientOrderID = strings.Clone(c.FillID), strings.Clone(c.OrderID), strings.Clone(c.ClientOrderID)
	c.FeeAsset, c.Liquidity = strings.Clone(c.FeeAsset), strings.Clone(c.Liquidity)
	b.send(ctx, msgFill{c})
}

func (b *book) balanceEvent(ctx context.Context, e []exchange.Balance) {
	c := make([]exchange.Balance, len(e))
	for i, x := range e {
		x.Asset = strings.Clone(x.Asset)
		c[i] = x
	}
	b.send(ctx, msgBalances{c})
}

func (b *book) positionEvent(ctx context.Context, e []exchange.Position) {
	c := make([]exchange.Position, len(e))
	for i, x := range e {
		x.Exch = cloneKey(x.Exch)
		c[i] = x
	}
	b.send(ctx, msgPositions{c})
}

// beginRecon holds the events that arrive from now on, until a recon is applied.
func (b *book) beginRecon(ctx context.Context) { b.send(ctx, msgBegin{}) }

// applyRecon replaces the state with snap, publishes the Recon, replays the held events, and
// returns once that is done. snap is the book's from then on.
func (b *book) applyRecon(ctx context.Context, snap *exchange.AccountSnapshot) {
	done := make(chan struct{})
	if b.send(ctx, msgRecon{snap, done}) {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
}

func (b *book) setState(ctx context.Context, s tdv1.AccountState, reason string) {
	b.send(ctx, msgState{s, reason})
}

func (b *book) setEvents(ctx context.Context, up bool) { b.send(ctx, msgEvents{up}) }

// view answers one td.view request from memory.
func (b *book) view(ctx context.Context, req *tdv1.ViewRequest) *tdv1.ViewReply {
	reply := make(chan *tdv1.ViewReply, 1)
	if !b.send(ctx, msgView{req, reply}) {
		return nil
	}
	select {
	case r := <-reply:
		return r
	case <-ctx.Done():
		return nil
	}
}

func (b *book) handle(m any) {
	switch m := m.(type) {
	case msgBegin:
		b.reconciling = true
	case msgRecon:
		b.recon(m.snap)
		close(m.done)
	case msgState:
		b.state, b.reason = m.state, m.reason
	case msgEvents:
		b.eventsUp = m.up
		if m.up {
			b.maybeHeal()
		}
	case msgAdmit:
		m.reply <- b.decide(m.req)
	case msgCall:
		m.fn()
		close(m.done)
	case msgUsage:
		b.rate.note(b.now(), m.used)
	case msgView:
		m.reply <- b.answer(m.req)
	default:
		if b.reconciling {
			if len(b.held) < maxBuffered {
				b.held = append(b.held, m)
			} else {
				b.dropped++
			}
			return
		}
		b.apply(m)
	}
}

func (b *book) apply(m any) {
	switch m := m.(type) {
	case msgOrder:
		b.applyOrder(&m.e, m.cause, m.code)
	case msgResult:
		b.applyResult(m)
	case msgFill:
		b.applyFill(&m.e)
	case msgBalances:
		b.applyBalances(m.e)
	case msgPositions:
		b.applyPositions(m.e)
	}
}

// ---------- orders ----------

// orderKey is the venue key. An order the venue has numbered is stored under that order id.
// One it has not numbered yet is stored under its client id until the first message records the
// order id. An empty client id is stored only under the order id.
func orderKey(orderID, cid string) string {
	if orderID != "" {
		return "o:" + orderID
	}
	return "c:" + cid
}

// find joins a venue event onto a live order. The venue key is the order id. An order the venue
// has not numbered yet is joined by client id, then stored under the order id. An empty client
// id matches only by order id.
func (b *book) find(o *tdv1.Order) (string, *tdv1.Order) {
	if o.OrderId != "" {
		if cur, ok := b.orders["o:"+o.OrderId]; ok {
			return "o:" + o.OrderId, cur
		}
	}
	if o.ClientOrderId != "" {
		if k, ok := b.byCloid[o.ClientOrderId]; ok {
			return k, b.orders[k]
		}
	}
	return "", nil
}

func (b *book) put(old string, o *tdv1.Order) {
	b.remove(old)
	k := orderKey(o.OrderId, o.ClientOrderId)
	b.orders[k] = o
	if o.ClientOrderId != "" {
		b.byCloid[o.ClientOrderId] = k
	}
}

func (b *book) remove(k string) {
	if k == "" {
		return
	}
	if o, ok := b.orders[k]; ok {
		if b.byCloid[o.ClientOrderId] == k {
			delete(b.byCloid, o.ClientOrderId)
		}
		delete(b.orders, k)
	}
}

func terminal(s tdv1.OrderStatus) bool {
	switch s {
	case tdv1.OrderStatus_ORDER_STATUS_FILLED, tdv1.OrderStatus_ORDER_STATUS_CANCELLED,
		tdv1.OrderStatus_ORDER_STATUS_REJECTED, tdv1.OrderStatus_ORDER_STATUS_EXPIRED:
		return true
	}
	return false
}

// rank orders the statuses an order goes through. UNKNOWN is below everything: whatever the venue
// says next is news.
func rank(s tdv1.OrderStatus) int {
	switch s {
	case tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW:
		return 1
	case tdv1.OrderStatus_ORDER_STATUS_NEW:
		return 2
	case tdv1.OrderStatus_ORDER_STATUS_ACCEPTED:
		return 3
	case tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED:
		return 4
	}
	if terminal(s) {
		return 5
	}
	return 0
}

// stale: next says less than cur already does.
func stale(cur, next *tdv1.Order) bool {
	switch {
	case next.FilledQty < cur.FilledQty:
		return true
	case next.FilledQty > cur.FilledQty:
		return false
	}
	return rank(next.Status) < rank(cur.Status)
}

func causeOf(s tdv1.OrderStatus) tdv1.OrderCause {
	switch s {
	case tdv1.OrderStatus_ORDER_STATUS_NEW:
		return tdv1.OrderCause_ORDER_CAUSE_ACKNOWLEDGED
	case tdv1.OrderStatus_ORDER_STATUS_ACCEPTED:
		return tdv1.OrderCause_ORDER_CAUSE_ACCEPTED
	case tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED, tdv1.OrderStatus_ORDER_STATUS_FILLED:
		return tdv1.OrderCause_ORDER_CAUSE_FILLED
	case tdv1.OrderStatus_ORDER_STATUS_CANCELLED:
		return tdv1.OrderCause_ORDER_CAUSE_CANCELLED
	case tdv1.OrderStatus_ORDER_STATUS_REJECTED:
		return tdv1.OrderCause_ORDER_CAUSE_REJECTED
	case tdv1.OrderStatus_ORDER_STATUS_EXPIRED:
		return tdv1.OrderCause_ORDER_CAUSE_EXPIRED
	}
	return tdv1.OrderCause_ORDER_CAUSE_RECONCILED
}

// sameOrder: the two records say the same thing, apart from the times.
func sameOrder(a, b *tdv1.Order) bool {
	x, y := proto.Clone(a).(*tdv1.Order), proto.Clone(b).(*tdv1.Order)
	x.ExchTsNs, y.ExchTsNs, x.TdTsNs, y.TdTsNs = 0, 0, 0, 0
	return proto.Equal(x, y)
}

func (b *book) applyOrder(e *exchange.OrderUpdate, cause tdv1.OrderCause, code int32) {
	o, ok := b.convertOrder(e)
	if !ok {
		return
	}
	k, cur := b.find(o)
	if cur == nil && (b.finished.has("o:"+o.OrderId) || (o.ClientOrderId != "" && b.finished.has("c:"+o.ClientOrderId))) {
		return // the order already finished: this is a repeat, or older than its end
	}
	if cur != nil {
		if stale(cur, o) {
			return
		}
		o.TdTsNs, o.CancelSentNs = cur.TdTsNs, cur.CancelSentNs
		if sameOrder(cur, o) {
			b.settleQuery(cur)
			return
		}
	} else {
		o.TdTsNs = b.now().UnixNano()
	}
	if terminal(o.Status) {
		o.CancelSentNs = 0
		b.remove(k)
		if o.OrderId != "" {
			b.finished.add("o:" + o.OrderId)
		}
		if o.ClientOrderId != "" {
			b.finished.add("c:" + o.ClientOrderId)
		}
	} else {
		b.put(k, o)
	}
	if cause == tdv1.OrderCause_ORDER_CAUSE_UNSPECIFIED {
		cause = causeOf(o.Status)
	}
	b.publish(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{
		Order: o, Cause: cause, Code: code, Reason: e.RejectReason,
	}}})
	b.settleQuery(o)
}

// ---------- fills, balances, positions ----------

func (b *book) applyFill(e *exchange.Fill) {
	if e.FillID != "" && !b.fills.add(e.FillID) {
		return
	}
	info, ok := b.resolve(e.Exch)
	if !ok {
		return
	}
	b.publish(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_Fill{Fill: &tdv1.Fill{
		Uid: info.UID, FillId: e.FillID, OrderId: e.OrderID, ClientOrderId: e.ClientOrderID,
		Side: commonv1.Side(e.Side), Price: e.Price, Qty: scale(info, e.Qty),
		Fee: e.Fee, FeeAsset: e.FeeAsset, Liquidity: e.Liquidity, ExchTsNs: e.ExchTsNs,
	}}})
}

func (b *book) applyBalances(e []exchange.Balance) {
	out := make([]*tdv1.Balance, 0, len(e))
	for i := range e {
		x := convertBalance(&e[i])
		b.balances[x.Asset] = x
		out = append(out, proto.Clone(x).(*tdv1.Balance))
	}
	if len(out) > 0 {
		b.publish(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_BalanceUpdate{BalanceUpdate: &tdv1.BalanceUpdate{Balances: out}}})
	}
}

func (b *book) applyPositions(e []exchange.Position) {
	out := make([]*tdv1.Position, 0, len(e))
	for i := range e {
		x, ok := b.convertPosition(&e[i])
		if !ok {
			continue
		}
		// Bybit sends a flat row for a symbol whenever an order touches it. A flat row for a position
		// not held changes nothing but the leverage cache, so it is not published.
		if _, held := b.positions[x.Uid]; x.Size == 0 && !held {
			b.setPosition(x)
			continue
		}
		b.setPosition(x)
		out = append(out, proto.Clone(x).(*tdv1.Position))
	}
	if len(out) > 0 {
		b.publish(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{Positions: out}}})
	}
}

// setPosition keeps a position, or forgets one whose size is zero. Its leverage goes to the cache,
// which keeps it after the position closes: leverage is the instrument's setting (D606).
func (b *book) setPosition(x *tdv1.Position) {
	if x.Leverage != nil {
		b.leverage[x.Uid] = *x.Leverage
	}
	if x.Size == 0 {
		delete(b.positions, x.Uid)
		return
	}
	b.positions[x.Uid] = x
}

// ---------- recon ----------

func (b *book) recon(snap *exchange.AccountSnapshot) {
	now := b.now().UnixNano()
	prev := b.orders
	b.orders, b.byCloid = map[string]*tdv1.Order{}, map[string]string{}
	seen := map[*tdv1.Order]bool{}
	for i := range snap.OpenOrders {
		o, ok := b.convertOrder(&snap.OpenOrders[i])
		if !ok || terminal(o.Status) {
			continue
		}
		o.TdTsNs = now
		if cur := findIn(prev, o); cur != nil {
			o.TdTsNs, o.CancelSentNs = cur.TdTsNs, cur.CancelSentNs
			seen[cur] = true
		}
		b.put("", o)
	}
	// A live order the snapshot does not have ended while the worker could not see it, or was
	// never placed. Until the venue is asked (E6-08), it is UNKNOWN (D615). One the venue has not
	// answered yet, or one already UNKNOWN, stays as it was.
	for _, cur := range prev {
		if seen[cur] {
			continue
		}
		o := proto.Clone(cur).(*tdv1.Order)
		if o.Status != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
			o.Status = tdv1.OrderStatus_ORDER_STATUS_UNKNOWN
		}
		b.put("", o)
	}

	b.positions = map[uint32]*tdv1.Position{}
	for i := range snap.Positions {
		if x, ok := b.convertPosition(&snap.Positions[i]); ok {
			b.setPosition(x)
		}
	}
	b.balances = map[string]*tdv1.Balance{}
	for i := range snap.Balances {
		x := convertBalance(&snap.Balances[i])
		b.balances[x.Asset] = x
	}
	b.reconciledAt = snap.AsOfNs

	reason := tdv1.ReconReason_RECON_REASON_RECONNECT
	if b.recons == 0 {
		reason = tdv1.ReconReason_RECON_REASON_INCARNATION
	}
	b.recons++
	b.publish(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_Recon{Recon: &tdv1.Recon{
		Incarnation: b.source.GetIncarnation(), Reason: reason,
		OpenOrders: b.openOrders(0, false), Positions: b.positionList(nil), Balances: b.balanceList(),
		AsOfNs: snap.AsOfNs,
	}}})

	held := b.held
	b.held, b.reconciling = nil, false
	if b.dropped > 0 {
		b.log.Warn().Int("dropped", b.dropped).Msg("events dropped while a recon ran")
		b.dropped = 0
	}
	for _, m := range held {
		b.apply(m)
	}
	// A live order the snapshot did not have is UNKNOWN. Ask the venue, unless a send is still
	// inside its validity window and already has a query waiting (D615, D605).
	for _, o := range b.orders {
		if o.GetStatus() == tdv1.OrderStatus_ORDER_STATUS_UNKNOWN && o.GetClientOrderId() != "" {
			b.ensureQuery(o, b.now())
		}
	}
	b.wakeQueries()
}

// findIn joins a recon row onto a previous live order, by order id, else by client id.
// An empty client id joins only by order id, so the old row is not left UNKNOWN beside a copy.
func findIn(orders map[string]*tdv1.Order, o *tdv1.Order) *tdv1.Order {
	if o.OrderId != "" {
		if cur, ok := orders["o:"+o.OrderId]; ok {
			return cur
		}
	}
	if o.ClientOrderId != "" {
		for _, cur := range orders {
			if cur.ClientOrderId == o.ClientOrderId {
				return cur
			}
		}
	}
	return nil
}

// ---------- td.view ----------

func (b *book) answer(req *tdv1.ViewRequest) *tdv1.ViewReply {
	r := &tdv1.ViewReply{State: b.state, ReconciledAtNs: b.reconciledAt, Incarnation: b.source.GetIncarnation(), Seq: b.seq}
	switch q := req.GetReq().(type) {
	case *tdv1.ViewRequest_Oms:
		// Only live orders: a finished one is on td.evt and in the store, not here (D615).
		r.Reply = &tdv1.ViewReply_Oms{Oms: &tdv1.OmsView{Orders: b.openOrders(q.Oms.GetAlgoId(), true)}}
	case *tdv1.ViewRequest_Ledger:
		r.Reply = &tdv1.ViewReply_Ledger{Ledger: &tdv1.LedgerView{Balances: b.balanceList(), Positions: b.positionList(q.Ledger.GetUids())}}
	case *tdv1.ViewRequest_Leverage:
		info := &tdv1.LeverageInfo{Uid: q.Leverage.GetUid()}
		if v, ok := b.leverage[info.Uid]; ok {
			info.Leverage = &v
		}
		r.Reply = &tdv1.ViewReply_Leverage{Leverage: info}
	default:
		r.Reply = &tdv1.ViewReply_Error{Error: &commonv1.Error{Code: "invalid", Message: "empty view request"}}
	}
	return r
}

// openOrders are the live orders, copied, sorted by client order id then order id. With filter, only
// those whose client order id carries algo: an order that is not the platform's carries none.
func (b *book) openOrders(algo uint32, filter bool) []*tdv1.Order {
	out := make([]*tdv1.Order, 0, len(b.orders))
	for _, o := range b.orders {
		if filter && algo != 0 {
			if a, ok := cloid.AlgoOf(o.ClientOrderId); !ok || a != algo {
				continue
			}
		}
		out = append(out, proto.Clone(o).(*tdv1.Order))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ClientOrderId != out[j].ClientOrderId {
			return out[i].ClientOrderId < out[j].ClientOrderId
		}
		return out[i].OrderId < out[j].OrderId
	})
	return out
}

func (b *book) positionList(uids []uint32) []*tdv1.Position {
	want := map[uint32]bool{}
	for _, u := range uids {
		want[u] = true
	}
	out := make([]*tdv1.Position, 0, len(b.positions))
	for uid, p := range b.positions {
		if len(want) > 0 && !want[uid] {
			continue
		}
		out = append(out, proto.Clone(p).(*tdv1.Position))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Uid < out[j].Uid })
	return out
}

func (b *book) balanceList() []*tdv1.Balance {
	out := make([]*tdv1.Balance, 0, len(b.balances))
	for _, x := range b.balances {
		out = append(out, proto.Clone(x).(*tdv1.Balance))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Asset < out[j].Asset })
	return out
}

// ---------- td.evt ----------

func (b *book) publish(ev *tdv1.AccountEvent) {
	b.seq++
	ev.Seq, ev.Source = b.seq, b.source
	data, err := proto.Marshal(ev)
	if err == nil {
		err = b.bus.Publish(b.subj, data)
	}
	if err != nil {
		// Core NATS: a subscriber that missed it rebuilds from td.view and the seq (D606).
		b.log.Warn().Err(err).Uint64("seq", b.seq).Msg("td.evt")
	}
}

// ---------- conversion (D203) ----------

// resolve is the instrument a venue spelling names: the active table, else SYM's terminal table
// for an instrument delisted since. One SYM has never seen is logged and dropped (D615).
func (b *book) resolve(k symbol.ExchKey) (*symbol.Info, bool) {
	info, err := b.ins.Reverse(b.ctx, k, false)
	if err == nil && info != nil {
		return info, true
	}
	ctx, cancel := context.WithTimeout(b.ctx, inactiveWait)
	defer cancel()
	info, err = b.ins.Reverse(ctx, k, true)
	if err == nil && info != nil {
		return info, true
	}
	b.log.Warn().Str("venue", k.Venue).Str("category", string(k.Category)).Str("exch_ticker", k.ExchTicker).
		Err(err).Msg("instrument not in SYM; event dropped")
	return nil, false
}

// scale turns a venue quantity into the platform's: contracts times ContractSize (D203). Spot and
// venues that already count in contract_value_ccy have ContractSize 0 or 1.
func scale(info *symbol.Info, q float64) float64 {
	if info.ContractSize > 0 && decimal.Has(q) {
		return q * info.ContractSize
	}
	return q
}

func opt(v float64) *float64 {
	if !decimal.Has(v) {
		return nil
	}
	return &v
}

func (b *book) convertOrder(e *exchange.OrderUpdate) (*tdv1.Order, bool) {
	info, ok := b.resolve(e.Exch)
	if !ok {
		return nil, false
	}
	return &tdv1.Order{
		Uid: info.UID, OrderId: e.OrderID, ClientOrderId: e.ClientOrderID,
		Side: commonv1.Side(e.Side), OrderType: commonv1.OrderType(e.Type), Status: tdv1.OrderStatus(e.Status),
		Qty: scale(info, e.Qty), QuoteQty: opt(e.QuoteQty), Price: opt(e.Price),
		FilledQty: scale(info, e.FilledQty), AvgPrice: opt(e.AvgPrice), ExchTsNs: e.ExchTsNs,
	}, true
}

func (b *book) convertPosition(e *exchange.Position) (*tdv1.Position, bool) {
	info, ok := b.resolve(e.Exch)
	if !ok {
		return nil, false
	}
	return &tdv1.Position{
		Uid: info.UID, Size: scale(info, e.Size), EntryPrice: opt(e.EntryPrice), MarkPrice: opt(e.MarkPrice),
		UnrealizedPnl: opt(e.UnrealizedPnl), Leverage: opt(e.Leverage),
	}, true
}

// convertBalance: prelock stays 0. Submit and cancel do not reserve balance.
func convertBalance(e *exchange.Balance) *tdv1.Balance {
	return &tdv1.Balance{Asset: e.Asset, Free: e.Free, Locked: e.Locked}
}

func cloneKey(k symbol.ExchKey) symbol.ExchKey {
	return symbol.ExchKey{Venue: strings.Clone(k.Venue), Category: symbol.Category(strings.Clone(string(k.Category))), ExchTicker: strings.Clone(k.ExchTicker)}
}

// idSet remembers the last n ids added.
type idSet struct {
	n    int
	set  map[string]struct{}
	ring []string
	next int
}

func newIDSet(n int) idSet { return idSet{n: n, set: make(map[string]struct{}, n)} }

func (s *idSet) has(id string) bool { _, ok := s.set[id]; return ok }

// add reports whether id is new.
func (s *idSet) add(id string) bool {
	if _, ok := s.set[id]; ok {
		return false
	}
	if len(s.ring) < s.n {
		s.ring = append(s.ring, id)
	} else {
		delete(s.set, s.ring[s.next])
		s.ring[s.next] = id
		s.next = (s.next + 1) % s.n
	}
	s.set[id] = struct{}{}
	return true
}
