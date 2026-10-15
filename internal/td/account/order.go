package account

import (
	"context"
	"errors"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

const (
	// seenMax and seenAge bound the in-memory cloid record (D618). Whichever is reached first drops
	// the oldest entry. It is not persisted.
	seenMax = 100_000
	seenAge = 24 * time.Hour

	queryMin = time.Second
	queryMax = 30 * time.Second
	queryFor = 5 * time.Minute
)

// errNotSent: the request never left the process, so the caller may try the other path (D624).
var errNotSent = errors.New("account: order was not written")

// Reasons on OrderReply and on Report. The code is what a program branches on.
const (
	reasonAlgo       = "algo_id does not match the client_order_id"
	reasonNoCloid    = "cancel has no client_order_id"
	reasonCloid      = "client_order_id does not decode"
	reasonContent    = "client_order_id was already used for a different order"
	reasonInstrument = "instrument is missing or not tradable"
	reasonVenue      = "venue is not tradable for this account"
	reasonInvalid    = "invalid order"
	reasonNotOpen    = "order is not open"
	reasonRate       = "rate limited"
	reasonNoBatch    = "batch cancel is not available"
	reasonDegraded   = "account is degraded"
	reasonHedge      = "position side is not allowed"
	reasonNotPlaced  = "order was not placed"
	reasonQuery      = "query_failed"
)

var hedgeParam = map[string]bool{"positionSide": true, "posSide": true, "positionIdx": true}

// orderEnv is what Init knows about orders (D618–D624).
type orderEnv struct {
	trades   []string
	limits   exchange.AccountLimits
	validity time.Duration
	skew     time.Duration
	hold     time.Duration
	onState  func(tdv1.AccountState, string)
	report   func(*tdctlv1.Report) error
}

func (b *book) configure(e orderEnv) {
	b.trades = append([]string(nil), e.trades...)
	b.limits = e.limits
	b.validity = e.validity
	b.skew = e.skew
	b.hold = e.hold
	b.onState = e.onState
	b.reportFn = e.report
	b.rate = newLimiter(e.limits.OrderRate)
	b.seen = newSeen()
	b.queries = map[string]*query{}
	b.first = map[uint32]int64{}
	b.qwake = make(chan struct{}, 1)
}

func (b *book) clockSkew() time.Duration {
	if b.skew > 0 {
		return b.skew
	}
	return time.Second
}

func (b *book) rateHold() time.Duration {
	if b.hold > 0 {
		return b.hold
	}
	return time.Minute
}

// window resolves recv_window_ms: 0 is the instance default, and anything past the venue maximum
// is clamped to it (D620).
func (b *book) window(ms int64) time.Duration {
	w := b.validity
	if w <= 0 {
		w = exchange.DefaultValidity
	}
	if ms > 0 {
		w = time.Duration(ms) * time.Millisecond
	}
	if w > exchange.MaxValidity {
		w = exchange.MaxValidity
	}
	if w < time.Millisecond {
		w = time.Millisecond
	}
	return w
}

type resultKind uint8

const (
	resultSubmit resultKind = iota + 1
	resultCancel
	resultQuery
	resultBatch
)

type admitOut struct {
	reply  *tdv1.OrderReply
	report *tdctlv1.Report
	jobs   []*sendJob
}

type sendJob struct {
	cancel bool
	cloid  string
	window time.Duration
	req    *exchange.OrderRequest
	ref    exchange.CancelRef
	refs   []exchange.CancelRef // set on a BatchCancel: one CancelBatch, however long the list is
	info   *symbol.Info
}

// ---------- td.order ----------

func (w *Worker) handleOrder(ctx context.Context, raw []byte) []byte {
	req := &tdv1.OrderRequest{}
	if err := proto.Unmarshal(raw, req); err != nil {
		return marshalReply(orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid, ""))
	}
	out := w.book.admit(ctx, req)
	if out.report != nil && w.report != nil {
		// A failed report does not stop the order (D601).
		_ = w.report(out.report)
	}
	for _, job := range out.jobs {
		go w.dispatch(ctx, job)
	}
	return marshalReply(out.reply)
}

func (b *book) admit(ctx context.Context, req *tdv1.OrderRequest) admitOut {
	reply := make(chan admitOut, 1)
	if !b.send(ctx, msgAdmit{req: req, reply: reply}) {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_DEGRADED), reasonDegraded, reqCloid(req))}
	}
	select {
	case out := <-reply:
		return out
	case <-ctx.Done():
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_DEGRADED), reasonDegraded, reqCloid(req))}
	}
}

func (b *book) decide(req *tdv1.OrderRequest) admitOut {
	switch r := req.GetReq().(type) {
	case *tdv1.OrderRequest_Submit:
		return b.decideSubmit(r.Submit)
	case *tdv1.OrderRequest_Cancel:
		return b.decideCancel(r.Cancel)
	case *tdv1.OrderRequest_BatchCancel:
		return b.decideBatch(r.BatchCancel)
	default:
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid, "")}
	}
}

func (b *book) decideSubmit(s *tdv1.Submit) admitOut {
	if s == nil {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid, "")}
	}
	cid := s.GetClientOrderId()
	now := b.now()
	id, err := cloid.Decode(cid)
	if err != nil {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonCloid, now)
	}
	if id.Algo != s.GetAlgoId() {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_ALGO_MISMATCH), reasonAlgo, now)
	}
	if rec, ok := b.seen.get(cid, now); ok {
		if sameSubmit(rec.sub, s, rec.qtyStep, rec.pxStep) {
			return admitOut{reply: proto.Clone(rec.reply).(*tdv1.OrderReply)}
		}
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonContent, cid)}
	}
	info, ierr := b.ins.GetByUID(b.ctx, s.GetUid(), false)
	qtyStep, pxStep := stepsOf(info)
	if live := b.liveByCloid(cid); live != nil {
		if sameLive(s, live, qtyStep, pxStep) {
			rep := orderReply(true, 0, "", cid)
			b.seen.add(seenRec(cid, now, s, rep, qtyStep, pxStep))
			return admitOut{reply: rep}
		}
		// A mismatched attempt must not be remembered: it would hide the live order (D619).
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonContent, cid)}
	}
	if ierr != nil || info == nil || !info.IsTradable() {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInstrument, now)
	}
	if !tradableVenue(b.trades, info.Ticker.Venue) {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_VENUE_NOT_TRADABLE), reasonVenue, now)
	}
	sent, vErr := normalize(s, info)
	if vErr != "" {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), vErr, now)
	}
	if hedgeParams(sent.GetParams()) {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_HEDGE_MODE), reasonHedge, now)
	}
	if b.state == tdv1.AccountState_ACCOUNT_STATE_DEGRADED {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_DEGRADED), reasonDegraded, now)
	}
	if !b.rate.allow(now) {
		return b.rejectSubmit(s, int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED), reasonRate, now)
	}
	win := b.window(s.GetRecvWindowMs())
	b.rate.consume(now)
	o := &tdv1.Order{
		Uid: sent.GetUid(), ClientOrderId: cid, Side: sent.GetSide(), OrderType: sent.GetOrderType(),
		Status: tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW, Qty: sent.GetQty(), QuoteQty: sent.QuoteQty, Price: sent.Price,
		TdTsNs: now.UnixNano(),
	}
	b.put("", o)
	b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_SUBMITTED, 0, "")
	rep := orderReply(true, 0, "", cid)
	b.seen.add(seenRec(cid, now, s, rep, qtyStep, pxStep))
	var report *tdctlv1.Report
	if fresh, all := b.noteFirst(sent.GetUid(), now.UnixNano()); fresh {
		report = &tdctlv1.Report{State: b.state, Reason: b.reason, ReconciledAtNs: b.reconciledAt, FirstSubmitNs: all}
	}
	req := &exchange.OrderRequest{
		Instrument: info, Side: exchange.Side(sent.GetSide()), Type: exchange.OrderType(sent.GetOrderType()),
		TIF: exchange.TimeInForce(sent.GetTif()), ReduceOnly: sent.GetReduceOnly(), ClientOrderID: cid,
		Params: cloneParams(sent.GetParams()), Validity: win,
		Qty: qtyOrAbsent(sent.Qty), QuoteQty: qtyOrAbsent(sent.QuoteQty), Price: qtyOrAbsent(sent.Price),
	}
	return admitOut{reply: rep, report: report, jobs: []*sendJob{{cloid: cid, window: win, req: req, info: info}}}
}

func (b *book) decideCancel(c *tdv1.Cancel) admitOut {
	if c == nil {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid, "")}
	}
	cid := c.GetClientOrderId()
	if cid == "" {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_ALGO_MISMATCH), reasonNoCloid, "")}
	}
	id, err := cloid.Decode(cid)
	if err != nil {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonCloid, cid)}
	}
	if id.Algo != c.GetAlgoId() {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_ALGO_MISMATCH), reasonAlgo, cid)}
	}
	_, o := b.find(&tdv1.Order{ClientOrderId: cid})
	if o == nil || terminal(o.GetStatus()) || o.GetClientOrderId() != cid {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN), reasonNotOpen, cid)}
	}
	if o.CancelSentNs != 0 {
		return admitOut{reply: orderReply(true, 0, "", cid)}
	}
	switch o.GetStatus() {
	case tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW, tdv1.OrderStatus_ORDER_STATUS_NEW,
		tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
		tdv1.OrderStatus_ORDER_STATUS_UNKNOWN:
	default:
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN), reasonNotOpen, cid)}
	}
	now := b.now()
	if !b.rate.allowCancel(now, 1) {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED), reasonRate, cid)}
	}
	info, ierr := b.ins.GetByUID(b.ctx, o.GetUid(), false)
	if ierr != nil || info == nil {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInstrument, cid)}
	}
	b.rate.consume(now)
	o.CancelSentNs = now.UnixNano()
	b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED, 0, "")
	win := b.window(c.GetRecvWindowMs())
	ref := exchange.CancelRef{Instrument: info, OrderID: o.GetOrderId(), ClientOrderID: cid, Validity: win}
	return admitOut{reply: orderReply(true, 0, "", cid), jobs: []*sendJob{{cancel: true, cloid: cid, window: win, ref: ref, info: info}}}
}

// decideBatch forwards the named orders as one CancelBatch (D625). Each entry is a client order id.
// The worker looks up the live order by that id and sends the order's order id (or the client id
// when the venue has not numbered it yet). It does not choose orders, split the list, queue, retry,
// or check which algo an order belongs to. The reply only says the request was taken. Each order
// that is sent gets its own CANCEL_REQUESTED; the venue answer is that order's later update (D610).
func (b *book) decideBatch(c *tdv1.BatchCancel) admitOut {
	if c == nil || len(c.GetOrders()) == 0 {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid, "")}
	}
	if b.limits.CancelBatch == 1 {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonNoBatch, "")}
	}
	// No recv_window_ms on this message; the instance default applies (D620).
	win := b.window(0)
	refs := make([]exchange.CancelRef, 0, len(c.GetOrders()))
	orders := make([]*tdv1.Order, 0, len(c.GetOrders()))
	for _, item := range c.GetOrders() {
		o, code, reason := b.batchOrder(item)
		if code != 0 {
			return admitOut{reply: orderReply(false, code, reason, "")}
		}
		info, err := b.ins.GetByUID(b.ctx, o.GetUid(), false)
		if err != nil || info == nil {
			// One unresolvable instrument rejects this request and no other (D625).
			return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInstrument, "")}
		}
		refs = append(refs, exchange.CancelRef{
			Instrument: info, OrderID: o.GetOrderId(), ClientOrderID: o.GetClientOrderId(), Validity: win,
		})
		orders = append(orders, o)
	}
	now := b.now()
	if !b.rate.allowCancel(now, 1) {
		return admitOut{reply: orderReply(false, int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED), reasonRate, "")}
	}
	b.rate.consume(now)
	for _, o := range orders {
		o.CancelSentNs = now.UnixNano()
		b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REQUESTED, 0, "")
	}
	return admitOut{reply: orderReply(true, 0, "", ""), jobs: []*sendJob{{cancel: true, window: win, refs: refs}}}
}

// batchOrder is the live order one client order id names. An empty id, or one that names no live
// order, rejects the whole batch: TD does not drop items, and it does not check algo ownership.
func (b *book) batchOrder(item *tdv1.BatchCancelOrder) (*tdv1.Order, int32, string) {
	if item == nil || item.GetClientOrderId() == "" {
		return nil, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid
	}
	cid := item.GetClientOrderId()
	_, o := b.find(&tdv1.Order{ClientOrderId: cid})
	if o == nil || terminal(o.GetStatus()) || o.GetClientOrderId() != cid {
		return nil, int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN), reasonNotOpen
	}
	switch o.GetStatus() {
	case tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW, tdv1.OrderStatus_ORDER_STATUS_NEW,
		tdv1.OrderStatus_ORDER_STATUS_ACCEPTED, tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
		tdv1.OrderStatus_ORDER_STATUS_UNKNOWN:
		return o, 0, ""
	default:
		return nil, int32(tdv1.RejectCode_REJECT_CODE_ORDER_NOT_OPEN), reasonNotOpen
	}
}

// rejectSubmit remembers the reply so a retry of the same cloid returns it and is not sent (D619).
func (b *book) rejectSubmit(s *tdv1.Submit, code int32, reason string, now time.Time) admitOut {
	cid := s.GetClientOrderId()
	rep := orderReply(false, code, reason, cid)
	if cid != "" {
		qtyStep, pxStep := 1e-8, 1e-8
		if info, err := b.ins.GetByUID(b.ctx, s.GetUid(), false); err == nil && info != nil {
			qtyStep, pxStep = stepsOf(info)
		}
		b.seen.add(seenRec(cid, now, s, rep, qtyStep, pxStep))
	}
	return admitOut{reply: rep}
}

func (b *book) publishOrder(o *tdv1.Order, cause tdv1.OrderCause, code int32, reason string) {
	b.publish(&tdv1.AccountEvent{Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{
		Order: proto.Clone(o).(*tdv1.Order), Cause: cause, Code: code, Reason: reason,
	}}})
}

func (b *book) noteFirst(uid uint32, ns int64) (bool, map[uint32]int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.first == nil {
		b.first = map[uint32]int64{}
	}
	if _, ok := b.first[uid]; ok {
		return false, nil
	}
	b.first[uid] = ns
	return true, b.copyFirst()
}

func (b *book) firstCopy() map[uint32]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.copyFirst()
}

func (b *book) copyFirst() map[uint32]int64 {
	if len(b.first) == 0 {
		return nil
	}
	out := make(map[uint32]int64, len(b.first))
	maps.Copy(out, b.first)
	return out
}

// ---------- send ----------

func (w *Worker) dispatch(ctx context.Context, job *sendJob) {
	now := w.now()
	deadline := now.Add(job.window + w.book.clockSkew())
	if len(job.refs) > 0 {
		// One BatchCancel is one CancelBatch, and it stays on REST (D303, D625).
		perrs, used, err := w.restBatch(ctx, job, now)
		w.book.send(ctx, msgResult{
			kind: resultBatch, refs: job.refs, perrs: perrs, err: err,
			sent: now, window: job.window, deadline: deadline, used: used,
		})
		return
	}
	if err := w.tryWS(ctx, job, now, deadline); err == nil {
		return // the stream's answer arrives on its own
	} else if !errors.Is(err, errNotSent) {
		w.finish(ctx, job, now, deadline, nil, nil, err)
		return
	}
	res, used, err := w.rest(ctx, job, now)
	w.finish(ctx, job, now, deadline, res, used, err)
}

func (w *Worker) finish(ctx context.Context, job *sendJob, sent time.Time, deadline time.Time, res *exchange.OrderResult, used []exchange.Used, err error) {
	kind := resultSubmit
	orderID := ""
	if job.cancel {
		kind = resultCancel
		orderID = job.ref.OrderID
	}
	w.book.send(ctx, msgResult{
		kind: kind, cloid: job.cloid, orderID: orderID, res: res, err: err,
		sent: sent, window: job.window, deadline: deadline, used: used,
	})
}

func (w *Worker) rest(ctx context.Context, job *sendJob, now time.Time) (*exchange.OrderResult, []exchange.Used, error) {
	tctx, cancel := context.WithTimeout(ctx, job.window+w.book.clockSkew())
	defer cancel()
	if job.cancel {
		call, err := w.acct.Cancel(now, job.ref)
		if err != nil {
			return nil, nil, errNotSent
		}
		resp, err := w.cfg.Doer.Do(tctx, &call.Req)
		if err != nil {
			return nil, nil, unknownOr(err)
		}
		_, perr := call.Parse(resp)
		return nil, w.acct.Usage(resp, now), perr
	}
	call, err := w.acct.Submit(now, job.req)
	if err != nil {
		return nil, nil, errNotSent
	}
	resp, err := w.cfg.Doer.Do(tctx, &call.Req)
	if err != nil {
		return nil, nil, unknownOr(err)
	}
	res, perr := call.Parse(resp)
	return res, w.acct.Usage(resp, now), perr
}

func (w *Worker) restBatch(ctx context.Context, job *sendJob, now time.Time) ([]error, []exchange.Used, error) {
	tctx, cancel := context.WithTimeout(ctx, job.window+w.book.clockSkew())
	defer cancel()
	call, err := w.acct.CancelBatch(now, job.refs)
	if err != nil {
		return nil, nil, errNotSent
	}
	resp, err := w.cfg.Doer.Do(tctx, &call.Req)
	if err != nil {
		return nil, nil, unknownOr(err)
	}
	perrs, perr := call.Parse(resp)
	return perrs, w.acct.Usage(resp, now), perr
}

func unknownOr(err error) error {
	if errors.Is(err, exchange.ErrUnknownOutcome) {
		return err
	}
	return fmtError(err)
}

func fmtError(err error) error {
	return errors.Join(exchange.ErrUnknownOutcome, err)
}

func (b *book) applyResult(m msgResult) {
	b.noteLimit(m.used, limitErr(m))
	if m.kind == resultQuery {
		b.queryAnswer(m)
		return
	}
	if m.kind == resultBatch {
		b.applyBatch(m)
		return
	}
	b.applyOne(m)
}

// noteLimit records venue usage and, on a 429 or 418, pauses every send (D420).
func (b *book) noteLimit(used []exchange.Used, err error) {
	var reset time.Time
	if len(used) > 0 {
		reset = b.rate.note(b.now(), used)
	}
	var rl *exchange.RateLimited
	if !errors.As(err, &rl) {
		return
	}
	if len(used) == 0 {
		reset = time.Time{}
	}
	b.rate.pause(b.now(), time.Duration(rl.RetryAfterMs)*time.Millisecond, b.rateHold(), reset)
	b.wakeQueries()
}

// limitErr is the rate-limit error on a result: the whole request, or one order of a batch.
func limitErr(m msgResult) error {
	var rl *exchange.RateLimited
	if errors.As(m.err, &rl) {
		return m.err
	}
	for _, e := range m.perrs {
		if errors.As(e, &rl) {
			return e
		}
	}
	return nil
}

func (b *book) applyBatch(m msgResult) {
	one := func(ref exchange.CancelRef, err error) {
		b.applyOne(msgResult{
			kind: resultCancel, cloid: ref.ClientOrderID, orderID: ref.OrderID, err: err,
			sent: m.sent, window: m.window, deadline: m.deadline,
		})
	}
	if m.err != nil || len(m.perrs) != len(m.refs) {
		// A batch with no per-order answer is unknown for every order in it: query by cloid,
		// do not resend (D626). A readable whole-request refusal applies to each order.
		err := m.err
		if err == nil {
			err = exchange.ErrUnknownOutcome
		}
		for _, ref := range m.refs {
			one(ref, err)
		}
		return
	}
	for i, ref := range m.refs {
		one(ref, m.perrs[i])
	}
}

func (b *book) applyOne(m msgResult) {
	var rl *exchange.RateLimited
	if errors.As(m.err, &rl) {
		code := rl.Status
		if code == 0 {
			code = 429
		}
		b.venueRefuse(m, code, "rate limited")
		return
	}
	var rej *exchange.Reject
	if errors.As(m.err, &rej) {
		b.venueRefuse(m, rej.Code, rej.Reason)
		return
	}
	if errors.Is(m.err, errNotSent) {
		b.venueRefuse(m, int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), reasonInvalid)
		return
	}
	if m.err != nil {
		b.markUnknown(m)
		return
	}
	b.clearQuery(m.cloid)
	if m.kind == resultCancel {
		b.markCancelled(m.cloid)
		return
	}
	if m.res != nil {
		b.mergeSubmit(m)
	}
}

// venueRefuse is a readable refusal. A submit becomes REJECTED; a cancel clears cancel_sent_ns (D610).
func (b *book) venueRefuse(m msgResult, code int32, reason string) {
	b.clearQuery(m.cloid)
	_, o := b.find(&tdv1.Order{ClientOrderId: m.cloid, OrderId: m.orderID})
	if o == nil {
		return
	}
	if m.kind == resultCancel {
		o.CancelSentNs = 0
		b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_CANCEL_REJECTED, code, reason)
		b.settleQuery(o)
		return
	}
	o.Status = tdv1.OrderStatus_ORDER_STATUS_REJECTED
	k, _ := b.find(o)
	o.CancelSentNs = 0
	b.remove(k)
	if o.OrderId != "" {
		b.finished.add("o:" + o.OrderId)
	}
	if o.ClientOrderId != "" {
		b.finished.add("c:" + o.ClientOrderId)
	}
	b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_REJECTED, code, reason)
}

func (b *book) mergeSubmit(m msgResult) {
	_, cur := b.find(&tdv1.Order{ClientOrderId: m.cloid})
	if cur == nil {
		return
	}
	info := b.infoOf(cur.GetUid())
	e := &exchange.OrderUpdate{
		OrderID: m.res.OrderID, ClientOrderID: m.cloid,
		Side: exchange.Side(cur.GetSide()), Type: exchange.OrderType(cur.GetOrderType()),
		Status: m.res.Status, Qty: cur.GetQty(), Price: valueOr(cur.Price), QuoteQty: valueOr(cur.QuoteQty),
		FilledQty: 0, AvgPrice: decimal.Absent(), ExchTsNs: m.res.ExchTsNs,
	}
	if e.OrderID == "" {
		e.OrderID = cur.GetOrderId()
	}
	if e.Status == exchange.OrderStatusUnspecified {
		e.Status = exchange.OrderStatus(cur.GetStatus())
	}
	// convertOrder scales qty by ContractSize. The OMS qty is already in contract_value_ccy, so
	// hand the venue the contract count.
	if info != nil {
		e.Exch = info.Exch()
		e.Qty = contracts(info, cur.GetQty())
	}
	b.applyOrder(e, 0, 0)
}

func (b *book) markCancelled(cloid string) {
	_, o := b.find(&tdv1.Order{ClientOrderId: cloid})
	if o == nil {
		return
	}
	e := &exchange.OrderUpdate{
		OrderID: o.GetOrderId(), ClientOrderID: cloid, Side: exchange.Side(o.GetSide()),
		Type: exchange.OrderType(o.GetOrderType()), Status: exchange.OrderStatusCancelled,
		Qty: o.GetQty(), Price: valueOr(o.Price), QuoteQty: valueOr(o.QuoteQty),
		FilledQty: o.GetFilledQty(), AvgPrice: valueOr(o.AvgPrice), ExchTsNs: b.now().UnixNano(),
	}
	if info := b.infoOf(o.GetUid()); info != nil {
		e.Exch = info.Exch()
		e.Qty = contracts(info, o.GetQty())
		e.FilledQty = contracts(info, o.GetFilledQty())
	}
	b.applyOrder(e, 0, 0)
}

func (b *book) markUnknown(m msgResult) {
	_, o := b.find(&tdv1.Order{ClientOrderId: m.cloid, OrderId: m.orderID})
	if o == nil {
		return
	}
	if m.kind == resultSubmit && o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_UNKNOWN && rank(o.GetStatus()) < rank(tdv1.OrderStatus_ORDER_STATUS_NEW) {
		o.Status = tdv1.OrderStatus_ORDER_STATUS_UNKNOWN
		b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_OUTCOME_UNKNOWN, 0, "")
	}
	// A cancel with no answer keeps its status and cancel_sent_ns (D610).
	b.armQuery(o, m.deadline, m.kind == resultCancel)
}

// ---------- queries ----------

type query struct {
	cloid     string
	uid       uint32
	orderID   string
	next      time.Time
	backoff   time.Duration
	failSince time.Time
	inflight  bool
	forCancel bool
}

func (b *book) ensureQuery(o *tdv1.Order, due time.Time) {
	b.armQuery(o, due, o.GetCancelSentNs() != 0)
}

func (b *book) armQuery(o *tdv1.Order, due time.Time, forCancel bool) {
	if o.GetClientOrderId() == "" {
		return
	}
	if b.queries == nil {
		b.queries = map[string]*query{}
	}
	if _, ok := b.queries[o.GetClientOrderId()]; ok {
		return
	}
	b.queries[o.GetClientOrderId()] = &query{
		cloid: o.GetClientOrderId(), uid: o.GetUid(), orderID: o.GetOrderId(),
		next: due, backoff: queryMin, forCancel: forCancel,
	}
	b.wakeQueries()
}

func (b *book) clearQuery(cloid string) {
	if cloid == "" || b.queries == nil {
		return
	}
	delete(b.queries, cloid)
	b.wakeQueries()
}

func (b *book) settleQuery(o *tdv1.Order) {
	if o == nil || b.queries == nil {
		return
	}
	q := b.queries[o.GetClientOrderId()]
	if q == nil {
		return
	}
	done := terminal(o.GetStatus()) || (!q.forCancel && o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_UNKNOWN) || (q.forCancel && o.GetCancelSentNs() == 0)
	if !done {
		if o.GetOrderId() != "" {
			q.orderID = o.GetOrderId()
		}
		return
	}
	delete(b.queries, o.GetClientOrderId())
	b.maybeHeal()
	b.wakeQueries()
}

func (b *book) wakeQueries() {
	if b.qwake == nil {
		return
	}
	select {
	case b.qwake <- struct{}{}:
	default:
	}
}

func (b *book) call(ctx context.Context, fn func()) bool {
	done := make(chan struct{})
	if !b.send(ctx, msgCall{fn: fn, done: done}) {
		return false
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (b *book) nextQuery(now time.Time) time.Time {
	if b.rate.paused(now) {
		return b.rate.pauseUntil
	}
	var t time.Time
	for _, q := range b.queries {
		if q.inflight {
			continue
		}
		if t.IsZero() || q.next.Before(t) {
			t = q.next
		}
	}
	return t
}

func (b *book) takeDue(now time.Time) []query {
	if b.rate.paused(now) {
		return nil
	}
	var out []query
	for _, q := range b.queries {
		if q.inflight || q.next.After(now) {
			continue
		}
		q.inflight = true
		out = append(out, *q)
	}
	return out
}

func (w *Worker) queries(ctx context.Context) {
	for {
		var due, now time.Time
		if !w.book.call(ctx, func() {
			now = w.now()
			due = w.book.nextQuery(now)
		}) {
			return
		}
		if due.IsZero() {
			select {
			case <-w.book.qwake:
			case <-ctx.Done():
				return
			}
			continue
		}
		wait := due.Sub(now)
		if wait < 0 {
			wait = 0
		}
		select {
		case <-w.after(wait):
			w.fireQueries(ctx)
		case <-w.book.qwake:
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) after(d time.Duration) <-chan time.Time {
	if w.cfg.After != nil {
		return w.cfg.After(d)
	}
	return time.After(d)
}

func (w *Worker) fireQueries(ctx context.Context) {
	var due []query
	if !w.book.call(ctx, func() { due = w.book.takeDue(w.now()) }) {
		return
	}
	for _, q := range due {
		upd, used, err := w.execQuery(ctx, q)
		now := w.now()
		qq := q
		w.book.call(ctx, func() { w.book.queryDone(qq, upd, used, err, now) })
	}
}

func (w *Worker) execQuery(ctx context.Context, q query) (*exchange.OrderUpdate, []exchange.Used, error) {
	info, err := w.book.ins.GetByUID(ctx, q.uid, false)
	if err != nil || info == nil {
		if err == nil {
			err = errors.New("instrument not in the active table")
		}
		return nil, nil, err
	}
	// The venue key is the order id. A client id is only for an order the venue has not numbered yet.
	ref := exchange.CancelRef{Instrument: info, OrderID: q.orderID}
	if q.orderID == "" {
		ref.ClientOrderID = q.cloid
	}
	call, err := w.acct.QueryOrder(w.now(), ref)
	if err != nil {
		return nil, nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := w.cfg.Doer.Do(tctx, &call.Req)
	if err != nil {
		return nil, nil, err
	}
	upd, perr := call.Parse(resp)
	return upd, w.acct.Usage(resp, w.now()), perr
}

func (b *book) queryDone(q query, upd *exchange.OrderUpdate, used []exchange.Used, err error, now time.Time) {
	if len(used) > 0 {
		b.rate.note(now, used)
	}
	cur := b.queries[q.cloid]
	if cur == nil {
		return
	}
	cur.inflight = false
	if rl := (*exchange.RateLimited)(nil); errors.As(err, &rl) {
		// A pause is not a failed query (D420). The send already paused when the response carried
		// usage; otherwise pause from Retry-After here.
		if b.rate == nil || !b.rate.paused(now) {
			b.rate.pause(now, time.Duration(rl.RetryAfterMs)*time.Millisecond, b.rateHold(), time.Time{})
		}
		cur.next = now.Add(b.rateHold())
		if b.rate != nil && !b.rate.pauseUntil.IsZero() {
			cur.next = b.rate.pauseUntil
		}
		b.wakeQueries()
		return
	}
	if err == nil && upd != nil {
		delete(b.queries, q.cloid)
		b.applyOrder(upd, tdv1.OrderCause_ORDER_CAUSE_RECONCILED, 0)
		b.maybeHeal()
		b.wakeQueries()
		return
	}
	if errors.Is(err, exchange.ErrNotFound) {
		delete(b.queries, q.cloid)
		b.notPlaced(q.cloid)
		b.maybeHeal()
		b.wakeQueries()
		return
	}
	if cur.failSince.IsZero() {
		cur.failSince = now
	}
	if cur.backoff < queryMin {
		cur.backoff = queryMin
	}
	cur.next = now.Add(cur.backoff)
	if cur.backoff < queryMax {
		cur.backoff *= 2
		if cur.backoff > queryMax {
			cur.backoff = queryMax
		}
	}
	if !now.Before(cur.failSince.Add(queryFor)) {
		b.markDegraded()
	}
	b.wakeQueries()
}

func (b *book) queryAnswer(m msgResult) {
	b.queryDone(query{cloid: m.cloid}, m.upd, m.used, m.err, b.now())
}

func (b *book) notPlaced(cloid string) {
	_, o := b.find(&tdv1.Order{ClientOrderId: cloid})
	if o == nil {
		return
	}
	k, _ := b.find(o)
	o.Status = tdv1.OrderStatus_ORDER_STATUS_REJECTED
	o.CancelSentNs = 0
	b.remove(k)
	if o.GetOrderId() != "" {
		b.finished.add("o:" + o.GetOrderId())
	}
	b.finished.add("c:" + cloid)
	b.publishOrder(o, tdv1.OrderCause_ORDER_CAUSE_REJECTED, int32(tdv1.RejectCode_REJECT_CODE_NOT_PLACED), reasonNotPlaced)
}

func (b *book) markDegraded() {
	if b.reason == ReasonStreamLost && b.state == tdv1.AccountState_ACCOUNT_STATE_DEGRADED {
		return
	}
	if b.state == tdv1.AccountState_ACCOUNT_STATE_DEGRADED && b.reason == reasonQuery {
		return
	}
	b.state = tdv1.AccountState_ACCOUNT_STATE_DEGRADED
	b.reason = reasonQuery
	if b.onState != nil {
		b.onState(b.state, b.reason)
	}
	if b.reportFn != nil {
		_ = b.reportFn(&tdctlv1.Report{State: b.state, Reason: b.reason, ReconciledAtNs: b.reconciledAt, FirstSubmitNs: b.firstCopy()})
	}
}

func (b *book) maybeHeal() {
	if b.reason != reasonQuery || !b.eventsUp || b.state != tdv1.AccountState_ACCOUNT_STATE_DEGRADED {
		return
	}
	for _, q := range b.queries {
		if !q.failSince.IsZero() {
			return
		}
	}
	b.state = tdv1.AccountState_ACCOUNT_STATE_READY
	b.reason = ""
	if b.onState != nil {
		b.onState(b.state, "")
	}
	if b.reportFn != nil {
		_ = b.reportFn(&tdctlv1.Report{State: b.state, ReconciledAtNs: b.reconciledAt, FirstSubmitNs: b.firstCopy()})
	}
}

// ---------- checks ----------

func normalize(s *tdv1.Submit, info *symbol.Info) (*tdv1.Submit, string) {
	out := proto.Clone(s).(*tdv1.Submit)
	if out.GetSide() != commonv1.Side_SIDE_BUY && out.GetSide() != commonv1.Side_SIDE_SELL {
		return nil, reasonInvalid
	}
	switch out.GetOrderType() {
	case commonv1.OrderType_ORDER_TYPE_LIMIT:
		if out.Price == nil {
			return nil, reasonInvalid
		}
	case commonv1.OrderType_ORDER_TYPE_MARKET:
	default:
		return nil, reasonInvalid
	}
	if out.Qty == nil && out.QuoteQty == nil {
		return nil, reasonInvalid
	}
	tick := info.Filters[symbol.PriceTick]
	step := info.Filters[symbol.QtyStep]
	if out.Price != nil {
		px := *out.Price
		if !(px > 0) {
			return nil, reasonInvalid
		}
		if tick > 0 {
			if out.GetSide() == commonv1.Side_SIDE_SELL {
				px = decimal.SnapUp(px, tick)
			} else {
				px = decimal.SnapDown(px, tick)
			}
		}
		if !within(px, info.Filters, symbol.MinPrice, symbol.MaxPrice) {
			return nil, reasonInvalid
		}
		out.Price = &px
	}
	if out.Qty != nil {
		q := *out.Qty
		if !(q > 0) {
			return nil, reasonInvalid
		}
		if step > 0 {
			q = decimal.SnapDown(q, step)
		}
		if !(q > 0) || !within(q, info.Filters, symbol.MinQty, symbol.MaxQty) {
			return nil, reasonInvalid
		}
		// Qty is in contract_value_ccy. Where one contract is not 1, it has to be a whole number of
		// contracts: the leftover after dividing by ContractSize is a reject.
		if info.ContractSize > 0 && info.ContractSize != 1 {
			whole := decimal.Snap(q/info.ContractSize, 1) * info.ContractSize
			if !decimal.Equal(q, whole, stepOr(step)) {
				return nil, reasonInvalid
			}
		}
		out.Qty = &q
	}
	if out.QuoteQty != nil && !(*out.QuoteQty > 0) {
		return nil, reasonInvalid
	}
	if out.Price != nil && out.Qty != nil {
		if !within(*out.Price**out.Qty, info.Filters, symbol.MinNotional, symbol.MaxNotional) {
			return nil, reasonInvalid
		}
	}
	return out, ""
}

func within(v float64, f symbol.Filters, minName, maxName string) bool {
	if min, ok := f[minName]; ok && min > 0 && v < min && !decimal.Equal(v, min, min) {
		return false
	}
	if max, ok := f[maxName]; ok && max > 0 && v > max && !decimal.Equal(v, max, max) {
		return false
	}
	return true
}

func hedgeParams(p map[string]string) bool {
	for k := range p {
		if hedgeParam[k] {
			return true
		}
	}
	return false
}

func tradableVenue(trades []string, venue string) bool {
	for _, v := range trades {
		if v == venue {
			return true
		}
	}
	return false
}

func stepsOf(info *symbol.Info) (qty, px float64) {
	qty, px = 1e-8, 1e-8
	if info == nil {
		return qty, px
	}
	if s := info.Filters[symbol.QtyStep]; s > 0 {
		qty = s
	}
	if s := info.Filters[symbol.PriceTick]; s > 0 {
		px = s
	}
	return qty, px
}

func sameSubmit(a, b *tdv1.Submit, qtyStep, pxStep float64) bool {
	if a.GetUid() != b.GetUid() || a.GetSide() != b.GetSide() || a.GetOrderType() != b.GetOrderType() ||
		a.GetTif() != b.GetTif() || a.GetReduceOnly() != b.GetReduceOnly() || a.GetAlgoId() != b.GetAlgoId() ||
		a.GetClientOrderId() != b.GetClientOrderId() {
		return false
	}
	return sameOpt(a.Qty, b.Qty, qtyStep) && sameOpt(a.QuoteQty, b.QuoteQty, qtyStep) && sameOpt(a.Price, b.Price, pxStep) && sameParams(a.GetParams(), b.GetParams())
}

func sameLive(s *tdv1.Submit, o *tdv1.Order, qtyStep, pxStep float64) bool {
	if s.GetUid() != o.GetUid() || s.GetSide() != o.GetSide() || s.GetOrderType() != o.GetOrderType() {
		return false
	}
	if s.Qty != nil && !decimal.Equal(*s.Qty, o.GetQty(), stepOr(qtyStep)) {
		return false
	}
	if s.Qty == nil && o.GetQty() != 0 {
		return false
	}
	return sameOpt(s.QuoteQty, o.QuoteQty, qtyStep) && sameOpt(s.Price, o.Price, pxStep)
}

func sameOpt(a, b *float64, step float64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return decimal.Equal(*a, *b, stepOr(step))
	}
}

func sameParams(a, b map[string]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return maps.Equal(a, b)
}

func stepOr(s float64) float64 {
	if s > 0 {
		return s
	}
	return 1e-8
}

func qtyOrAbsent(p *float64) float64 {
	if p == nil {
		return decimal.Absent()
	}
	return *p
}

func valueOr(p *float64) float64 {
	if p == nil {
		return decimal.Absent()
	}
	return *p
}

func cloneParams(p map[string]string) map[string]string {
	if len(p) == 0 {
		return nil
	}
	return maps.Clone(p)
}

func (b *book) liveByCloid(cid string) *tdv1.Order {
	k, ok := b.byCloid[cid]
	if !ok {
		return nil
	}
	return b.orders[k]
}

func (b *book) infoOf(uid uint32) *symbol.Info {
	info, err := b.ins.GetByUID(b.ctx, uid, false)
	if err != nil {
		return nil
	}
	return info
}

func contracts(info *symbol.Info, qty float64) float64 {
	if info != nil && info.ContractSize > 0 && info.ContractSize != 1 && decimal.Has(qty) {
		return qty / info.ContractSize
	}
	return qty
}

func orderReply(ok bool, code int32, reason, cid string) *tdv1.OrderReply {
	return &tdv1.OrderReply{Accepted: ok, Code: code, Reason: reason, ClientOrderId: cid}
}

func marshalReply(r *tdv1.OrderReply) []byte {
	b, _ := proto.Marshal(r)
	return b
}

func reqCloid(req *tdv1.OrderRequest) string {
	switch r := req.GetReq().(type) {
	case *tdv1.OrderRequest_Submit:
		return r.Submit.GetClientOrderId()
	case *tdv1.OrderRequest_Cancel:
		return r.Cancel.GetClientOrderId()
	default:
		return ""
	}
}

func seenRec(id string, at time.Time, s *tdv1.Submit, reply *tdv1.OrderReply, qtyStep, pxStep float64) *seenRecT {
	sub := proto.Clone(s).(*tdv1.Submit)
	sub.SessionId, sub.RecvWindowMs = "", 0
	return &seenRecT{id: id, at: at, sub: sub, reply: proto.Clone(reply).(*tdv1.OrderReply), qtyStep: qtyStep, pxStep: pxStep}
}

// ---------- seen cloids ----------

type seenRecT struct {
	id              string
	at              time.Time
	sub             *tdv1.Submit
	reply           *tdv1.OrderReply
	qtyStep, pxStep float64
}

type seenMem struct {
	by   map[string]*seenRecT
	ring []*seenRecT
	next int
}

func newSeen() *seenMem {
	return &seenMem{by: map[string]*seenRecT{}}
}

func (s *seenMem) get(id string, now time.Time) (*seenRecT, bool) {
	if s == nil || id == "" {
		return nil, false
	}
	r := s.by[id]
	if r == nil || now.Sub(r.at) > seenAge {
		return nil, false
	}
	return r, true
}

func (s *seenMem) add(r *seenRecT) {
	if s == nil || r == nil || r.id == "" {
		return
	}
	if old := s.by[r.id]; old != nil {
		if r.at.Sub(old.at) <= seenAge {
			return
		}
		*old = *r
		return
	}
	if len(s.ring) < seenMax {
		s.ring = append(s.ring, r)
	} else {
		old := s.ring[s.next]
		if s.by[old.id] == old {
			delete(s.by, old.id)
		}
		s.ring[s.next] = r
		s.next = (s.next + 1) % seenMax
	}
	s.by[r.id] = r
}
