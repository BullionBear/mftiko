package tdpath

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/cloidgen"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
)

// Order is a new order, in the units of td.Submit.
type Order struct {
	APIID        int64
	AlgoID       uint32
	UID          uint32
	Side         commonv1.Side
	Type         commonv1.OrderType
	TIF          commonv1.TimeInForce
	Qty          float64
	QuoteQty     float64
	Price        float64
	RecvWindowMs int64
	ReduceOnly   bool
	Params       map[string]string
}

const reasonOffline = "account offline"

// Submit records a local pending_new, then sends td.order. The id is in OpenOrders
// before Submit returns. An accepted reply does not change the order. A 1xx, including
// no responder (101), marks it rejected. A timeout marks it UNKNOWN and does not query
// or send it again (E08-D043, E06-D005).
func (p *Path) Submit(o Order) (string, error) {
	if err := p.ready(o.APIID); err != nil {
		return "", err
	}
	acc, err := p.account(o.APIID)
	if err != nil {
		return "", err
	}
	now, gen, recorded := p.callTime()
	id, err := p.gen.Next(acc.decl, o.AlgoID, now.UnixMilli())
	if err != nil {
		if errors.Is(err, cloidgen.ErrAlgoNotDeclared) {
			return "", ErrAlgoNotDeclared
		}
		return "", err
	}
	row := &tdv1.Order{
		Uid:           o.UID,
		ClientOrderId: id,
		Side:          o.Side,
		OrderType:     o.Type,
		Status:        tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW,
		Qty:           present(o.Qty),
		QuoteQty:      opt(o.QuoteQty),
		Price:         opt(o.Price),
	}
	p.mu.Lock()
	if p.failed {
		p.mu.Unlock()
		return "", ErrStopping
	}
	if !recorded {
		gen = acc.sentGen
	}
	acc.orders[id] = row
	acc.submitGen[id] = gen
	p.mu.Unlock()

	req := &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
		SessionId:     p.sessionID,
		Uid:           o.UID,
		Side:          o.Side,
		OrderType:     o.Type,
		Qty:           opt(o.Qty),
		QuoteQty:      opt(o.QuoteQty),
		Price:         opt(o.Price),
		Tif:           o.TIF,
		ReduceOnly:    o.ReduceOnly,
		ClientOrderId: id,
		Params:        maps.Clone(o.Params),
		RecvWindowMs:  o.RecvWindowMs,
	}}}
	call := p.record(eventlog.OpSubmit, now, o.APIID, gen, id)
	p.send(call, o.APIID, id, nil, req, orderSubmit)
	return id, nil
}

// callTime is the clock of one order call. A replay reads it, and the generation a Submit
// records, from the recorded call it is about to repeat: the client order id is made from that
// time, so the id repeats as well (E20-D004). ok is false outside a replay.
func (p *Path) callTime() (at time.Time, gen uint64, ok bool) {
	if p.replay != nil && p.replay.Call != nil {
		if ns, g, found := p.replay.Call(p.calls.Load() + 1); found {
			return time.Unix(0, ns), g, true
		}
	}
	return p.now(), 0, false
}

// record writes one order_call and returns its call id: the order_publish and the order_reply of
// that request carry the same id (E20-D003). at is the call's clock; the zero time reads it.
// With the log off record does nothing and returns 0.
func (p *Path) record(flags uint16, at time.Time, apiID int64, gen uint64, ids ...string) uint64 {
	if p.rec == nil {
		return 0
	}
	if at.IsZero() {
		at, _, _ = p.callTime()
	}
	var buf [256]byte
	body := eventlog.AppendOrderCall(buf[:0], apiID, gen, ids)
	call := p.calls.Add(1)
	p.rec.Append(eventlog.KindOrderCall, flags, 0, at.UnixNano(), call, body, nil)
	return call
}

// send starts the round of one request on its own goroutine. A replay sends nothing: it writes
// the request as the order_publish it would have been and keeps the round for ReplayReply.
func (p *Path) send(call uint64, apiID int64, id string, ids []string, req *tdv1.OrderRequest, kind orderKind) {
	if p.replay != nil {
		p.replayed(call, apiID, id, ids, req, kind)
		return
	}
	wait, release := p.reserveSend()
	p.track(func() { p.roundOrder(wait, release, call, apiID, id, ids, req, kind) })
}

// pending is a replayed request waiting for its recorded reply.
type pending struct {
	apiID int64
	id    string
	ids   []string
	kind  orderKind
}

func (p *Path) replayed(call uint64, apiID int64, id string, ids []string, req *tdv1.OrderRequest, kind orderKind) {
	if body, err := proto.Marshal(req); err == nil {
		p.rec.Append(eventlog.KindOrderPublish, 0, 0, p.now().UnixNano(), call, body, nil)
	}
	p.mu.Lock()
	p.rounds[call] = pending{apiID: apiID, id: id, ids: ids, kind: kind}
	p.mu.Unlock()
}

// Cancel asks TD to cancel one order of this session. A 1xx, including no responder,
// is a RejectEvent and does not change the order. A timeout changes nothing and is
// not reported (E08-D045).
func (p *Path) Cancel(apiID int64, clientOrderID string) error {
	if err := p.ready(apiID); err != nil {
		return err
	}
	if _, err := p.ownAlgo(apiID, clientOrderID); err != nil {
		return err
	}
	var uid uint32
	p.mu.Lock()
	if o := p.ordersOf(apiID)[clientOrderID]; o != nil {
		uid = o.GetUid()
	}
	p.mu.Unlock()
	req := &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{
		SessionId:     p.sessionID,
		Uid:           uid,
		ClientOrderId: clientOrderID,
	}}}
	call := p.record(eventlog.OpCancel, time.Time{}, apiID, 0, clientOrderID)
	p.send(call, apiID, clientOrderID, []string{clientOrderID}, req, orderCancel)
	return nil
}

// BatchCancel cancels the live orders of algoID on the account, limited to uids when
// any are given. The list comes from OpenOrders and is split by the account's
// cancel_batch. A value of 1 sends one Cancel per order (E06-D031).
func (p *Path) BatchCancel(apiID int64, algoID uint32, uids ...uint32) error {
	if err := p.ready(apiID); err != nil {
		return err
	}
	acc, err := p.account(apiID)
	if err != nil {
		return err
	}
	if !acc.algos[algoID] {
		return ErrAlgoNotDeclared
	}
	p.mu.Lock()
	if p.failed {
		p.mu.Unlock()
		return ErrStopping
	}
	batch := acc.cancelBatch
	known := acc.haveBatch
	ids := liveIDs(acc.orders, algoID, uids)
	p.mu.Unlock()
	if !known || batch == 0 {
		return ErrNoCancelBatch
	}
	if len(ids) == 0 {
		return nil
	}
	call := p.recordBatch(apiID, batch, ids)
	if p.replay != nil {
		p.sendBatches(apiID, batch, ids, call)
		return nil
	}
	p.track(func() { p.sendBatches(apiID, batch, ids, call) })
	return nil
}

// recordBatch writes one order_call for each round of a BatchCancel, all at the call's clock and
// marked FromBatch, and returns the first round's call id; the others follow it. A cancel_batch of
// 1 makes each round a Cancel (E06-D031).
func (p *Path) recordBatch(apiID int64, batch uint32, ids []string) uint64 {
	if p.rec == nil {
		return 0
	}
	at, _, _ := p.callTime()
	flags := eventlog.OpBatchCancel | eventlog.FromBatch
	if batch == 1 {
		flags = eventlog.OpCancel | eventlog.FromBatch
	}
	n := int(batch)
	var first uint64
	for i := 0; i < len(ids); i += n {
		call := p.record(flags, at, apiID, 0, ids[i:min(i+n, len(ids))]...)
		if first == 0 {
			first = call
		}
	}
	return first
}

// sendBatches sends the rounds one after another, each after the previous round's reply. call is
// the first round's call id, or 0 with the log off.
func (p *Path) sendBatches(apiID int64, batch uint32, ids []string, call uint64) {
	n := int(batch)
	for i := 0; i < len(ids); i += n {
		var (
			id    string
			chunk []string
			req   *tdv1.OrderRequest
			kind  orderKind
		)
		if batch == 1 {
			id = ids[i]
			var uid uint32
			p.mu.Lock()
			if o := p.ordersOf(apiID)[id]; o != nil {
				uid = o.GetUid()
			}
			p.mu.Unlock()
			chunk = []string{id}
			req = &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{
				SessionId: p.sessionID, Uid: uid, ClientOrderId: id,
			}}}
			kind = orderCancel
		} else {
			chunk = append([]string(nil), ids[i:min(i+n, len(ids))]...)
			orders := make([]*tdv1.BatchCancelOrder, len(chunk))
			for k, c := range chunk {
				orders[k] = &tdv1.BatchCancelOrder{ClientOrderId: c}
			}
			req = &tdv1.OrderRequest{Req: &tdv1.OrderRequest_BatchCancel{BatchCancel: &tdv1.BatchCancel{Orders: orders}}}
			kind = orderBatch
		}
		if p.replay != nil {
			p.replayed(call, apiID, id, chunk, req, kind)
		} else {
			wait, release := p.reserveSend()
			p.roundOrder(wait, release, call, apiID, id, chunk, req, kind)
		}
		if call != 0 {
			call++
		}
	}
}

type orderKind int

const (
	orderSubmit orderKind = iota + 1
	orderCancel
	orderBatch
)

func (p *Path) roundOrder(wait <-chan struct{}, release func(), call uint64, apiID int64, id string, ids []string, req *tdv1.OrderRequest, kind orderKind) {
	if release != nil {
		defer release()
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.recv)
	defer cancel()
	body, err := proto.Marshal(req)
	var msg *nats.Msg
	if err == nil {
		msg, err = p.requestInOrder(ctx, wait, release, call, orderSubject(apiID), body)
	}
	if p.ctx.Err() != nil {
		return
	}
	in := p.replyIn(call, msg, err)
	reply := &tdv1.OrderReply{}
	if err == nil {
		err = proto.Unmarshal(msg.Data, reply)
	}
	p.enqueue(in, func() { p.finishOrder(apiID, id, ids, kind, reply, err) })
}

// replyIn is the order_reply input of a round: the reply as it came, or how the request failed.
func (p *Path) replyIn(call uint64, msg *nats.Msg, err error) dispatch.Input {
	if p.rec == nil {
		return dispatch.Input{}
	}
	in := dispatch.Input{Kind: eventlog.KindOrderReply, RecvNs: p.now().UnixNano(), Ref: call}
	if err == nil {
		in.Body = msg.Data
		return in
	}
	flags, text := evlog.Outcome(err)
	in.Flags = flags
	in.Body = []byte(text)
	return in
}

// requestInOrder publishes body after wait, then lets the next td.order
// publish start, and only then reads the reply. The subscription is flushed
// before the publish so the reply cannot arrive with nobody listening. With the
// event log on, the bytes are written as call's order_publish once they are on the
// connection.
func (p *Path) requestInOrder(ctx context.Context, wait <-chan struct{}, release func(), call uint64, subject string, body []byte) (*nats.Msg, error) {
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		}
	}
	inbox := p.nc.NewInbox()
	sub, err := p.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := p.nc.FlushWithContext(ctx); err != nil {
		return nil, err
	}
	if err := p.nc.PublishRequest(subject, inbox, body); err != nil {
		return nil, err
	}
	if p.rec != nil {
		p.rec.Append(eventlog.KindOrderPublish, 0, 0, p.now().UnixNano(), call, body, nil)
	}
	if release != nil {
		release()
	}
	return sub.NextMsgWithContext(ctx)
}

func (p *Path) finishOrder(apiID int64, id string, ids []string, kind orderKind, reply *tdv1.OrderReply, callErr error) {
	// After the hook. Callers poll this once the reply has been applied.
	defer p.settled.Add(1)
	switch {
	case callErr == nil && reply.GetAccepted():
		return
	case callErr == nil:
		p.refuse(apiID, id, ids, kind, reply.GetCode(), reply.GetReason())
	case errors.Is(callErr, nats.ErrNoResponders):
		p.refuse(apiID, id, ids, kind, int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_OFFLINE), reasonOffline)
	default:
		// A timeout, or a reply that is not an OrderReply. Cancel keeps its state
		// and is not reported (E08-D045). Submit becomes UNKNOWN (E08-D043).
		if kind != orderSubmit {
			return
		}
		p.mu.Lock()
		o := p.ordersOf(apiID)[id]
		if o == nil || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
			p.mu.Unlock()
			return
		}
		o.Status = tdv1.OrderStatus_ORDER_STATUS_UNKNOWN
		shown := cloneOrder(o)
		p.mu.Unlock()
		if p.hooks.Order != nil {
			p.hooks.Order(OrderEvent{TD: TD{APIID: apiID}, Origin: Timeout, Order: shown})
		}
	}
}

func (p *Path) refuse(apiID int64, id string, ids []string, kind orderKind, code int32, reason string) {
	if kind == orderSubmit {
		p.mu.Lock()
		o := p.ordersOf(apiID)[id]
		if o == nil || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW {
			p.mu.Unlock()
			return
		}
		o.Status = tdv1.OrderStatus_ORDER_STATUS_REJECTED
		shown := cloneOrder(o)
		p.mu.Unlock()
		if p.hooks.Order != nil {
			p.hooks.Order(OrderEvent{
				TD: TD{APIID: apiID}, Origin: LocalReject, Order: shown, Code: code, Reason: reason,
			})
		}
		p.dropTerminal(apiID, id)
		return
	}
	kindOf := RejectedCancel
	if kind == orderBatch {
		kindOf = RejectedBatchCancel
	}
	if p.hooks.Reject != nil {
		p.hooks.Reject(RejectEvent{
			TD: TD{APIID: apiID}, Request: kindOf, ClientOrderIDs: append([]string(nil), ids...),
			Code: code, Reason: reason,
		})
	}
}

// Order is the order as of now. The pointer is the caller's; the book keeps its own.
func (p *Path) Order(apiID int64, clientOrderID string) (*tdv1.Order, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	o := p.ordersOf(apiID)[clientOrderID]
	if o == nil {
		return nil, false
	}
	return cloneOrder(o), true
}

// OpenOrders are the account's live orders of algoID. Zero lists every algo declared
// for the account.
func (p *Path) OpenOrders(apiID int64, algoID uint32) []*tdv1.Order {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[apiID]
	if acc == nil {
		return nil
	}
	var out []*tdv1.Order
	for _, o := range acc.orders {
		if terminal(o.GetStatus()) {
			continue
		}
		algo, ok := cloid.AlgoOf(o.GetClientOrderId())
		if !ok || (algoID != 0 && algo != algoID) || !acc.algos[algo] {
			continue
		}
		out = append(out, cloneOrder(o))
	}
	slices.SortFunc(out, func(a, b *tdv1.Order) int {
		if a.GetClientOrderId() < b.GetClientOrderId() {
			return -1
		}
		if a.GetClientOrderId() > b.GetClientOrderId() {
			return 1
		}
		return 0
	})
	return out
}

func (p *Path) ready(apiID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return ErrStopping
	}
	if !p.listening {
		return ErrNotListening
	}
	if p.byAPI[apiID] == nil {
		return ErrUnknownAccount
	}
	return nil
}

func (p *Path) account(apiID int64) (*account, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[apiID]
	if acc == nil {
		return nil, ErrUnknownAccount
	}
	return acc, nil
}

func (p *Path) ownAlgo(apiID int64, clientOrderID string) (uint32, error) {
	acc, err := p.account(apiID)
	if err != nil {
		return 0, err
	}
	algo, ok := cloid.AlgoOf(clientOrderID)
	if !ok || !acc.algos[algo] {
		return 0, ErrAlgoNotDeclared
	}
	return algo, nil
}

func (p *Path) ordersOf(apiID int64) map[string]*tdv1.Order {
	if acc := p.byAPI[apiID]; acc != nil {
		return acc.orders
	}
	return nil
}

func (p *Path) dropTerminal(apiID int64, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[apiID]
	if acc == nil {
		return
	}
	if o := acc.orders[id]; o != nil && terminal(o.GetStatus()) {
		delete(acc.orders, id)
		delete(acc.submitGen, id)
	}
}

func liveIDs(orders map[string]*tdv1.Order, algo uint32, uids []uint32) []string {
	want := map[uint32]struct{}{}
	for _, u := range uids {
		want[u] = struct{}{}
	}
	var ids []string
	for id, o := range orders {
		if terminal(o.GetStatus()) {
			continue
		}
		got, ok := cloid.AlgoOf(id)
		if !ok || got != algo {
			continue
		}
		if len(want) > 0 {
			if _, ok := want[o.GetUid()]; !ok {
				continue
			}
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// merge replaces this account's orders with the snapshot's live rows.
// A client id the snapshot lists is decided by the snapshot, including a terminal
// row, which is not stored. fromRequest is the reply of one ReconRequest: a local
// PENDING_NEW that Submit recorded at or after requestGen, and that the snapshot
// does not list, stays (E08-D054). A pushed Recon is not that reply, so an omitted
// order is gone.
func (a *account) merge(filtered []*tdv1.Order, requestGen uint64, fromRequest bool) {
	next := make(map[string]*tdv1.Order, len(filtered)+len(a.orders))
	seen := make(map[string]bool, len(filtered))
	for _, o := range filtered {
		if o == nil {
			continue
		}
		id := o.GetClientOrderId()
		if id == "" {
			continue
		}
		seen[id] = true
		if terminal(o.GetStatus()) {
			continue
		}
		next[id] = cloneOrder(o)
	}
	if fromRequest {
		for id, o := range a.orders {
			if seen[id] {
				continue
			}
			gen, ok := a.submitGen[id]
			if !ok || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW || gen < requestGen {
				continue
			}
			next[id] = o
		}
	}
	nextGen := make(map[string]uint64, len(next))
	for id := range next {
		if gen, ok := a.submitGen[id]; ok {
			nextGen[id] = gen
		}
	}
	a.orders = next
	a.submitGen = nextGen
}

// applyUpdate stores the event's order as the whole row. The hook then sees that
// row. A terminal status is removed after the hook returns, so OnOrder can still
// read it and OpenOrders already omits it.
func (p *Path) applyUpdate(apiID int64, ev *tdv1.AccountEvent) {
	upd := ev.GetOrderUpdate()
	o := upd.GetOrder()
	if o == nil {
		return
	}
	if !p.keeps(apiID, o.GetClientOrderId()) {
		return
	}
	row := cloneOrder(o)
	p.mu.Lock()
	acc := p.byAPI[apiID]
	if acc == nil {
		p.mu.Unlock()
		return
	}
	acc.orders[row.GetClientOrderId()] = row
	p.mu.Unlock()
	if p.hooks.Order != nil {
		p.hooks.Order(OrderEvent{
			TD:     eventTD(apiID, ev),
			Origin: FromTD,
			Order:  cloneOrder(row),
			Cause:  upd.GetCause(),
			Code:   upd.GetCode(),
			Reason: upd.GetReason(),
		})
	}
	p.dropTerminal(apiID, row.GetClientOrderId())
}

// applyFill delivers the fill. It does not change the order or the ledger (E08-D055).
// The order's new state arrives as its own OrderUpdate.
func (p *Path) applyFill(apiID int64, ev *tdv1.AccountEvent) {
	f := ev.GetFill()
	if f == nil || !p.keeps(apiID, f.GetClientOrderId()) || p.hooks.Fill == nil {
		return
	}
	p.hooks.Fill(FillEvent{TD: eventTD(apiID, ev), Fill: proto.Clone(f).(*tdv1.Fill)})
}

func (p *Path) applyPushedRecon(apiID int64, ev *tdv1.AccountEvent) {
	body := ev.GetRecon()
	if body == nil {
		return
	}
	p.deliverRecon(apiID, eventTD(apiID, ev), body, 0, false, 0)
}

func (p *Path) keeps(apiID int64, clientOrderID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[apiID]
	if acc == nil {
		return false
	}
	algo, ok := cloid.AlgoOf(clientOrderID)
	return ok && acc.algos[algo]
}

func eventTD(apiID int64, ev *tdv1.AccountEvent) TD {
	return TD{APIID: apiID, Incarnation: ev.GetSource().GetIncarnation(), Seq: ev.GetSeq()}
}

func terminal(s tdv1.OrderStatus) bool {
	switch s {
	case tdv1.OrderStatus_ORDER_STATUS_FILLED, tdv1.OrderStatus_ORDER_STATUS_CANCELLED,
		tdv1.OrderStatus_ORDER_STATUS_REJECTED, tdv1.OrderStatus_ORDER_STATUS_EXPIRED:
		return true
	}
	return false
}

func present(v float64) float64 {
	if decimal.Has(v) {
		return v
	}
	return 0
}

func opt(v float64) *float64 {
	if !decimal.Has(v) {
		return nil
	}
	x := v
	return &x
}

func cloneOrder(o *tdv1.Order) *tdv1.Order {
	if o == nil {
		return nil
	}
	return proto.Clone(o).(*tdv1.Order)
}

func clonePositions(in []*tdv1.Position) []*tdv1.Position {
	out := make([]*tdv1.Position, 0, len(in))
	for _, p := range in {
		if p != nil {
			out = append(out, proto.Clone(p).(*tdv1.Position))
		}
	}
	return out
}

func cloneBalances(in []*tdv1.Balance) []*tdv1.Balance {
	out := make([]*tdv1.Balance, 0, len(in))
	for _, b := range in {
		if b != nil {
			out = append(out, proto.Clone(b).(*tdv1.Balance))
		}
	}
	return out
}
