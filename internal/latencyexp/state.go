package latencyexp

import (
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/eventlog"
)

// maxPending bounds hook inputs and orders kept while waiting for the frame
// that closes them. A hook that never returns, or an order the exchange never
// answers, must not grow without a limit.
const maxPending = 100_000

type inKey struct {
	inc uint32
	seq uint64
}

type input struct {
	kind eventlog.Kind
	tNs  int64
}

type callKey struct {
	inc uint32
	id  uint64
}

type call struct {
	tNs    int64
	cloids []string
}

type waitKey struct {
	inc   uint32
	cloid string
}

type wait struct {
	tNs  int64
	call uint64
}

type recState struct {
	seen bool
	last uint64
}

type mdKey struct {
	kind  eventlog.Kind
	uid   uint32
	shape string
	epoch uint64
}

// session is one session_id's matching state. It is touched only by Poll.
type session struct {
	rec    map[uint32]*recState
	inputs map[inKey]input
	calls  map[callKey]call
	waits  map[waitKey]wait
	md     map[mdKey]*recState
}

func (s *session) collect(id string, inc uint32, frames []rawFrame, recs []eventlog.Record) []point {
	var ps []point
	ri := 0
	for _, fr := range frames {
		if gap := s.noteRec(inc, fr.seq); gap > 0 {
			ps = append(ps, point{name: mRecGap, session: id, n: gap})
		}
		if !fr.kind.Known() {
			continue
		}
		if ri >= len(recs) || recs[ri].Seq != fr.seq || recs[ri].Kind != fr.kind {
			continue
		}
		ps = append(ps, s.on(id, inc, recs[ri])...)
		ri++
	}
	return ps
}

func (s *session) noteRec(inc uint32, seq uint64) uint64 {
	if s.rec == nil {
		s.rec = map[uint32]*recState{}
	}
	st := s.rec[inc]
	if st == nil {
		st = &recState{}
		s.rec[inc] = st
	}
	return noteSeq(st, seq)
}

// noteSeq returns how many sequence numbers were skipped. The first value of a
// stream establishes the baseline, so a file whose first frame is not 1 counts
// the missing prefix: those frames were dropped or deleted before we read them.
func noteSeq(st *recState, seq uint64) uint64 {
	if !st.seen {
		st.seen = true
		st.last = seq
		if seq > 1 {
			return seq - 1
		}
		return 0
	}
	var gap uint64
	if seq > st.last+1 {
		gap = seq - st.last - 1
	}
	if seq > st.last {
		st.last = seq
	}
	return gap
}

func (s *session) on(id string, inc uint32, rec eventlog.Record) []point {
	if rec.Kind.Input() {
		s.remember(inc, rec)
	}
	switch rec.Kind {
	case eventlog.KindMDFeed, eventlog.KindMDBook:
		return s.onMD(id, rec)
	case eventlog.KindHookEnd:
		return s.onHookEnd(id, inc, rec)
	case eventlog.KindOrderCall:
		s.onOrderCall(inc, rec)
	case eventlog.KindOrderReply:
		return s.onOrderReply(id, inc, rec)
	case eventlog.KindTDEvent:
		return s.onTD(id, inc, rec)
	case eventlog.KindEnd:
		s.dropInc(inc)
	}
	return nil
}

func (s *session) remember(inc uint32, rec eventlog.Record) {
	if s.inputs == nil || len(s.inputs) >= maxPending {
		s.inputs = map[inKey]input{}
	}
	s.inputs[inKey{inc, rec.Seq}] = input{kind: rec.Kind, tNs: rec.TNs}
}

func (s *session) onHookEnd(id string, inc uint32, rec eventlog.Record) []point {
	in, ok := s.inputs[inKey{inc, rec.Ref}]
	if !ok {
		return nil
	}
	delete(s.inputs, inKey{inc, rec.Ref})
	if rec.TNs < in.tNs {
		return nil
	}
	return []point{{name: mHook, session: id, kind: in.kind.String(), hist: true, ns: rec.TNs - in.tNs}}
}

func (s *session) onMD(id string, rec eventlog.Record) []point {
	var ps []point
	kind := rec.Kind.String()
	if rec.RecvNs > 0 && rec.TNs >= rec.RecvNs {
		ps = append(ps, point{name: mMDHook, session: id, kind: kind, hist: true, ns: rec.TNs - rec.RecvNs})
	}
	_, epoch, seq, _, payload, err := envelope.Parse(rec.Body)
	if err != nil {
		return ps
	}
	ev := &mdv1.FeedEvent{}
	if proto.Unmarshal(payload, ev) != nil {
		return ps
	}
	if ts := exchTs(ev); ts != 0 {
		ps = append(ps, point{name: mExchHook, session: id, kind: kind, hist: true, ns: rec.TNs - ts})
	}
	if s.md == nil {
		s.md = map[mdKey]*recState{}
	}
	key := mdKey{kind: rec.Kind, uid: ev.GetUid(), shape: shapeOf(ev), epoch: epoch}
	st := s.md[key]
	if st == nil {
		st = &recState{}
		s.md[key] = st
	}
	if gap := noteSeq(st, seq); gap > 0 {
		ps = append(ps, point{name: mMDGap, session: id, kind: kind, n: gap})
	}
	return ps
}

func (s *session) onOrderCall(inc uint32, rec eventlog.Record) {
	c, err := eventlog.ParseOrderCall(rec.Body)
	if err != nil {
		return
	}
	if s.calls == nil {
		s.calls = map[callKey]call{}
	}
	if len(s.calls) >= maxPending {
		s.calls = map[callKey]call{}
	}
	s.calls[callKey{inc, rec.Ref}] = call{tNs: rec.TNs, cloids: c.Cloids}
	if s.waits == nil || len(s.waits) >= maxPending {
		s.waits = map[waitKey]wait{}
	}
	for _, id := range c.Cloids {
		if id == "" {
			continue
		}
		k := waitKey{inc, id}
		if _, exists := s.waits[k]; exists {
			continue
		}
		s.waits[k] = wait{tNs: rec.TNs, call: rec.Ref}
	}
}

func (s *session) onOrderReply(id string, inc uint32, rec eventlog.Record) []point {
	var ps []point
	switch rec.Flags {
	case eventlog.Timeout:
		ps = append(ps, point{name: mReply, session: id, outcome: "timeout", n: 1})
	case eventlog.NoResponders:
		ps = append(ps, point{name: mReply, session: id, outcome: "no_responders", n: 1})
		s.dropCall(inc, rec.Ref)
	case eventlog.OK:
		reply := &tdv1.OrderReply{}
		if proto.Unmarshal(rec.Body, reply) != nil {
			break
		}
		call, ok := s.calls[callKey{inc, rec.Ref}]
		// Same clock: a reply that arrived before the call is not a sample.
		latency := ok && rec.RecvNs >= call.tNs
		switch {
		case reply.GetAccepted():
			if latency {
				ps = append(ps, point{name: mOrderAck, session: id, outcome: "accepted", hist: true, ns: rec.RecvNs - call.tNs})
			}
		case reply.GetCode() >= 100 && reply.GetCode() <= 199:
			if latency {
				ps = append(ps, point{name: mOrderAck, session: id, outcome: "local_reject", hist: true, ns: rec.RecvNs - call.tNs})
			}
			s.dropCall(inc, rec.Ref)
		}
	}
	delete(s.calls, callKey{inc, rec.Ref})
	return ps
}

func (s *session) onTD(id string, inc uint32, rec eventlog.Record) []point {
	ev := &tdv1.AccountEvent{}
	if proto.Unmarshal(rec.Body, ev) != nil {
		return nil
	}
	upd := ev.GetOrderUpdate()
	if upd.GetOrder() == nil {
		return nil
	}
	cloid := upd.GetOrder().GetClientOrderId()
	if cloid == "" {
		return nil
	}
	outcome, matched, terminal := exchangeOutcome(upd.GetOrder().GetStatus())
	if !terminal {
		return nil
	}
	w, ok := s.waits[waitKey{inc, cloid}]
	if !ok {
		return nil
	}
	delete(s.waits, waitKey{inc, cloid})
	if !matched || rec.RecvNs < w.tNs {
		return nil
	}
	return []point{{name: mOrderEx, session: id, outcome: outcome, hist: true, ns: rec.RecvNs - w.tNs}}
}

// exchangeOutcome classifies the first exchange OrderUpdate. PENDING_NEW is the
// TD's own mark and is not an exchange response. A later status that is not
// new, accepted, or rejected still consumes the wait: it was the first answer.
func exchangeOutcome(st tdv1.OrderStatus) (outcome string, matched, terminal bool) {
	switch st {
	case tdv1.OrderStatus_ORDER_STATUS_NEW:
		return "new", true, true
	case tdv1.OrderStatus_ORDER_STATUS_ACCEPTED:
		return "accepted", true, true
	case tdv1.OrderStatus_ORDER_STATUS_REJECTED:
		return "rejected", true, true
	case tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
		tdv1.OrderStatus_ORDER_STATUS_FILLED,
		tdv1.OrderStatus_ORDER_STATUS_CANCELLED,
		tdv1.OrderStatus_ORDER_STATUS_EXPIRED,
		tdv1.OrderStatus_ORDER_STATUS_UNKNOWN:
		return "", false, true
	default:
		return "", false, false
	}
}

func (s *session) dropCall(inc uint32, callID uint64) {
	for k, w := range s.waits {
		if k.inc == inc && w.call == callID {
			delete(s.waits, k)
		}
	}
}

func (s *session) dropInc(inc uint32) {
	for k := range s.inputs {
		if k.inc == inc {
			delete(s.inputs, k)
		}
	}
	for k := range s.calls {
		if k.inc == inc {
			delete(s.calls, k)
		}
	}
	for k := range s.waits {
		if k.inc == inc {
			delete(s.waits, k)
		}
	}
}

func exchTs(ev *mdv1.FeedEvent) int64 {
	var meta *mdv1.Meta
	switch e := ev.GetEvent().(type) {
	case *mdv1.FeedEvent_BestQuote:
		meta = e.BestQuote.GetMeta()
	case *mdv1.FeedEvent_Trade:
		meta = e.Trade.GetMeta()
	case *mdv1.FeedEvent_AggTrade:
		meta = e.AggTrade.GetMeta()
	case *mdv1.FeedEvent_Kline:
		meta = e.Kline.GetMeta()
	case *mdv1.FeedEvent_Ticker:
		meta = e.Ticker.GetMeta()
	case *mdv1.FeedEvent_Liquidation:
		meta = e.Liquidation.GetMeta()
	case *mdv1.FeedEvent_FundingRate:
		meta = e.FundingRate.GetMeta()
	case *mdv1.FeedEvent_OpenInterest:
		meta = e.OpenInterest.GetMeta()
	case *mdv1.FeedEvent_Greeks:
		meta = e.Greeks.GetMeta()
	case *mdv1.FeedEvent_MarkPrice:
		meta = e.MarkPrice.GetMeta()
	case *mdv1.FeedEvent_OrderBookDelta:
		meta = e.OrderBookDelta.GetMeta()
	}
	if meta == nil {
		return 0
	}
	return meta.GetExchTsNs()
}

func shapeOf(ev *mdv1.FeedEvent) string {
	switch ev.GetEvent().(type) {
	case *mdv1.FeedEvent_BestQuote:
		return "best_quote"
	case *mdv1.FeedEvent_Trade:
		return "trade"
	case *mdv1.FeedEvent_AggTrade:
		return "agg_trade"
	case *mdv1.FeedEvent_Kline:
		return "kline"
	case *mdv1.FeedEvent_Ticker:
		return "ticker"
	case *mdv1.FeedEvent_Liquidation:
		return "liquidation"
	case *mdv1.FeedEvent_FundingRate:
		return "funding_rate"
	case *mdv1.FeedEvent_OpenInterest:
		return "open_interest"
	case *mdv1.FeedEvent_Greeks:
		return "greeks"
	case *mdv1.FeedEvent_MarkPrice:
		return "mark_price"
	case *mdv1.FeedEvent_OrderBookDelta:
		return "order_book_delta"
	default:
		return "none"
	}
}
