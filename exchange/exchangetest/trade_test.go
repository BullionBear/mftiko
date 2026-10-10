package exchangetest_test

import (
	"errors"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

// sink records what a TradeStream reports.
type sink struct {
	ready     int
	submitted map[uint64]error
	results   map[uint64]*exchange.OrderResult
	canceled  map[uint64]error
	fills     []exchange.Fill
	used      int
}

func newSink() *sink {
	return &sink{submitted: map[uint64]error{}, results: map[uint64]*exchange.OrderResult{}, canceled: map[uint64]error{}}
}

func (s *sink) Ready()                        { s.ready++ }
func (s *sink) Expired(string)                {}
func (s *sink) Order(*exchange.OrderUpdate)   {}
func (s *sink) Fill(f *exchange.Fill)         { s.fills = append(s.fills, *f) }
func (s *sink) Balances([]exchange.Balance)   {}
func (s *sink) Positions([]exchange.Position) {}
func (s *sink) Used(u []exchange.Used)        { s.used += len(u) }
func (s *sink) Canceled(id uint64, err error) { s.canceled[id] = err }
func (s *sink) Submitted(id uint64, r *exchange.OrderResult, err error) {
	s.submitted[id] = err
	if r != nil {
		c := *r
		s.results[id] = &c
	}
}

func order(cid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: &symbol.Info{ExchTicker: "BTCUSDT", Filters: symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001}},
		Side:       exchange.SideBuy, Type: exchange.OrderTypeLimit, Price: 65000.1, Qty: 0.5, ClientOrderID: cid,
	}
}

// TestTradeVenue drives a TradeStream the way the account worker will: ids from Inflight, answers
// out of order, one answer that never comes, one refusal, and a late answer after the deadline.
func TestTradeVenue(t *testing.T) {
	v := exchangetest.NewTradeVenue("k", false)
	s := v.Stream()
	t0 := time.Unix(1_767_225_600, 0)
	d, err := s.Dial(t0)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range d.Frames {
		if err := v.Handle(f); err != nil {
			t.Fatal(err)
		}
	}
	out := newSink()
	for _, f := range v.Release() {
		if err := s.Decode(f, out); err != nil {
			t.Fatal(err)
		}
	}
	if out.ready != 1 {
		t.Fatalf("Ready %d times after the logon", out.ready)
	}

	var inflight exchange.Inflight[string]
	send := func(cid string, deadline time.Duration) uint64 {
		id := inflight.Add(cid, t0.Add(deadline))
		f, err := s.Submit(t0, id, order(cid))
		if err != nil {
			t.Fatal(err)
		}
		if err := v.Handle(f); err != nil {
			t.Fatal(err)
		}
		return id
	}
	v.Reject(5, -2010) // the fifth request will be refused
	a, b, c, late := send("a", time.Second), send("b", time.Second), send("c", time.Second), send("late", time.Second)
	refused := send("refused", 3*time.Second)
	if refused != 5 {
		t.Fatalf("ids are not sequential: %d", refused)
	}
	v.Drop(b) // b's answer never comes

	// The answers to c and a come first, out of order.
	frames := v.Release(c, a)
	for _, f := range frames[:2] {
		if err := s.Decode(f, out); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{c, a} {
		if out.submitted[id] != nil || out.results[id] == nil {
			t.Fatalf("%d: %v %+v", id, out.submitted[id], out.results[id])
		}
		if _, ok := inflight.Answer(id); !ok {
			t.Fatalf("answer to %d not matched", id)
		}
	}
	if out.results[c].OrderID != "o3" || out.results[a].OrderID != "o1" {
		t.Fatalf("answers matched to the wrong requests: a %+v, c %+v", out.results[a], out.results[c])
	}

	// One second on: b never answered and late's answer has not been read yet. Both are unknown
	// outcomes; refused has until second three.
	exp := inflight.Expire(t0.Add(time.Second))
	if len(exp) != 2 || exp[0].ID != b || exp[1].ID != late || !errors.Is(exp[0].Err, exchange.ErrUnknownOutcome) {
		t.Fatalf("Expire = %+v", exp)
	}

	// The rest arrives: late's answer is not taken as live; refused is a refusal.
	for _, f := range frames[2:] {
		if err := s.Decode(f, out); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := inflight.Answer(late); ok {
		t.Fatal("an answer after its deadline was matched")
	}
	if _, answered := out.submitted[b]; answered {
		t.Fatal("the dropped answer came")
	}
	var rej *exchange.Reject
	if !errors.As(out.submitted[refused], &rej) || rej.Code != 201 {
		t.Fatalf("refused: %v", out.submitted[refused])
	}
	if _, ok := inflight.Answer(refused); !ok {
		t.Fatal("the refusal was not matched")
	}
	if out.used == 0 {
		t.Fatal("no usage reported")
	}
}

// TestTradeVenueEvents: on a connection that carries private events, Ready waits for the
// subscription, and fills arrive through the TradeSink.
func TestTradeVenueEvents(t *testing.T) {
	v := exchangetest.NewTradeVenue("k", true)
	s := v.Stream()
	if !s.Limits().Events {
		t.Fatal("Limits().Events is false")
	}
	d, _ := s.Dial(time.Unix(0, 0))
	if len(d.Frames) != 2 {
		t.Fatalf("Dial frames %q, want logon and subscribe", d.Frames)
	}
	out := newSink()
	v.Handle(d.Frames[0])
	for _, f := range v.Release() {
		if err := s.Decode(f, out); err != nil {
			t.Fatal(err)
		}
	}
	if out.ready != 0 {
		t.Fatal("Ready before the subscription")
	}
	v.Handle(d.Frames[1])
	for _, f := range append(v.Release(), v.Fill("f1", "o1", "BTCUSDT", exchange.SideSell, "65000.1", "0.25")) {
		if err := s.Decode(f, out); err != nil {
			t.Fatal(err)
		}
	}
	if out.ready != 1 || len(out.fills) != 1 || out.fills[0].FillID != "f1" || out.fills[0].Qty != 0.25 {
		t.Fatalf("ready %d, fills %+v", out.ready, out.fills)
	}
}

func TestTradeVenueRefusesLogon(t *testing.T) {
	v := exchangetest.NewTradeVenue("k", false)
	v.RefuseLogon()
	s := v.Stream()
	d, _ := s.Dial(time.Unix(0, 0))
	v.Handle(d.Frames[0])
	f := v.Release()
	if err := s.Decode(f[0], newSink()); !errors.Is(err, exchange.ErrAuth) {
		t.Fatalf("refused logon: %v", err)
	}
}
