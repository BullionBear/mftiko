package tdpath

import (
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/cloidgen"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
)

// inputs is a Push that keeps each input the path hands the dispatcher and runs fn as runq does.
type inputs struct {
	mu sync.Mutex
	in []dispatch.Input
	q  runq
}

func (r *inputs) push(in dispatch.Input, fn func()) error {
	r.mu.Lock()
	r.in = append(r.in, in)
	r.mu.Unlock()
	return r.q.push(in, fn)
}

func (r *inputs) all() []dispatch.Input {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.in)
}

func (r *inputs) of(kind eventlog.Kind) []dispatch.Input {
	var out []dispatch.Input
	for _, in := range r.all() {
		if in.Kind == kind {
			out = append(out, in)
		}
	}
	return out
}

// ticks is a Now that reads 09:00:00 on 2026-10-31 and one millisecond more on each call.
type ticks struct {
	mu sync.Mutex
	n  int64
}

func (c *ticks) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC).Add(time.Duration(c.n) * time.Millisecond)
}

func recordingPath(t *testing.T, f *fake, push *inputs, rec *evlog.Log, hooks Hooks) *Path {
	t.Helper()
	clk := &ticks{}
	p, err := Open(Config{
		SessionID:  "c0ffee",
		Accounts:   []*sessionv1.AccountDecl{decl(7, 1)},
		RecvWindow: time.Second,
		NC:         connect(t, f.nc.ConnectedUrl()),
		Push:       push.push,
		Now:        clk.now,
		Hooks:      hooks,
		Rec:        rec,
		Gen:        cloidgen.NewGen(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestEventLogRecordsCallsAndInputs(t *testing.T) {
	f := &fake{t: t, nc: connect(t, startNATS(t))}
	f.serve()
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 2, nil) }
	f.onOrder = accept
	push := &inputs{}
	rec := evlog.Memory()
	p := recordingPath(t, f, push, rec, Hooks{})
	listenRecon(t, f, p)

	a, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1, Price: 100})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 2, Price: 100})
	if err != nil {
		t.Fatal(err)
	}
	c, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1002, Qty: 3, Price: 200})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 3 })
	publish(t, f.nc, 7, orderEv(1, 1, c, tdv1.OrderStatus_ORDER_STATUS_FILLED))
	waitFor(t, 5*time.Second, func() bool { return len(push.of(eventlog.KindTDEvent)) == 1 })
	if err := p.Cancel(7, a); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 4 })
	if err := p.BatchCancel(7, 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 5 })
	if err := p.RequestRecon(7); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(push.of(eventlog.KindTDView)) == 2 })

	type call struct {
		flags uint16
		ids   []string
		gen   uint64
	}
	want := []call{
		{eventlog.OpSubmit, []string{a}, 1},
		{eventlog.OpSubmit, []string{b}, 1},
		{eventlog.OpSubmit, []string{c}, 1},
		{eventlog.OpCancel, []string{a}, 0},
		{eventlog.OpBatchCancel | eventlog.FromBatch, slices.Sorted(slices.Values([]string{a, b})), 0},
		{eventlog.OpRecon, nil, 0},
	}
	var calls []eventlog.Record
	published := map[uint64]*tdv1.OrderRequest{}
	for _, r := range rec.Records() {
		switch r.Kind {
		case eventlog.KindOrderCall:
			calls = append(calls, r)
		case eventlog.KindOrderPublish:
			req := &tdv1.OrderRequest{}
			if err := proto.Unmarshal(r.Body, req); err != nil {
				t.Fatal(err)
			}
			published[r.Ref] = req
		default:
			t.Fatalf("the path wrote %v itself; inputs are the dispatcher's", r.Kind)
		}
	}
	if len(calls) != len(want) {
		t.Fatalf("%d order calls, want %d", len(calls), len(want))
	}
	for i, r := range calls {
		got, err := eventlog.ParseOrderCall(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if r.Ref != uint64(i+1) || r.Flags != want[i].flags || got.APIID != 7 || got.SubmitGen != want[i].gen ||
			!slices.Equal(got.Cloids, want[i].ids) || r.TNs == 0 {
			t.Fatalf("call %d: %+v %+v, want %+v", i+1, r, got, want[i])
		}
	}
	if len(published) != 5 || published[1].GetSubmit().GetClientOrderId() != a || published[4].GetCancel().GetClientOrderId() != a ||
		len(published[5].GetBatchCancel().GetOrders()) != 2 {
		t.Fatalf("published %v", published)
	}

	replies := push.of(eventlog.KindOrderReply)
	refs := make([]uint64, len(replies))
	for i, in := range replies {
		refs[i] = in.Ref
		rep := &tdv1.OrderReply{}
		if in.Flags != eventlog.OK || proto.Unmarshal(in.Body, rep) != nil || !rep.GetAccepted() || in.RecvNs == 0 {
			t.Fatalf("reply %+v", in)
		}
	}
	slices.Sort(refs)
	if !slices.Equal(refs, []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("reply refs %v", refs)
	}
	views := push.of(eventlog.KindTDView)
	for i, in := range views {
		gen, raw, err := eventlog.ParseTDView(append(slices.Clone(in.Head), in.Body...))
		rep := &tdv1.ViewReply{}
		if err != nil || gen != uint64(i+1) || in.Ref != 7 || proto.Unmarshal(raw, rep) != nil || rep.GetRecon() == nil {
			t.Fatalf("view %d: %+v gen %d", i, in, gen)
		}
	}
	ev := push.of(eventlog.KindTDEvent)[0]
	got := &tdv1.AccountEvent{}
	if ev.Ref != 7 || proto.Unmarshal(ev.Body, got) != nil || got.GetOrderUpdate().GetOrder().GetClientOrderId() != c {
		t.Fatalf("event %+v", ev)
	}
}

func TestEventLogOffHandsNoInputs(t *testing.T) {
	f := &fake{t: t, nc: connect(t, startNATS(t))}
	f.serve()
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 2, nil) }
	f.onOrder = accept
	push := &inputs{}
	p := recordingPath(t, f, push, nil, Hooks{})
	listenRecon(t, f, p)
	if _, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: 1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 1 })
	all := push.all()
	if len(all) != 2 {
		t.Fatalf("%d pushes", len(all))
	}
	for _, in := range all {
		if in.Kind != 0 || in.Body != nil || in.Head != nil || in.RecvNs != 0 {
			t.Fatalf("an input with the log off: %+v", in)
		}
	}
}

// TestReplayRepeatsTheRecording runs the same calls live and then offline from the recording. The
// replay issues the same client order ids from a different clock, makes the same order calls and
// requests, and gives the hooks the same order and reject events.
func TestReplayRepeatsTheRecording(t *testing.T) {
	f := &fake{t: t, nc: connect(t, startNATS(t))}
	f.serve()
	f.onView = func(int64, *tdv1.ViewRequest) *tdv1.ViewReply { return snap(1, 0, 2, nil) }
	f.onOrder = func(_ int64, req *tdv1.OrderRequest) *tdv1.OrderReply {
		if req.GetCancel() != nil {
			return &tdv1.OrderReply{Code: 106, Reason: "not yet"}
		}
		return &tdv1.OrderReply{Accepted: true}
	}
	live := &tape{}
	push := &inputs{}
	rec := evlog.Memory()
	p := recordingPath(t, f, push, rec, live.hooks())
	listenRecon(t, f, p)
	var ids []string
	for _, qty := range []float64{1, 2} {
		id, err := p.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: qty, Price: 100})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 2 })
	if err := p.Cancel(7, ids[0]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 3 })
	if err := p.BatchCancel(7, 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return p.settled.Load() == 4 })
	p.Close()

	recorded := map[uint64]eventlog.Record{}
	for _, r := range rec.Records() {
		if r.Kind == eventlog.KindOrderCall {
			recorded[r.Ref] = r
		}
	}
	replies := map[uint64]dispatch.Input{}
	for _, in := range push.of(eventlog.KindOrderReply) {
		replies[in.Ref] = in
	}

	again := &tape{}
	out := evlog.Memory()
	r, err := Open(Config{
		SessionID:  "c0ffee",
		Accounts:   []*sessionv1.AccountDecl{decl(7, 1)},
		RecvWindow: time.Second,
		Now:        func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) },
		Hooks:      again.hooks(),
		Rec:        out,
		Gen:        cloidgen.NewGen(0),
		Replay: &Replay{Call: func(id uint64) (int64, uint64, bool) {
			c, ok := recorded[id]
			if !ok {
				return 0, 0, false
			}
			body, err := eventlog.ParseOrderCall(c.Body)
			return c.TNs, body.SubmitGen, err == nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	reply := func(call uint64) {
		t.Helper()
		in := replies[call]
		if err := r.ReplayReply(call, in.Flags, in.Body); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Listen(); err != nil {
		t.Fatal(err)
	}
	if err := r.Recon(); err != nil {
		t.Fatal(err)
	}
	view := push.of(eventlog.KindTDView)[0]
	if err := r.ReplayView(7, append(slices.Clone(view.Head), view.Body...)); err != nil {
		t.Fatal(err)
	}
	for i, qty := range []float64{1, 2} {
		id, err := r.Submit(Order{APIID: 7, AlgoID: 1, UID: 1001, Qty: qty, Price: 100})
		if err != nil {
			t.Fatal(err)
		}
		if id != ids[i] {
			t.Fatalf("submit %d: replay %s, live %s", i, id, ids[i])
		}
	}
	reply(1)
	reply(2)
	if err := r.Cancel(7, ids[0]); err != nil {
		t.Fatal(err)
	}
	reply(3)
	if err := r.BatchCancel(7, 1); err != nil {
		t.Fatal(err)
	}
	reply(4)
	if err := r.ReplayReply(4, 0, nil); err == nil {
		t.Fatal("a reply was taken twice")
	}

	var calls, callsAgain []eventlog.Record
	pubs, pubsAgain := map[uint64][]byte{}, map[uint64][]byte{}
	split := func(recs []eventlog.Record, calls *[]eventlog.Record, pubs map[uint64][]byte) {
		for _, x := range recs {
			switch x.Kind {
			case eventlog.KindOrderCall:
				x.Seq = 0
				*calls = append(*calls, x)
			case eventlog.KindOrderPublish:
				pubs[x.Ref] = x.Body
			}
		}
	}
	split(rec.Records(), &calls, pubs)
	split(out.Records(), &callsAgain, pubsAgain)
	if len(calls) != 4 || len(callsAgain) != len(calls) {
		t.Fatalf("calls: live %d, replay %d", len(calls), len(callsAgain))
	}
	for i := range calls {
		a, b := calls[i], callsAgain[i]
		if a.Flags != b.Flags || a.TNs != b.TNs || a.Ref != b.Ref || string(a.Body) != string(b.Body) {
			t.Fatalf("call %d: live %+v, replay %+v", i+1, a, b)
		}
	}
	if len(pubsAgain) != len(pubs) {
		t.Fatalf("requests: live %d, replay %d", len(pubs), len(pubsAgain))
	}
	for call, body := range pubs {
		x, y := &tdv1.OrderRequest{}, &tdv1.OrderRequest{}
		if proto.Unmarshal(body, x) != nil || proto.Unmarshal(pubsAgain[call], y) != nil || !proto.Equal(x, y) {
			t.Fatalf("request of call %d: live %v, replay %v", call, x, y)
		}
	}
	if len(live.rejects()) != 1 || len(again.rejects()) != 1 ||
		!slices.Equal(live.rejects()[0].ClientOrderIDs, again.rejects()[0].ClientOrderIDs) ||
		again.rejects()[0].Reason != "not yet" {
		t.Fatalf("rejects: live %+v, replay %+v", live.rejects(), again.rejects())
	}
	if len(live.recons()) != 1 || len(again.recons()) != 1 {
		t.Fatalf("recons: live %d, replay %d", len(live.recons()), len(again.recons()))
	}
	if got, want := idsOf(r.OpenOrders(7, 1)), idsOf(p.OpenOrders(7, 1)); !slices.Equal(got, want) || len(got) != 2 {
		t.Fatalf("open orders: replay %v, live %v", got, want)
	}
}
