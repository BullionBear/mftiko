package mdpath

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
)

type readResult struct {
	msg proto.Message
	err error
}

// liveReads runs one read of each kind against the fake md, the first before anything serves
// md.view. It returns each callback's result and the read_reply inputs, in read id order.
func liveReads(t *testing.T, rec *evlog.Log) ([]readResult, map[uint64]dispatch.Input) {
	t.Helper()
	r := newRig(t, 200*time.Millisecond, nil, func(c *Config) { c.Rec = rec })
	var mu sync.Mutex
	ins := map[uint64]dispatch.Input{}
	push := r.p.pushIn
	r.p.pushIn = func(in dispatch.Input, fn func()) error {
		mu.Lock()
		ins[in.Ref] = in
		mu.Unlock()
		return push(in, fn)
	}
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")), feedBind(topicBook, subj(topicBook)))); err != nil {
		t.Fatal(err)
	}
	var out []readResult
	page, err := awaitTape(t, r, 42, "trade", TapeQuery{Limit: 2})
	out = append(out, readResult{page, err})

	r.fake.listen()
	r.fake.onView = func(subject string, _ *mdv1.ViewRequest) *mdv1.ViewReply {
		if subject == "md.v1.view.md-tw-1."+topicBook+".Binance_Spot_BTCUSDT" {
			return &mdv1.ViewReply{Error: "not_synced"}
		}
		return tapeReply(1, 5)
	}
	r.fake.onFetch = func(_ string, req *mdv1.FetchRequest) *mdv1.FetchReply {
		if req.GetKlines() != nil {
			return &mdv1.FetchReply{Uid: 42, Result: &mdv1.FetchReply_Klines{Klines: &mdv1.Klines{
				Klines: []*mdv1.Kline{{Interval: "1m", Open: 100, Close: 101}, {Interval: "1m", Open: 101, Close: 102}},
			}}}
		}
		return &mdv1.FetchReply{Error: "unsupported", Reason: "no open interest on spot"}
	}
	page, err = awaitTape(t, r, 42, "trade", TapeQuery{Limit: 2, SinceNs: 1000})
	out = append(out, readResult{page, err})
	book, err := awaitBook(t, r, 42)
	out = append(out, readResult{book, err})
	kl, err := awaitKlines(t, r, 42)
	out = append(out, readResult{kl, err})
	oi, err := awaitOI(t, r, 42)
	out = append(out, readResult{oi, err})
	mu.Lock()
	defer mu.Unlock()
	return out, ins
}

func TestEventLogRecordsReads(t *testing.T) {
	rec := evlog.Memory()
	results, ins := liveReads(t, rec)
	if !errors.Is(results[0].err, nats.ErrNoResponders) || results[1].err != nil || results[2].err == nil ||
		results[3].err != nil || results[4].err == nil {
		t.Fatalf("results %+v", results)
	}
	want := []struct {
		flags   uint16
		subject string
	}{
		{eventlog.ReadTape, "md.v1.view.md-tw-1.trade.Binance_Spot_BTCUSDT"},
		{eventlog.ReadTape, "md.v1.view.md-tw-1.trade.Binance_Spot_BTCUSDT"},
		{eventlog.ReadBookSnapshot, "md.v1.view.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT"},
		{eventlog.ReadKlines, "md.v1.fetch.Binance"},
		{eventlog.ReadOpenInterest, "md.v1.fetch.Binance"},
	}
	recs := rec.Records()
	if len(recs) != len(want) {
		t.Fatalf("%d records", len(recs))
	}
	for i, r := range recs {
		subject, req, err := eventlog.ParseReadCall(r.Body)
		if err != nil || r.Kind != eventlog.KindReadCall || r.Ref != uint64(i+1) || r.Flags != want[i].flags ||
			subject != want[i].subject || r.TNs == 0 {
			t.Fatalf("read %d: %+v %q", i+1, r, subject)
		}
		if i == 1 {
			q := &mdv1.ViewRequest{}
			if err := proto.Unmarshal(req, q); err != nil || q.GetLimit() != 2 || q.GetSinceNs() != 1000 {
				t.Fatalf("tape request %v", q)
			}
		}
	}
	if len(ins) != 5 {
		t.Fatalf("%d replies", len(ins))
	}
	if in := ins[1]; in.Kind != eventlog.KindReadReply || in.Flags != eventlog.NoResponders || string(in.Body) != nats.ErrNoResponders.Error() {
		t.Fatalf("offline reply %+v", in)
	}
	for id := uint64(2); id <= 5; id++ {
		if in := ins[id]; in.Kind != eventlog.KindReadReply || in.Flags != eventlog.OK || len(in.Body) == 0 || in.RecvNs == 0 {
			t.Fatalf("reply %d: %+v", id, in)
		}
	}
}

// TestReplayReads gives the callbacks of a replay the recorded replies: the same pages, and the
// same errors, as the live reads got.
func TestReplayReads(t *testing.T) {
	rec := evlog.Memory()
	live, ins := liveReads(t, rec)

	out := evlog.Memory()
	var tap Tap
	p, err := Open(Config{
		RecvWindow: time.Second,
		Disp:       dispatch.New(dispatch.Config{Replay: true}),
		Tap:        &tap,
		Resolve:    testResolve,
		Rec:        out,
		Replay:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Apply(snap(1, 1, feedBind("trade", subj("trade")), feedBind(topicBook, subj(topicBook)))); err != nil {
		t.Fatal(err)
	}
	var got []readResult
	keep := func(m proto.Message, err error) { got = append(got, readResult{m, err}) }
	steps := []func() error{
		func() error {
			return p.Tape(42, "trade", TapeQuery{Limit: 2}, func(x *mdv1.TapePage, err error) { keep(x, err) })
		},
		func() error {
			return p.Tape(42, "trade", TapeQuery{Limit: 2, SinceNs: 1000}, func(x *mdv1.TapePage, err error) { keep(x, err) })
		},
		func() error { return p.BookSnapshot(42, func(x *mdv1.BookSnapshot, err error) { keep(x, err) }) },
		func() error { return p.FetchKlines(42, "1m", 3, func(x *mdv1.Klines, err error) { keep(x, err) }) },
		func() error { return p.FetchOpenInterest(42, func(x *mdv1.OpenInterest, err error) { keep(x, err) }) },
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatal(err)
		}
		in := ins[uint64(i+1)]
		if err := p.ReplayReply(uint64(i+1), in.Flags, in.Body); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.ReplayReply(1, 0, nil); !errors.Is(err, ErrNoRead) {
		t.Fatalf("a reply was taken twice: %v", err)
	}
	for i := range live {
		a, b := live[i], got[i]
		if (a.err == nil) != (b.err == nil) || (a.err != nil && a.err.Error() != b.err.Error()) {
			t.Fatalf("read %d: live %v, replay %v", i+1, a.err, b.err)
		}
		if a.err == nil && !proto.Equal(a.msg, b.msg) {
			t.Fatalf("read %d: live %v, replay %v", i+1, a.msg, b.msg)
		}
	}
	if !errors.Is(got[0].err, nats.ErrNoResponders) {
		t.Fatalf("replayed offline read %v", got[0].err)
	}
	recs, again := rec.Records(), out.Records()
	if len(again) != len(recs) {
		t.Fatalf("read calls: live %d, replay %d", len(recs), len(again))
	}
	for i := range recs {
		if recs[i].Flags != again[i].Flags || recs[i].Ref != again[i].Ref || string(recs[i].Body) != string(again[i].Body) {
			t.Fatalf("read %d: live %+v, replay %+v", i+1, recs[i], again[i])
		}
	}
}
