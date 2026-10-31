package dispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
)

// t0 is the start of every recording test's clock. Times in the expected lines are relative to it.
var t0 = time.Unix(1_700_000_000, 0)

func TestRecordsInputsInDeliveryOrder(t *testing.T) {
	clk := newClock(t0)
	rec := evlog.Memory()
	var clock atomic.Int64
	var seen []string
	tick := func(name string) {
		seen = append(seen, fmt.Sprintf("%s@%s", name, rel(clock.Load())))
		clk.Add(time.Millisecond)
	}
	var d *Disp
	d = New(Config{
		RecvWindow: time.Hour,
		Now:        clk.Now,
		Rec:        rec,
		Clock:      &clock,
		Feed:       func([]byte) { tick("feed") },
		Book: func([]byte) {
			tick("book")
			d.Exit()
		},
		OnStop: func(context.Context) error {
			tick("onstop")
			return nil
		},
	})
	if err := d.PushIn(Input{Kind: eventlog.KindTDEvent, RecvNs: t0.Add(-5 * time.Millisecond).UnixNano(), Ref: 7, Body: []byte("ev-1")}, func() { tick("td") }); err != nil {
		t.Fatal(err)
	}
	if err := d.PushMD(mdAt(t0.UnixNano(), "md-1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Post(func() { tick("post") }); err != nil {
		t.Fatal(err)
	}
	d.Timer(func() { tick("timer") }).Fire()
	if err := d.PushBook(mdAt(t0.UnixNano(), "book-1")); err != nil {
		t.Fatal(err)
	}
	if err := d.PushTD(func() { tick("unrecorded") }); err != nil {
		t.Fatal(err)
	}
	if err := d.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := d.OnStop(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait(t, d.Exited())

	// Each hook saw the clock at its own start, and every TD entry ran before MD.
	if got, want := strings.Join(seen, " "), "td@0s post@1ms timer@2ms unrecorded@3ms feed@4ms book@5ms onstop@6ms"; got != want {
		t.Fatalf("hooks\n got %s\nwant %s", got, want)
	}
	want := []string{
		`1 td_event ref=7 recv=-5ms t=0s "ev-1"`,
		`2 hook_end ref=1 t=1ms`,
		`3 post ref=1 recv=0s t=1ms`,
		`4 hook_end ref=3 t=2ms`,
		`5 timer ref=1 recv=0s t=2ms`,
		`6 hook_end ref=5 t=3ms`,
		`7 md_feed recv=0s t=4ms "md-1"`,
		`8 hook_end ref=7 t=5ms`,
		`9 md_book recv=0s t=5ms "book-1"`,
		`10 hook_end ref=9 t=6ms`,
		`11 stop flags=1 recv=6ms t=6ms`,
		`12 onstop recv=6ms t=6ms`,
		`13 hook_end ref=12 t=7ms`,
	}
	checkLines(t, rec.Records(), want)
}

func TestRecordsPanicAndEnding(t *testing.T) {
	clk := newClock(t0)
	rec := evlog.Memory()
	d := New(Config{
		Now: clk.Now,
		Rec: rec,
		OnStop: func(context.Context) error {
			return errors.New("left open")
		},
	})
	if err := d.PushIn(Input{Kind: eventlog.KindOrderReply, Ref: 3, Body: []byte("reply-3")}, func() {
		clk.Add(time.Millisecond)
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.PushIn(Input{Kind: eventlog.KindTDEvent, Ref: 7, Body: []byte("never")}, func() {
		t.Error("a hook ran after the panic")
	}); err != nil {
		t.Fatal(err)
	}
	err := d.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("run %v", err)
	}
	if err := d.OnStop(context.Background()); err == nil {
		t.Fatal("OnStop's error was lost")
	}
	wait(t, d.Exited())
	// The input is in the log before the hook that panicked on it, the hook_end carries the panic,
	// and a CRASHED ending delivers nothing: the stop frame has no StopDeliver.
	checkLines(t, rec.Records(), []string{
		`1 order_reply ref=3 recv=0s t=0s "reply-3"`,
		`2 hook_end ref=1 flags=1 t=1ms "boom"`,
		`3 stop recv=1ms t=1ms`,
		`4 onstop recv=1ms t=1ms`,
		`5 hook_end ref=4 flags=2 t=1ms "left open"`,
	})
}

func TestRecordsHungHookWithoutEnd(t *testing.T) {
	hold, arrived, open := gate(t)
	rec := evlog.Memory()
	d := New(Config{HookTimeout: 50 * time.Millisecond, Rec: rec})
	if err := d.PushIn(Input{Kind: eventlog.KindTimer, Ref: 1}, hold); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- d.Run(context.Background()) }()
	arrived()
	if x := exitOf(t, waitErr(t, errc)); x.Outcome != commonv1.Outcome_OUTCOME_HUNG {
		t.Fatalf("exit %v", x)
	}
	recs := rec.Records()
	if len(recs) != 2 || recs[0].Kind != eventlog.KindTimer || recs[1].Kind != eventlog.KindStop || recs[1].Flags != eventlog.StopHung {
		t.Fatalf("records %v", lines(recs))
	}
	if recs[0].TNs == 0 {
		t.Fatal("the hung hook has no start")
	}
	open()
	wait(t, d.Exited())
}

func TestStepClockWithoutLog(t *testing.T) {
	clk := newClock(t0)
	var clock atomic.Int64
	got := make(chan int64, 1)
	d := New(Config{Now: clk.Now, Clock: &clock})
	if err := d.PushTD(func() { got <- clock.Load() }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	select {
	case ns := <-got:
		if ns != t0.UnixNano() {
			t.Fatalf("clock %s", rel(ns))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hook did not run")
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestLogOffKeepsNothing(t *testing.T) {
	d := New(Config{RecvWindow: time.Hour, RingBytes: 1 << 16, Feed: func([]byte) {}})
	if d.ring.timed || d.td.meta != nil {
		t.Fatal("a session without the log keeps receive times or inputs")
	}
	msg := mdAt(time.Now().UnixNano(), "md-1")
	fn := func() {}
	in := Input{Kind: eventlog.KindTDEvent, Ref: 7, Body: []byte("ev-1")}
	mdAllocs := testing.AllocsPerRun(1000, func() {
		_ = d.PushMD(msg)
		runOne(d)
	})
	tdAllocs := testing.AllocsPerRun(1000, func() {
		_ = d.PushIn(in, fn)
		runOne(d)
	})
	if mdAllocs != 0 || tdAllocs != 0 {
		t.Fatalf("allocations per hook: md %v td %v", mdAllocs, tdAllocs)
	}
	if d.ring.at != nil {
		t.Fatal("receive times were kept")
	}
}

func TestLogOnAllocatesNothingPerHook(t *testing.T) {
	rec := evlog.Open(evlog.Config{Dir: t.TempDir(), Create: func(string) (evlog.File, error) { return discard{}, nil }})
	t.Cleanup(rec.Close)
	d := New(Config{RecvWindow: time.Hour, RingBytes: 1 << 16, Feed: func([]byte) {}, Rec: rec})
	msg := mdAt(time.Now().UnixNano(), "md-1")
	fn := func() {}
	in := Input{Kind: eventlog.KindTDEvent, Ref: 7, Body: []byte("ev-1")}
	// The first frame opens the file. Measure after that.
	_ = d.PushMD(msg)
	runOne(d)
	rec.Flush()
	mdAllocs := testing.AllocsPerRun(1000, func() {
		_ = d.PushMD(msg)
		runOne(d)
	})
	tdAllocs := testing.AllocsPerRun(1000, func() {
		_ = d.PushIn(in, fn)
		runOne(d)
	})
	if mdAllocs != 0 || tdAllocs != 0 {
		t.Fatalf("allocations per hook: md %v td %v", mdAllocs, tdAllocs)
	}
	rec.Flush()
	if _, dropped := rec.Stats(); dropped != 0 {
		t.Fatalf("dropped %d", dropped)
	}
}

func TestStuckLogDoesNotBlockDispatcher(t *testing.T) {
	stuck := make(chan struct{})
	var released sync.Once
	release := func() { released.Do(func() { close(stuck) }) }
	t.Cleanup(release)
	rec := evlog.Open(evlog.Config{
		Dir:        t.TempDir(),
		RingBytes:  4 << 10,
		FlushEvery: time.Millisecond,
		Create: func(string) (evlog.File, error) {
			return blocked{stuck}, nil
		},
	})
	const n = 2000
	var fed atomic.Int32
	done := make(chan struct{})
	d := New(Config{
		RecvWindow: time.Hour,
		Rec:        rec,
		Feed: func([]byte) {
			if fed.Add(1) == n {
				close(done)
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	now := time.Now().UnixNano()
	for i := range n {
		if err := d.PushMD(mdAt(now, fmt.Sprintf("md-%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// The disk never returns from its first write. Every hook still runs.
	wait(t, done)
	written, dropped := rec.Stats()
	if written != 0 || dropped == 0 {
		t.Fatalf("written %d dropped %d while the disk is stuck", written, dropped)
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
	release()
	rec.Close()
	written, dropped = rec.Stats()
	// Each hook wrote its input and a hook_end; the ending wrote stop; Close wrote end.
	if total := written + dropped; total != 2*n+2 {
		t.Fatalf("written %d + dropped %d = %d, want %d", written, dropped, total, 2*n+2)
	}
}

func TestReplayDispatcher(t *testing.T) {
	var clock atomic.Int64
	d := New(Config{Replay: true, Clock: &clock, RingBytes: 1 << 10})
	var ran []string
	tm := d.Timer(func() { ran = append(ran, fmt.Sprintf("timer@%s", rel(clock.Load()))) })
	tm.After(0)
	tm.Fire()
	if err := d.PushTD(func() { t.Error("a replay ran a queued entry") }); !errors.Is(err, errClosed) {
		t.Fatalf("push in a replay %v", err)
	}
	if err := d.Post(func() { ran = append(ran, "post-1") }); err != nil {
		t.Fatal(err)
	}
	if err := d.Post(func() { ran = append(ran, "post-2") }); err != nil {
		t.Fatal(err)
	}
	if p := d.Step(t0.Add(2*time.Millisecond).UnixNano(), d.TakePost(2)); p != nil {
		t.Fatal(p)
	}
	if p := d.Step(t0.Add(3*time.Millisecond).UnixNano(), d.TimerFunc(tm.id)); p != nil {
		t.Fatal(p)
	}
	if d.TakePost(2) != nil || d.TimerFunc(2) != nil {
		t.Fatal("a post was handed out twice, or a timer that was never made")
	}
	p := d.Step(t0.UnixNano(), func() { panic("boom") })
	if fmt.Sprint(p) != "boom" {
		t.Fatalf("panic %v", p)
	}
	if got := strings.Join(ran, " "); got != "post-2 timer@3ms" {
		t.Fatalf("ran %s", got)
	}
	time.Sleep(10 * time.Millisecond)
	if got := strings.Join(ran, " "); got != "post-2 timer@3ms" {
		t.Fatalf("an armed timer ran in a replay: %s", got)
	}
}

// BenchmarkHook is the dispatcher's own cost of one MD message or one TD entry, push to hook
// return, on the calling goroutine. on writes the event log to a file that discards; off is a
// session without the log (E20-D007). The watchdog cases arm hook_timeout_s around each hook, as
// a session always does; the others leave it off so the log's share is easier to see.
func BenchmarkHook(b *testing.B) {
	msg := mdAt(time.Now().UnixNano(), strings.Repeat("x", 120))
	for _, limit := range []time.Duration{0, time.Hour} {
		for _, mode := range []string{"off", "on"} {
			var rec *evlog.Log
			if mode == "on" {
				rec = evlog.Open(evlog.Config{Dir: b.TempDir(), Create: func(string) (evlog.File, error) { return discard{}, nil }})
			}
			d := New(Config{RecvWindow: time.Hour, HookTimeout: limit, RingBytes: 1 << 20, Feed: func([]byte) {}, Rec: rec})
			suffix := ""
			if limit > 0 {
				suffix = "/watchdog"
			}
			b.Run("md/"+mode+suffix, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = d.PushMD(msg)
					runOne(d)
				}
			})
			in := Input{Kind: eventlog.KindTDEvent, Ref: 7, Body: msg[envelope.Size:]}
			fn := func() {}
			b.Run("td/"+mode+suffix, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = d.PushIn(in, fn)
					runOne(d)
				}
			})
			if rec != nil {
				rec.Close()
				if _, dropped := rec.Stats(); dropped != 0 {
					b.Logf("%s: the ring dropped %d frames", mode, dropped)
				}
			}
		}
	}
}

// runOne is one turn of the loop on the calling goroutine: the oldest TD entry, or else one MD message.
func runOne(d *Disp) {
	d.mu.Lock()
	if d.td.n > 0 {
		it, in := d.popLocked()
		d.mu.Unlock()
		d.runHook(in, it.fn)
		return
	}
	d.mu.Unlock()
	d.runMD()
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
func (discard) Sync() error                 { return nil }
func (discard) Close() error                { return nil }

// blocked is a disk whose writes do not return until the channel closes.
type blocked struct{ stuck chan struct{} }

func (b blocked) Write(p []byte) (int, error) { <-b.stuck; return len(p), nil }
func (blocked) Sync() error                   { return nil }
func (blocked) Close() error                  { return nil }

func checkLines(t *testing.T, recs []eventlog.Record, want []string) {
	t.Helper()
	got := lines(recs)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("records\n got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// lines prints records one per line: rec_seq, kind, and the fields that are set. Times are
// relative to t0. An MD body is printed without its envelope.
func lines(recs []eventlog.Record) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		s := fmt.Sprintf("%d %s", r.Seq, r.Kind)
		if r.Ref != 0 {
			s += fmt.Sprintf(" ref=%d", r.Ref)
		}
		if r.Flags != 0 {
			s += fmt.Sprintf(" flags=%d", r.Flags)
		}
		if r.RecvNs != 0 {
			s += " recv=" + rel(r.RecvNs)
		}
		if r.TNs != 0 {
			s += " t=" + rel(r.TNs)
		}
		body := r.Body
		if (r.Kind == eventlog.KindMDFeed || r.Kind == eventlog.KindMDBook) && len(body) >= envelope.Size {
			body = body[envelope.Size:]
		}
		if len(body) > 0 {
			s += fmt.Sprintf(" %q", body)
		}
		out = append(out, s)
	}
	return out
}

func rel(ns int64) string { return time.Duration(ns - t0.UnixNano()).String() }
