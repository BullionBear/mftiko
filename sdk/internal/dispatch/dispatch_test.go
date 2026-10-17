package dispatch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

func TestTDBeforeMD(t *testing.T) {
	var order []string
	done := make(chan struct{})
	d := New(Config{
		RecvWindow: time.Hour,
		Log:        func(string) {},
		Feed: func(msg []byte) {
			order = append(order, "md:"+string(msg[envelope.Size:]))
			// b is delivered last: both TD entries run before either MD entry.
			if msg[envelope.Size] == 'b' {
				close(done)
			}
		},
	})
	now := time.Now().UnixNano()
	if err := d.PushMD(mdAt(now, "a")); err != nil {
		t.Fatal(err)
	}
	if err := d.PushTD(func() { order = append(order, "td1") }); err != nil {
		t.Fatal(err)
	}
	if err := d.PushMD(mdAt(now, "b")); err != nil {
		t.Fatal(err)
	}
	if err := d.PushTD(func() { order = append(order, "td2") }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	wait(t, done)
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
	if got := strings.Join(order, ","); got != "td1,td2,md:a,md:b" {
		t.Fatalf("order %s", got)
	}
}

func TestTDArrivingDuringMD(t *testing.T) {
	hold, arrived, open := gate(t)
	var order []string
	done := make(chan struct{})
	d := New(Config{
		RecvWindow: time.Hour,
		Log:        func(string) {},
		Feed: func(msg []byte) {
			order = append(order, "md:"+string(msg[envelope.Size:]))
			switch msg[envelope.Size] {
			case 'a':
				hold()
			case 'b':
				close(done)
			}
		},
	})
	now := time.Now().UnixNano()
	if err := d.PushMD(mdAt(now, "a")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	arrived()
	if err := d.PushTD(func() { order = append(order, "td") }); err != nil {
		t.Fatal(err)
	}
	if err := d.PushMD(mdAt(now, "b")); err != nil {
		t.Fatal(err)
	}
	open()
	wait(t, done)
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
	if got := strings.Join(order, ","); got != "md:a,td,md:b" {
		t.Fatalf("order %s", got)
	}
}

func TestTimerCoalesce(t *testing.T) {
	hold, arrived, open := gate(t)
	var n atomic.Int32
	d := New(Config{Log: func(string) {}})
	if err := d.PushTD(hold); err != nil {
		t.Fatal(err)
	}
	tm := d.Timer(func() { n.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	arrived()
	tm.Fire()
	tm.Fire()
	tm.Fire()
	done := make(chan struct{})
	if err := d.PushTD(func() { close(done) }); err != nil {
		t.Fatal(err)
	}
	open()
	wait(t, done)
	if n.Load() != 1 {
		t.Fatalf("timer ran %d times", n.Load())
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestTimerRearmsFromItsCallback(t *testing.T) {
	var n atomic.Int32
	done := make(chan struct{})
	d := New(Config{Log: func(string) {}})
	var tm *Timer
	tm = d.Timer(func() {
		if n.Add(1) == 1 {
			tm.Fire()
			if err := d.PushTD(func() { close(done) }); err != nil {
				close(done)
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	tm.Fire()
	wait(t, done)
	if n.Load() != 2 {
		t.Fatalf("runs %d", n.Load())
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestTimerAfter(t *testing.T) {
	d := New(Config{Log: func(string) {}})
	got := make(chan struct{})
	d.Timer(func() { close(got) }).After(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	wait(t, got)
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestPrehookDropsStale(t *testing.T) {
	clk := newClock(time.Unix(1_700_000_000, 0))
	window := 10 * time.Millisecond
	var feeds []byte
	var books []byte
	var once sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	d := New(Config{
		RecvWindow: window,
		Now:        clk.Now,
		Log:        func(string) {},
		Feed: func(msg []byte) {
			feeds = append(feeds, msg[envelope.Size])
			if msg[envelope.Size] == 'a' {
				close(entered)
				<-release
			}
			if msg[envelope.Size] == 'z' {
				close(done)
			}
		},
		Book: func(msg []byte) {
			books = append(books, msg[envelope.Size])
		},
	})
	base := clk.Now().UnixNano()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	if err := d.PushMD(mdAt(base, "a")); err != nil {
		t.Fatal(err)
	}
	wait(t, entered)
	clk.Add(window + time.Millisecond)
	now := clk.Now().UnixNano()
	// b is stale. The book is just as old and must still be delivered. exact is the window
	// itself (`>`), so it is kept. past is one nanosecond over. z is fresh and last.
	for _, m := range []struct {
		ts   int64
		body string
		book bool
	}{
		{base, "b", false},
		{base, "k", true},
		{now - int64(window), "e", false},
		{now - int64(window) - 1, "p", false},
		{now, "z", false},
	} {
		var err error
		if m.book {
			err = d.PushBook(mdAt(m.ts, m.body))
		} else {
			err = d.PushMD(mdAt(m.ts, m.body))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	once.Do(func() { close(release) })
	wait(t, done)
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
	if string(feeds) != "aez" {
		t.Fatalf("feeds %q", feeds)
	}
	if string(books) != "k" {
		t.Fatalf("books %q", books)
	}
	_, md, _, _ := d.Stats()
	// b and p were prehook drops. The book was not.
	if md.Dropped != 2 {
		t.Fatalf("dropped %d", md.Dropped)
	}
}

func TestPrehookMaxAge(t *testing.T) {
	clk := newClock(time.Unix(1_700_000_000, 0))
	done := make(chan struct{})
	d := New(Config{
		RecvWindow: time.Hour,
		Now:        clk.Now,
		Log:        func(string) {},
		Feed:       func([]byte) { close(done) },
	})
	// TD sits for 40ms. The MD timestamp is 50ms behind the clock at dequeue, which is after that wait.
	if err := d.PushTD(func() {}); err != nil {
		t.Fatal(err)
	}
	clk.Add(40 * time.Millisecond)
	old := clk.Now().Add(-50 * time.Millisecond).UnixNano()
	if err := d.PushMD(mdAt(old, "a")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	wait(t, done)
	td, md, _, _ := d.Stats()
	if md.MaxAgeMs < 50 {
		t.Fatalf("md max age %d", md.MaxAgeMs)
	}
	if td.MaxAgeMs < 40 {
		t.Fatalf("td max age %d", td.MaxAgeMs)
	}
	td, md, _, _ = d.Stats()
	if td.MaxAgeMs != 0 || md.MaxAgeMs != 0 {
		t.Fatalf("age not reset td %d md %d", td.MaxAgeMs, md.MaxAgeMs)
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestHookSlow(t *testing.T) {
	clk := newClock(time.Unix(1_700_000_000, 0))
	hold, arrived, open := gate(t)
	d := New(Config{Now: clk.Now, Log: func(string) {}})
	if err := d.PushTD(func() { hold() }); err != nil {
		t.Fatal(err)
	}
	if err := d.PushTD(func() { clk.Add(SlowAfter) }); err != nil {
		t.Fatal(err)
	}
	if err := d.PushTD(func() { clk.Add(SlowAfter + time.Nanosecond) }); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if err := d.PushTD(func() { close(done) }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	arrived()
	clk.Add(150 * time.Millisecond)
	_, _, blocked, slow := d.Stats()
	if blocked < 150 || slow != 0 {
		t.Fatalf("while running blocked %d slow %d", blocked, slow)
	}
	open()
	wait(t, done)
	_, _, blocked, slow = d.Stats()
	// The gated hook (150ms) and the hook that advances SlowAfter+1ns each count.
	// The hook that advances exactly SlowAfter does not.
	if blocked != 0 || slow != 2 {
		t.Fatalf("after blocked %d slow %d", blocked, slow)
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestDispatcherDropsOldest(t *testing.T) {
	const msg = envelope.Size + 1
	var got []byte
	done := make(chan struct{})
	d := New(Config{
		RecvWindow: time.Hour,
		RingBytes:  msg * 2,
		Log:        func(string) {},
		Feed: func(m []byte) {
			got = append(got, m[envelope.Size])
			if m[envelope.Size] == 'c' {
				close(done)
			}
		},
	})
	now := time.Now().UnixNano()
	for _, body := range []string{"a", "b", "c"} {
		if err := d.PushMD(mdAt(now, body)); err != nil {
			t.Fatal(err)
		}
	}
	_, md, _, _ := d.Stats()
	if md.Dropped != 1 || md.Depth != 2 {
		t.Fatalf("before run dropped %d depth %d", md.Dropped, md.Depth)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	wait(t, done)
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
	if string(got) != "bc" {
		t.Fatalf("got %q", got)
	}
}

func TestBadEnvelopeDropped(t *testing.T) {
	var logs []string
	var mu sync.Mutex
	seen := make(chan struct{})
	var n atomic.Int32
	good := make(chan struct{})
	d := New(Config{
		RecvWindow: time.Second,
		Log: func(s string) {
			mu.Lock()
			logs = append(logs, s)
			mu.Unlock()
			if n.Add(1) == 2 {
				close(seen)
			}
		},
		Feed: func(msg []byte) {
			if len(msg) < envelope.Size || string(msg[envelope.Size:]) != "ok" {
				t.Errorf("feed %q", msg)
				return
			}
			close(good)
		},
		Book: func([]byte) { t.Error("book hook ran") },
	})
	now := time.Now().UnixNano()
	if err := d.PushMD([]byte{0x7f}); err != nil {
		t.Fatal(err)
	}
	badVer := envelope.Append(nil, now, 1, 1, []byte("v"))
	badVer[0] = 2
	if err := d.PushBook(badVer); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	wait(t, seen)
	select {
	case err := <-errc:
		t.Fatalf("session ended on a bad envelope: %v", err)
	default:
	}
	if err := d.PushMD(mdAt(now, "ok")); err != nil {
		t.Fatal(err)
	}
	wait(t, good)
	_, md, _, _ := d.Stats()
	if md.Dropped != 2 {
		t.Fatalf("dropped %d", md.Dropped)
	}
	mu.Lock()
	got := append([]string(nil), logs...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("logs %q", got)
	}
	for _, line := range got {
		if !strings.Contains(line, "md envelope rejected") {
			t.Fatalf("log %q", line)
		}
	}
	cancel()
	waitErr(t, errc)
	mustStop(t, d)
}

func TestDefaultRingBytes(t *testing.T) {
	d := New(Config{})
	if len(d.ring.buf) != MDBytes {
		t.Fatalf("ring %d", len(d.ring.buf))
	}
}

func TestTDOverflow(t *testing.T) {
	hold, arrived, open := gate(t)
	var ran atomic.Int32
	d := New(Config{Log: func(string) {}})
	if err := d.PushTD(hold); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	arrived()
	for i := 0; i < TDCap; i++ {
		if err := d.PushTD(func() { ran.Add(1) }); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if err := d.PushTD(func() { ran.Add(1) }); !errors.Is(err, errFull) {
		t.Fatalf("overflow %v", err)
	}
	td, _, _, _ := d.Stats()
	if td.Depth != TDCap {
		t.Fatalf("depth %d", td.Depth)
	}
	open()
	err := waitErr(t, errc)
	x := exitOf(t, err)
	if x.Outcome != commonv1.Outcome_OUTCOME_FAILED || x.Reason != ReasonTDFull {
		t.Fatalf("exit %v", x)
	}
	if ran.Load() != 0 {
		t.Fatalf("delivered %d before Run returned", ran.Load())
	}
	if err := d.OnStop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != TDCap {
		t.Fatalf("delivered %d, want %d", ran.Load(), TDCap)
	}
	wait(t, d.Exited())
}

func TestEndings(t *testing.T) {
	t.Run("done", func(t *testing.T) { testEnding(t, endDone) })
	t.Run("failed", func(t *testing.T) { testEnding(t, endFailed) })
	t.Run("crashed", func(t *testing.T) { testEnding(t, endCrashed) })
	t.Run("hung", func(t *testing.T) { testEnding(t, endHung) })
	t.Run("onstop timeout", func(t *testing.T) { testEnding(t, endOnStopTimeout) })
	t.Run("stop grace", func(t *testing.T) { testEnding(t, endStopGrace) })
}

type endKind int

const (
	endDone endKind = iota
	endFailed
	endCrashed
	endHung
	endOnStopTimeout
	endStopGrace
)

func testEnding(t *testing.T, kind endKind) {
	t.Helper()
	hookLimit := 5 * time.Second
	if kind == endHung {
		hookLimit = 200 * time.Millisecond
	}
	if kind == endOnStopTimeout {
		hookLimit = 500 * time.Millisecond
	}
	if kind == endStopGrace {
		hookLimit = 200 * time.Millisecond
	}
	var (
		hookGID atomic.Int64
		tdGID   atomic.Int64
		stopGID atomic.Int64
		remain  atomic.Int64
	)
	hold, arrived, open := gate(t)
	var hooked chan struct{}
	var d *Disp
	d = New(Config{
		HookTimeout: hookLimit,
		Log:         func(string) {},
		UnsubscribeTD: func() {
			d.step("unsub-fn")
		},
		OnStop: func(ctx context.Context) error {
			d.step("user")
			stopGID.Store(int64(goid()))
			if dl, ok := ctx.Deadline(); ok {
				remain.Store(int64(time.Until(dl)))
			} else {
				remain.Store(-1)
			}
			if kind == endOnStopTimeout {
				hold()
			}
			if err := d.PushMD([]byte{1}); !errors.Is(err, errClosed) {
				return fmt.Errorf("PushMD %v", err)
			}
			if err := d.PushTD(func() {}); !errors.Is(err, errClosed) {
				return fmt.Errorf("PushTD %v", err)
			}
			return nil
		},
	})
	switch kind {
	case endDone, endOnStopTimeout:
		if err := d.PushTD(func() {
			d.step("hook")
			hookGID.Store(int64(goid()))
			d.Exit()
		}); err != nil {
			t.Fatal(err)
		}
	case endFailed:
		if err := d.PushTD(func() {
			d.step("hook")
			hookGID.Store(int64(goid()))
			d.Fail("because")
		}); err != nil {
			t.Fatal(err)
		}
	case endCrashed:
		if err := d.PushTD(func() {
			d.step("hook")
			hookGID.Store(int64(goid()))
			panic("boom")
		}); err != nil {
			t.Fatal(err)
		}
	case endHung:
		if err := d.PushTD(func() {
			d.step("hook")
			hookGID.Store(int64(goid()))
			hold()
		}); err != nil {
			t.Fatal(err)
		}
	case endStopGrace:
		hooked = make(chan struct{})
		if err := d.PushTD(func() {
			d.step("hook")
			hookGID.Store(int64(goid()))
			close(hooked)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if kind != endHung && kind != endCrashed && kind != endStopGrace && kind != endOnStopTimeout {
		if err := d.PushTD(func() {
			d.step("td")
			tdGID.Store(int64(goid()))
		}); err != nil {
			t.Fatal(err)
		}
	} else if kind == endHung || kind == endCrashed {
		if err := d.PushTD(func() { d.step("td") }); err != nil {
			t.Fatal(err)
		}
	}
	p := &plane{Disp: d}
	sh := boot(t, t.Context(), p)
	switch kind {
	case endStopGrace:
		wait(t, hooked)
		sh.send(stopGrace("ended_by_spec", 5000))
		sh.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec"), stopped(true))
		if c := sh.exitCode(); c != c4.ExitDone {
			t.Fatalf("exit %d", c)
		}
	case endHung:
		arrived()
		// A hook that passes hook_timeout_s is HUNG: OnStop does not run, and c4 still sends
		// Stopped(false). Omitting Stopped is only the OnStop overrun (D803).
		sh.expect(exiting(commonv1.Outcome_OUTCOME_HUNG, ReasonHung), stopped(false))
		if c := sh.exitCode(); c != c4.ExitHung {
			t.Fatalf("exit %d", c)
		}
		open()
	case endOnStopTimeout:
		arrived()
		sh.expect(exiting(commonv1.Outcome_OUTCOME_DONE, ""))
		sh.closedWithoutStopped()
		if c := sh.exitCode(); c != c4.ExitHung {
			t.Fatalf("exit %d", c)
		}
		open()
	case endDone:
		sh.expect(exiting(commonv1.Outcome_OUTCOME_DONE, ""), stopped(true))
		if c := sh.exitCode(); c != c4.ExitDone {
			t.Fatalf("exit %d", c)
		}
	case endFailed:
		sh.expect(exiting(commonv1.Outcome_OUTCOME_FAILED, "because"), stopped(true))
		if c := sh.exitCode(); c != c4.ExitFailed {
			t.Fatalf("exit %d", c)
		}
	case endCrashed:
		sh.expect(exiting(commonv1.Outcome_OUTCOME_CRASHED, "hook panic: boom"), stopped(true))
		if c := sh.exitCode(); c != c4.ExitCrash {
			t.Fatalf("exit %d", c)
		}
	}
	wait(t, d.Exited())
	tr := d.trace()
	switch kind {
	case endDone, endFailed:
		want := []string{"hook", "stop-md", "c4-onstop", "unsub", "unsub-fn", "td", "onstop", "user"}
		if strings.Join(tr, ",") != strings.Join(want, ",") {
			t.Fatalf("trace %v", tr)
		}
		if hookGID.Load() == 0 || hookGID.Load() != tdGID.Load() || hookGID.Load() != stopGID.Load() {
			t.Fatalf("gids hook %d td %d stop %d", hookGID.Load(), tdGID.Load(), stopGID.Load())
		}
		if kind == endDone {
			left := time.Duration(remain.Load())
			if left < 3*time.Second || left > 5*time.Second {
				t.Fatalf("OnStop budget %s, want about hook_timeout", left)
			}
		}
	case endCrashed:
		want := []string{"hook", "stop-md", "c4-onstop", "onstop", "user"}
		if strings.Join(tr, ",") != strings.Join(want, ",") {
			t.Fatalf("trace %v", tr)
		}
		if hookGID.Load() == 0 || hookGID.Load() != stopGID.Load() {
			t.Fatalf("gids hook %d stop %d", hookGID.Load(), stopGID.Load())
		}
	case endHung:
		if strings.Join(tr, ",") != "hook" {
			t.Fatalf("trace %v", tr)
		}
	case endOnStopTimeout:
		want := []string{"hook", "stop-md", "c4-onstop", "unsub", "unsub-fn", "onstop", "user"}
		if strings.Join(tr, ",") != strings.Join(want, ",") {
			t.Fatalf("trace %v", tr)
		}
		left := time.Duration(remain.Load())
		if left <= 0 || left > hookLimit {
			t.Fatalf("OnStop budget %s", left)
		}
	case endStopGrace:
		want := []string{"hook", "stop-md", "c4-onstop", "unsub", "unsub-fn", "onstop", "user"}
		if strings.Join(tr, ",") != strings.Join(want, ",") {
			t.Fatalf("trace %v", tr)
		}
		if hookGID.Load() == 0 || hookGID.Load() != stopGID.Load() {
			t.Fatalf("gids hook %d stop %d", hookGID.Load(), stopGID.Load())
		}
		left := time.Duration(remain.Load())
		if left < 2*time.Second || left > 5*time.Second {
			t.Fatalf("grace left %s", left)
		}
	}
}

// plane is the c4 plane the ending tests host. OnStop marks the call, which c4 makes after Exiting,
// then runs the dispatcher's OnStop on the dispatcher goroutine.
type plane struct{ *Disp }

func (p *plane) Init(context.Context, *c4.Session, []byte) error { return nil }
func (p *plane) Start(context.Context, []byte) error             { return nil }
func (p *plane) Live([]byte) error                               { return nil }

func (p *plane) OnStop(ctx context.Context) error {
	p.step("c4-onstop")
	return p.Disp.OnStop(ctx)
}

type shim struct {
	t     *testing.T
	w     *frame.Writer
	ups   chan *workerv1.WorkerUp
	codes chan int
}

func boot(t *testing.T, ctx context.Context, p c4.Plane) *shim {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	s := &shim{t: t, w: frame.NewWriter(a), ups: make(chan *workerv1.WorkerUp, 64), codes: make(chan int, 1)}
	go func() {
		s.codes <- c4.Serve(ctx, b, c4.Config{Kind: "test", Contract: "test.v1"}, p)
	}()
	go func() {
		r := frame.NewReader(bufio.NewReader(a))
		for {
			m := &workerv1.WorkerUp{}
			if err := r.Read(m); err != nil {
				close(s.ups)
				return
			}
			if m.GetBeat() != nil {
				continue
			}
			s.ups <- m
		}
	}()
	s.expect(func(m *workerv1.WorkerUp) bool { return m.GetHello() != nil })
	s.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{Contract: "test.v1"}}})
	s.expect(doneOK(workerv1.Verb_VERB_INIT))
	s.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: "test.v1"}}})
	s.expect(doneOK(workerv1.Verb_VERB_START), func(m *workerv1.WorkerUp) bool {
		return m.GetStage().GetName() == "running"
	})
	return s
}

func (s *shim) send(m proto.Message) {
	s.t.Helper()
	if err := s.w.Write(m); err != nil {
		s.t.Fatal(err)
	}
}

func (s *shim) next() *workerv1.WorkerUp {
	s.t.Helper()
	select {
	case m, ok := <-s.ups:
		if !ok {
			s.t.Fatal("worker closed fd 3")
		}
		return m
	case <-time.After(5 * time.Second):
		s.t.Fatal("no frame")
	}
	return nil
}

func (s *shim) expect(want ...func(*workerv1.WorkerUp) bool) {
	s.t.Helper()
	for i, ok := range want {
		m := s.next()
		if !ok(m) {
			s.t.Fatalf("frame %d: %v", i, m)
		}
	}
}

func (s *shim) exitCode() int {
	s.t.Helper()
	select {
	case c := <-s.codes:
		return c
	case <-time.After(5 * time.Second):
		s.t.Fatal("serve did not return")
	}
	return 0
}

func (s *shim) closedWithoutStopped() {
	s.t.Helper()
	for {
		select {
		case m, ok := <-s.ups:
			if !ok {
				return
			}
			if m.GetStopped() != nil {
				s.t.Fatal("Stopped was sent")
			}
			s.t.Fatalf("frame after the ending: %v", m)
		case <-time.After(5 * time.Second):
			s.t.Fatal("fd 3 did not close")
		}
	}
}

func doneOK(v workerv1.Verb) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		d := m.GetDone()
		return d != nil && d.GetVerb() == v && d.GetOk()
	}
}

func exiting(o commonv1.Outcome, reason string) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		e := m.GetExiting()
		return e != nil && e.GetOutcome() == o && e.GetReason() == reason
	}
}

func stopped(onStop bool) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		st := m.GetStopped()
		return st != nil && st.GetOnStopCompleted() == onStop
	}
}

func stopGrace(reason string, graceMs uint32) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: reason, GraceMs: graceMs}}}
}

func mdAt(ts int64, body string) []byte {
	return envelope.Append(nil, ts, 1, 1, []byte(body))
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock(t time.Time) *clock { return &clock{t: t} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// gate parks a hook until the test opens it. Cleanup opens it so a failed test does not leak the hook.
func gate(t *testing.T) (hold func(), arrived func(), open func()) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	open = func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	var onceEnter sync.Once
	hold = func() {
		onceEnter.Do(func() { close(entered) })
		<-release
	}
	arrived = func() { wait(t, entered) }
	return hold, arrived, open
}

func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func waitErr(t *testing.T, errc <-chan error) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	return nil
}

func mustStop(t *testing.T, d *Disp) {
	t.Helper()
	if err := d.OnStop(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait(t, d.Exited())
}

func exitOf(t *testing.T, err error) *c4.Exit {
	t.Helper()
	var x *c4.Exit
	if !errors.As(err, &x) || x == nil {
		t.Fatalf("got %v", err)
	}
	return x
}

func goid() int {
	var b [64]byte
	n := runtime.Stack(b[:], false)
	var id int
	fmt.Sscanf(string(b[:n]), "goroutine %d", &id)
	return id
}
