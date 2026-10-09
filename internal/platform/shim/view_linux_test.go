package shim

import (
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// fakeProc is the worker process behind a view under test: it records the signals sent to its group, with when.
type fakeProc struct {
	mu      sync.Mutex
	sent    []sentSignal
	done    chan struct{}
	exitOn  syscall.Signal // the signal that ends it; 0 for none
	exited  bool
	started time.Time
}

type sentSignal struct {
	sig syscall.Signal
	at  time.Duration // after the fake was made
}

func newFakeProc(exitOn syscall.Signal) *fakeProc {
	return &fakeProc{done: make(chan struct{}), exitOn: exitOn, started: time.Now()}
}

func (p *fakeProc) Signal(sig syscall.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exited {
		return ErrExited
	}
	p.sent = append(p.sent, sentSignal{sig, time.Since(p.started)})
	if sig == p.exitOn {
		p.exitLocked()
	}
	return nil
}

func (p *fakeProc) Done() <-chan struct{} { return p.done }

// Status is exit 0, or the signal that ended it.
func (p *fakeProc) Status() syscall.WaitStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exitOn != 0 && len(p.sent) > 0 && p.sent[len(p.sent)-1].sig == p.exitOn {
		return syscall.WaitStatus(p.exitOn) // the low 7 bits are the signal
	}
	return 0
}

func (p *fakeProc) exit() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exitLocked()
}

func (p *fakeProc) exitLocked() {
	if !p.exited {
		p.exited = true
		close(p.done)
	}
}

func (p *fakeProc) signals() []sentSignal {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.sent)
}

// c4Worker is the test's end of the view's C4 channel.
type c4Worker struct {
	t *testing.T
	f *os.File
	r *frame.Reader
	w *frame.Writer
}

// newTestView is a view of p, with a socketpair as its C4 channel; the test plays the worker on the other end.
func newTestView(t *testing.T, p proc) (*view, *c4Worker) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	ours, theirs := os.NewFile(uintptr(fds[0]), "ctl"), os.NewFile(uintptr(fds[1]), "worker")
	t.Cleanup(func() { ours.Close(); theirs.Close() })
	// Not t's log: the view's goroutines outlive the test, and would log into a finished one.
	v := newViewOf(zerolog.Nop(), p, 4242, 4242, 77, ours)
	go v.follow(ours)
	return v, &c4Worker{t: t, f: theirs, r: frame.NewReader(theirs), w: frame.NewWriter(theirs)}
}

// next reads what the shim sent the worker.
func (w *c4Worker) next() *workerv1.WorkerDown {
	w.t.Helper()
	w.f.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck // a socketpair from os.NewFile may not take deadlines; the test timeout then bounds it
	m := &workerv1.WorkerDown{}
	if err := w.r.Read(m); err != nil {
		w.t.Fatalf("worker read: %v", err)
	}
	return m
}

func (w *c4Worker) send(m *workerv1.WorkerUp) {
	w.t.Helper()
	if err := w.w.Write(m); err != nil {
		w.t.Fatalf("worker write: %v", err)
	}
}

func (w *c4Worker) done(verb workerv1.Verb, ok bool) {
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: verb, Ok: ok}}})
}

// waitPhase waits for the view to reach phase, which the worker's messages move on another goroutine.
func waitPhase(t *testing.T, v *view, phase shimv1.Phase) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for v.Attached().GetPhase() != phase {
		if time.Now().After(deadline) {
			t.Fatalf("phase %v, want %v", v.Attached().GetPhase(), phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func wantStage(t *testing.T, err error, phase shimv1.Phase) {
	t.Helper()
	var se *stageError
	if !errors.As(err, &se) || se.phase != phase {
		t.Fatalf("want bad_stage in %v, got %v", phase, err)
	}
}

// Init, then Start once the Init is done: each passed to the worker as is, each moving the phase; a verb out of
// turn is refused, and the worker hears nothing of it.
func TestViewPhases(t *testing.T) {
	v, w := newTestView(t, newFakeProc(0))
	if p := v.Attached().GetPhase(); p != shimv1.Phase_PHASE_NEW {
		t.Fatalf("phase %v at spawn", p)
	}
	wantStage(t, v.Start(&workerv1.Start{}), shimv1.Phase_PHASE_NEW)

	if err := v.Init(&workerv1.Init{Contract: "fake.v1", Payload: []byte("i"), DeadlineMs: 7}); err != nil {
		t.Fatal(err)
	}
	if m := w.next().GetInit(); m.GetContract() != "fake.v1" || string(m.GetPayload()) != "i" || m.GetDeadlineMs() != 7 {
		t.Fatalf("init passed as %v", m)
	}
	waitPhase(t, v, shimv1.Phase_PHASE_INITIALIZING)
	wantStage(t, v.Init(&workerv1.Init{}), shimv1.Phase_PHASE_INITIALIZING)
	wantStage(t, v.Start(&workerv1.Start{}), shimv1.Phase_PHASE_INITIALIZING) // before the worker's Done

	w.done(workerv1.Verb_VERB_START, true) // a Done for the wrong verb moves nothing
	w.done(workerv1.Verb_VERB_INIT, true)
	waitPhase(t, v, shimv1.Phase_PHASE_INITIALIZED)

	if err := v.Relay(&workerv1.Relay{Contract: "fake.v1", CorrId: []byte("c"), Payload: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if m := w.next().GetRelay(); string(m.GetCorrId()) != "c" || string(m.GetPayload()) != "r" {
		t.Fatalf("relay passed as %v", m)
	}
	if err := v.Start(&workerv1.Start{Payload: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	if m := w.next().GetStart(); string(m.GetPayload()) != "s" {
		t.Fatalf("start passed as %v", m)
	}
	w.done(workerv1.Verb_VERB_START, true)
	waitPhase(t, v, shimv1.Phase_PHASE_STARTED)
	wantStage(t, v.Init(&workerv1.Init{}), shimv1.Phase_PHASE_STARTED)
	wantStage(t, v.Start(&workerv1.Start{}), shimv1.Phase_PHASE_STARTED)
}

// A failed Init or Start, or an Exiting, means the worker is ending: STOPPING, and no verb after.
func TestViewWorkerEnds(t *testing.T) {
	t.Run("init failed", func(t *testing.T) {
		v, w := newTestView(t, newFakeProc(0))
		v.Init(&workerv1.Init{}) //nolint:errcheck
		w.next()
		w.done(workerv1.Verb_VERB_INIT, false)
		waitPhase(t, v, shimv1.Phase_PHASE_STOPPING)
		wantStage(t, v.Start(&workerv1.Start{}), shimv1.Phase_PHASE_STOPPING)
	})
	t.Run("exiting", func(t *testing.T) {
		v, w := newTestView(t, newFakeProc(0))
		w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{Reason: "exit"}}})
		waitPhase(t, v, shimv1.Phase_PHASE_STOPPING)
		wantStage(t, v.Init(&workerv1.Init{}), shimv1.Phase_PHASE_STOPPING)
	})
}

// Stop: C4 Stop at once, carrying the grace; SIGTERM to the group when the grace is over, SIGKILL kill_after later.
func TestViewStopEscalates(t *testing.T) {
	p := newFakeProc(syscall.SIGKILL) // ends only on SIGKILL, as a worker hung in OnStop would
	v, w := newTestView(t, p)
	const grace, killAfter = 150 * time.Millisecond, 200 * time.Millisecond
	begin := time.Since(p.started)
	first, err := v.Stop("why", grace, killAfter)
	if err != nil || !first {
		t.Fatalf("stop: %v %v", first, err)
	}
	if m := w.next().GetStop(); m.GetReason() != "why" || m.GetGraceMs() != 150 {
		t.Fatalf("stop passed as %v", m)
	}
	if v.Attached().GetPhase() != shimv1.Phase_PHASE_STOPPING {
		t.Fatalf("phase %v", v.Attached().GetPhase())
	}
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("not killed")
	}
	got := p.signals()
	if len(got) != 2 || got[0].sig != syscall.SIGTERM || got[1].sig != syscall.SIGKILL {
		t.Fatalf("signals %v, want SIGTERM then SIGKILL", got)
	}
	if got[0].at-begin < grace || got[1].at-got[0].at < killAfter {
		t.Errorf("SIGTERM at %v, SIGKILL at %v after the Stop: too early for grace %v, kill_after %v",
			got[0].at-begin, got[1].at-begin, grace, killAfter)
	}
}

// A worker that ends within the grace gets no signal, and one that ends on SIGTERM gets no SIGKILL.
func TestViewStopNoNeedlessSignal(t *testing.T) {
	p := newFakeProc(0)
	v, w := newTestView(t, p)
	v.Stop("why", 100*time.Millisecond, 100*time.Millisecond) //nolint:errcheck
	w.next()
	p.exit()
	time.Sleep(300 * time.Millisecond)
	if got := p.signals(); len(got) != 0 {
		t.Fatalf("signals %v to a worker that ended", got)
	}

	p = newFakeProc(syscall.SIGTERM)
	v, w = newTestView(t, p)
	v.Stop("why", 0, 100*time.Millisecond) //nolint:errcheck // grace 0: SIGTERM at once
	w.next()
	<-p.Done()
	time.Sleep(200 * time.Millisecond)
	if got := p.signals(); len(got) != 1 || got[0].sig != syscall.SIGTERM {
		t.Fatalf("signals %v, want SIGTERM only", got)
	}
}

// Only the first Stop sets the timers: a second, shorter one does not hurry them.
func TestViewStopOnce(t *testing.T) {
	p := newFakeProc(syscall.SIGTERM)
	v, w := newTestView(t, p)
	if first, _ := v.Stop("one", 300*time.Millisecond, time.Second); !first {
		t.Fatal("first Stop not first")
	}
	w.next()
	if first, err := v.Stop("two", 0, 0); first || err != nil {
		t.Fatalf("second Stop: first %v, err %v", first, err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := p.signals(); len(got) != 0 {
		t.Fatalf("signals %v: the second Stop hurried the first", got)
	}
	<-p.Done()
	if got := p.signals(); len(got) != 1 || got[0].at < 300*time.Millisecond {
		t.Fatalf("signals %v", got)
	}
}

// A worker that is already ending (it sent Exiting) is not told to Stop again, but the timers still run: its OnStop
// may hang.
func TestViewStopAfterExiting(t *testing.T) {
	p := newFakeProc(syscall.SIGTERM)
	v, w := newTestView(t, p)
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{Reason: "fail"}}})
	waitPhase(t, v, shimv1.Phase_PHASE_STOPPING)
	if first, err := v.Stop("why", 50*time.Millisecond, time.Second); !first || err != nil {
		t.Fatalf("stop: %v %v", first, err)
	}
	if err := v.Relay(&workerv1.Relay{CorrId: []byte("after")}); err != nil { // a marker: what came before it
		t.Fatal(err)
	}
	if m := w.next(); string(m.GetRelay().GetCorrId()) != "after" {
		t.Fatalf("the worker got %v before the marker: no Stop should have gone down", m)
	}
	<-p.Done()
}

// Once reaped, nothing more goes to the worker, and Release is accepted.
func TestViewExited(t *testing.T) {
	p := newFakeProc(0)
	v, _ := newTestView(t, p)
	if !errors.Is(v.Release(), ErrRunning) {
		t.Fatal("release accepted while running")
	}
	p.exit()
	for name, err := range map[string]error{
		"init":  v.Init(&workerv1.Init{}),
		"start": v.Start(&workerv1.Start{}),
		"relay": v.Relay(&workerv1.Relay{}),
	} {
		if !errors.Is(err, ErrExited) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := v.Stop("x", 0, 0); !errors.Is(err, ErrExited) {
		t.Errorf("stop: %v", err)
	}
	if !errors.Is(v.Release(), ErrRunning) && !v.isRecorded() {
		t.Error("release accepted before the exit was recorded")
	}
	<-v.Recorded()
	if err := v.Release(); err != nil {
		t.Errorf("release: %v", err)
	}
	if v.Attached().GetState() != shimv1.WorkerState_WORKER_STATE_EXITED {
		t.Errorf("state %v", v.Attached().GetState())
	}
}

// A worker that stops reading its fd 3 does not hold up the shim: once the queue is full, requests are refused at
// once with ErrWorkerBusy.
func TestViewWorkerNotReading(t *testing.T) {
	ours, theirs := net.Pipe() // unbuffered: the first write blocks until the worker reads, which it never does
	t.Cleanup(func() { ours.Close(); theirs.Close() })
	v := newViewOf(zerolog.Nop(), newFakeProc(0), 1, 1, 0, ours)
	start := time.Now()
	var err error
	for i := 0; i <= downQueue+1 && err == nil; i++ {
		err = v.Relay(&workerv1.Relay{})
	}
	if !errors.Is(err, ErrWorkerBusy) {
		t.Fatalf("got %v, want ErrWorkerBusy", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %v", d)
	}
}
