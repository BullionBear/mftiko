package c4

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

const contract = "test.v1"

// fake is a Plane whose steps the test scripts.
type fake struct {
	initErr  error
	startErr error
	runErr   error
	// startWaits: Start blocks until its context ends, then returns that context's error.
	startWaits bool
	// runReturns: Run returns runErr at once instead of waiting for its context.
	runReturns bool
	liveErr    error

	mu      sync.Mutex
	relays  []string
	lives   []string
	started bool
	sess    *Session
}

func (f *fake) Init(_ context.Context, s *Session, payload []byte) error {
	f.sess = s
	if f.initErr != nil {
		return f.initErr
	}
	if string(payload) != "init" {
		return errors.New("payload " + string(payload))
	}
	return nil
}

func (f *fake) Start(ctx context.Context, _ []byte) error {
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	if f.startWaits {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.startErr
}

func (f *fake) Run(ctx context.Context) error {
	if f.runReturns {
		return f.runErr
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fake) Live(payload []byte) error {
	if f.liveErr != nil {
		return f.liveErr
	}
	f.mu.Lock()
	f.lives = append(f.lives, string(payload))
	f.mu.Unlock()
	return nil
}

// peer is the shim's end: it writes WorkerDown and records every WorkerUp but Beat.
type peer struct {
	t    *testing.T
	conn net.Conn
	w    *frame.Writer
	ups  chan *workerv1.WorkerUp
	code chan int
}

func run(t *testing.T, ctx context.Context, cfg Config, p Plane) *peer {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	pr := &peer{t: t, conn: a, w: frame.NewWriter(a), ups: make(chan *workerv1.WorkerUp, 64), code: make(chan int, 1)}
	if cfg.Contract == "" {
		cfg.Contract = contract
	}
	if cfg.Kind == "" {
		cfg.Kind = "test"
	}
	go func() { pr.code <- Serve(ctx, b, cfg, p) }()
	go func() {
		r := frame.NewReader(bufio.NewReader(a))
		for {
			m := &workerv1.WorkerUp{}
			if err := r.Read(m); err != nil {
				close(pr.ups)
				return
			}
			if m.GetBeat() != nil {
				continue
			}
			pr.ups <- m
		}
	}()
	return pr
}

func (p *peer) send(m *workerv1.WorkerDown) {
	p.t.Helper()
	if err := p.w.Write(m); err != nil {
		p.t.Fatal(err)
	}
}

func (p *peer) next() *workerv1.WorkerUp {
	p.t.Helper()
	select {
	case m, ok := <-p.ups:
		if !ok {
			p.t.Fatal("worker closed fd 3")
		}
		return m
	case <-time.After(5 * time.Second):
		p.t.Fatal("no frame")
	}
	return nil
}

// expect reads the next frames and checks each against want, in order.
func (p *peer) expect(want ...func(*workerv1.WorkerUp) bool) []*workerv1.WorkerUp {
	p.t.Helper()
	got := make([]*workerv1.WorkerUp, 0, len(want))
	for i, ok := range want {
		m := p.next()
		if !ok(m) {
			p.t.Fatalf("frame %d: %v", i, m)
		}
		got = append(got, m)
	}
	return got
}

func (p *peer) exit() int {
	p.t.Helper()
	select {
	case c := <-p.code:
		return c
	case <-time.After(5 * time.Second):
		p.t.Fatal("serve did not return")
	}
	return 0
}

// closedWithoutStopped reads until the worker closes fd 3. A Stopped frame is a failure: a
// hook-timeout overrun must not send one (E08-D003).
func (p *peer) closedWithoutStopped() {
	p.t.Helper()
	for {
		select {
		case m, ok := <-p.ups:
			if !ok {
				return
			}
			if m.GetStopped() != nil {
				p.t.Fatal("Stopped was sent")
			}
			p.t.Fatalf("frame after the ending: %v", m)
		case <-time.After(5 * time.Second):
			p.t.Fatal("fd 3 did not close")
		}
	}
}

func hello(m *workerv1.WorkerUp) bool {
	h := m.GetHello()
	return h != nil && h.GetKind() == "test" && len(h.GetContracts()) == 1 && h.GetContracts()[0] == contract && h.GetPid() > 0
}

func doneOf(v workerv1.Verb, ok bool) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		return m.GetDone() != nil && m.GetDone().GetVerb() == v && m.GetDone().GetOk() == ok
	}
}

func applied(d string) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool { return m.GetApplied() != nil && m.GetApplied().GetDigest() == d }
}

func stage(m *workerv1.WorkerUp) bool { return m.GetStage().GetName() == "running" }

func exiting(o commonv1.Outcome, reason string) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		e := m.GetExiting()
		return e != nil && e.GetOutcome() == o && e.GetReason() == reason
	}
}

func stopped(onStop bool) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		return m.GetStopped() != nil && m.GetStopped().GetOnStopCompleted() == onStop
	}
}

func initMsg(c string) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{Contract: c, Payload: []byte("init"), LiveDigest: "d0"}}}
}

func startMsg() *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: contract}}}
}

func liveMsg(payload, digest string) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Live{Live: &workerv1.Live{Contract: contract, Payload: []byte(payload), Digest: digest}}}
}

func stopMsg(reason string) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: reason}}}
}

func TestLifecycle(t *testing.T) {
	f := &fake{}
	p := run(t, t.Context(), Config{}, f)
	p.expect(hello)
	// A Live before Init is held, and applied before Done(INIT).
	p.send(liveMsg("early", "d1"))
	p.send(initMsg(contract))
	p.expect(applied("d1"), doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(liveMsg("later", "d2"))
	p.expect(applied("d2"))
	p.send(startMsg())
	p.expect(doneOf(workerv1.Verb_VERB_START, true), stage)

	if err := f.sess.Report(&workerv1.Stage{Name: "x"}); err != nil {
		t.Fatal(err)
	}
	rep := p.next().GetReport()
	var got workerv1.Stage
	if rep.GetContract() != contract || proto.Unmarshal(rep.GetPayload(), &got) != nil || got.GetName() != "x" {
		t.Fatalf("report %v", rep)
	}

	p.send(stopMsg("ended_by_spec"))
	p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec"), stopped(true))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("exit %d", c)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.lives) != 2 || f.lives[0] != "early" || f.lives[1] != "later" {
		t.Fatalf("lives %v", f.lives)
	}
}

func TestInitCarriesLive(t *testing.T) {
	p := run(t, t.Context(), Config{InitCarriesLive: true}, &fake{})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(applied("d0"), doneOf(workerv1.Verb_VERB_INIT, true))
}

func TestInitFails(t *testing.T) {
	for name, c := range map[string]struct {
		f        *fake
		contract string
		reason   string
	}{
		"plane":    {&fake{initErr: errors.New("unknown venue X")}, contract, "unknown venue X"},
		"contract": {&fake{}, "other.v1", "contract other.v1"},
	} {
		t.Run(name, func(t *testing.T) {
			p := run(t, t.Context(), Config{InitCarriesLive: true}, c.f)
			p.expect(hello)
			p.send(initMsg(c.contract))
			p.expect(doneOf(workerv1.Verb_VERB_INIT, false), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, c.reason), stopped(false))
			if code := p.exit(); code != ExitInit {
				t.Fatalf("exit %d", code)
			}
		})
	}
}

func TestStartFails(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{startErr: errors.New("hedge_mode")})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(startMsg())
	p.expect(doneOf(workerv1.Verb_VERB_START, false), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "hedge_mode"), stopped(true))
	if c := p.exit(); c != ExitInit {
		t.Fatalf("exit %d", c)
	}
}

// A Stop that arrives while Start blocks ends Start's context, and the worker stops: DONE, not
// INIT_FAILED, and no Done(START).
func TestStopDuringStart(t *testing.T) {
	f := &fake{startWaits: true}
	p := run(t, t.Context(), Config{}, f)
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(startMsg())
	waitFor(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.started })
	p.send(stopMsg("deps_failed"))
	p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "deps_failed"), stopped(true))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("exit %d", c)
	}
}

func TestRunFails(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{runReturns: true, runErr: errors.New("connection loop: boom")})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(startMsg())
	p.expect(doneOf(workerv1.Verb_VERB_START, true), stage,
		exiting(commonv1.Outcome_OUTCOME_CRASHED, "connection loop: boom"), stopped(true))
	if c := p.exit(); c != ExitCrash {
		t.Fatalf("exit %d", c)
	}
}

func TestRunEnds(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{runReturns: true})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(startMsg())
	p.expect(doneOf(workerv1.Verb_VERB_START, true), stage, exiting(commonv1.Outcome_OUTCOME_DONE, ""), stopped(true))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("exit %d", c)
	}
}

func TestLiveRefused(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{liveErr: errors.New("bad")})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(liveMsg("x", "d1"))
	// A Live in another contract is ignored too.
	p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Live{Live: &workerv1.Live{Contract: "other.v1", Digest: "d2"}}})
	p.send(startMsg())
	// No Applied before Done(START): neither Live was applied.
	p.expect(doneOf(workerv1.Verb_VERB_START, true), stage)
}

// The shim's EOF before Init, and the caller's context before Start, are stops.
func TestEOFAndContext(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{})
	p.expect(hello)
	p.conn.Close()
	if c := p.exit(); c != ExitDone {
		t.Fatalf("eof: exit %d", c)
	}

	ctx, cancel := context.WithCancel(t.Context())
	p = run(t, ctx, Config{}, &fake{})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	cancel()
	p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "signal"), stopped(false))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("ctx: exit %d", c)
	}
}

func TestBeat(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	go Serve(t.Context(), b, Config{Kind: "test", Contract: contract}, &fake{})
	r := frame.NewReader(bufio.NewReader(a))
	_ = a.SetReadDeadline(time.Now().Add(3 * BeatEvery))
	var seq uint64
	for seq < 2 {
		m := &workerv1.WorkerUp{}
		if err := r.Read(m); err != nil {
			t.Fatal(err)
		}
		if b := m.GetBeat(); b != nil {
			if b.GetSeq() != seq+1 {
				t.Fatalf("beat %d after %d", b.GetSeq(), seq)
			}
			seq = b.GetSeq()
		}
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// relayer is a fake that takes Commands.
type relayer struct{ fake }

func (f *relayer) Relay(corrID, payload []byte) {
	f.mu.Lock()
	f.relays = append(f.relays, string(corrID)+"="+string(payload))
	f.mu.Unlock()
}

func relayMsg(c, corr, payload string) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Relay{Relay: &workerv1.Relay{Contract: c, CorrId: []byte(corr), Payload: []byte(payload)}}}
}

func TestRelay(t *testing.T) {
	f := &relayer{}
	p := run(t, t.Context(), Config{}, f)
	p.expect(hello)
	p.send(relayMsg(contract, "c0", "before init")) // dropped: the agent sends a full snapshot after Init
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(relayMsg(contract, "c1", "drain"))
	p.send(relayMsg("other.v1", "c2", "x"))
	p.send(relayMsg("", "c3", "fail"))
	// A Live after the Relays: its Applied says the reader has passed them.
	p.send(liveMsg("l", "d1"))
	p.expect(applied("d1"))
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.relays) != 2 || f.relays[0] != "c1=drain" || f.relays[1] != "c3=fail" {
		t.Fatalf("relays %v", f.relays)
	}
}

// A plane that is not a Relayer drops Commands and keeps reading.
func TestRelayDropped(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(relayMsg(contract, "c1", "drain"))
	p.send(liveMsg("l", "d1"))
	p.expect(applied("d1"))
}

func reachRunning(t *testing.T, p *peer) {
	t.Helper()
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(startMsg())
	p.expect(doneOf(workerv1.Verb_VERB_START, true), stage)
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func stopGrace(reason string, graceMs uint32) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: reason, GraceMs: graceMs}}}
}

func initDeadline(ms uint32) *workerv1.WorkerDown {
	m := initMsg(contract)
	m.GetInit().DeadlineMs = ms
	return m
}

func startDeadline(ms uint32) *workerv1.WorkerDown {
	m := startMsg()
	m.GetStart().DeadlineMs = ms
	return m
}

func doneErr(v workerv1.Verb, text string) func(*workerv1.WorkerUp) bool {
	return func(m *workerv1.WorkerUp) bool {
		d := m.GetDone()
		return d != nil && d.GetVerb() == v && !d.GetOk() && d.GetError() == text
	}
}

// script is a Plane and a Stopper. A nil hook falls through to fake.
type script struct {
	fake
	initFn  func(context.Context, *Session, []byte) error
	startFn func(context.Context, []byte) error
	runFn   func(context.Context) error
	stopFn  func(context.Context) error
	stopN   int
	hook    time.Duration // HookTimeout; 0 is no self-end limit
}

func (s *script) Init(ctx context.Context, sess *Session, payload []byte) error {
	if s.initFn != nil {
		return s.initFn(ctx, sess, payload)
	}
	return s.fake.Init(ctx, sess, payload)
}

func (s *script) Start(ctx context.Context, payload []byte) error {
	if s.startFn != nil {
		return s.startFn(ctx, payload)
	}
	return s.fake.Start(ctx, payload)
}

func (s *script) Run(ctx context.Context) error {
	if s.runFn != nil {
		return s.runFn(ctx)
	}
	return s.fake.Run(ctx)
}

func (s *script) OnStop(ctx context.Context) error {
	s.mu.Lock()
	s.stopN++
	fn := s.stopFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (s *script) stops() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopN
}

func (s *script) HookTimeout() time.Duration { return s.hook }

// OnStop runs after Exiting has been sent and before Stopped. A Stop's grace is the deadline,
// measured from when the Stop was read, and the context is not the one Serve already cancelled.
func TestStopperBetweenExitingAndStopped(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	entered := make(chan struct{})
	var got context.Context
	f := &script{stopFn: func(ctx context.Context) error {
		got = ctx
		close(entered)
		<-release
		return nil
	}}
	p := run(t, t.Context(), Config{}, f)
	reachRunning(t, p)
	p.send(stopGrace("ended_by_spec", 5000))
	await(t, entered)
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	d, ok := got.Deadline()
	if !ok {
		t.Fatal("no deadline")
	}
	if remain := time.Until(d); remain < time.Second || remain > 5*time.Second {
		t.Fatalf("remaining grace %s", remain)
	}
	p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec"))
	select {
	case m := <-p.ups:
		t.Fatalf("frame before OnStop returned: %v", m)
	default:
	}
	once.Do(func() { close(release) })
	p.expect(stopped(true))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("exit %d", c)
	}
	if f.stops() != 1 {
		t.Fatalf("OnStop calls %d", f.stops())
	}
}

func TestStopGraceZero(t *testing.T) {
	f := &script{stopFn: func(ctx context.Context) error {
		if ctx.Err() == nil {
			return errors.New("grace 0 still open")
		}
		return nil
	}}
	p := run(t, t.Context(), Config{}, f)
	reachRunning(t, p)
	p.send(stopGrace("ended_by_spec", 0))
	p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec"), stopped(true))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("exit %d", c)
	}
}

// The worker ending itself is not given a deadline (E08-D009). A Stopper error is Stopped(false);
// the exit code stays, because Exiting was already sent.
func TestStopperSelfEndAndError(t *testing.T) {
	t.Run("run", func(t *testing.T) {
		f := &script{fake: fake{runReturns: true}, stopFn: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); ok || ctx.Err() != nil {
				return errors.New("self-end was limited")
			}
			return nil
		}}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, ""), stopped(true))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
	})
	t.Run("error", func(t *testing.T) {
		f := &script{stopFn: func(context.Context) error { return errors.New("disk") }}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		p.send(stopMsg("ended_by_spec"))
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec"), stopped(false))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
	})
}

func TestStopperNotCalled(t *testing.T) {
	t.Run("hung", func(t *testing.T) {
		f := &script{fake: fake{runReturns: true, runErr: &Exit{Outcome: commonv1.Outcome_OUTCOME_HUNG, Reason: "on_order"}}}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		p.expect(exiting(commonv1.Outcome_OUTCOME_HUNG, "on_order"), stopped(false))
		if c := p.exit(); c != ExitHung {
			t.Fatalf("exit %d", c)
		}
		if f.stops() != 0 {
			t.Fatal("OnStop ran")
		}
	})
	t.Run("init", func(t *testing.T) {
		f := &script{fake: fake{initErr: errors.New("unknown venue X")}}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initMsg(contract))
		p.expect(doneOf(workerv1.Verb_VERB_INIT, false), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "unknown venue X"), stopped(false))
		if c := p.exit(); c != ExitInit {
			t.Fatalf("exit %d", c)
		}
		if f.stops() != 0 {
			t.Fatal("OnStop ran")
		}
	})
}

// A Start that fails after Init succeeded runs OnStop, with no deadline of its own (E08-D008, E08-D009).
func TestStopperOnStartFailure(t *testing.T) {
	f := &script{
		fake: fake{startErr: errors.New("hedge_mode")},
		stopFn: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); ok {
				return errors.New("start failure was limited")
			}
			return nil
		},
	}
	p := run(t, t.Context(), Config{}, f)
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(startMsg())
	p.expect(doneOf(workerv1.Verb_VERB_START, false), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "hedge_mode"), stopped(true))
	if c := p.exit(); c != ExitInit {
		t.Fatalf("exit %d", c)
	}
	if f.stops() != 1 {
		t.Fatalf("OnStop calls %d", f.stops())
	}
}

func TestDeadline(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		entered := make(chan struct{})
		hold := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(hold) }) })
		returned := make(chan struct{})
		f := &script{initFn: func(context.Context, *Session, []byte) error {
			close(entered)
			<-hold
			close(returned)
			return errors.New("late")
		}}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initDeadline(50))
		await(t, entered)
		p.expect(doneErr(workerv1.Verb_VERB_INIT, "deadline"), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "deadline"), stopped(false))
		if c := p.exit(); c != ExitInit {
			t.Fatalf("exit %d", c)
		}
		select {
		case <-returned:
			t.Fatal("init was waited for")
		default:
		}
		if f.stops() != 0 {
			t.Fatal("OnStop ran")
		}
	})
	t.Run("start", func(t *testing.T) {
		entered := make(chan struct{})
		hold := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(hold) }) })
		f := &script{startFn: func(context.Context, []byte) error {
			close(entered)
			<-hold
			return errors.New("late")
		}}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initMsg(contract))
		p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
		p.send(startDeadline(50))
		await(t, entered)
		p.expect(doneErr(workerv1.Verb_VERB_START, "deadline"), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "deadline"), stopped(false))
		if c := p.exit(); c != ExitInit {
			t.Fatalf("exit %d", c)
		}
		if f.stops() != 0 {
			t.Fatal("OnStop ran")
		}
	})
	// The hook notices its context and returns. That is still the deadline, not a Start failure.
	t.Run("start returns", func(t *testing.T) {
		f := &script{startFn: func(ctx context.Context, _ []byte) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initMsg(contract))
		p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
		p.send(startDeadline(50))
		p.expect(doneErr(workerv1.Verb_VERB_START, "deadline"), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "deadline"), stopped(false))
		if c := p.exit(); c != ExitInit {
			t.Fatalf("exit %d", c)
		}
		if f.stops() != 0 {
			t.Fatal("OnStop ran")
		}
	})
	// Zero is no limit: a Stop while Init blocks is a stop, not INIT_FAILED.
	t.Run("zero", func(t *testing.T) {
		entered := make(chan struct{})
		f := &script{}
		f.initFn = func(ctx context.Context, sess *Session, payload []byte) error {
			if err := f.fake.Init(ctx, sess, payload); err != nil {
				return err
			}
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initDeadline(0))
		await(t, entered)
		p.send(stopMsg("ended_by_spec"))
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "ended_by_spec"), stopped(false))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
		if f.stops() != 0 {
			t.Fatal("OnStop ran")
		}
	})
	// A long deadline does not turn a Stop during Start into a timeout.
	t.Run("stop during start", func(t *testing.T) {
		entered := make(chan struct{})
		f := &script{startFn: func(ctx context.Context, _ []byte) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initMsg(contract))
		p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
		p.send(startDeadline(30_000))
		await(t, entered)
		p.send(stopGrace("deps_failed", 5000))
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "deps_failed"), stopped(true))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
		if f.stops() != 1 {
			t.Fatalf("OnStop calls %d", f.stops())
		}
	})
}

// SIGTERM is the caller's context, the way a worker main builds it (E08-D006). Serve does not watch
// the signal. With no Stop carrying a grace, OnStop is not given a deadline.
func TestSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	open := make(chan struct{})
	f := &script{stopFn: func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); ok || ctx.Err() != nil {
			return errors.New("bare SIGTERM limited OnStop")
		}
		close(open)
		return nil
	}}
	p := run(t, ctx, Config{}, f)
	reachRunning(t, p)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	await(t, open)
	p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "signal"), stopped(true))
	if c := p.exit(); c != ExitDone {
		t.Fatalf("exit %d", c)
	}
}

// Run returning takes the plane's hook budget. The budget is read after Init returns. Past it,
// Serve does not wait and does not send Stopped; the exit code is 21 and Exiting is unchanged.
// Stop and SIGTERM keep the grace they already had. A plane that reports 0 is not limited.
func TestOnStopHookTimeout(t *testing.T) {
	t.Run("blocks", func(t *testing.T) {
		entered := make(chan struct{})
		hold := make(chan struct{})
		returned := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(hold) }) })
		f := &script{runFn: func(context.Context) error { return nil }}
		f.initFn = func(ctx context.Context, sess *Session, payload []byte) error {
			if err := f.fake.Init(ctx, sess, payload); err != nil {
				return err
			}
			f.hook = 50 * time.Millisecond
			return nil
		}
		f.stopFn = func(context.Context) error {
			close(entered)
			<-hold
			close(returned)
			return nil
		}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		await(t, entered)
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, ""))
		if c := p.exit(); c != ExitHung {
			t.Fatalf("exit %d", c)
		}
		select {
		case <-returned:
			t.Fatal("OnStop was waited for")
		default:
		}
		p.closedWithoutStopped()
	})
	t.Run("returns", func(t *testing.T) {
		f := &script{
			hook:  50 * time.Millisecond,
			runFn: func(context.Context) error { return nil },
			stopFn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, ""))
		if c := p.exit(); c != ExitHung {
			t.Fatalf("exit %d", c)
		}
		p.closedWithoutStopped()
	})
	t.Run("within budget", func(t *testing.T) {
		f := &script{
			hook:  5 * time.Second,
			runFn: func(context.Context) error { return nil },
		}
		var deadline time.Time
		var has bool
		f.stopFn = func(ctx context.Context) error {
			deadline, has = ctx.Deadline()
			return nil
		}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, ""), stopped(true))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
		if !has {
			t.Fatal("OnStop had no deadline")
		}
		if remain := time.Until(deadline); remain < 3*time.Second || remain > 5*time.Second {
			t.Fatalf("deadline in %s", remain)
		}
	})
	t.Run("stop keeps grace", func(t *testing.T) {
		f := &script{hook: 50 * time.Millisecond}
		var deadline time.Time
		var has bool
		f.stopFn = func(ctx context.Context) error {
			deadline, has = ctx.Deadline()
			return nil
		}
		p := run(t, t.Context(), Config{}, f)
		reachRunning(t, p)
		p.send(stopGrace("bye", 5000))
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "bye"), stopped(true))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
		if !has {
			t.Fatal("OnStop had no deadline")
		}
		if remain := time.Until(deadline); remain < 3*time.Second {
			t.Fatalf("Stop used the hook budget, deadline in %s", remain)
		}
	})
	t.Run("start failure", func(t *testing.T) {
		f := &script{
			hook: time.Second,
			startFn: func(context.Context, []byte) error {
				return errors.New("no feed")
			},
			stopFn: func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); ok || ctx.Err() != nil {
					return errors.New("start failure was limited")
				}
				return nil
			},
		}
		p := run(t, t.Context(), Config{}, f)
		p.expect(hello)
		p.send(initMsg(contract))
		p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
		p.send(startMsg())
		p.expect(doneErr(workerv1.Verb_VERB_START, "no feed"), exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "no feed"), stopped(true))
		if c := p.exit(); c != ExitInit {
			t.Fatalf("exit %d", c)
		}
	})
	t.Run("signal", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("SIGTERM")
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		open := make(chan struct{})
		f := &script{
			hook: 50 * time.Millisecond,
			stopFn: func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); ok || ctx.Err() != nil {
					return errors.New("SIGTERM used the hook budget")
				}
				close(open)
				return nil
			},
		}
		p := run(t, ctx, Config{}, f)
		reachRunning(t, p)
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		await(t, open)
		p.expect(exiting(commonv1.Outcome_OUTCOME_DONE, "signal"), stopped(true))
		if c := p.exit(); c != ExitDone {
			t.Fatalf("exit %d", c)
		}
	})
}

func TestHelloListsContracts(t *testing.T) {
	want := []string{"worker.v1", "stsctl.v1", "md.v1", "td.v1"}
	p := run(t, t.Context(), Config{Contracts: append([]string(nil), want...)}, &fake{})
	h := p.next().GetHello()
	got := h.GetContracts()
	if h == nil || h.GetKind() != "test" || len(got) != len(want) {
		t.Fatalf("hello %v", h)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("contracts %v", got)
		}
	}
}

func TestRunExit(t *testing.T) {
	for name, c := range map[string]struct {
		x      *Exit
		code   int
		onStop bool
	}{
		"failed": {&Exit{commonv1.Outcome_OUTCOME_FAILED, "agent asked"}, ExitFailed, true},
		"hung":   {&Exit{commonv1.Outcome_OUTCOME_HUNG, "on_order"}, ExitHung, false},
		"other":  {&Exit{commonv1.Outcome_OUTCOME_UNSPECIFIED, "?"}, ExitCrash, true},
	} {
		t.Run(name, func(t *testing.T) {
			p := run(t, t.Context(), Config{}, &fake{runReturns: true, runErr: fmt.Errorf("hook: %w", c.x)})
			p.expect(hello)
			p.send(initMsg(contract))
			p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
			p.send(startMsg())
			want := c.x.Outcome
			if name == "other" {
				want = commonv1.Outcome_OUTCOME_CRASHED
			}
			p.expect(doneOf(workerv1.Verb_VERB_START, true), stage, exiting(want, c.x.Reason), stopped(c.onStop))
			if got := p.exit(); got != c.code {
				t.Fatalf("exit %d", got)
			}
		})
	}
}

func TestStartBeforeInit(t *testing.T) {
	f := &fake{}
	p := run(t, t.Context(), Config{}, f)
	p.expect(hello)
	p.send(startMsg())
	p.expect(
		doneOf(workerv1.Verb_VERB_START, false),
		exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, errStartBeforeInit.Error()),
		stopped(false),
	)
	if c := p.exit(); c != ExitInit {
		t.Fatalf("exit %d", c)
	}
	f.mu.Lock()
	started := f.started
	f.mu.Unlock()
	if started {
		t.Fatal("Start ran")
	}
}

func TestSecondInit(t *testing.T) {
	p := run(t, t.Context(), Config{}, &fake{})
	p.expect(hello)
	p.send(initMsg(contract))
	p.expect(doneOf(workerv1.Verb_VERB_INIT, true))
	p.send(initMsg(contract))
	p.expect(
		doneOf(workerv1.Verb_VERB_INIT, false),
		exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, errInitAgain.Error()),
		stopped(false),
	)
	if c := p.exit(); c != ExitInit {
		t.Fatalf("exit %d", c)
	}
}
