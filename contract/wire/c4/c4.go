// Package c4 is the worker's end of C4, the protocol between the shim and the worker on fd 3
// (PROTOCOL §5). It speaks the part every plane speaks the same way, so that each plane writes only
// what its C5 payloads mean:
//
//   - Hello first, then Beat once a second from a goroutine of its own.
//   - A goroutine of its own reads fd 3, so Stop and Live are taken while Init, Start or Run block.
//   - Live that arrives before Init has succeeded is held and applied before Done(INIT) (D111).
//   - Start before the first Init, and a second Init, end INIT_FAILED without a hook (D837).
//     Live before Init stays held, and Relay before Init stays dropped.
//   - A Relay from the agent (a C5 Command) goes to the plane, when the plane takes them.
//   - Init.deadline_ms and Start.deadline_ms, when not zero, bound that call. Past the deadline
//     Serve does not wait: Done(verb, false), Exiting(INIT_FAILED), Stopped(false), exit 10 (D807).
//   - The ending of §5.3: Exiting(outcome, reason), then OnStop when the plane is a Stopper and
//     §5.3 runs it, then Stopped(on_stop_completed), then the exit code (D809). When Run returns,
//     a plane that implements HookTimeout limits that OnStop; past the limit Serve does not wait
//     and does not send Stopped (exit 21, D803).
//
// The plane is a Plane. Its Init, Start and Run take one context, which ends on Stop, on the shim's
// EOF, or when the caller's context ends. That last one is how SIGTERM arrives. Worker mains pass a
// signal.NotifyContext for SIGTERM — mftiko-md-conn, mftiko-td-account, and the SDK session worker —
// and Serve does not watch signals itself (D806). Cancelling that context is a Stop.
//
// It is in contract so that every worker can use it: the MD conn worker, the TD account worker, and
// the Go SDK's session worker, which depends only on contract (D802). Like the rest of contract it
// depends only on the standard library and protobuf, so logging is a function the worker supplies.
// Relay up to the agent, Controller and Undeliverable are not here: stsctl has no worker-to-agent
// request (D903).
package c4

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// Exit codes (PROTOCOL §5.3).
const (
	ExitDone   = 0
	ExitOther  = 1 // the control connection could not be spoken
	ExitInit   = 10
	ExitCrash  = 20
	ExitHung   = 21
	ExitFailed = 30
)

// BeatEvery is how often Beat goes out (PROTOCOL §5.2).
const BeatEvery = time.Second

// Exit is how Run asks for an ending other than DONE or CRASHED: FAILED (exit 30) when the worker
// was told to fail, HUNG (exit 21) when a hook overran its budget. Return it, or an error wrapping
// it, from Run.
type Exit struct {
	Outcome commonv1.Outcome
	Reason  string
}

func (e *Exit) Error() string { return "c4: exit " + e.Outcome.String() + ": " + e.Reason }

// Relayer is a Plane that takes the agent's C5 Commands. Each arrives as a Relay (PROTOCOL §4.2);
// Relay is called on the goroutine that reads fd 3, in the order they arrived, so it must not
// block. A Plane that is not a Relayer drops them.
type Relayer interface {
	Relay(corrID, payload []byte)
}

// Stopper is a Plane that cleans up between Exiting and Stopped (PROTOCOL §5.2, D809). OnStop runs
// on Serve's goroutine, except when Run returned and HookTimeout limited it: that call is on its
// own goroutine so a timeout can return without waiting. Worker mains exit with os.Exit, so the
// leftover call does not keep the process up. A Plane that is not a Stopper is unchanged: nothing
// is called, and Stopped.on_stop_completed stays what §5.3 already records.
//
// OnStop runs only where §5.3 runs OnStop, narrowed by D807 and D808. It does not run after HUNG,
// after Init failed, or after Start timed out. A Start that fails for any other reason does run
// it: Init succeeded, and that work needs cleaning up.
//
// The context is not the one Serve cancelled to end Init, Start or Run, so OnStop can still run
// after that cancellation. Its limit:
//
//   - A Stop was read. The deadline is that message's grace_ms added to the time it was read: what
//     remains of the shim's grace. grace_ms 0 is already due. A later Stop does not move it; only
//     the first Stop sets the shim's timer (PROTOCOL §4.3). Serve waits for OnStop. The shim is
//     what ends the process when grace runs out.
//   - The caller's context ended and no Stop was read (SIGTERM on its own, D806). There is no
//     deadline. That signal carries no grace_ms, and c4 does not invent one.
//   - Run returned, and no Stop was read. The deadline is the duration the plane handed over after
//     Init, when it implements HookTimeout. Zero, or a plane that does not, does not limit OnStop:
//     MD and TD are that case. Past the deadline Serve does not wait. Exiting has already been
//     sent and is not changed; Stopped is not sent; the exit code is 21 (D803).
//   - Start failed for a reason other than its deadline. There is no deadline. That ending is not
//     Run returning.
//
// A nil error is Stopped(true). Any other error is logged and is Stopped(false). Exiting has
// already been sent, so the outcome and the exit code stay. A hook-timeout overrun is the
// exception: there is no Stopped, and the exit code is 21.
type Stopper interface {
	OnStop(ctx context.Context) error
}

// HookTimeout is a Plane that limits OnStop when Run returns (D809). Serve calls it once, after
// Init has returned success, and keeps that duration. The duration is the plane's own reading of
// hook_timeout_s — for STS, stsctl.InitPayload.hook_timeout_s as a time.Duration. c4 does not parse
// the C5 payload. A Plane that is not a HookTimeout, or that returns zero, does not limit that
// OnStop, which is how MD and TD behave. Stop and SIGTERM do not use this duration; their limit
// stays the remaining grace_ms.
type HookTimeout interface {
	HookTimeout() time.Duration
}

// Plane is what one plane's worker does with its C5 payloads.
type Plane interface {
	// Init reads Init.payload, whose contract Serve has checked. An error fails Init: Done(INIT,
	// false), Exiting(INIT_FAILED), Stopped(false), exit 10. Its text is the reason, so it must not
	// carry secrets. Init.deadline_ms above zero that expires first is the same ending, and Init is
	// not waited for (D807). OnStop does not run.
	Init(ctx context.Context, s *Session, payload []byte) error
	// Start runs before Done(START). An error fails Start: Done(START, false), Exiting(INIT_FAILED),
	// Stopped(true), exit 10, and OnStop runs when the plane is a Stopper (D808). Start.deadline_ms
	// above zero that expires first is the same frames with Stopped(false), and OnStop does not run;
	// Start is not waited for. A Start that ends because ctx ended is a stop, not a failure.
	Start(ctx context.Context, payload []byte) error
	// Run runs after Stage(running) until ctx ends. Returning nil, or returning after ctx ended, is
	// DONE (exit 0); any other error is CRASHED with its text as the reason (exit 20).
	Run(ctx context.Context) error
	// Live applies one Live.payload. It is called only once Init has succeeded, never while another
	// Live is being applied, and may run while Start or Run does. nil sends Applied(digest); an error
	// is logged and nothing is sent, so the agent sends it again (D111).
	Live(payload []byte) error
}

// Config is the process around the Plane.
type Config struct {
	Kind     string // Hello.kind
	Contract string // the C5 contract; Init, Start and Live must name it
	// Contracts is Hello.contracts. Nil or empty means []string{Contract}, which is
	// what a plane that speaks one C5 contract already sent. A session worker lists
	// worker.v1, stsctl.v1, md.v1, and td.v1 (PROTOCOL §9.2).
	Contracts []string
	Release   string // Hello.release
	// InitCarriesLive: Init.payload is the first live config, and Init.live_digest is its digest,
	// sent as Applied before Done(INIT) (D111). False when live config only arrives as Live (MD).
	InitCarriesLive bool
	// Log receives what the worker would log: an init or start that failed, a Live that was not
	// applied. Nil discards.
	Log func(msg string, err error)
}

// Session is what a Plane sends on its own. Safe for concurrent use.
type Session struct {
	contract string
	out      *frame.Writer
}

// Report sends one Report whose payload is m in the plane's contract. It returns once the frame
// has been handed to fd 3.
func (s *Session) Report(m proto.Message) error {
	payload, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("c4: report: %w", err)
	}
	return s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Report{Report: &workerv1.Report{
		Contract: s.contract, Payload: payload,
	}}})
}

func (s *Session) send(m *workerv1.WorkerUp) error {
	if err := s.out.Write(m); err != nil {
		return fmt.Errorf("c4: %w", err)
	}
	return nil
}

// Serve speaks C4 on rw, the worker's end of fd 3, until the plane ends, Stop, the shim's EOF, or
// ctx. It closes rw when it returns, and returns the exit code.
func Serve(ctx context.Context, rw io.ReadWriteCloser, cfg Config, p Plane) int {
	if ctx == nil {
		ctx = context.Background()
	}
	log := cfg.Log
	if log == nil {
		log = func(string, error) {}
	}
	life, cancel := context.WithCancel(ctx)
	s := &ctl{
		cfg:      cfg,
		plane:    p,
		log:      log,
		sess:     &Session{contract: cfg.Contract, out: frame.NewWriter(rw)},
		cancel:   cancel,
		incoming: make(chan *workerv1.WorkerDown, 8),
		stopped:  make(chan struct{}),
	}
	defer func() {
		s.halt("exit")
		rw.Close()
	}()
	go s.read(rw)

	contracts := cfg.Contracts
	if len(contracts) == 0 {
		contracts = []string{cfg.Contract}
	}
	if err := s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Hello{Hello: &workerv1.Hello{
		Kind: cfg.Kind, Release: cfg.Release, Contracts: contracts, Pid: int32(os.Getpid()),
	}}}); err != nil {
		return ExitOther
	}
	go s.beat(life)

	initMsg, err := s.waitInit(life)
	if errors.Is(err, errStartBeforeInit) {
		// Init never ran, so OnStop does not (D837, D808).
		return s.failPhase(workerv1.Verb_VERB_START, errStartBeforeInit.Error(), false)
	}
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, ExitDone, false)
	}
	if code, ok := s.init(life, initMsg.GetInit()); !ok {
		return code
	}
	startMsg, err := s.waitStart(life)
	if errors.Is(err, errInitAgain) {
		return s.failPhase(workerv1.Verb_VERB_INIT, errInitAgain.Error(), false)
	}
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, ExitDone, false)
	}
	if code, ok := s.start(life, startMsg.GetStart()); !ok {
		return code
	}

	err = p.Run(life)
	if x := (*Exit)(nil); errors.As(err, &x) {
		return s.exit(x)
	}
	// Canceled is halt ending life, which happens before stopped is closed; it is the stop, not a
	// failure of the plane.
	if s.halted() || ctx.Err() != nil || life.Err() != nil || errors.Is(err, context.Canceled) {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, ExitDone, false)
	}
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_CRASHED, err.Error(), true, ExitCrash, true)
	}
	return s.finish(commonv1.Outcome_OUTCOME_DONE, "", true, ExitDone, true)
}

// ctl is one C4 session.
type ctl struct {
	cfg   Config
	plane Plane
	log   func(msg string, err error)
	sess  *Session

	cancel   context.CancelFunc
	incoming chan *workerv1.WorkerDown
	stopped  chan struct{}
	stopOnce sync.Once

	mu          sync.Mutex
	inited      bool
	queued      []*workerv1.Live
	reason      string
	stopping    bool          // set before cancel, so halted() is true when a hook wakes (D806)
	hadStop     bool          // a Stop was read; its grace bounds OnStop (D809)
	stopAt      time.Time     // when that Stop was read
	grace       time.Duration // its grace_ms
	hookTimeout time.Duration // OnStop budget when Run returns; 0 means none (D809)

	liveMu sync.Mutex // one Live applied at a time
}

func (s *ctl) read(rw io.Reader) {
	r := frame.NewReader(bufio.NewReader(rw))
	for {
		m := &workerv1.WorkerDown{}
		if err := r.Read(m); err != nil {
			s.halt("shim closed")
			return
		}
		switch msg := m.Msg.(type) {
		case *workerv1.WorkerDown_Live:
			s.live(msg.Live)
		case *workerv1.WorkerDown_Stop:
			s.noteStop(msg.Stop)
			s.halt(msg.Stop.GetReason())
		case *workerv1.WorkerDown_Relay:
			s.relay(msg.Relay)
		case *workerv1.WorkerDown_Init, *workerv1.WorkerDown_Start:
			select {
			case s.incoming <- m:
			case <-s.stopped:
				return
			}
		}
	}
}

func (s *ctl) beat(ctx context.Context) {
	t := time.NewTicker(BeatEvery)
	defer t.Stop()
	var seq uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopped:
			return
		case <-t.C:
			seq++
			if err := s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Beat{Beat: &workerv1.Beat{Seq: seq}}}); err != nil {
				return
			}
		}
	}
}

// waitInit blocks until the first Init. A Start in that gap is INIT_FAILED (D837).
// Anything else that shares the incoming channel is not a Start or an Init; Live and
// Relay do not use this channel.
func (s *ctl) waitInit(ctx context.Context) (*workerv1.WorkerDown, error) {
	for {
		select {
		case m := <-s.incoming:
			if m.GetInit() != nil {
				return m, nil
			}
			if m.GetStart() != nil {
				return nil, errStartBeforeInit
			}
		case <-s.stopped:
			return nil, errHalted
		case <-ctx.Done():
			s.halt("signal")
			return nil, ctx.Err()
		}
	}
}

// waitStart blocks until Start. A second Init is INIT_FAILED (D837).
func (s *ctl) waitStart(ctx context.Context) (*workerv1.WorkerDown, error) {
	for {
		select {
		case m := <-s.incoming:
			if m.GetStart() != nil {
				return m, nil
			}
			if m.GetInit() != nil {
				return nil, errInitAgain
			}
		case <-s.stopped:
			return nil, errHalted
		case <-ctx.Done():
			s.halt("signal")
			return nil, ctx.Err()
		}
	}
}

func (s *ctl) init(ctx context.Context, m *workerv1.Init) (code int, ok bool) {
	if m.GetContract() != s.cfg.Contract {
		return s.failPhase(workerv1.Verb_VERB_INIT, "contract "+m.GetContract(), false), false
	}
	err, timedOut := runLimited(ctx, m.GetDeadlineMs(), func(ctx context.Context) error {
		return s.plane.Init(ctx, s.sess, m.GetPayload())
	})
	if timedOut {
		return s.failPhase(workerv1.Verb_VERB_INIT, errDeadline.Error(), false), false
	}
	if err != nil {
		// cancel() runs before stopped is closed, so a Stop that woke Init may not be visible
		// through halted() yet. The context ending is the same stop (D806).
		if s.halted() || ctx.Err() != nil {
			return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, ExitDone, false), false
		}
		return s.failPhase(workerv1.Verb_VERB_INIT, err.Error(), false), false
	}
	s.noteHookTimeout()
	if s.cfg.InitCarriesLive {
		if err := s.applied(m.GetLiveDigest()); err != nil {
			return ExitOther, false
		}
	}
	s.mu.Lock()
	s.inited = true
	q := s.queued
	s.queued = nil
	s.mu.Unlock()
	for _, l := range q {
		s.apply(l)
	}
	if err := s.sess.send(done(workerv1.Verb_VERB_INIT, true, "")); err != nil {
		return ExitOther, false
	}
	return 0, true
}

func (s *ctl) start(ctx context.Context, m *workerv1.Start) (code int, ok bool) {
	if m.GetContract() != "" && m.GetContract() != s.cfg.Contract {
		return s.failPhase(workerv1.Verb_VERB_START, "contract "+m.GetContract(), true), false
	}
	err, timedOut := runLimited(ctx, m.GetDeadlineMs(), func(ctx context.Context) error {
		return s.plane.Start(ctx, m.GetPayload())
	})
	if timedOut {
		// The hook is not waited for, so it may still be inside Start. OnStop does not run (D808).
		return s.failPhase(workerv1.Verb_VERB_START, errDeadline.Error(), false), false
	}
	if err != nil {
		if s.halted() || ctx.Err() != nil {
			return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, ExitDone, false), false
		}
		return s.failPhase(workerv1.Verb_VERB_START, err.Error(), true), false
	}
	if s.halted() {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, ExitDone, false), false
	}
	if err := s.sess.send(done(workerv1.Verb_VERB_START, true, "")); err != nil {
		return ExitOther, false
	}
	if err := s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: "running"}}}); err != nil {
		return ExitOther, false
	}
	return 0, true
}

// failPhase is an Init or a Start the worker will not run: Done, then the ending. onStop is false
// for Init and for a deadline (D807, D808), and true for any other Start failure.
func (s *ctl) failPhase(verb workerv1.Verb, why string, onStop bool) int {
	if verb == workerv1.Verb_VERB_INIT {
		s.log("init failed", errors.New(why))
	} else {
		s.log("start failed", errors.New(why))
	}
	_ = s.sess.send(done(verb, false, why))
	return s.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, why, onStop, ExitInit, false)
}

// live applies one Live, or holds it until Init has succeeded.
func (s *ctl) live(m *workerv1.Live) {
	if m == nil || (m.GetContract() != "" && m.GetContract() != s.cfg.Contract) {
		return
	}
	s.mu.Lock()
	if !s.inited {
		s.queued = append(s.queued, m)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.apply(m)
}

func (s *ctl) apply(m *workerv1.Live) {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if err := s.plane.Live(m.GetPayload()); err != nil {
		s.log("live not applied", err)
		return
	}
	_ = s.applied(m.GetDigest())
}

// relay hands a C5 Command to the plane. One in another contract is dropped, as is every one that
// arrives before Init has succeeded: Commands are full snapshots (PROTOCOL §6), and the agent sends
// the next one after Init.
func (s *ctl) relay(m *workerv1.Relay) {
	r, ok := s.plane.(Relayer)
	if !ok || m == nil || (m.GetContract() != "" && m.GetContract() != s.cfg.Contract) {
		return
	}
	s.mu.Lock()
	inited := s.inited
	s.mu.Unlock()
	if !inited {
		return
	}
	r.Relay(m.GetCorrId(), m.GetPayload())
}

// exit ends the worker as Run asked. HUNG skips OnStop (§5.3). The others are Run returning, so
// OnStop takes the plane's hook budget when one was handed over (D809). A Stop already read still
// wins: its remaining grace is the deadline, and Serve waits.
func (s *ctl) exit(x *Exit) int {
	switch x.Outcome {
	case commonv1.Outcome_OUTCOME_FAILED:
		return s.finish(x.Outcome, x.Reason, true, ExitFailed, true)
	case commonv1.Outcome_OUTCOME_HUNG:
		return s.finish(x.Outcome, x.Reason, false, ExitHung, false)
	case commonv1.Outcome_OUTCOME_INIT_FAILED:
		return s.finish(x.Outcome, x.Reason, true, ExitInit, true)
	case commonv1.Outcome_OUTCOME_DONE:
		return s.finish(x.Outcome, x.Reason, true, ExitDone, true)
	default:
		return s.finish(commonv1.Outcome_OUTCOME_CRASHED, x.Reason, true, ExitCrash, true)
	}
}

func (s *ctl) applied(digest string) error {
	return s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Applied{Applied: &workerv1.Applied{Digest: digest}}})
}

func (s *ctl) halt(reason string) {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		if s.reason == "" {
			s.reason = reason
		}
		// Visible to halted() before cancel returns. stopped is closed after cancel, and a hook
		// blocked on ctx.Done() can run in between.
		s.stopping = true
		s.mu.Unlock()
		s.cancel()
		close(s.stopped)
	})
}

func (s *ctl) halted() bool {
	s.mu.Lock()
	stopping := s.stopping
	s.mu.Unlock()
	if stopping {
		return true
	}
	select {
	case <-s.stopped:
		return true
	default:
		return false
	}
}

func (s *ctl) reasonOr(fallback string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reason != "" {
		return s.reason
	}
	return fallback
}

// finish sends Exiting, then OnStop when onStop is set and the plane is a Stopper. self is true
// when Run returned this ending. That is the only ending whose OnStop is limited by HookTimeout.
func (s *ctl) finish(outcome commonv1.Outcome, reason string, onStop bool, code int, self bool) int {
	_ = s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{
		Outcome: outcome, Reason: reason,
	}}})
	completed := onStop
	if onStop {
		if stop, ok := s.plane.(Stopper); ok {
			ctx, cancel, abandon := s.stopLimit(self)
			err, hung := awaitStop(ctx, abandon, stop)
			cancel()
			if hung {
				// Exiting already names the outcome Run chose. Do not send Stopped (D803).
				return ExitHung
			}
			if err != nil {
				s.log("on stop", err)
				completed = false
			}
		}
	}
	_ = s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stopped{Stopped: &workerv1.Stopped{
		OnStopCompleted: completed,
	}}})
	return code
}

// noteHookTimeout records the plane's self-end OnStop budget after Init has succeeded. c4 does not
// read the payload that number came from (D809).
func (s *ctl) noteHookTimeout() {
	h, ok := s.plane.(HookTimeout)
	if !ok {
		return
	}
	d := h.HookTimeout()
	if d <= 0 {
		return
	}
	s.mu.Lock()
	s.hookTimeout = d
	s.mu.Unlock()
}

// noteStop records the first Stop. Its grace is the one OnStop is given the rest of (D809); a later
// Stop does not replace it, matching the shim, which lets only the first Stop set the timer.
func (s *ctl) noteStop(m *workerv1.Stop) {
	if m == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hadStop {
		return
	}
	s.hadStop = true
	s.stopAt = time.Now()
	s.grace = time.Duration(m.GetGraceMs()) * time.Millisecond
}

// stopLimit is the context OnStop runs with, and whether a timeout may return without it.
// A Stop's remaining grace always wins, and that call is waited for. See Stopper.
func (s *ctl) stopLimit(self bool) (context.Context, context.CancelFunc, bool) {
	s.mu.Lock()
	had, at, grace, hook := s.hadStop, s.stopAt, s.grace, s.hookTimeout
	s.mu.Unlock()
	if had {
		ctx, cancel := context.WithDeadline(context.Background(), at.Add(grace))
		return ctx, cancel, false
	}
	if self && hook > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), hook)
		return ctx, cancel, true
	}
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, false
}

// awaitStop runs OnStop. abandon is the hook budget: a timeout does not wait, hung is true, and
// the caller exits 21 without Stopped (D803). A Stop's grace still waits; the shim is what kills
// the process.
func awaitStop(ctx context.Context, abandon bool, stop Stopper) (err error, hung bool) {
	if !abandon {
		return stop.OnStop(ctx), false
	}
	errc := make(chan error, 1)
	go func() { errc <- stop.OnStop(ctx) }()
	select {
	case err = <-errc:
		return err, hookHung(ctx, err)
	case <-ctx.Done():
	}
	select {
	case err = <-errc:
		if err == nil {
			return nil, false
		}
		return err, hookHung(ctx, err)
	default:
		return context.DeadlineExceeded, true
	}
}

// hookHung reports that OnStop ended because the hook budget elapsed. A nil error finished in time.
// Any other error is that error: the hook returned, and Stopped still records it.
func hookHung(ctx context.Context, err error) bool {
	return err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && errors.Is(err, context.DeadlineExceeded)
}

// runLimited runs fn. ms is Init.deadline_ms or Start.deadline_ms: zero means no limit (D807).
// timedOut is true when that deadline expired and this call is not keeping fn's result. A nil
// result is success even if the deadline has also expired. The parent context ending is a stop,
// not a deadline, and fn is waited for, as a Stop during Start is today. On timeout fn is left
// running: the process exits with code 10, and waiting would turn the overrun into §9.7's HUNG.
func runLimited(ctx context.Context, ms uint32, fn func(context.Context) error) (err error, timedOut bool) {
	if ms == 0 {
		return fn(ctx), false
	}
	cctx, cancel := context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- fn(cctx) }()
	select {
	case err = <-errc:
		return err, timeoutResult(ctx, cctx, err)
	case <-cctx.Done():
	}
	select {
	case err = <-errc:
		return err, timeoutResult(ctx, cctx, err)
	default:
	}
	if ctx.Err() != nil {
		return <-errc, false
	}
	return errDeadline, true
}

// timeoutResult reports whether a finished fn failed because its own deadline elapsed. The parent
// ending is a stop. A nil error finished in time. Any other error is that error, even if the
// deadline has also elapsed: the hook returned, and a Start failure still cleans up (D808).
func timeoutResult(parent, limited context.Context, err error) bool {
	if err == nil || parent.Err() != nil {
		return false
	}
	return errors.Is(limited.Err(), context.DeadlineExceeded) && errors.Is(err, context.DeadlineExceeded)
}

func done(verb workerv1.Verb, ok bool, errText string) *workerv1.WorkerUp {
	return &workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: verb, Ok: ok, Error: errText}}}
}

var (
	errHalted          = errors.New("c4: control connection closed")
	errDeadline        = errors.New("deadline")
	errStartBeforeInit = errors.New("c4: start before init")
	errInitAgain       = errors.New("c4: init after init")
)
