// Package c4 is the worker's end of C4, the protocol between the shim and the worker on fd 3
// (PROTOCOL §5). It speaks the part every plane speaks the same way, so that each plane writes only
// what its C5 payloads mean:
//
//   - Hello first, then Beat once a second from a goroutine of its own.
//   - A goroutine of its own reads fd 3, so Stop and Live are taken while Init, Start or Run block.
//   - Live that arrives before Init has succeeded is held and applied before Done(INIT) (D111).
//   - A Relay from the agent (a C5 Command) goes to the plane, when the plane takes them.
//   - The ending of §5.3: Done, then Exiting(outcome, reason), then Stopped(on_stop_completed),
//     then the exit code.
//
// The plane is a Plane. Its Init, Start and Run take one context, which ends on Stop, on the shim's
// EOF, or when the caller's context ends.
//
// It is in contract so that every worker can use it: the MD conn worker, the TD account worker, and
// the Go SDK's session worker, which depends only on contract (D802). Like the rest of contract it
// depends only on the standard library and protobuf, so logging is a function the worker supplies.
// What only the strategy plane needs (OnStop, Relay to the agent, Controller and Undeliverable) is
// added with the SDK (E8).
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

// Plane is what one plane's worker does with its C5 payloads.
type Plane interface {
	// Init reads Init.payload, whose contract Serve has checked. An error fails Init: Done(INIT,
	// false), Exiting(INIT_FAILED), Stopped(false), exit 10. Its text is the reason, so it must not
	// carry secrets.
	Init(ctx context.Context, s *Session, payload []byte) error
	// Start runs before Done(START). An error fails Start: Done(START, false), Exiting(INIT_FAILED),
	// Stopped(true), exit 10. A Start that ends because ctx ended is a stop, not a failure.
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
	Release  string // Hello.release
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

	if err := s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Hello{Hello: &workerv1.Hello{
		Kind: cfg.Kind, Release: cfg.Release, Contracts: []string{cfg.Contract}, Pid: int32(os.Getpid()),
	}}}); err != nil {
		return ExitOther
	}
	go s.beat(life)

	initMsg, err := s.wait(life, func(m *workerv1.WorkerDown) bool { return m.GetInit() != nil })
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, ExitDone)
	}
	if code, ok := s.init(life, initMsg.GetInit()); !ok {
		return code
	}
	startMsg, err := s.wait(life, func(m *workerv1.WorkerDown) bool { return m.GetStart() != nil })
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, ExitDone)
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
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, ExitDone)
	}
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_CRASHED, err.Error(), true, ExitCrash)
	}
	return s.finish(commonv1.Outcome_OUTCOME_DONE, "", true, ExitDone)
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

	mu     sync.Mutex
	inited bool
	queued []*workerv1.Live
	reason string

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

func (s *ctl) wait(ctx context.Context, want func(*workerv1.WorkerDown) bool) (*workerv1.WorkerDown, error) {
	for {
		select {
		case m := <-s.incoming:
			if want(m) {
				return m, nil
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
		return s.failInit("contract " + m.GetContract()), false
	}
	if err := s.plane.Init(ctx, s.sess, m.GetPayload()); err != nil {
		if s.halted() {
			return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, ExitDone), false
		}
		return s.failInit(err.Error()), false
	}
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
		return s.failStart("contract " + m.GetContract()), false
	}
	if err := s.plane.Start(ctx, m.GetPayload()); err != nil {
		if s.halted() || ctx.Err() != nil {
			return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, ExitDone), false
		}
		return s.failStart(err.Error()), false
	}
	if s.halted() {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, ExitDone), false
	}
	if err := s.sess.send(done(workerv1.Verb_VERB_START, true, "")); err != nil {
		return ExitOther, false
	}
	if err := s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: "running"}}}); err != nil {
		return ExitOther, false
	}
	return 0, true
}

// failInit is an Init the worker will not run: Done, then Exiting, and OnStop does not run.
func (s *ctl) failInit(why string) int {
	s.log("init failed", errors.New(why))
	_ = s.sess.send(done(workerv1.Verb_VERB_INIT, false, why))
	return s.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, why, false, ExitInit)
}

// failStart is a Start that failed: OnStop runs (§5.3).
func (s *ctl) failStart(why string) int {
	s.log("start failed", errors.New(why))
	_ = s.sess.send(done(workerv1.Verb_VERB_START, false, why))
	return s.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, why, true, ExitInit)
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

// exit ends the worker as Run asked. HUNG skips OnStop (§5.3).
func (s *ctl) exit(x *Exit) int {
	switch x.Outcome {
	case commonv1.Outcome_OUTCOME_FAILED:
		return s.finish(x.Outcome, x.Reason, true, ExitFailed)
	case commonv1.Outcome_OUTCOME_HUNG:
		return s.finish(x.Outcome, x.Reason, false, ExitHung)
	case commonv1.Outcome_OUTCOME_INIT_FAILED:
		return s.finish(x.Outcome, x.Reason, true, ExitInit)
	case commonv1.Outcome_OUTCOME_DONE:
		return s.finish(x.Outcome, x.Reason, true, ExitDone)
	default:
		return s.finish(commonv1.Outcome_OUTCOME_CRASHED, x.Reason, true, ExitCrash)
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
		s.mu.Unlock()
		s.cancel()
		close(s.stopped)
	})
}

func (s *ctl) halted() bool {
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

func (s *ctl) finish(outcome commonv1.Outcome, reason string, onStop bool, code int) int {
	_ = s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{
		Outcome: outcome, Reason: reason,
	}}})
	_ = s.sess.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stopped{Stopped: &workerv1.Stopped{
		OnStopCompleted: onStop,
	}}})
	return code
}

func done(verb workerv1.Verb, ok bool, errText string) *workerv1.WorkerUp {
	return &workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: verb, Ok: ok, Error: errText}}}
}

var errHalted = errors.New("c4: control connection closed")
