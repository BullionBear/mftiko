package conn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/exchange"
)

// Contract is the C5 contract this worker speaks (PROTOCOL §5.2, §6.2).
const Contract = "mdctl.v1"

const (
	exitDone  = 0
	exitOther = 1
	exitInit  = 10 // PROTOCOL §5.3, INIT_FAILED
	exitCrash = 20

	beatEvery = time.Second
)

// ServeConfig is the process around one C4 connection. Lookup and Publish come from the
// binary; tests pass fakes. The venue, route, url and starting epoch come from Init.
type ServeConfig struct {
	// Lookup returns the venue's public market. The bool is false when the venue is unknown.
	Lookup func(venue string) (exchange.Market, bool)
	// Publish sends one md.f message. It runs on the websocket read, so it must return promptly.
	Publish func(subject string, msg []byte) error
	// Now is the clock written into md_ts_ns. Nil means time.Now.
	Now func() time.Time
	// RetryAfter and RetryMax are Conn's dial backoff. Zero means 1s growing to 30s.
	RetryAfter time.Duration
	RetryMax   time.Duration
	// Log is optional. Nil discards.
	Log *zerolog.Logger
	// Release is Hello.release. Empty leaves it empty.
	Release string
}

// Serve speaks C4 on rw, which is the worker's end of fd 3, and runs one connection until Stop,
// EOF, or ctx (PROTOCOL §5). It closes rw when it returns.
//
// Hello goes out first, then Beat once a second on its own goroutine. Init.payload is
// mdctl.InitPayload; its conn_epoch is the last number reported, and the dial uses one more.
// That number is written as a Report before Dial, so a peer that reads it before accepting the
// socket has it before the connection is up. Live.payload is an mdctl.Desired. Applying it
// answers Applied with the digest the sender gave.
//
// The int is an exit code: 0 done, 10 init failed, 20 the connection loop failed, 1 the control
// connection could not be spoken.
func Serve(ctx context.Context, rw io.ReadWriteCloser, cfg ServeConfig) int {
	if ctx == nil {
		ctx = context.Background()
	}
	log := zerolog.Nop()
	if cfg.Log != nil {
		log = *cfg.Log
	}
	s := &ctl{
		rw:       rw,
		out:      frame.NewWriter(rw),
		cfg:      cfg,
		log:      log,
		incoming: make(chan *workerv1.WorkerDown, 8),
		stopped:  make(chan struct{}),
	}
	defer func() {
		s.halt("exit")
		rw.Close()
	}()
	go s.read()

	if err := s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Hello{Hello: &workerv1.Hello{
		Kind: "conn", Release: cfg.Release, Contracts: []string{Contract}, Pid: int32(os.Getpid()),
	}}}); err != nil {
		return exitOther
	}
	go s.beat(ctx)

	initMsg, err := s.wait(ctx, func(m *workerv1.WorkerDown) bool { return m.GetInit() != nil })
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, exitDone)
	}
	if code, ok := s.onInit(initMsg.GetInit()); !ok {
		return code
	}
	startMsg, err := s.wait(ctx, func(m *workerv1.WorkerDown) bool { return m.GetStart() != nil })
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), false, exitDone)
	}
	if code, ok := s.onStart(startMsg.GetStart()); !ok {
		return code
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.runCancel = cancel
	already := s.stopping
	s.mu.Unlock()
	if already {
		cancel()
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, exitDone)
	}
	err = s.worker.Run(runCtx)
	cancel()
	// context.Canceled is halt canceling runCtx. That runs before stopped is closed, so
	// halted() can still be false; the cancellation is the stop, not a failure of the loop.
	if s.halted() || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return s.finish(commonv1.Outcome_OUTCOME_DONE, s.reasonOr("signal"), true, exitDone)
	}
	if err != nil {
		return s.finish(commonv1.Outcome_OUTCOME_CRASHED, err.Error(), true, exitCrash)
	}
	return s.finish(commonv1.Outcome_OUTCOME_DONE, "", true, exitDone)
}

// ctl is one C4 session. The read goroutine handles Live and Stop so a dial, which blocks,
// cannot stop the worker from reading fd 3.
type ctl struct {
	rw  io.ReadWriteCloser
	out *frame.Writer
	cfg ServeConfig
	log zerolog.Logger

	incoming chan *workerv1.WorkerDown
	stopped  chan struct{}
	stopOnce sync.Once

	mu        sync.Mutex
	worker    *Conn
	queued    []queuedLive
	runCancel context.CancelFunc
	stopping  bool
	reason    string
}

type queuedLive struct {
	desired *mdctlv1.Desired
	digest  string
}

func (s *ctl) read() {
	r := frame.NewReader(bufio.NewReader(s.rw))
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
	t := time.NewTicker(beatEvery)
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
			if err := s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Beat{Beat: &workerv1.Beat{Seq: seq}}}); err != nil {
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

func (s *ctl) onInit(m *workerv1.Init) (code int, ok bool) {
	if m.GetContract() != Contract {
		return s.failInit("contract " + m.GetContract()), false
	}
	p := &mdctlv1.InitPayload{}
	if err := proto.Unmarshal(m.GetPayload(), p); err != nil {
		return s.failInit("init payload"), false
	}
	if p.GetVenue() == "" || p.GetRoute() == "" || p.GetMdInstance() == "" {
		return s.failInit("venue, route and md_instance are required"), false
	}
	if s.cfg.Lookup == nil || s.cfg.Publish == nil {
		return s.failInit("lookup and publish are required"), false
	}
	market, found := s.cfg.Lookup(p.GetVenue())
	if !found || market == nil {
		return s.failInit("unknown venue " + p.GetVenue()), false
	}
	s.log.Info().Str("venue", p.GetVenue()).Str("route", p.GetRoute()).Uint32("n", p.GetN()).
		Uint64("conn_epoch", p.GetConnEpoch()).Msg("init")
	c := New(Config{
		Venue: p.GetVenue(), Route: p.GetRoute(), Instance: p.GetMdInstance(), URL: p.GetUrl(),
		Market: market, Publish: s.cfg.Publish, Now: s.cfg.Now, Epoch: p.GetConnEpoch(),
		Report: s.reportEpoch, RetryAfter: s.cfg.RetryAfter, RetryMax: s.cfg.RetryMax, Log: s.cfg.Log,
	})
	s.install(c)
	if err := s.send(done(workerv1.Verb_VERB_INIT, true, "")); err != nil {
		return exitOther, false
	}
	return 0, true
}

func (s *ctl) onStart(m *workerv1.Start) (code int, ok bool) {
	if m.GetContract() != "" && m.GetContract() != Contract {
		_ = s.send(done(workerv1.Verb_VERB_START, false, "contract"))
		return s.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, "contract", true, exitInit), false
	}
	if err := s.send(done(workerv1.Verb_VERB_START, true, "")); err != nil {
		return exitOther, false
	}
	if err := s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: "running"}}}); err != nil {
		return exitOther, false
	}
	return 0, true
}

// failInit is an Init the worker will not run: Done, then Exiting, and OnStop does not run.
func (s *ctl) failInit(why string) int {
	s.log.Info().Str("error", why).Msg("init failed")
	_ = s.send(done(workerv1.Verb_VERB_INIT, false, why))
	return s.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, why, false, exitInit)
}

// reportEpoch writes the number a dial is about to use. Dial is not called until this returns,
// and the write returns only once the frame has been handed to rw.
func (s *ctl) reportEpoch(epoch uint64) error {
	payload, err := proto.Marshal(&mdctlv1.Report{
		ConnEpoch: epoch,
		State:     mdctlv1.ConnState_CONN_STATE_CONNECTING,
	})
	if err != nil {
		return err
	}
	return s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Report{Report: &workerv1.Report{
		Contract: Contract, Payload: payload,
	}}})
}

// install makes c the worker and applies Lives that arrived before it existed.
func (s *ctl) install(c *Conn) {
	s.mu.Lock()
	s.worker = c
	q := s.queued
	s.queued = nil
	s.mu.Unlock()
	for _, p := range q {
		c.SetDesired(p.desired)
		_ = s.applied(p.digest)
	}
}

// live applies one Desired. One that arrives before Init has created the worker is held and
// applied by install, which runs before Done(INIT).
func (s *ctl) live(m *workerv1.Live) {
	if m == nil || (m.GetContract() != "" && m.GetContract() != Contract) {
		return
	}
	d := &mdctlv1.Desired{}
	if err := proto.Unmarshal(m.GetPayload(), d); err != nil {
		s.log.Warn().Err(err).Msg("live")
		return
	}
	s.mu.Lock()
	c := s.worker
	if c == nil {
		s.queued = append(s.queued, queuedLive{d, m.GetDigest()})
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	c.SetDesired(d)
	_ = s.applied(m.GetDigest())
}

func (s *ctl) applied(digest string) error {
	return s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Applied{Applied: &workerv1.Applied{Digest: digest}}})
}

func (s *ctl) halt(reason string) {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		if s.reason == "" {
			s.reason = reason
		}
		cancel := s.runCancel
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
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
	_ = s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{
		Outcome: outcome, Reason: reason,
	}}})
	_ = s.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stopped{Stopped: &workerv1.Stopped{
		OnStopCompleted: onStop,
	}}})
	return code
}

func (s *ctl) send(m *workerv1.WorkerUp) error {
	if err := s.out.Write(m); err != nil {
		return fmt.Errorf("conn: c4: %w", err)
	}
	return nil
}

func done(verb workerv1.Verb, ok bool, errText string) *workerv1.WorkerUp {
	return &workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: verb, Ok: ok, Error: errText}}}
}

var errHalted = errors.New("conn: control connection closed")
