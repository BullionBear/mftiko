package shim

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// proc is the worker process as the view needs it: *Worker, or a fake in tests.
type proc interface {
	// Signal sends sig to the worker's process group; ErrExited once the worker has been reaped.
	Signal(sig syscall.Signal) error
	// Done is closed once the worker has been reaped.
	Done() <-chan struct{}
}

// Errors of the lifecycle requests (PROTOCOL §4.3). The server turns them into codes; ErrExited is the third.
var (
	// ErrRunning is a Release while the worker has not been reaped.
	ErrRunning = errors.New("shim: the worker has not exited")
	// ErrWorkerBusy is a request for the worker while the queue to it is full: it has stopped reading fd 3.
	ErrWorkerBusy = errors.New("shim: the worker is not reading its control channel")
)

// stageError is a lifecycle verb the worker's phase does not allow.
type stageError struct {
	verb  string
	phase shimv1.Phase
}

func (e *stageError) Error() string {
	return fmt.Sprintf("shim: %s in phase %s", e.verb, phaseName(e.phase))
}

// phaseName is the phase without its enum prefix, in lower case: "initialized".
func phaseName(p shimv1.Phase) string {
	const prefix = len("PHASE_")
	n := p.String()
	if len(n) > prefix {
		n = n[prefix:]
	}
	b := []byte(n)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// downQueue is how many messages may wait to be written to the worker. Init, Start and Stop are one each; the rest
// is Relay, which a worker that reads its fd 3 takes as fast as the shim writes.
const downQueue = 256

// view is the worker as the C3 server sees it: what the spawn and the reaper know, what the worker said on C4 that
// Attached reports (PROTOCOL §4.2), and its lifecycle (§4.3): the phase the shim keeps, the messages it passes down,
// and the timers of a Stop.
//
// The timers of a Stop run on the view's own goroutine. They belong to no connection: the agent that asked can go,
// and the SIGTERM and the SIGKILL still come when they are due.
type view struct {
	log       zerolog.Logger
	p         proc
	pid, pgid int
	startTime uint64
	down      chan *workerv1.WorkerDown // to the writer goroutine; never closed

	mu         sync.Mutex
	hello      *workerv1.Hello
	stage      string
	lastBeatNs int64
	phase      shimv1.Phase
	exiting    bool // the worker sent Exiting: it is ending on its own, or on a Stop it already had
	stopping   bool // a Stop's timers are running
}

func newView(log zerolog.Logger, w *Worker) *view {
	return newViewOf(log, w, w.Pid, w.Pgid, w.StartTime, w.Conn)
}

// newViewOf is a view of p, which writes to the worker on conn.
func newViewOf(log zerolog.Logger, p proc, pid, pgid int, startTime uint64, conn io.Writer) *view {
	v := &view{log: log, p: p, pid: pid, pgid: pgid, startTime: startTime,
		down: make(chan *workerv1.WorkerDown, downQueue), phase: shimv1.Phase_PHASE_NEW}
	go v.write(conn)
	return v
}

// Signal sends sig to the worker's process group.
func (v *view) Signal(sig syscall.Signal) error { return v.p.Signal(sig) }

// exited reports whether the worker has been reaped.
func (v *view) exited() bool {
	select {
	case <-v.p.Done():
		return true
	default:
		return false
	}
}

// Attached reports the worker. Its state is EXITED once reaped, RUNNING once it said Hello, STARTING before.
func (v *view) Attached() *shimv1.Attached {
	a := &shimv1.Attached{Pid: int32(v.pid), Pgid: int32(v.pgid), StartTime: v.startTime}
	v.mu.Lock()
	a.Hello, a.Stage, a.LastBeatAtNs, a.Phase = proto.CloneOf(v.hello), v.stage, v.lastBeatNs, v.phase
	v.mu.Unlock()
	switch {
	case v.exited():
		a.State = shimv1.WorkerState_WORKER_STATE_EXITED
	case a.Hello != nil:
		a.State = shimv1.WorkerState_WORKER_STATE_RUNNING
	default:
		a.State = shimv1.WorkerState_WORKER_STATE_STARTING
	}
	return a
}

// Init passes Init to the worker. Only the first verb: the phase must be NEW.
func (v *view) Init(m *workerv1.Init) error {
	return v.verb("init", shimv1.Phase_PHASE_NEW, shimv1.Phase_PHASE_INITIALIZING,
		&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: m}})
}

// Start passes Start to the worker, once its Init has succeeded: the phase must be INITIALIZED.
func (v *view) Start(m *workerv1.Start) error {
	return v.verb("start", shimv1.Phase_PHASE_INITIALIZED, shimv1.Phase_PHASE_STARTING,
		&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: m}})
}

func (v *view) verb(name string, from, to shimv1.Phase, m *workerv1.WorkerDown) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.exited() {
		return ErrExited
	}
	if v.phase != from {
		return &stageError{verb: name, phase: v.phase}
	}
	if !v.sendLocked(m) {
		return ErrWorkerBusy
	}
	v.phase = to
	return nil
}

// Relay passes a C5 request to the worker, in any phase: the shim does not read C5 (S5).
func (v *view) Relay(m *workerv1.Relay) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.exited() {
		return ErrExited
	}
	if !v.sendLocked(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Relay{Relay: m}}) {
		return ErrWorkerBusy
	}
	return nil
}

// Stop stops the worker (PROTOCOL §4.3): C4 Stop now, SIGTERM to its group after grace, SIGKILL killAfter later,
// each only if the worker is still there. A worker that already sent Exiting is not told again; the timers run all
// the same, since its OnStop may hang.
//
// Only the first Stop sets the timers; a later one, from the agent or from the shim's own SIGTERM, changes nothing
// and returns false. Nothing can stop a Stop: the agent that wants it sooner sends Signal.
func (v *view) Stop(reason string, grace, killAfter time.Duration) (first bool, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.exited() {
		return false, ErrExited
	}
	if v.stopping {
		return false, nil
	}
	v.stopping = true
	v.phase = shimv1.Phase_PHASE_STOPPING
	if !v.exiting {
		m := &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{
			Reason: reason, GraceMs: uint32(min(grace.Milliseconds(), maxMillis)),
		}}}
		if !v.sendLocked(m) {
			// The SIGTERM still comes, after the same grace: what the worker would have done on Stop, its SDK
			// does on SIGTERM.
			v.log.Warn().Msg("stop: the worker is not reading fd 3; it gets SIGTERM when the grace is over")
		}
	}
	v.log.Info().Str("reason", reason).Dur("grace", grace).Dur("kill_after", killAfter).Msg("stopping the worker")
	go v.escalate(grace, killAfter)
	return true, nil
}

// maxMillis is the largest grace C4 Stop can carry.
const maxMillis = int64(^uint32(0))

// escalate is the timers of a Stop. It returns as soon as the worker is reaped.
func (v *view) escalate(grace, killAfter time.Duration) {
	for _, step := range []struct {
		after time.Duration
		sig   syscall.Signal
	}{{grace, syscall.SIGTERM}, {killAfter, syscall.SIGKILL}} {
		t := time.NewTimer(step.after)
		select {
		case <-v.p.Done():
			t.Stop()
			return
		case <-t.C:
		}
		err := v.p.Signal(step.sig)
		switch {
		case errors.Is(err, ErrExited):
			return
		case err != nil:
			v.log.Error().Err(err).Str("signal", step.sig.String()).Msg("stop: signal failed")
		default:
			v.log.Info().Str("signal", step.sig.String()).Dur("after", step.after).Msg("stop: signalled the worker's group")
		}
	}
}

// Release is accepted only once the worker has been reaped (PROTOCOL §4.3).
func (v *view) Release() error {
	if !v.exited() {
		return ErrRunning
	}
	return nil
}

// sendLocked queues m for the worker, unless the queue is full.
func (v *view) sendLocked(m *workerv1.WorkerDown) bool {
	select {
	case v.down <- m:
		return true
	default:
		return false
	}
}

// write writes what is queued to the worker. After a failed write, the worker's end is gone: what comes after is
// dropped, since the worker is ending or has ended, and the reaper will say so.
func (v *view) write(conn io.Writer) {
	w := frame.NewWriter(conn)
	broken := false
	for m := range v.down {
		if broken {
			continue
		}
		if err := w.Write(m); err != nil {
			v.log.Warn().Err(err).Msg("worker channel: write failed")
			broken = true
		}
	}
}

// follow reads the worker's messages until it closes its end, so the worker never blocks on a full socket, and keeps
// what Attached reports and what moves the phase: Done and Exiting.
func (v *view) follow(conn io.Reader) {
	r := newFrameReader(conn)
	for {
		m := &workerv1.WorkerUp{}
		if err := r.Read(m); err != nil {
			if !errors.Is(err, io.EOF) {
				v.log.Warn().Err(err).Msg("worker channel")
			}
			v.log.Info().Msg("worker channel closed")
			return
		}
		v.mu.Lock()
		switch m := m.GetMsg().(type) {
		case *workerv1.WorkerUp_Hello:
			if v.hello == nil { // the first one counts; a worker says Hello once
				v.hello = m.Hello
			}
		case *workerv1.WorkerUp_Stage:
			v.stage = m.Stage.GetName()
		case *workerv1.WorkerUp_Beat:
			v.lastBeatNs = time.Now().UnixNano()
		case *workerv1.WorkerUp_Done:
			v.doneLocked(m.Done)
		case *workerv1.WorkerUp_Exiting:
			v.exiting = true
			v.phase = shimv1.Phase_PHASE_STOPPING
		}
		v.mu.Unlock()
		v.log.Debug().Str("msg", fmt.Sprintf("%T", m.GetMsg())).Msg("from worker")
	}
}

// doneLocked moves the phase on the worker's Done. A Done once stopping, or for a verb the shim did not pass down,
// changes nothing.
func (v *view) doneLocked(d *workerv1.Done) {
	var want, next shimv1.Phase
	switch d.GetVerb() {
	case workerv1.Verb_VERB_INIT:
		want, next = shimv1.Phase_PHASE_INITIALIZING, shimv1.Phase_PHASE_INITIALIZED
	case workerv1.Verb_VERB_START:
		want, next = shimv1.Phase_PHASE_STARTING, shimv1.Phase_PHASE_STARTED
	}
	if v.phase == shimv1.Phase_PHASE_STOPPING { // a Done that crossed a Stop
		return
	}
	if want == shimv1.Phase_PHASE_UNSPECIFIED || v.phase != want {
		v.log.Warn().Str("verb", d.GetVerb().String()).Str("phase", phaseName(v.phase)).Msg("unexpected Done from the worker")
		return
	}
	if !d.GetOk() { // the worker ends: Exiting follows
		next = shimv1.Phase_PHASE_STOPPING
		v.log.Info().Str("verb", d.GetVerb().String()).Str("error", d.GetError()).Msg("the worker failed a verb")
	}
	v.phase = next
}
