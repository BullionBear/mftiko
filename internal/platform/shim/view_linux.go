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

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
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
	// Status is the worker's wait status, once Done is closed.
	Status() syscall.WaitStatus
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
	sink       sink // where Beat, Report and the worker's Relay go; nil until set
	exitingMsg *workerv1.Exiting
	stoppedMsg *workerv1.Stopped
	persist    func(*commonv1.ExitInfo) error // writes the exit file; nil for none
	exit       *commonv1.ExitInfo             // set once recorded

	followDone chan struct{} // closed when follow has read the worker's channel to its end
	recorded   chan struct{} // closed once the worker's exit is recorded: ExitInfo composed, written, Exited appended

	events *eventLog // what an agent reads back (PROTOCOL §4.4); its own lock
}

// sink is the C3 server as the view needs it: something that hands an event to the controller now, if there is one
// that streams events.
type sink interface {
	// Deliver sends ev to the controller and reports whether there was one to send it to.
	Deliver(ev *shimv1.Event) bool
}

// beatEvery is how often at most a Beat goes on to the agent (PROTOCOL §4.4).
const beatEvery = time.Second

// exitReadGrace bounds the wait, after the reap, for the rest of what the worker sent: its Exiting and Stopped may be
// in the socket still. A worker that leaked fd 3 to a process that outlives it never gives an EOF.
const exitReadGrace = time.Second

func newView(log zerolog.Logger, w *Worker) *view {
	return newViewOf(log, w, w.Pid, w.Pgid, w.StartTime, w.Conn)
}

// newViewOf is a view of p, which writes to the worker on conn.
func newViewOf(log zerolog.Logger, p proc, pid, pgid int, startTime uint64, conn io.Writer) *view {
	v := &view{log: log, p: p, pid: pid, pgid: pgid, startTime: startTime,
		down: make(chan *workerv1.WorkerDown, downQueue), phase: shimv1.Phase_PHASE_NEW, events: newEventLog(),
		followDone: make(chan struct{}), recorded: make(chan struct{})}
	v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Started{Started: &shimv1.Started{
		Pid: int32(pid), Pgid: int32(pgid), StartTime: startTime,
	}}})
	go v.write(conn)
	go v.record()
	return v
}

// SetPersist sets what writes the exit file. Call it before the worker can end.
func (v *view) SetPersist(f func(*commonv1.ExitInfo) error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.persist = f
}

// Recorded is closed once the worker's exit is recorded (PROTOCOL §4.5 S3): its ExitInfo composed and written to the
// exit file, and Exited appended to the events, after everything else the worker sent.
func (v *view) Recorded() <-chan struct{} { return v.recorded }

// Exit is the worker's ExitInfo, nil until recorded.
func (v *view) Exit() *commonv1.ExitInfo {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.exit
}

// record waits for the reap, then for the rest of the worker's messages, and records how it ended.
func (v *view) record() {
	<-v.p.Done()
	t := time.NewTimer(exitReadGrace)
	select {
	case <-v.followDone:
		t.Stop()
	case <-t.C:
		v.log.Warn().Dur("after", exitReadGrace).Msg("worker channel still open after the reap: something the worker started holds fd 3")
	}
	v.mu.Lock()
	info := composeExit(v.exitingMsg, v.stoppedMsg, v.p.Status(), time.Now())
	persist := v.persist
	v.mu.Unlock()
	if persist != nil {
		if err := persist(info); err != nil { // the Exited event still says it
			v.log.Error().Err(err).Msg("exit file not written")
		}
	}
	v.mu.Lock() // the Exited event, the exit and EXITED: one moment for Attached
	v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Exited{Exited: &shimv1.Exited{Exit: info}}})
	v.exit = info
	close(v.recorded)
	v.mu.Unlock()
	v.log.Info().Str("outcome", info.GetOutcome().String()).Int32("code", info.GetCode()).Int32("signal", info.GetSignal()).
		Str("reason", info.GetReason()).Bool("on_stop_completed", info.GetOnStopCompleted()).Msg("exit recorded")
}

// isRecorded reports whether the worker's exit has been recorded.
func (v *view) isRecorded() bool {
	select {
	case <-v.recorded:
		return true
	default:
		return false
	}
}

// Events is the log of what the worker said.
func (v *view) Events() *eventLog { return v.events }

// SetSink sets where the events that are not kept go. Before it is set, they go nowhere, and the worker's Relay is
// answered Undeliverable.
func (v *view) SetSink(s sink) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sink = s
}

// Controller tells the worker whether an agent is there to take its Relay (PROTOCOL §5.2), when that changes or the
// epoch does. It never blocks: a worker that does not read fd 3 misses it, and its Relay still gets Undeliverable
// from the shim.
func (v *view) Controller(attached bool, epoch int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.exited() {
		return
	}
	if !v.sendLocked(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Controller{Controller: &workerv1.Controller{
		Attached: attached, Epoch: epoch,
	}}}) {
		v.log.Warn().Bool("attached", attached).Msg("controller: the worker is not reading fd 3")
	}
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

// Attached reports the worker: everything an agent decides by (DECISIONS D37). Its state is EXITED once the exit is
// recorded, with the ExitInfo; RUNNING once it said Hello; STARTING before.
func (v *view) Attached() *shimv1.Attached {
	a := &shimv1.Attached{Pid: int32(v.pid), Pgid: int32(v.pgid), StartTime: v.startTime}
	// One moment (PROTOCOL §4.4): what follow and record change, and the events they append, change together under
	// v.mu, so next_seq is exactly where this state stands.
	v.mu.Lock()
	defer v.mu.Unlock()
	a.Hello, a.Stage, a.LastBeatAtNs, a.Phase = proto.CloneOf(v.hello), v.stage, v.lastBeatNs, v.phase
	a.Exiting, a.Exit = proto.CloneOf(v.exitingMsg), proto.CloneOf(v.exit) // exit is set only once recorded
	a.OldestSeq, a.NextSeq = v.events.Bounds()
	switch {
	case v.exit != nil: // recorded, not merely reaped: Exited is in the events, and Release is accepted
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

// Release is accepted only once the worker's exit has been recorded (PROTOCOL §4.3, §4.5 S3): reaped, and its exit
// file written.
func (v *view) Release() error {
	if !v.isRecorded() {
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

// follow reads the worker's messages until it closes its end, so the worker never blocks on a full socket. It keeps
// what Attached reports and what moves the phase (Done, Exiting), numbers into the event log what an agent reads
// back, and passes on the rest: the latest Report, a Beat a second, and the worker's Relay, which gets Undeliverable
// when there is no controller to take it.
func (v *view) follow(conn io.Reader) {
	defer close(v.followDone)
	r := newFrameReader(conn)
	var lastBeat time.Time
	for {
		m := &workerv1.WorkerUp{}
		if err := r.Read(m); err != nil {
			if !errors.Is(err, io.EOF) {
				v.log.Warn().Err(err).Msg("worker channel")
			}
			v.log.Info().Msg("worker channel closed")
			return
		}
		// The state and the event that tells of it change together, under v.mu: Attached reads both at one moment.
		v.mu.Lock()
		switch m := m.GetMsg().(type) {
		case *workerv1.WorkerUp_Hello:
			if v.hello == nil { // the first one counts; a worker says Hello once
				v.hello = m.Hello
			}
			v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Hello{Hello: m.Hello}})
		case *workerv1.WorkerUp_Stage:
			v.stage = m.Stage.GetName()
			v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Stage{Stage: m.Stage}})
		case *workerv1.WorkerUp_Beat:
			v.lastBeatNs = time.Now().UnixNano()
		case *workerv1.WorkerUp_Done:
			v.doneLocked(m.Done)
			v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Done{Done: m.Done}})
		case *workerv1.WorkerUp_Exiting:
			v.exiting = true
			v.phase = shimv1.Phase_PHASE_STOPPING
			if v.exitingMsg == nil { // the first one counts
				v.exitingMsg = m.Exiting
			}
			v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Exiting{Exiting: m.Exiting}})
		case *workerv1.WorkerUp_Stopped:
			if v.stoppedMsg == nil {
				v.stoppedMsg = m.Stopped
			}
			v.events.Append(&shimv1.Event{Kind: &shimv1.Event_Stopped{Stopped: m.Stopped}})
		}
		out := v.sink
		v.mu.Unlock()
		v.log.Debug().Str("msg", fmt.Sprintf("%T", m.GetMsg())).Msg("from worker")

		// Not under v.mu: Deliver takes the server's lock, which is held while it calls into the view.
		deliver := func(ev *shimv1.Event) bool { return out != nil && out.Deliver(ev) }
		switch m := m.GetMsg().(type) {
		case *workerv1.WorkerUp_Report:
			deliver(v.events.SetReport(&shimv1.Event{Kind: &shimv1.Event_Report{Report: m.Report}}))
		case *workerv1.WorkerUp_Beat:
			if now := time.Now(); now.Sub(lastBeat) >= beatEvery {
				lastBeat = now
				deliver(v.events.Transient(&shimv1.Event{Kind: &shimv1.Event_Beat{Beat: m.Beat}}))
			}
		case *workerv1.WorkerUp_Relay:
			if !deliver(v.events.Transient(&shimv1.Event{Kind: &shimv1.Event_Relay{Relay: m.Relay}})) {
				v.undeliverable(m.Relay.GetCorrId())
			}
		}
	}
}

// undeliverable answers a Relay of the worker that no agent can take (PROTOCOL §4.4).
func (v *view) undeliverable(corrID []byte) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.sendLocked(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Undeliverable{
		Undeliverable: &workerv1.Undeliverable{CorrId: corrID},
	}}) {
		v.log.Warn().Msg("undeliverable: the worker is not reading fd 3")
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
