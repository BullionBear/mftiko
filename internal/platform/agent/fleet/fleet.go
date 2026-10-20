// Package fleet is the agent's workers, each behind its shim (D101, D102): it spawns a shim per incarnation, attaches
// to it over C3, sends the lifecycle and the live config, and observes each worker from its shim's Attached and
// events (D108). It is the runner.Fleet of every plane; a plane supplies its Program.
//
// At most one incarnation of a worker key lives at a time (D106): the next one is spawned only once the last is
// known to be gone, by its recorded exit or, when its shim was lost, by its pid and start time (E1-16).
//
// Each worker's last Beat is observed (at most one a second, as the shim passes them on), for the Policy's
// timeout (E1-18).
//
// A new term adopts the shims the last one left (Adopt, E1-17), from their sockets and exit files.
package fleet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/c3"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Defaults of Config.
const (
	DefaultStopGrace     = 10 * time.Second // as the shim's own on SIGTERM (PROTOCOL §4.3)
	DefaultStopKillAfter = 5 * time.Second
	DefaultDialTimeout   = 10 * time.Second // the shim reads its Launch within 10s, then listens (§4.6)
	// releaseWait bounds the wait for a released shim to exit, so that the next incarnation's shim does not find
	// the socket still there.
	releaseWait = 10 * time.Second
)

// Program is a plane's part: what a worker runs and what it is sent. It does no I/O.
type Program[S, L agent.Hashed] interface {
	// Launch is what the worker runs (PROTOCOL §4.6): path, args, env (the manifest's, then the plane's §5.4
	// variables), dir, cred, output. The Fleet fills plane, agent_instance, worker, spec_hash and work_dir, and adds
	// the identity variables MFTIKO_PLANE, MFTIKO_INSTANCE, MFTIKO_WORKER_KEY, MFTIKO_INCARNATION.
	Launch(key agent.WorkerKey, spec S) (*shimv1.Launch, error)
	// Init carries live, the first live config; the Fleet sets its live_digest.
	Init(key agent.WorkerKey, spec S, live L) (*workerv1.Init, error)
	Start(key agent.WorkerKey, spec S) (*workerv1.Start, error)
	// Push is live as the worker takes it: its C5 contract and payload. The Fleet sets the digest (D111).
	Push(key agent.WorkerKey, live L) (*workerv1.Live, error)
}

// carriesLive reports whether Init's payload is the live config the worker applies. A Program that
// says otherwise (MD: Init is mdctl.InitPayload, Desired arrives as Live) is not recorded as
// already pushed at spawn.
func carriesLive[S, L agent.Hashed](p Program[S, L]) bool {
	c, ok := p.(interface{ CarriesLive() bool })
	return !ok || c.CarriesLive()
}

// killSilentOf reports whether a missing beat stops the worker with SIGKILL and no
// C4 Stop (D966). A Program that does not implement KillSilent, or whose method
// returns false, is stopped with Stop, grace, SIGTERM, then SIGKILL. MD and TD
// do not implement it.
func killSilentOf[S, L agent.Hashed](p Program[S, L], key agent.WorkerKey, inc uint32) bool {
	k, ok := p.(interface {
		KillSilent(agent.WorkerKey, uint32) bool
	})
	return ok && k.KillSilent(key, inc)
}

// stopGraceOf is the Stop grace for one worker. A Program may supply it (STS, from the session
// spec, D962); 0, or a Program that does not implement it, uses the fleet's StopGrace. MD and TD
// do not implement it.
func stopGraceOf[S, L agent.Hashed](p Program[S, L], key agent.WorkerKey, fallback time.Duration) time.Duration {
	g, ok := p.(interface {
		StopGrace(agent.WorkerKey) time.Duration
	})
	if !ok {
		return fallback
	}
	if d := g.StopGrace(key); d > 0 {
		return d
	}
	return fallback
}

// stampIncarnation lets a Program write the incarnation into Init after it has been allocated.
// The number is not known when Init is built (D962's neighbor: InitPayload.incarnation). A Program
// that does not implement it is unchanged. MD and TD do not implement it.
func stampIncarnation[S, L agent.Hashed](p Program[S, L], init *workerv1.Init, n uint32) error {
	s, ok := p.(interface {
		StampIncarnation(*workerv1.Init, uint32) error
	})
	if !ok {
		return nil
	}
	return s.StampIncarnation(init, n)
}

// Numbers hands out incarnations: a *state.Incarnations (PROTOCOL §3.2 W6).
type Numbers interface {
	Next(ctx context.Context, workerKey string) (uint32, error)
}

var _ Numbers = (*state.Incarnations)(nil)

// Config is the agent the Fleet works for, and how it starts shims.
type Config[S, L agent.Hashed] struct {
	Plane    state.Plane
	Instance string
	Epoch    int64  // the term's: Attach carries it (D105)
	Release  string // Attach.agent_release
	WorkDir  string // ${WORK_DIR}: sockets, exit files, the workers' output (§4.1, §4.5)
	Shim     string // the mftiko-shim executable
	ShimEnv  []string
	Numbers  Numbers
	Program  Program[S, L]

	StopGrace, StopKillAfter time.Duration // DefaultStopGrace, DefaultStopKillAfter if 0
	DialTimeout              time.Duration // DefaultDialTimeout if 0
	// Started is told each shim's pid once it runs. Optional: tests use it to clean up the shims they made.
	Started func(pid int)
	Log     zerolog.Logger
}

// Fleet is the agent's workers. Its calls come from the agent's one loop; the shims' events update it on
// goroutines of their own. Safe for concurrent use.
type Fleet[S, L agent.Hashed] struct {
	cfg     Config[S, L]
	changed chan struct{}

	preempted   chan struct{}
	preemptOnce sync.Once
	preemptErr  error

	mu          sync.Mutex
	workers     map[agent.WorkerKey]*worker[S]
	unreachable map[string]string // worker hash h -> the socket of a shim that answers but could not be attached
}

// ErrStillRunning: the worker of a lost shim, or that shim, is still running. Its next incarnation waits; the
// spawn is tried again after a backoff (D104 says it must die with its shim: Pdeathsig, or its own EOF on fd 3).
var ErrStillRunning = errors.New("fleet: the last incarnation is still running")

// worker is one incarnation and its shim.
type worker[S agent.Hashed] struct {
	conn      *c3.Conn        // nil for an exit file whose shim is gone (Adopt)
	socket    string          // the shim's C3 socket
	shimDone  <-chan struct{} // closed once the shim this Fleet started has exited; nil for an adopted one
	pid       int             // the worker's, and its start time: Attached.pid and start_time
	startTime uint64
	obs       agent.Observed // under Fleet.mu
	released  bool           // under Fleet.mu: Release was sent; the end of the connection is expected
}

// gone checks that neither the worker nor its shim runs any more: the shim this Fleet started has exited, and the
// worker's pid is free or belongs to a later process (pid reuse) or a zombie. A /proc that cannot be read is not
// taken for gone.
func (w *worker[S]) gone() error {
	if w.shimDone != nil {
		select {
		case <-w.shimDone:
		default:
			return fmt.Errorf("%w: its shim", ErrStillRunning)
		}
	} else if answers(w.socket) { // a shim this agent did not start: gone once nothing listens
		return fmt.Errorf("%w: its shim answers on %s", ErrStillRunning, w.socket)
	}
	if w.pid == 0 {
		return nil // never started: Attached had no pid
	}
	running, err := shim.Running(w.pid, w.startTime)
	switch {
	case err != nil:
		return fmt.Errorf("fleet: is pid %d still running: %w", w.pid, err)
	case running:
		return fmt.Errorf("%w: pid %d", ErrStillRunning, w.pid)
	}
	return nil
}

// New is a Fleet with no workers.
func New[S, L agent.Hashed](cfg Config[S, L]) *Fleet[S, L] {
	if cfg.StopGrace == 0 {
		cfg.StopGrace = DefaultStopGrace
	}
	if cfg.StopKillAfter == 0 {
		cfg.StopKillAfter = DefaultStopKillAfter
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	return &Fleet[S, L]{cfg: cfg, changed: make(chan struct{}, 1), preempted: make(chan struct{}),
		workers: map[agent.WorkerKey]*worker[S]{}, unreachable: map[string]string{}}
}

// Observe is every worker as last seen.
func (f *Fleet[S, L]) Observe() map[agent.WorkerKey]agent.Observed {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[agent.WorkerKey]agent.Observed, len(f.workers))
	for k, w := range f.workers {
		out[k] = w.obs
	}
	return out
}

// Changed is signalled when Observe changes by an event; signals coalesce.
func (f *Fleet[S, L]) Changed() <-chan struct{} { return f.changed }

func (f *Fleet[S, L]) signal() {
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *Fleet[S, L]) get(key agent.WorkerKey) (*worker[S], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.workers[key]
	if w == nil {
		return nil, fmt.Errorf("fleet: no worker %s", key)
	}
	return w, nil
}

// update changes key's observation under the lock, if w is still its worker.
func (f *Fleet[S, L]) update(key agent.WorkerKey, w *worker[S], mod func(*agent.Observed)) {
	f.mu.Lock()
	if f.workers[key] == w {
		mod(&w.obs)
	}
	f.mu.Unlock()
	f.signal()
}

// Spawn starts a new incarnation of key: a worker of key that ended is released first (D106), the incarnation is
// taken (W6), the shim started with the Launch, attached, and the worker sent Init.
func (f *Fleet[S, L]) Spawn(ctx context.Context, key agent.WorkerKey, spec S, live L) error {
	f.mu.Lock()
	old := f.workers[key]
	_, held := f.unreachable[state.WorkerHash(string(key))]
	f.mu.Unlock()
	if held {
		return fmt.Errorf("fleet: spawn %s: %w", key, ErrUnreachable)
	}
	if old != nil {
		if !old.obs.State.Ended() {
			return fmt.Errorf("fleet: spawn %s: incarnation %d is still %s", key, old.obs.Incarnation, old.obs.State)
		}
		if err := f.Release(ctx, key); err != nil {
			return fmt.Errorf("fleet: spawn %s: %w", key, err)
		}
	}
	l, err := f.cfg.Program.Launch(key, spec)
	if err != nil {
		return fmt.Errorf("fleet: spawn %s: launch: %w", key, err)
	}
	init, err := f.cfg.Program.Init(key, spec, live)
	if err != nil {
		return fmt.Errorf("fleet: spawn %s: init: %w", key, err)
	}
	init.LiveDigest = live.Hash()
	n, err := f.cfg.Numbers.Next(ctx, string(key))
	if err != nil {
		return fmt.Errorf("fleet: spawn %s: incarnation: %w", key, err)
	}
	if err := stampIncarnation(f.cfg.Program, init, n); err != nil {
		return fmt.Errorf("fleet: spawn %s: incarnation: %w", key, err)
	}
	l.Plane, l.AgentInstance, l.SpecHash, l.WorkDir = string(f.cfg.Plane), f.cfg.Instance, spec.Hash(), f.cfg.WorkDir
	l.Worker = &commonv1.WorkerRef{Key: string(key), Incarnation: n}
	l.Env = append(l.Env, "MFTIKO_PLANE="+string(f.cfg.Plane), "MFTIKO_INSTANCE="+f.cfg.Instance,
		"MFTIKO_WORKER_KEY="+string(key), "MFTIKO_INCARNATION="+strconv.FormatUint(uint64(n), 10))

	shimDone, err := f.startShim(l)
	if err != nil {
		return fmt.Errorf("fleet: spawn %s/%d: %w", key, n, err)
	}
	dctx, cancel := context.WithTimeout(ctx, f.cfg.DialTimeout)
	go func() { // a shim that exits before it listens fails the dial at once
		select {
		case <-shimDone:
			cancel()
		case <-dctx.Done():
		}
	}()
	socket := f.socket(key)
	conn, err := c3.Dial(dctx, socket, f.attachConfig())
	cancel()
	if err != nil {
		return fmt.Errorf("fleet: spawn %s/%d: attach: %w", key, n, err)
	}
	at := conn.Attached()
	obs := spawned(at, spec.Hash(), live.Hash(), time.Now())
	if !carriesLive(f.cfg.Program) {
		// Init's payload is not the live config, so it has not been pushed. MD's Desired is a
		// later Live; recording it here would make the Reconciler wait out LiveRetry.
		obs.Pushed = ""
		obs.PushedAt = time.Time{}
	}
	w := &worker[S]{conn: conn, socket: socket, shimDone: shimDone, pid: int(at.GetPid()), startTime: at.GetStartTime(),
		obs: obs}
	f.mu.Lock()
	f.workers[key] = w
	f.mu.Unlock()
	go f.follow(key, w)
	if err := conn.Init(ctx, init); err != nil {
		// The worker is up and observed; it was not initialized. Kill it: its end is a failure the Policy sees.
		conn.Signal(context.WithoutCancel(ctx), syscall.SIGKILL) //nolint:errcheck // exited already is fine
		return fmt.Errorf("fleet: spawn %s/%d: init: %w", key, n, err)
	}
	f.cfg.Log.Info().Str("worker", string(key)).Uint32("incarnation", n).Int32("pid", at.GetPid()).Msg("worker spawned")
	return nil
}

// spawned is a worker just spawned, as its shim's Attached has it (D108): what the worker said before the attach
// (its Hello, a Stage, a Beat) is in the snapshot, and the events of it are skipped as older than next_seq, so the
// snapshot is the only place to read it from. Init has not been sent yet: the worker is initializing, and its live
// config is the one Init carries. A worker that has exited already stays exited: its Exited event is skipped too,
// and the Reconciler would otherwise wait out the Beat timeout on it.
func spawned(at *shimv1.Attached, specHash, liveHash string, now time.Time) agent.Observed {
	o := observedOf(at)
	o.SpecHash, o.Pushed, o.PushedAt, o.StartedAt = specHash, liveHash, now, now
	if o.State != agent.StateExited {
		o.State = agent.StateInitializing
	}
	return o
}

// startShim execs the shim in a session of its own (§4.6 step 1), its log appended to a file under the work dir,
// writes the Launch on its stdin, and returns a channel closed once it has exited.
func (f *Fleet[S, L]) startShim(l *shimv1.Launch) (<-chan struct{}, error) {
	if err := shim.EnsureLogDir(f.cfg.WorkDir, string(f.cfg.Plane)); err != nil {
		return nil, err
	}
	logPath := filepath.Join(shim.LogDir(f.cfg.WorkDir, string(f.cfg.Plane)), state.WorkerHash(l.GetWorker().GetKey())+".shim.log")
	logFile, err := shim.OpenLogFile(logPath)
	if err != nil {
		return nil, err
	}
	defer logFile.Close() // the shim has its own copy
	cmd := exec.Command(f.cfg.Shim)
	cmd.Env = append(os.Environ(), f.cfg.ShimEnv...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if f.cfg.Started != nil {
		f.cfg.Started(cmd.Process.Pid)
	}
	done := make(chan struct{})
	go func() { // the agent reaps its shims while it lives; after that, init does
		err := cmd.Wait()
		if err != nil {
			f.cfg.Log.Info().Err(err).Str("worker", l.GetWorker().GetKey()).Msg("shim exited")
		}
		close(done)
	}()
	err = frame.NewWriter(stdin).Write(l)
	stdin.Close()
	if err != nil {
		cmd.Process.Kill() //nolint:errcheck // it has no Launch: nothing was spawned
		return nil, fmt.Errorf("write launch: %w", err)
	}
	return done, nil
}

// follow applies key's events to its observation until the connection ends. An end that Release did not ask for
// leaves the worker lost, unless its exit was recorded: in an Exited event, or in the exit file its shim wrote
// before it went (S3).
//
// The events before Attached's next_seq are already in it (D108), so they are skipped: replayed over the snapshot
// they would take the observation back in time.
func (f *Fleet[S, L]) follow(key agent.WorkerKey, w *worker[S]) {
	from := w.conn.Attached().GetNextSeq()
	for ev := range w.conn.Events() {
		if ev.GetSeq() != 0 && ev.GetSeq() < from {
			continue
		}
		f.update(key, w, func(o *agent.Observed) { apply(o, ev) })
	}
	var pe *c3.PreemptedError
	if errors.As(w.conn.Err(), &pe) {
		// A newer term attached to this shim: the worker runs on under it. Nothing about it changed here, and
		// this agent must stop (PROTOCOL §4.2 step 3).
		f.preempt(pe)
		return
	}
	rec, _ := shim.ReadExitFile(f.exitPath(key)) // a missing or unreadable file: no record
	f.mu.Lock()
	if f.workers[key] == w && !w.released && w.obs.State != agent.StateExited {
		if rec.GetWorker().GetIncarnation() == w.obs.Incarnation && rec.GetExit() != nil {
			w.obs.State, w.obs.Exit = agent.StateExited, rec.GetExit()
			f.cfg.Log.Warn().Err(w.conn.Err()).Str("worker", string(key)).Msg("lost the shim; its exit is recorded")
		} else {
			w.obs.State = agent.StateLost
			f.cfg.Log.Warn().Err(w.conn.Err()).Str("worker", string(key)).Msg("lost the shim")
		}
	}
	f.mu.Unlock()
	f.signal()
}

// preempt records the first preemption and signals Preempted.
func (f *Fleet[S, L]) preempt(pe *c3.PreemptedError) {
	f.preemptOnce.Do(func() {
		f.preemptErr = pe
		f.cfg.Log.Warn().Int64("by_epoch", pe.ByEpoch).Msg("preempted by a newer term")
		close(f.preempted)
	})
}

// Preempted is closed once a newer term has attached to one of the shims: this agent is superseded, and must end
// without touching the workers. PreemptedErr says by which epoch.
func (f *Fleet[S, L]) Preempted() <-chan struct{} { return f.preempted }

// PreemptedErr is the first preemption, a *c3.PreemptedError; nil before.
func (f *Fleet[S, L]) PreemptedErr() error {
	select {
	case <-f.preempted:
		return f.preemptErr
	default:
		return nil
	}
}

func (f *Fleet[S, L]) exitPath(key agent.WorkerKey) string {
	return shim.ExitPath(f.cfg.WorkDir, string(f.cfg.Plane), string(key))
}

// apply is one event's effect on a worker's observation (PROTOCOL §4.4).
func apply(o *agent.Observed, ev *shimv1.Event) {
	switch k := ev.GetKind().(type) {
	case *shimv1.Event_Started:
		if ev.GetAtNs() != 0 {
			o.StartedAt = time.Unix(0, ev.GetAtNs())
		}
	case *shimv1.Event_Hello:
		o.Release = k.Hello.GetRelease()
	case *shimv1.Event_Stage:
		o.Stage = k.Stage.GetName()
	case *shimv1.Event_Applied:
		o.LiveHash = k.Applied.GetDigest()
	case *shimv1.Event_Report:
		if k.Report != nil {
			o.Report = proto.Clone(k.Report).(*workerv1.Report)
		}
	case *shimv1.Event_Beat:
		if ev.GetAtNs() != 0 {
			o.LastBeat = time.Unix(0, ev.GetAtNs())
		}
	case *shimv1.Event_Done:
		switch {
		case o.State.Ended() || o.State == agent.StateStopping:
		case !k.Done.GetOk():
			o.State = agent.StateStopping // the shim moves to STOPPING on a failed Done (§4.3)
		case k.Done.GetVerb() == workerv1.Verb_VERB_INIT:
			o.State = agent.StateInitialized
		case k.Done.GetVerb() == workerv1.Verb_VERB_START:
			o.State = agent.StateRunning
		}
	case *shimv1.Event_Exiting, *shimv1.Event_Stopped:
		if !o.State.Ended() {
			o.State = agent.StateStopping
		}
	case *shimv1.Event_Exited:
		o.State, o.Exit = agent.StateExited, k.Exited.GetExit()
	}
}

// Start sends key's worker Start, built from spec: the plan's, which the worker was spawned with.
func (f *Fleet[S, L]) Start(ctx context.Context, key agent.WorkerKey, spec S) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	if w.conn == nil {
		return fmt.Errorf("fleet: start %s: its shim is gone", key)
	}
	start, err := f.cfg.Program.Start(key, spec)
	if err != nil {
		return fmt.Errorf("fleet: start %s: %w", key, err)
	}
	if err := w.conn.Start(ctx, start); err != nil {
		return fmt.Errorf("fleet: start %s: %w", key, err)
	}
	f.update(key, w, func(o *agent.Observed) {
		if o.State == agent.StateInitialized {
			o.State = agent.StateStarting
		}
	})
	return nil
}

// Relay passes a C5 command to key's worker. It is not a reconciler action: a plane calls it from its
// own loop when a command is not the live config (STS bindings after Start, D961).
func (f *Fleet[S, L]) Relay(ctx context.Context, key agent.WorkerKey, msg *workerv1.Relay) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	if w.conn == nil {
		return fmt.Errorf("fleet: relay %s: its shim is gone", key)
	}
	if err := w.conn.Relay(ctx, msg); err != nil {
		return fmt.Errorf("fleet: relay %s: %w", key, err)
	}
	return nil
}

// Push hands live to key's worker. What it has stays what it says it applied (D111); the send is recorded, so
// that the Reconciler waits for the Applied before it sends again.
func (f *Fleet[S, L]) Push(ctx context.Context, key agent.WorkerKey, live L) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	if w.conn == nil {
		return fmt.Errorf("fleet: push %s: its shim is gone", key)
	}
	msg, err := f.cfg.Program.Push(key, live)
	if err != nil {
		return fmt.Errorf("fleet: push %s: %w", key, err)
	}
	msg.Digest = live.Hash()
	if err := w.conn.Live(ctx, msg); err != nil {
		return fmt.Errorf("fleet: push %s: %w", key, err)
	}
	f.update(key, w, func(o *agent.Observed) { o.Pushed, o.PushedAt = msg.Digest, time.Now() })
	return nil
}

// Stop stops key's worker. The grace is the Program's StopGrace when it implements that and returns
// a positive duration, otherwise the Config's (D962). The kill timer is the Config's. Both run in
// the shim (§4.3).
//
// A missing beat is a SIGKILL with no C4 Stop when the Program's KillSilent returns true (D966).
// MD and TD do not implement it, so their workers still get Stop.
func (f *Fleet[S, L]) Stop(ctx context.Context, key agent.WorkerKey, reason string) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	if w.conn == nil {
		return nil // gone with its shim: nothing to stop
	}
	f.mu.Lock()
	inc := w.obs.Incarnation
	f.mu.Unlock()
	if reason == agent.ReasonSilent && killSilentOf(f.cfg.Program, key, inc) {
		err = w.conn.Signal(ctx, syscall.SIGKILL)
	} else {
		err = w.conn.Stop(ctx, reason, stopGraceOf(f.cfg.Program, key, f.cfg.StopGrace), f.cfg.StopKillAfter)
	}
	if err != nil && c3.CodeOf(err) != shim.CodeExited { // exited meanwhile: its Exited is on the way
		return fmt.Errorf("fleet: stop %s: %w", key, err)
	}
	f.update(key, w, func(o *agent.Observed) {
		if !o.State.Ended() {
			o.State = agent.StateStopping
		}
	})
	return nil
}

// Release ends the shim of key's worker, which has ended, and forgets it. With its shim there, the shim does it
// (§4.3) and Release waits for it to exit, so that the socket is gone before the next incarnation's shim listens.
// With its shim lost, Release first checks that the worker and the shim are gone (ErrStillRunning otherwise),
// then removes the exit file the shim may have left, which would keep the next shim from starting (S3).
func (f *Fleet[S, L]) Release(ctx context.Context, key agent.WorkerKey) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	f.mu.Lock()
	w.released = true
	state, inc := w.obs.State, w.obs.Incarnation
	f.mu.Unlock()
	unrelease := func() {
		f.mu.Lock()
		w.released = false
		f.mu.Unlock()
	}
	viaShim := state == agent.StateExited && w.conn != nil && w.conn.Err() == nil
	if viaShim {
		if err := w.conn.Release(ctx); err != nil {
			unrelease()
			return fmt.Errorf("fleet: release %s: %w", key, err)
		}
	} else {
		if err := w.gone(); err != nil {
			unrelease()
			return fmt.Errorf("fleet: release %s/%d: %w", key, inc, err)
		}
		path := f.exitPath(key)
		if rec, err := shim.ReadExitFile(path); err == nil && rec.GetWorker().GetIncarnation() == inc {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				unrelease()
				return fmt.Errorf("fleet: release %s: %w", key, err)
			}
		}
	}
	if w.conn != nil {
		w.conn.Close()
	}
	if viaShim {
		f.waitShim(ctx, key, w)
	}
	f.mu.Lock()
	if f.workers[key] == w {
		delete(f.workers, key)
	}
	f.mu.Unlock()
	f.signal()
	return nil
}

// waitShim waits for a released shim to exit: the one this Fleet started by its exit, an adopted one by its
// socket, which it removes as it goes (§4.1).
func (f *Fleet[S, L]) waitShim(ctx context.Context, key agent.WorkerKey, w *worker[S]) {
	t := time.NewTimer(releaseWait)
	defer t.Stop()
	if w.shimDone != nil {
		select {
		case <-w.shimDone:
		case <-t.C:
			f.cfg.Log.Warn().Str("worker", string(key)).Msg("released shim still running")
		case <-ctx.Done():
		}
		return
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Lstat(w.socket); errors.Is(err, os.ErrNotExist) {
			return
		}
		select {
		case <-tick.C:
		case <-t.C:
			f.cfg.Log.Warn().Str("worker", string(key)).Msg("released shim still running")
			return
		case <-ctx.Done():
			return
		}
	}
}

func (f *Fleet[S, L]) socket(key agent.WorkerKey) string {
	return shim.SocketPath(f.cfg.WorkDir, string(f.cfg.Plane), string(key))
}

func (f *Fleet[S, L]) attachConfig() c3.Config {
	return c3.Config{Plane: string(f.cfg.Plane), Instance: f.cfg.Instance, Epoch: f.cfg.Epoch, Release: f.cfg.Release,
		Log: f.cfg.Log}
}

// Close closes every connection; the shims and their workers go on, for the next term to adopt (D102).
func (f *Fleet[S, L]) Close() {
	f.mu.Lock()
	ws := maps.Clone(f.workers)
	f.mu.Unlock()
	for _, w := range ws {
		if w.conn != nil {
			w.conn.Close()
		}
	}
}
