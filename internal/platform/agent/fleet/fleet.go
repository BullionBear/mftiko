// Package fleet is the agent's workers, each behind its shim (D101, D102): it spawns a shim per incarnation, attaches
// to it over C3, sends the lifecycle and the live config, and observes each worker from its shim's Attached and
// events (D108). It is the runner.Fleet of every plane; a plane supplies its Program.
//
// At most one incarnation of a worker key lives at a time (D106): the next one is spawned only once the last is
// known to be gone, by its recorded exit or, when its shim was lost, by its pid and start time (E1-16).
//
// Not yet here: adopting the shims a previous term left (Adopt finds none; E1-17), and Beat timeouts (E1-18).
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
	Init(key agent.WorkerKey, spec S, live L) (*workerv1.Init, error)
	Start(key agent.WorkerKey, spec S) (*workerv1.Start, error)
	// Push is the C5 command that hands live to a running worker.
	Push(key agent.WorkerKey, live L) (*workerv1.Relay, error)
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

	mu      sync.Mutex
	workers map[agent.WorkerKey]*worker[S]
}

// ErrStillRunning: the worker of a lost shim, or that shim, is still running. Its next incarnation waits; the
// spawn is tried again after a backoff (D104 says it must die with its shim: Pdeathsig, or its own EOF on fd 3).
var ErrStillRunning = errors.New("fleet: the last incarnation is still running")

// worker is one incarnation and its shim.
type worker[S agent.Hashed] struct {
	spec      S
	conn      *c3.Conn
	shimDone  <-chan struct{} // closed once the shim this Fleet started has exited
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
	return &Fleet[S, L]{cfg: cfg, changed: make(chan struct{}, 1), workers: map[agent.WorkerKey]*worker[S]{}}
}

// Adopt finds no shims yet: adoption is E1-17. A shim a previous term left keeps its socket, and the next spawn
// of its worker key fails until it is released.
func (f *Fleet[S, L]) Adopt(context.Context) error { return nil }

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
	f.mu.Unlock()
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
	n, err := f.cfg.Numbers.Next(ctx, string(key))
	if err != nil {
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
	conn, err := c3.Dial(dctx, shim.SocketPath(f.cfg.WorkDir, string(f.cfg.Plane), string(key)), c3.Config{
		Plane: string(f.cfg.Plane), Instance: f.cfg.Instance, Epoch: f.cfg.Epoch, Release: f.cfg.Release, Log: f.cfg.Log,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("fleet: spawn %s/%d: attach: %w", key, n, err)
	}
	at := conn.Attached()
	w := &worker[S]{spec: spec, conn: conn, shimDone: shimDone, pid: int(at.GetPid()), startTime: at.GetStartTime(),
		obs: agent.Observed{
			Key: key, Incarnation: n, SpecHash: spec.Hash(), LiveHash: live.Hash(), State: agent.StateInitializing,
			ShimRelease: at.GetShimRelease(), StartedAt: time.Now(),
		}}
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

// startShim execs the shim in a session of its own (§4.6 step 1), its log appended to a file under the work dir,
// writes the Launch on its stdin, and returns a channel closed once it has exited.
func (f *Fleet[S, L]) startShim(l *shimv1.Launch) (<-chan struct{}, error) {
	dir := filepath.Join(f.cfg.WorkDir, "log", string(f.cfg.Plane))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, state.WorkerHash(l.GetWorker().GetKey())+".shim.log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
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
func (f *Fleet[S, L]) follow(key agent.WorkerKey, w *worker[S]) {
	for ev := range w.conn.Events() {
		f.update(key, w, func(o *agent.Observed) { apply(o, ev) })
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

// Start sends key's worker Start.
func (f *Fleet[S, L]) Start(ctx context.Context, key agent.WorkerKey) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	start, err := f.cfg.Program.Start(key, w.spec)
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

// Push hands live to key's worker.
func (f *Fleet[S, L]) Push(ctx context.Context, key agent.WorkerKey, live L) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	relay, err := f.cfg.Program.Push(key, live)
	if err != nil {
		return fmt.Errorf("fleet: push %s: %w", key, err)
	}
	if err := w.conn.Relay(ctx, relay); err != nil {
		return fmt.Errorf("fleet: push %s: %w", key, err)
	}
	f.update(key, w, func(o *agent.Observed) { o.LiveHash = live.Hash() })
	return nil
}

// Stop stops key's worker, with the Config's grace and kill timers, which run in its shim (§4.3).
func (f *Fleet[S, L]) Stop(ctx context.Context, key agent.WorkerKey, reason string) error {
	w, err := f.get(key)
	if err != nil {
		return err
	}
	err = w.conn.Stop(ctx, reason, f.cfg.StopGrace, f.cfg.StopKillAfter)
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
	viaShim := state == agent.StateExited && w.conn.Err() == nil
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
	w.conn.Close()
	if viaShim && w.shimDone != nil {
		t := time.NewTimer(releaseWait)
		select {
		case <-w.shimDone:
		case <-t.C:
			f.cfg.Log.Warn().Str("worker", string(key)).Msg("released shim still running")
		case <-ctx.Done():
		}
		t.Stop()
	}
	f.mu.Lock()
	if f.workers[key] == w {
		delete(f.workers, key)
	}
	f.mu.Unlock()
	f.signal()
	return nil
}

// Close closes every connection; the shims and their workers go on, for the next term to adopt (D102).
func (f *Fleet[S, L]) Close() {
	f.mu.Lock()
	ws := maps.Clone(f.workers)
	f.mu.Unlock()
	for _, w := range ws {
		w.conn.Close()
	}
}
