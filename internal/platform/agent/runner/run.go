// Package runner runs an agent's term: it waits for the demand, has the Fleet adopt the workers its shims kept,
// brings WorkerStatus in line, writes the AgentRecord, then reconciles in one loop until the term ends (E01-D001,
// PROTOCOL §3.2 W7). The decisions are package agent's Reconciler; this package does the I/O around it.
package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Defaults of Config.
const (
	// DefaultStaleAfter is how old the demand may be before the agent stops changing anything by it (DECISIONS
	// E01-D009): 3 of the Mirror's probe intervals, which is how far LastContact lags while all is well.
	DefaultStaleAfter = 15 * time.Second
	// DefaultResync is the longest the loop sleeps with nothing to wake it: staleness comes and goes with no
	// notification, and the actions are recomputed from scratch each time, at the cost of a diff in memory.
	DefaultResync = time.Second
	// DefaultWriteTimeout bounds each etcd write the loop makes.
	DefaultWriteTimeout = 5 * time.Second
	// DefaultLogRetain is how long an MD or TD agent keeps a log file after its worker key
	// disappears (E12-D041). STS leaves LogRetain at zero and does not clean.
	DefaultLogRetain = 7 * 24 * time.Hour
)

// EffectiveLogRetain is d, or DefaultLogRetain when d is not positive. MD and TD apply it. The
// runner itself treats a non-positive LogRetain as off, so a caller that does not opt in
// (STS, the controller) keeps today's behavior.
func EffectiveLogRetain(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return DefaultLogRetain
}

// ErrTermEnded is what Run returns when the term ended: session lost, superseded, or closed, or a shim preempted
// the agent for a newer term. It wraps the cause. The agent exits and systemd starts it again; the next process
// campaigns for a new epoch (E1-05).
var ErrTermEnded = errors.New("agent: term ended")

// Fleet is the set of shim-backed workers (E01-D001). Its calls start an action and return: Observe reflects each one
// from the moment it returns (a spawned worker is initializing, a stopped one stopping), and the rest comes as
// the shims report it, signalled on Changed. A call must not block for long: the agent has one loop.
type Fleet[S, L agent.Hashed] interface {
	// Adopt finds the workers this instance's shims kept and attaches to each (§4.2); it is called once, before
	// anything else, and returns once Observe has them all.
	Adopt(ctx context.Context) error
	Observe() map[agent.WorkerKey]agent.Observed
	Changed() <-chan struct{}
	// Spawn starts a new incarnation of key with spec and live, and sends Init. A worker of key that ended is
	// released first (delete-before-create, E01-D006).
	Spawn(ctx context.Context, key agent.WorkerKey, spec S, live L) error
	// Start sends Start; spec is the plan's, which the worker was spawned with (the same Hash).
	Start(ctx context.Context, key agent.WorkerKey, spec S) error
	Push(ctx context.Context, key agent.WorkerKey, live L) error
	Stop(ctx context.Context, key agent.WorkerKey, reason string) error
	// Release ends the shim of a worker that ended; Observe no longer has it.
	Release(ctx context.Context, key agent.WorkerKey) error
	// Preempted is closed once a newer term attached to one of the shims (PROTOCOL §4.2): the agent ends, as it
	// does when its term ends, and touches no worker. PreemptedErr says why.
	Preempted() <-chan struct{}
	PreemptedErr() error
	// Unreachable are the worker hashes (h, PROTOCOL §2) of shims that answer but could not be attached: their
	// workers may be running, so their WorkerStatus stays (W7.1).
	Unreachable() []string
}

// Term is the agent's term: a *state.Term.
type Term interface {
	Plane() state.Plane
	Instance() string
	Epoch() int64
	Done() <-chan struct{}
	Err() error
	PutRecord(ctx context.Context, val string) error
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) error
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error
}

var _ Term = (*state.Term)(nil)

// Config is one agent: the term it runs, where its demand comes from, and the plane's parts.
type Config[I any, S, L agent.Hashed] struct {
	Term     Term
	Demand   Source // a Mirror of state.DemandPrefix of the term's plane and instance, run by the caller
	Statuses Lister // reads the instance's worker/ prefix
	Decode   Decode[I]
	Planner  agent.Planner[I, S, L]
	Policy   agent.Policy
	Fleet    Fleet[S, L]
	// Prepare is the plane's part of W7.3, once the Fleet has adopted and before the AgentRecord: its acks, for
	// MD and TD. Optional.
	Prepare func(ctx context.Context, d agent.Demand[I], observed map[agent.WorkerKey]agent.Observed) error
	// Commit runs before Plan: once at start, and in the loop while the demand is not stale. A plane writes
	// the state Plan is about to read, so a crash cannot push a placement that etcd does not hold yet (E01-D010).
	// Optional. A failure at start ends the term. In the loop it is logged and Plan keeps the previous commit.
	Commit func(ctx context.Context, d agent.Demand[I], observed map[agent.WorkerKey]agent.Observed) error
	// Sync runs after WorkerStatus is written: at start, before the AgentRecord, and at the end of a step
	// that is not stale. MD writes acks here (PROTOCOL §3.5). gaveUp is the Reconciler's; at start it is
	// never true. Optional.
	Sync func(ctx context.Context, d agent.Demand[I], observed map[agent.WorkerKey]agent.Observed, gaveUp func(agent.WorkerKey) bool) error
	// Conditions are this plane's WorkerStatus conditions for one observed worker (E12-D063).
	// Nil leaves the field unset, which is TD, STS, and the controller. MD reports NATS
	// connectivity. A later sync with the same bytes is not written again (W7.2).
	Conditions func(o agent.Observed) []*commonv1.Condition

	Release string // AgentRecord.release
	// WorkDir is this process's WORK_DIR, written on the AgentRecord when the term is elected (E10-D092).
	// Empty leaves the field unset. The controller does not use this runner.
	WorkDir string
	// LogAddr, when set, is written on AgentRecord.log_addr (E14-D002, E14-D021).
	// It is read again when LogReady is signaled, so a listen that succeeds
	// after the first record can fill the field in. Nil leaves the field empty:
	// MD, TD, the controller, and an STS endpoint that is not listening.
	LogAddr func() string
	// LogReady wakes one rewrite of the AgentRecord. Nil never fires. Do not
	// close it: a closed channel would spin this loop. A buffer of one is
	// enough, because the value is read from LogAddr at write time.
	LogReady <-chan struct{}
	// LogRetain is how long a log file of a worker key that no longer appears is kept (E12-D041).
	// Zero disables cleanup. MD and TD pass EffectiveLogRetain; STS leaves this zero.
	LogRetain time.Duration
	// LogRetainEvery is how often cleanup runs after the one at startup. Zero means one hour.
	// It is not a deployment setting. Tests set a short interval.
	LogRetainEvery time.Duration
	StaleAfter     time.Duration // DefaultStaleAfter if 0
	Resync         time.Duration // DefaultResync if 0
	StableAfter    time.Duration // agent.DefaultStableAfter if 0
	Retry          agent.Backoff // agent.DefaultRetry if zero
	LiveRetry      time.Duration // agent.DefaultLiveRetry if 0
	Log            zerolog.Logger
	Now            func() time.Time // time.Now if nil
}

// Run runs the term until it ends, which is ErrTermEnded, or ctx is done, which is ctx's error. A failure before
// the loop (adoption, the first WorkerStatus, Prepare, the AgentRecord) is returned as is: the agent exits and
// starts over. Run stops every action when it returns; the workers stay with their shims, for the next term.
func Run[I any, S, L agent.Hashed](ctx context.Context, cfg Config[I, S, L]) error {
	r := newRun(cfg)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { // a term that ends, or a newer one at a shim, cancels whatever is in flight
		select {
		case <-cfg.Term.Done():
		case <-cfg.Fleet.Preempted():
		case <-ctx.Done():
		}
		cancel()
	}()
	err := r.run(ctx)
	if pe := cfg.Fleet.PreemptedErr(); pe != nil {
		return fmt.Errorf("%w: %w", ErrTermEnded, pe)
	}
	if termErr := cfg.Term.Err(); termErr != nil {
		return fmt.Errorf("%w: %w", ErrTermEnded, termErr)
	}
	return err
}

type run[I any, S, L agent.Hashed] struct {
	cfg        Config[I, S, L]
	log        zerolog.Logger
	now        func() time.Time
	host       string // os.Hostname at newRun, before Run starts its goroutine (E10-D092)
	intake     *Intake[I]
	rec        *agent.Reconciler[S, L]
	st         *statuses
	stale      bool
	plans      map[agent.WorkerKey]agent.Plan[S, L] // last Plan, the keep-set for log cleanup
	startedAt  int64                                // AgentRecord.started_at_ns; a later rewrite keeps it
	publishLog bool                                 // LogReady asked for another AgentRecord
}

func newRun[I any, S, L agent.Hashed](cfg Config[I, S, L]) *run[I, S, L] {
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	if cfg.Resync == 0 {
		cfg.Resync = DefaultResync
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		cfg.Log.Warn().Err(err).Msg("agent hostname")
		host = ""
	}
	t := cfg.Term
	return &run[I, S, L]{
		cfg: cfg, now: now, host: host,
		log:    cfg.Log.With().Str("plane", string(t.Plane())).Str("instance", t.Instance()).Int64("epoch", t.Epoch()).Logger(),
		intake: NewIntake(cfg.Demand, cfg.Decode),
		rec: &agent.Reconciler[S, L]{Policy: cfg.Policy, StableAfter: cfg.StableAfter, Retry: cfg.Retry,
			LiveRetry: cfg.LiveRetry},
		st: newStatuses(t, t.Plane(), t.Instance()),
	}
}

func (r *run[I, S, L]) run(ctx context.Context) error {
	if err := r.start(ctx); err != nil {
		return err
	}
	var logTick <-chan time.Time
	if r.cfg.LogRetain > 0 {
		every := r.cfg.LogRetainEvery
		if every <= 0 {
			every = time.Hour
		}
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		logTick = ticker.C
	}
	r.log.Info().Msg("agent reconciling")
	for {
		r.takeLogReady()
		if r.publishLog {
			if err := r.putAgentRecord(ctx); err != nil {
				r.log.Warn().Err(err).Msg("log address")
			} else {
				r.publishLog = false
			}
		}
		wake := r.step(ctx)
		next := r.now().Add(r.cfg.Resync)
		if !wake.IsZero() && wake.Before(next) {
			next = wake
		}
		t := time.NewTimer(max(next.Sub(r.now()), 0))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-r.cfg.Demand.Notify():
		case <-r.cfg.Fleet.Changed():
		case <-t.C:
		case <-logTick:
			r.cleanLogs()
		case <-r.cfg.LogReady:
			r.publishLog = true
		}
		t.Stop()
	}
}

// takeLogReady notices a LogReady signal that arrived during start or step.
// A nil channel is ignored. The receive is non-blocking so a quiet endpoint
// does not stall the loop.
func (r *run[I, S, L]) takeLogReady() {
	if r.cfg.LogReady == nil {
		return
	}
	select {
	case <-r.cfg.LogReady:
		r.publishLog = true
	default:
	}
}

// start is what comes before the first reconcile (W7.3, W7.4): the demand listed, the workers adopted, WorkerStatus
// brought in line with them, the plane's Prepare, then the AgentRecord, which readers take as the sign that the
// agent's keys are current.
func (r *run[I, S, L]) start(ctx context.Context) error {
	select {
	case <-r.cfg.Demand.Synced():
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := r.cfg.Fleet.Adopt(ctx); err != nil {
		return fmt.Errorf("agent: adopt: %w", err)
	}
	if err := r.st.load(ctx, r.cfg.Statuses); err != nil {
		return err
	}
	d := r.intake.Demand()
	observed := r.cfg.Fleet.Observe()
	if r.cfg.Commit != nil {
		if err := r.cfg.Commit(ctx, d, observed); err != nil {
			return fmt.Errorf("agent: commit: %w", err)
		}
	}
	plans := r.cfg.Planner.Plan(d)
	r.plans = plans
	if err := r.writeStatuses(ctx, plans, observed); err != nil {
		return err
	}
	if r.cfg.Sync != nil {
		if err := r.cfg.Sync(ctx, d, observed, func(agent.WorkerKey) bool { return false }); err != nil {
			return fmt.Errorf("agent: sync: %w", err)
		}
	}
	if r.cfg.Prepare != nil {
		if err := r.cfg.Prepare(ctx, d, observed); err != nil {
			return fmt.Errorf("agent: prepare: %w", err)
		}
	}
	if err := r.putAgentRecord(ctx); err != nil {
		return fmt.Errorf("agent: put record: %w", err)
	}
	r.log.Info().Int("workers", len(observed)).Int("planned", len(plans)).Int64("demand_rev", d.Rev).
		Msg("agent record written")
	r.cleanLogs()
	return nil
}

// putAgentRecord writes the term's AgentRecord. The first call stamps
// started_at_ns; a later call, after LogReady, keeps that stamp and refreshes
// log_addr from LogAddr (E14-D021).
func (r *run[I, S, L]) putAgentRecord(ctx context.Context) error {
	if r.startedAt == 0 {
		r.startedAt = r.now().UnixNano()
	}
	t := r.cfg.Term
	rec := &statev1.AgentRecord{
		Plane: string(t.Plane()), Instance: t.Instance(), Epoch: t.Epoch(),
		Release: r.cfg.Release, StartedAtNs: r.startedAt, Host: r.host, WorkDir: r.cfg.WorkDir,
	}
	if r.cfg.LogAddr != nil {
		rec.LogAddr = r.cfg.LogAddr()
	}
	return t.PutRecord(ctx, encode(rec))
}

// step is one reconcile: the demand, the plans, the actions run in order, and WorkerStatus. It returns when the
// Reconciler wants to look again.
func (r *run[I, S, L]) step(ctx context.Context) time.Time {
	now := r.now()
	d := r.intake.Demand()
	stale := d.Stale(now, r.cfg.StaleAfter)
	if stale != r.stale {
		r.stale = stale
		ev := r.log.Info()
		if stale {
			ev = r.log.Warn()
		}
		ev.Bool("stale", stale).Time("last_contact", d.LastContact).Msg("demand staleness changed")
	}
	for k, err := range d.Rejected {
		r.log.Debug().Str("key", k).Err(err).Msg("demand entry rejected")
	}
	observed := r.cfg.Fleet.Observe()
	if !stale && r.cfg.Commit != nil {
		if err := r.cfg.Commit(ctx, d, observed); err != nil {
			r.log.Warn().Err(err).Msg("commit failed")
		}
	}
	plans := r.cfg.Planner.Plan(d)
	r.plans = plans
	acts, wake := r.rec.Step(now, stale, plans, r.cfg.Fleet.Observe())
	for _, a := range acts {
		if ctx.Err() != nil {
			return wake
		}
		if err := r.act(ctx, a); err != nil {
			r.rec.Failed(r.now(), a.Key)
			r.log.Warn().Err(err).Str("worker", string(a.Key)).Stringer("action", a.Kind).Msg("action failed")
		}
	}
	if !stale && ctx.Err() == nil { // the writes would wait on a cluster the agent cannot reach; they catch up later
		observed := r.cfg.Fleet.Observe()
		if err := r.writeStatuses(ctx, plans, observed); err != nil {
			r.log.Warn().Err(err).Msg("worker status not written")
		}
		if r.cfg.Sync != nil {
			if err := r.cfg.Sync(ctx, d, observed, r.rec.GaveUp); err != nil {
				r.log.Warn().Err(err).Msg("sync failed")
			}
		}
	}
	return wake
}

func (r *run[I, S, L]) act(ctx context.Context, a agent.Action[S, L]) error {
	f := r.cfg.Fleet
	log := r.log.Info().Str("worker", string(a.Key)).Stringer("action", a.Kind)
	switch a.Kind {
	case agent.ActSpawn:
		log.Str("spec", a.Spec.Hash()).Msg("spawn")
		return f.Spawn(ctx, a.Key, a.Spec, a.Live)
	case agent.ActStart:
		log.Msg("start")
		return f.Start(ctx, a.Key, a.Spec)
	case agent.ActPush:
		log.Str("live", a.Live.Hash()).Msg("push")
		return f.Push(ctx, a.Key, a.Live)
	case agent.ActStop:
		log.Str("reason", a.Reason).Msg("stop")
		return f.Stop(ctx, a.Key, a.Reason)
	case agent.ActRelease:
		log.Msg("release")
		return f.Release(ctx, a.Key)
	}
	return fmt.Errorf("agent: unknown action %d", a.Kind)
}

// writeStatuses writes the WorkerStatus of every observed worker, and deletes those of workers that are neither
// observed nor planned: released, or lost and no longer wanted (W7.1). A planned worker the Fleet does not have
// (waiting to be spawned again) keeps the status it had, and so does one whose shim could not be attached.
func (r *run[I, S, L]) writeStatuses(ctx context.Context, plans map[agent.WorkerKey]agent.Plan[S, L],
	observed map[agent.WorkerKey]agent.Observed) error {
	want := map[string]string{}
	for k, o := range observed {
		_, planned := plans[k]
		st := statusOf(o, planned, r.rec.GaveUp(k), r.rec.Restarts(k))
		if r.cfg.Conditions != nil {
			if conds := r.cfg.Conditions(o); len(conds) > 0 {
				st.Conditions = conds
			}
		}
		want[r.st.key(k)] = encode(st)
	}
	keep := map[string]bool{}
	for k := range plans {
		keep[r.st.key(k)] = true
	}
	for _, h := range r.cfg.Fleet.Unreachable() {
		keep[state.WorkerStatusKey(r.cfg.Term.Plane(), r.cfg.Term.Instance(), h)] = true
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultWriteTimeout)
	defer cancel()
	return r.st.sync(ctx, want, func(key string) bool { return keep[key] })
}

// cleanLogs deletes MD/TD log files whose worker key no longer appears and whose mtime is strictly
// older than LogRetain (E12-D041). A failure is logged and the term continues. LogRetain <= 0
// does nothing, which is the STS and controller path.
func (r *run[I, S, L]) cleanLogs() {
	if r.cfg.LogRetain <= 0 || r.cfg.WorkDir == "" {
		return
	}
	keep, err := r.logKeep()
	if err != nil {
		r.log.Warn().Err(err).Msg("log cleanup")
		return
	}
	dir := shim.LogDir(r.cfg.WorkDir, string(r.cfg.Term.Plane()))
	if err := shim.CleanAbsentLogs(dir, r.cfg.LogRetain, r.now(), keep); err != nil {
		r.log.Warn().Err(err).Msg("log cleanup")
	}
}

// logKeep is the set of worker hashes that still appear: observed, planned, still holding a
// WorkerStatus, unreachable shims, and live sockets. A socket directory that cannot be read
// aborts the cleanup; a missing one does not.
func (r *run[I, S, L]) logKeep() (map[string]struct{}, error) {
	keep := map[string]struct{}{}
	add := func(h string) {
		if h != "" {
			keep[h] = struct{}{}
		}
	}
	for k := range r.cfg.Fleet.Observe() {
		add(state.WorkerHash(string(k)))
	}
	for _, h := range r.cfg.Fleet.Unreachable() {
		add(h)
	}
	for k := range r.plans {
		add(state.WorkerHash(string(k)))
	}
	prefix := state.WorkerStatusPrefix(r.cfg.Term.Plane(), r.cfg.Term.Instance())
	for key := range r.st.held {
		rest, ok := strings.CutPrefix(key, prefix)
		if ok {
			add(rest)
		}
	}
	socks, err := shim.SocketHashes(shim.RunDir(r.cfg.WorkDir, string(r.cfg.Term.Plane())))
	if err != nil {
		return nil, err
	}
	for h := range socks {
		add(h)
	}
	return keep, nil
}
