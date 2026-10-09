// Package runner runs an agent's term: it waits for the demand, has the Fleet adopt the workers its shims kept,
// brings WorkerStatus in line, writes the AgentRecord, then reconciles in one loop until the term ends (D101,
// PROTOCOL §3.2 W7). The decisions are package agent's Reconciler; this package does the I/O around it.
package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Defaults of Config.
const (
	// DefaultStaleAfter is how old the demand may be before the agent stops changing anything by it (DECISIONS
	// D109): 3 of the Mirror's probe intervals, which is how far LastContact lags while all is well.
	DefaultStaleAfter = 15 * time.Second
	// DefaultResync is the longest the loop sleeps with nothing to wake it: staleness comes and goes with no
	// notification, and the actions are recomputed from scratch each time, at the cost of a diff in memory.
	DefaultResync = time.Second
	// DefaultWriteTimeout bounds each etcd write the loop makes.
	DefaultWriteTimeout = 5 * time.Second
)

// ErrTermEnded is what Run returns when the term ended: session lost, superseded, or closed, or a shim preempted
// the agent for a newer term. It wraps the cause. The agent exits and systemd starts it again; the next process
// campaigns for a new epoch (E1-05).
var ErrTermEnded = errors.New("agent: term ended")

// Fleet is the set of shim-backed workers (D101). Its calls start an action and return: Observe reflects each one
// from the moment it returns (a spawned worker is initializing, a stopped one stopping), and the rest comes as
// the shims report it, signalled on Changed. A call must not block for long: the agent has one loop.
type Fleet[S, L agent.Hashed] interface {
	// Adopt finds the workers this instance's shims kept and attaches to each (§4.2); it is called once, before
	// anything else, and returns once Observe has them all.
	Adopt(ctx context.Context) error
	Observe() map[agent.WorkerKey]agent.Observed
	Changed() <-chan struct{}
	// Spawn starts a new incarnation of key with spec and live, and sends Init. A worker of key that ended is
	// released first (delete-before-create, D106).
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

	Release     string        // AgentRecord.release
	StaleAfter  time.Duration // DefaultStaleAfter if 0
	Resync      time.Duration // DefaultResync if 0
	StableAfter time.Duration // agent.DefaultStableAfter if 0
	Retry       agent.Backoff // agent.DefaultRetry if zero
	LiveRetry   time.Duration // agent.DefaultLiveRetry if 0
	Log         zerolog.Logger
	Now         func() time.Time // time.Now if nil
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
	cfg    Config[I, S, L]
	log    zerolog.Logger
	now    func() time.Time
	intake *Intake[I]
	rec    *agent.Reconciler[S, L]
	st     *statuses
	stale  bool
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
	t := cfg.Term
	return &run[I, S, L]{
		cfg: cfg, now: now,
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
	r.log.Info().Msg("agent reconciling")
	for {
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
		}
		t.Stop()
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
	plans := r.cfg.Planner.Plan(d)
	observed := r.cfg.Fleet.Observe()
	if err := r.writeStatuses(ctx, plans, observed); err != nil {
		return err
	}
	if r.cfg.Prepare != nil {
		if err := r.cfg.Prepare(ctx, d, observed); err != nil {
			return fmt.Errorf("agent: prepare: %w", err)
		}
	}
	t := r.cfg.Term
	rec := &statev1.AgentRecord{Plane: string(t.Plane()), Instance: t.Instance(), Epoch: t.Epoch(),
		Release: r.cfg.Release, StartedAtNs: r.now().UnixNano()}
	if err := t.PutRecord(ctx, encode(rec)); err != nil {
		return fmt.Errorf("agent: put record: %w", err)
	}
	r.log.Info().Int("workers", len(observed)).Int("planned", len(plans)).Int64("demand_rev", d.Rev).
		Msg("agent record written")
	return nil
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
	plans := r.cfg.Planner.Plan(d)
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
		if err := r.writeStatuses(ctx, plans, r.cfg.Fleet.Observe()); err != nil {
			r.log.Warn().Err(err).Msg("worker status not written")
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
		want[r.st.key(k)] = encode(statusOf(o, planned, r.rec.GaveUp(k), r.rec.Restarts(k)))
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
