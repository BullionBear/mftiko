package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// writeTimeout bounds one pass of SessionStatus writes.
const writeTimeout = 5 * time.Second

// Config is one STS agent.
type Config struct {
	Topology state.Topology
	Instance string
	Dial     state.DialFunc
	Term     state.TermOptions

	WorkDir   string
	Shim      string // mftiko-shim
	Worker    string // the executable every new spawn runs (D943)
	ShimEnv   []string
	WorkerEnv []string
	// ExtraEnv is appended to one worker's environment at launch. Tests use it.
	ExtraEnv func(agent.WorkerKey) []string

	// Registry, RegistryCreds, DataRepository, PlainHTTP, and MaxArtifactBytes
	// are the deployment's registry settings (D910, D913, D948). This ticket
	// parses them and does not pull. RegistryCreds is a filesystem path; the
	// file is not read.
	Registry         string
	RegistryCreds    string
	DataRepository   string
	PlainHTTP        bool
	MaxArtifactBytes int64
	RecvWindowMS     int
	HookTimeoutS     int
	StartTimeoutS    int
	VolumeMB         int
	Grace            time.Duration
	MaxRestarts      int
	RestartWindowS   int

	Policy     agent.Policy
	Resync     time.Duration
	StaleAfter time.Duration
	Mirror     state.MirrorOptions
	Release    string
	Started    func(pid int)
	Log        zerolog.Logger
	Now        func() time.Time
}

func (cfg Config) validate() error {
	if cfg.Instance == "" || cfg.WorkDir == "" || cfg.Shim == "" || cfg.Worker == "" {
		return errors.New("sts: agent config incomplete")
	}
	return nil
}

// Run is the agent's main: win the term, mirror spec, assignment, and status,
// and reconcile until the term ends or ctx is done. Workers outlive the process
// (D102).
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cfg.Log.Info().
		Str("registry", cfg.Registry).
		Str("registry_creds", cfg.RegistryCreds).
		Str("data_repository", cfg.DataRepository).
		Bool("plain_http", cfg.PlainHTTP).
		Int64("max_artifact_bytes", cfg.MaxArtifactBytes).
		Int("recv_window_ms", cfg.RecvWindowMS).
		Int("hook_timeout_s", cfg.HookTimeoutS).
		Int("start_timeout_s", cfg.StartTimeoutS).
		Int("volume_mb", cfg.VolumeMB).
		Dur("grace", cfg.Grace).
		Int("max_restarts", cfg.MaxRestarts).
		Int("restart_window_s", cfg.RestartWindowS).
		Msg("deployment config")
	cl, term, err := runner.Connect(ctx, cfg.Topology, state.PlaneSTS, cfg.Instance, cfg.Dial, cfg.Term)
	if err != nil {
		return err
	}
	defer cl.Close()
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		term.Close(cctx) //nolint:errcheck // the record goes with the session either way
		cancel()
	}()
	home, err := runner.Home(cl, cfg.Instance)
	if err != nil {
		return err
	}
	reader, err := cl.Reader(home)
	if err != nil {
		return err
	}
	mopt := cfg.Mirror
	if mopt.Log == nil {
		mopt.Log = &cfg.Log
	}
	specM, err := state.NewMirror(reader, state.SessionSpecPrefix(cfg.Instance), mopt)
	if err != nil {
		return err
	}
	demandM, err := state.NewMirror(reader, state.DemandPrefix(state.PlaneSTS, cfg.Instance), mopt)
	if err != nil {
		return err
	}
	statusM, err := state.NewMirror(reader, state.SessionStatusPrefix(cfg.Instance), mopt)
	if err != nil {
		return err
	}
	mctx, stop := context.WithCancel(ctx)
	defer stop()
	go specM.Run(mctx)   //nolint:errcheck // ends with mctx
	go demandM.Run(mctx) //nolint:errcheck // ends with mctx
	go statusM.Run(mctx) //nolint:errcheck // ends with mctx
	src := newSources(specM, demandM, statusM)
	go src.run(mctx.Done())

	numbers, err := state.LoadIncarnations(ctx, term)
	if err != nil {
		return err
	}
	prog := &Program{Worker: cfg.Worker, Env: cfg.WorkerEnv, ExtraEnv: cfg.ExtraEnv}
	f := fleet.New(fleet.Config[Spec, Live]{
		Plane: state.PlaneSTS, Instance: cfg.Instance, Epoch: term.Epoch(), Release: cfg.Release,
		WorkDir: cfg.WorkDir, Shim: cfg.Shim, ShimEnv: cfg.ShimEnv, Numbers: numbers, Program: prog,
		StopGrace: cfg.Grace, Started: cfg.Started, Log: cfg.Log,
	})
	defer f.Close()
	var policy agent.Policy = DefaultPolicy
	if cfg.Policy != nil {
		policy = cfg.Policy
	}
	p := &plane{
		inst: cfg.Instance, spec: specM, demand: demandM, status: statusM,
		w: term, log: cfg.Log, now: cfg.Now,
		local: map[string]statusView{}, remembered: map[string]demandView{},
		loggedSpec: map[string]bool{}, loggedDemand: map[string]bool{},
	}
	return runner.Run(ctx, runner.Config[Entry, Spec, Live]{
		Term: term, Demand: src, Statuses: reader, Decode: decoder{inst: cfg.Instance}.Decode, Planner: p,
		Policy: policy, Fleet: f, Commit: p.commit, Sync: p.sync, Release: cfg.Release,
		Resync: cfg.Resync, StaleAfter: cfg.StaleAfter, Log: cfg.Log, Now: cfg.Now,
	})
}

type writer interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) error
}

// plane is one term's planning and session status.
type plane struct {
	inst   string
	spec   *state.Mirror
	demand *state.Mirror
	status *state.Mirror
	w      writer
	log    zerolog.Logger
	now    func() time.Time

	plans        map[agent.WorkerKey]agent.Plan[Spec, Live]
	intents      map[string]intent
	hashes       map[string]string
	remembered   map[string]demandView
	local        map[string]statusView
	mirrored     map[string]statusView
	loggedSpec   map[string]bool
	loggedDemand map[string]bool
}

// Plan returns the last commit. A stale step does not recompute it (D109).
func (p *plane) Plan(agent.Demand[Entry]) map[agent.WorkerKey]agent.Plan[Spec, Live] {
	if p.plans == nil {
		return map[agent.WorkerKey]agent.Plan[Spec, Live]{}
	}
	return p.plans
}

func (p *plane) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// commit plans from the mirrors. The runner's demand value is not the input:
// Decode is not given the spec's mod_revision (D947).
func (p *plane) commit(_ context.Context, _ agent.Demand[Entry], observed map[agent.WorkerKey]agent.Observed) error {
	sessions := buildViews(p.inst, p.spec.Snapshot(), p.demand.Snapshot(), p.status.Snapshot(), p.local, p.remembered, observed)
	d := decide(sessions, p.hashes)
	for _, id := range slices.Sorted(maps.Keys(sessions)) {
		s := sessions[id]
		if s.spec != nil {
			delete(p.loggedSpec, id)
		}
		if s.demandErr != nil && !p.loggedDemand[id] {
			p.loggedDemand[id] = true
			p.log.Warn().Str("session", id).Err(s.demandErr).Msg("assignment demand will not decode")
		}
	}
	for _, id := range d.Missing {
		if p.loggedSpec[id] {
			continue
		}
		p.loggedSpec[id] = true
		p.log.Warn().Str("session", id).Msg("assignment has no usable spec")
	}
	for _, b := range d.Bad {
		if p.loggedSpec[b.ID] {
			continue
		}
		p.loggedSpec[b.ID] = true
		p.log.Warn().Str("session", b.ID).Err(b.Err).Msg("assignment has no usable spec")
	}
	for _, id := range d.Changed {
		p.log.Warn().Str("session", id).Msg("spec changed; replacing the incarnation")
	}
	p.plans, p.intents, p.hashes = d.Plans, d.Intents, d.Hashes
	p.remembered = remember(sessions, p.remembered)
	p.mirrored = map[string]statusView{}
	for id, s := range sessions {
		if s.mirrored != nil {
			p.mirrored[id] = *s.mirrored
		}
	}
	return nil
}

// sync writes SessionStatus where the phase or the reason changed (W7). Keys
// are not deleted: the API server removes a terminal session's status (W5).
func (p *plane) sync(ctx context.Context, _ agent.Demand[Entry], observed map[agent.WorkerKey]agent.Observed, _ func(agent.WorkerKey) bool) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	now := p.clock()
	for _, id := range slices.Sorted(maps.Keys(p.intents)) {
		in := p.intents[id]
		var obs *agent.Observed
		if o, ok := observed[WorkerKey(id)]; ok {
			obs = &o
		}
		phase, reason, write := phaseOf(in, obs)
		if !write || p.same(id, phase, reason) {
			continue
		}
		msg := &statev1.SessionStatus{
			ObservedGeneration: in.Rev, Phase: phase, Reason: reason, UpdatedAtNs: now.UnixNano(),
		}
		if obs != nil {
			msg.Incarnation = obs.Incarnation
			if obs.Exit != nil {
				msg.LastExit = proto.Clone(obs.Exit).(*commonv1.ExitInfo)
			}
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return err
		}
		if err := p.w.Put(ctx, state.SessionStatusKey(p.inst, id), string(b)); err != nil {
			return fmt.Errorf("sts: status %s: %w", id, err)
		}
		p.local[id] = statusView{Phase: phase, Reason: reason}
	}
	return nil
}

func (p *plane) same(id string, phase statev1.Phase, reason string) bool {
	if st, ok := p.local[id]; ok {
		return st.Phase == phase && st.Reason == reason
	}
	if st, ok := p.mirrored[id]; ok {
		return st.Phase == phase && st.Reason == reason
	}
	return false
}
