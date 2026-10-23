package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// writeTimeout bounds one etcd read or write of the acks.
const writeTimeout = 5 * time.Second

// Config is one TD agent.
type Config struct {
	Topology state.Topology
	Instance string
	Dial     state.DialFunc
	Term     state.TermOptions

	WorkDir   string
	Shim      string // mftiko-shim
	Account   string // mftiko-td-account, the binary every new spawn runs (P9)
	ShimEnv   []string
	WorkerEnv []string // MFTIKO_NATS_URL: the worker's NATS credentials come in Init (D611)
	// ExtraEnv is appended to one worker's environment at launch. Tests use it.
	ExtraEnv func(agent.WorkerKey) []string

	Kinds     Kinds
	Endpoints map[string]Endpoint // by account kind (D616)
	Open      Opener              // checks sealed credentials, at plan time
	Unseal    Unsealer            // opens them, at spawn
	User      NATSUser            // signs the worker's NATS user, at spawn

	Policy     agent.Policy // DefaultPolicy if nil
	Linger     time.Duration
	Resync     time.Duration
	StaleAfter time.Duration
	Mirror     state.MirrorOptions
	Release    string
	Started    func(pid int)
	Log        zerolog.Logger
	Now        func() time.Time
}

func (cfg Config) validate() error {
	switch {
	case cfg.Instance == "" || cfg.WorkDir == "" || cfg.Shim == "" || cfg.Account == "":
		return errors.New("td: agent config incomplete")
	case cfg.Kinds == nil || cfg.Open == nil || cfg.Unseal == nil || cfg.User == nil:
		return errors.New("td: agent config incomplete")
	}
	return nil
}

// Run is the agent's main: connect, win the term, mirror demand and accounts, and reconcile until
// the term ends or ctx is done. Account workers outlive the process (D102).
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.Linger <= 0 {
		cfg.Linger = DefaultLinger
	}
	cl, term, err := runner.Connect(ctx, cfg.Topology, state.PlaneTD, cfg.Instance, cfg.Dial, cfg.Term)
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
	demand, err := state.NewMirror(reader, state.DemandPrefix(state.PlaneTD, cfg.Instance), mopt)
	if err != nil {
		return err
	}
	accounts, err := state.NewMirror(reader, state.AccountPrefix(cfg.Instance), mopt)
	if err != nil {
		return err
	}
	mctx, stop := context.WithCancel(ctx)
	defer stop()
	go demand.Run(mctx)   //nolint:errcheck // ends with mctx
	go accounts.Run(mctx) //nolint:errcheck // ends with mctx
	src := newSources(demand, accounts)
	go src.run(mctx.Done())

	numbers, err := state.LoadIncarnations(ctx, term)
	if err != nil {
		return err
	}
	prog := &Program{
		Account: cfg.Account, Instance: cfg.Instance, Env: cfg.WorkerEnv,
		Unseal: cfg.Unseal, User: cfg.User, ExtraEnv: cfg.ExtraEnv,
	}
	f := fleet.New(fleet.Config[Spec, Live]{
		Plane: state.PlaneTD, Instance: cfg.Instance, Epoch: term.Epoch(), Release: cfg.Release,
		WorkDir: cfg.WorkDir, Shim: cfg.Shim, ShimEnv: cfg.ShimEnv, Numbers: numbers, Program: prog,
		Started: cfg.Started, Log: cfg.Log,
	})
	defer f.Close()
	var policy agent.Policy = DefaultPolicy
	if cfg.Policy != nil {
		policy = cfg.Policy
	}
	p := &plane{
		inst: cfg.Instance, linger: cfg.Linger, kinds: cfg.Kinds, open: cfg.Open, endpoints: cfg.Endpoints,
		get: reader, w: term, log: cfg.Log, now: cfg.Now,
	}
	return runner.Run(ctx, runner.Config[Entry, Spec, Live]{
		Term: term, Demand: src, Statuses: reader, Decode: decoder{inst: cfg.Instance}.Decode, Planner: p,
		Policy: policy, Fleet: f, Commit: p.commit, Sync: p.sync, Release: cfg.Release, WorkDir: cfg.WorkDir,
		Resync: cfg.Resync, StaleAfter: cfg.StaleAfter, Log: cfg.Log, Now: cfg.Now,
	})
}

type writer interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) error
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error
}

type reader interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

// plane is one term's planning and acks.
type plane struct {
	inst      string
	linger    time.Duration
	kinds     Kinds
	open      Opener
	endpoints map[string]Endpoint
	get       reader
	w         writer
	log       zerolog.Logger
	now       func() time.Time

	loaded    bool
	acks      map[string]string
	lingering map[int64]time.Time
	specs     map[int64]Spec
	outcomes  map[string]outcome
	plans     map[agent.WorkerKey]agent.Plan[Spec, Live]
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

// commit plans from the demand and the accounts. TD writes nothing before the plan: a restart
// rebuilds the owners from the demand, and a linger in progress starts over, which is harmless.
func (p *plane) commit(ctx context.Context, d agent.Demand[Entry], _ map[agent.WorkerKey]agent.Observed) error {
	if !p.loaded {
		if err := p.load(ctx); err != nil {
			return err
		}
		p.loaded = true
	}
	out := plan(planIn{
		now: p.clock(), linger: p.linger, kinds: p.kinds, open: p.open, endpoints: p.endpoints,
		demand: d, lingering: p.lingering, prev: p.specs,
	})
	p.lingering, p.specs, p.outcomes, p.plans = out.lingering, out.specs, out.outcomes, out.plans
	return nil
}

// load reads the acks this instance wrote, so the first sync writes only what changed.
func (p *plane) load(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	p.acks = map[string]string{}
	resp, err := p.get.Get(ctx, state.TDAckPrefix(p.inst), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("td: acks: %w", err)
	}
	for _, kv := range resp.Kvs {
		p.acks[string(kv.Key)] = string(kv.Value)
	}
	return nil
}
