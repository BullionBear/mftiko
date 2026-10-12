package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// DefaultLinger is how long an atom with no owners stays subscribed (D905).
const DefaultLinger = 5 * time.Second

// writeTimeout bounds one etcd read or write of placement and acks.
const writeTimeout = 5 * time.Second

// Config is one MD agent.
type Config struct {
	Topology state.Topology
	Instance string
	Dial     state.DialFunc
	Term     state.TermOptions

	WorkDir   string
	Shim      string // mftiko-shim
	Conn      string // mftiko-md-conn, the binary every new spawn runs (P9)
	ShimEnv   []string
	WorkerEnv []string // MFTIKO_NATS_URL and MFTIKO_NATS_CREDS for the conn worker
	// JournalDir is where each conn worker appends a journal. Tests set it; production leaves it empty.
	JournalDir string
	// ExtraEnv is appended to one worker's environment at launch. Tests use it to crash the first spawn.
	ExtraEnv func(agent.WorkerKey) []string

	Markets   Markets
	Overrides map[string]string // venue -> Endpoint override; empty uses the venue default

	Policy     agent.Policy
	Linger     time.Duration // DefaultLinger if <= 0
	Resync     time.Duration
	StaleAfter time.Duration
	LiveRetry  time.Duration
	Mirror     state.MirrorOptions
	Release    string
	Started    func(pid int)
	Log        zerolog.Logger
	Now        func() time.Time
}

// Run is the agent's main: connect, win the term, mirror demand, and reconcile until the term
// ends or ctx is done. Conn workers outlive the process (D102).
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.Linger <= 0 {
		cfg.Linger = DefaultLinger
	}
	cl, term, err := runner.Connect(ctx, cfg.Topology, state.PlaneMD, cfg.Instance, cfg.Dial, cfg.Term)
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
	m, err := state.NewMirror(reader, state.DemandPrefix(state.PlaneMD, cfg.Instance), mopt)
	if err != nil {
		return err
	}
	mctx, stop := context.WithCancel(ctx)
	defer stop()
	go m.Run(mctx) //nolint:errcheck // ends with mctx
	numbers, err := state.LoadIncarnations(ctx, term)
	if err != nil {
		return err
	}
	epochs := &epochTable{}
	prog := &Program{
		Conn: cfg.Conn, Instance: cfg.Instance, Env: cfg.WorkerEnv, JournalDir: cfg.JournalDir,
		ExtraEnv: cfg.ExtraEnv, Epoch: term.Epoch, epochs: epochs,
	}
	f := fleet.New(fleet.Config[Spec, Live]{
		Plane: state.PlaneMD, Instance: cfg.Instance, Epoch: term.Epoch(), Release: cfg.Release,
		WorkDir: cfg.WorkDir, Shim: cfg.Shim, ShimEnv: cfg.ShimEnv, Numbers: numbers, Program: prog,
		Started: cfg.Started, Log: cfg.Log,
	})
	defer f.Close()
	policy := cfg.Policy
	if policy == nil {
		policy = agent.Always{Backoff: agent.Backoff{Min: time.Second, Max: 30 * time.Second}, Beat: 30 * time.Second}
	}
	p := &plane{
		inst: cfg.Instance, linger: cfg.Linger, markets: cfg.Markets,
		overrides: cfg.Overrides, get: reader, w: term, log: cfg.Log, now: cfg.Now,
	}
	return runner.Run(ctx, runner.Config[Entry, Spec, Live]{
		Term: term, Demand: m, Statuses: reader, Decode: Decode, Planner: p,
		Policy: policy, Fleet: &reporting{Fleet: f, epochs: epochs},
		Commit: p.commit, Sync: p.sync, Release: cfg.Release,
		Resync: cfg.Resync, StaleAfter: cfg.StaleAfter, LiveRetry: cfg.LiveRetry, Log: cfg.Log, Now: cfg.Now,
	})
}

func (cfg Config) validate() error {
	switch {
	case cfg.Instance == "" || cfg.WorkDir == "" || cfg.Shim == "" || cfg.Conn == "":
		return errors.New("md: agent config incomplete")
	case cfg.Markets == nil:
		return errors.New("md: agent config incomplete")
	default:
		return nil
	}
}

// registryMarkets adapts the exchange registry.
type registryMarkets struct{ r *exchange.Registry }

// MarketsFrom is the registry as a Markets.
func MarketsFrom(r *exchange.Registry) Markets { return registryMarkets{r} }

func (m registryMarkets) Market(venue string) (exchange.Market, bool) {
	if m.r == nil {
		return nil, false
	}
	v, ok := m.r.Venue(venue)
	if !ok || v.Market == nil {
		return nil, false
	}
	return v.Market, true
}

type writer interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) error
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error
}

type reader interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

// plane is the commit of one term: placements, the owner set, and the plan the reconciler pushes.
type plane struct {
	inst      string
	linger    time.Duration
	markets   Markets
	overrides map[string]string
	get       reader
	w         writer
	log       zerolog.Logger
	now       func() time.Time

	loaded   bool
	placed   map[string]string // feed hash -> conn, as etcd holds it
	acks     map[string]string
	snap     snapshot
	outcomes map[string]outcome
	plans    map[agent.WorkerKey]agent.Plan[Spec, Live]
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

// commit places the demand, writes placements, then publishes the plan. A failure leaves the
// previous plan in place. The etcd write happens before that publish (D110). Symbol status is
// the conn worker's concern.
func (p *plane) commit(ctx context.Context, d agent.Demand[Entry], _ map[agent.WorkerKey]agent.Observed) error {
	if !p.loaded {
		if err := p.load(ctx); err != nil {
			return err
		}
		p.loaded = true
	}
	next := assign(p.inst, p.clock(), p.linger, p.markets, p.overrides, feedsOf(d), p.snap)
	if err := p.persist(ctx, next.Atoms); err != nil {
		return err
	}
	p.snap = snapshot{Atoms: next.Atoms, Linger: next.Linger}
	p.outcomes, p.plans = next.Outcomes, next.Plans
	p.log.Debug().Int("feeds", len(next.Atoms)).Int("conns", len(next.Plans)).Msg("placement committed")
	return nil
}

// feedsOf is the demand this commit places. A key Decode rejected is a reject, not a feed.
func feedsOf(d agent.Demand[Entry]) []feedIn {
	var feeds []feedIn
	for _, k := range slices.Sorted(maps.Keys(d.Items)) {
		feeds = append(feeds, feedIn{Key: k, Entry: d.Items[k]})
	}
	for k, err := range d.Rejected {
		in := feedIn{Key: k, Reject: err.Error()}
		if _, entry, ok := splitDemand(k); ok {
			if kind, feed, ok := strings.Cut(entry, "/"); ok && kind == "feed" {
				in.Entry.Feed = feed
			}
		}
		feeds = append(feeds, in)
	}
	return feeds
}

func (p *plane) load(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	p.placed = map[string]string{}
	p.snap.ByHash = map[string]string{}
	prefix := state.PlacementPrefix(p.inst)
	resp, err := p.get.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("md: placements: %w", err)
	}
	for _, kv := range resp.Kvs {
		h := strings.TrimPrefix(string(kv.Key), prefix)
		msg := &statev1.Placement{}
		if h == "" || proto.Unmarshal(kv.Value, msg) != nil || msg.GetConn() == "" {
			continue
		}
		p.placed[h] = msg.GetConn()
		p.snap.ByHash[h] = msg.GetConn()
	}
	p.acks = map[string]string{}
	ap := state.MDAckPrefix(p.inst)
	aresp, err := p.get.Get(ctx, ap, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("md: acks: %w", err)
	}
	for _, kv := range aresp.Kvs {
		p.acks[string(kv.Key)] = string(kv.Value)
	}
	return nil
}

func (p *plane) persist(ctx context.Context, atoms map[string]placed) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	want := map[string]string{}
	for id, a := range atoms {
		if a.Conn == "" {
			continue
		}
		want[state.WorkerHash(id)] = a.Conn
	}
	for h, conn := range want {
		if p.placed[h] == conn {
			continue
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(&statev1.Placement{Conn: conn})
		if err != nil {
			return err
		}
		if err := p.w.Put(ctx, state.PlacementKey(p.inst, h), string(b)); err != nil {
			return fmt.Errorf("md: placement %s: %w", h, err)
		}
		p.placed[h] = conn
	}
	for h := range p.placed {
		if _, ok := want[h]; ok {
			continue
		}
		if err := p.w.Delete(ctx, state.PlacementKey(p.inst, h)); err != nil {
			return fmt.Errorf("md: delete placement %s: %w", h, err)
		}
		delete(p.placed, h)
	}
	return nil
}
