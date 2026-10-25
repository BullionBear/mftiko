package daemon

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Defaults of Config. Zero selects them (E13-D018, E13-D010).
const (
	// DefaultStaleAfter is how old a region's LastContact may be before the
	// view marks the region stale (E13-D006, W9). It matches the agent's default.
	DefaultStaleAfter = 15 * time.Second
	// DefaultResync is how often the reconcile loop runs with nothing else
	// waking it (E13-D010). A mirror change does not wait for it (E13-D019).
	DefaultResync = 60 * time.Second
)

// Stats counts the reconcile loop.
type Stats struct {
	Rounds         int // rounds finished
	Full           int // rounds the resync timer, or the election round, asked for
	Undecodable    int // keys in the current view
	DecodeFailures int // key versions that failed to decode, each counted once
}

// intake watches one mirror per instance and publishes the global view.
type intake struct {
	ctx    context.Context
	cancel context.CancelFunc
	term   *state.Term
	cl     *state.Clusters
	log    zerolog.Logger
	now    func() time.Time
	opt    state.MirrorOptions
	planes map[string]controller.Placement
	// derive runs after each snapshot. The bool is the election round or a
	// resync tick (E13-D010): orphan removal runs only then. The time is the next
	// deps deadline, or zero when nothing is waiting (E13-D031). Nil until Start
	// wires it, which is before the loop starts.
	derive func(controller.View, bool) time.Time

	staleAfter time.Duration
	resync     time.Duration
	regions    []string
	byRegion   map[string][]*watched
	// retired watches /mftiko/v1/retired/ in each region. It is not in byRegion:
	// writable waits on instance mirrors only, so an unsynced retirement mirror
	// does not stall demands (E12-D034). An unsynced mirror contributes no names,
	// which is the behavior of a cluster that has no retirement key.
	retired map[string]*watched

	dirty atomic.Bool
	wake  chan struct{}

	wg sync.WaitGroup

	mu         sync.Mutex
	view       controller.View
	stats      Stats
	logged     map[string]int64                  // undecodable key -> mod revision last warned
	drifted    map[string]int64                  // session ref -> spec revision last warned (E13-D022)
	issued     map[string]int64                  // session demand key -> issued_at_ns this term already wrote (E13-D002)
	posted     map[string]*statev1.SessionDemand // session demand key -> last value this term posted (E13-D037)
	prevStale  map[string]bool
	minBackoff time.Duration
	maxBackoff time.Duration
}

type watched struct {
	plane  state.Plane
	name   string
	region string
	prefix string

	mirror atomic.Pointer[state.Mirror]
	synced atomic.Bool
}

func newIntake(parent context.Context, term *state.Term, cl *state.Clusters, cfg Config, now func() time.Time, log zerolog.Logger) *intake {
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	if cfg.Resync == 0 {
		cfg.Resync = DefaultResync
	}
	ctx, cancel := context.WithCancel(parent)
	opt := cfg.Mirror
	in := &intake{
		ctx: ctx, cancel: cancel, term: term, cl: cl, log: log, now: now, opt: opt,
		staleAfter: cfg.StaleAfter, resync: cfg.Resync,
		regions:   slices.Sorted(maps.Keys(cfg.Topology.Regions)),
		byRegion:  map[string][]*watched{},
		retired:   map[string]*watched{},
		wake:      make(chan struct{}, 1),
		logged:    map[string]int64{},
		drifted:   map[string]int64{},
		issued:    map[string]int64{},
		posted:    map[string]*statev1.SessionDemand{},
		prevStale: map[string]bool{},
		planes:    placements(cfg),
		view: controller.View{
			Regions: map[string]controller.Region{}, Sessions: map[string]controller.Session{},
			Demands: map[string]controller.Demand{}, Acks: map[string]controller.Ack{},
			Workers: map[string]controller.Worker{}, Agents: map[string]controller.Agent{},
		},
	}
	if in.opt.Log == nil {
		in.opt.Log = &in.log
	}
	in.minBackoff = in.opt.MinBackoff
	if in.minBackoff == 0 {
		in.minBackoff = state.DefaultMirrorMinBackoff
	}
	in.maxBackoff = in.opt.MaxBackoff
	if in.maxBackoff == 0 {
		in.maxBackoff = state.DefaultMirrorMaxBackoff
	}
	if in.maxBackoff < in.minBackoff {
		in.maxBackoff = in.minBackoff
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Topology.Instances)) {
		if name == state.ControllerInstance {
			continue
		}
		w := &watched{
			plane: cfg.Planes[name], name: name, region: cfg.Topology.Instances[name],
		}
		w.prefix = instancePrefix(w.plane, name)
		in.byRegion[w.region] = append(in.byRegion[w.region], w)
	}
	for _, region := range in.regions {
		in.retired[region] = &watched{name: "retired", region: region, prefix: state.RetiredPrefix}
	}
	return in
}

// placements is the deployment's plane and region of every instance except
// the controller (E13-D017, E13-D027).
func placements(cfg Config) map[string]controller.Placement {
	out := make(map[string]controller.Placement, len(cfg.Topology.Instances))
	for name, region := range cfg.Topology.Instances {
		if name == state.ControllerInstance {
			continue
		}
		out[name] = controller.Placement{Plane: string(cfg.Planes[name]), Region: region}
	}
	return out
}

// start runs the mirrors and the reconcile loop. derive must already be set.
func (in *intake) start() {
	in.wg.Add(1)
	go in.loop()
	for _, ws := range in.byRegion {
		for _, w := range ws {
			in.wg.Add(1)
			go in.runMirror(w)
		}
	}
	for _, region := range in.regions {
		in.wg.Add(1)
		go in.runMirror(in.retired[region])
	}
}

// stop ends the mirrors and the reconcile loop and waits until they return.
func (in *intake) stop() {
	in.cancel()
	in.wg.Wait()
}

// writable reports whether a write to region may be attempted. A region with
// no instances has nothing unread to protect. Any other region waits until
// every instance mirror has finished its first list (E13-D018).
func (in *intake) writable(region string) bool {
	ws := in.byRegion[region]
	if len(ws) == 0 {
		return true
	}
	for _, w := range ws {
		if !w.synced.Load() {
			return false
		}
	}
	return true
}

func (c *Controller) View() controller.View {
	v, _ := c.reading()
	return v
}

func (c *Controller) Stats() Stats {
	_, s := c.reading()
	return s
}

func (c *Controller) reading() (controller.View, Stats) {
	c.intake.mu.Lock()
	defer c.intake.mu.Unlock()
	return c.intake.view, c.intake.stats
}

func (in *intake) loop() {
	defer in.wg.Done()
	tick := time.NewTicker(in.resync)
	defer tick.Stop()
	// deps wakes a round at the earliest deps deadline. A nil channel is never
	// selected, which is the state when nothing is waiting (E13-D031).
	var deps *time.Timer
	var depsC <-chan time.Time
	stopDeps := func() {
		if deps == nil {
			return
		}
		if !deps.Stop() {
			select {
			case <-deps.C:
			default:
			}
		}
		deps = nil
		depsC = nil
	}
	defer stopDeps()
	armDeps := func(at time.Time) {
		stopDeps()
		if at.IsZero() {
			return
		}
		d := at.Sub(in.now())
		if d < 0 {
			d = 0
		}
		deps = time.NewTimer(d)
		depsC = deps.C
	}
	full := true // the election round is a full resync, before any derived write (E13-D010)
	for {
		if in.stopped() {
			return
		}
		// Cleared before the read, so a notify during the round sets it again
		// and the next round runs at once (E13-D019).
		in.dirty.Store(false)
		next := in.reconcile(full)
		full = false
		if in.dirty.Load() {
			select {
			case <-tick.C:
				full = true
			default:
			}
			continue
		}
		armDeps(next)
		select {
		case <-in.ctx.Done():
			return
		case <-in.term.Done():
			return
		case <-in.wake:
		case <-tick.C:
			full = true
		case <-depsC:
		}
	}
}

func (in *intake) stopped() bool {
	select {
	case <-in.ctx.Done():
		return true
	case <-in.term.Done():
		return true
	default:
		return false
	}
}

func (in *intake) reconcile(full bool) time.Time {
	var snaps []controller.Snapshot
	var fails []failure
	for _, name := range in.regions {
		for _, w := range in.byRegion[name] {
			view, contact, synced := w.observe()
			body, bad := classify(w.plane, w.name, view.Entries)
			body.Plane = string(w.plane)
			body.Instance = w.name
			body.Region = w.region
			body.Rev = view.Rev
			body.Synced = synced
			body.LastContact = contact
			snaps = append(snaps, body)
			fails = append(fails, bad...)
		}
	}
	v := controller.Assemble(in.now(), in.staleAfter, in.regions, snaps)
	v.Retired = in.retiredNames()
	in.publish(v, full, fails)
	// The snapshot is the relist. Derivation, the gate and lifecycle run on
	// this round's view, after it is published, so a new leader compares
	// before it writes (N6, E13-D019). full selects the orphan scan (N5, E13-D010).
	if in.derive == nil {
		return time.Time{}
	}
	return in.derive(v, full)
}

// retiredNames is the instances whose home-region mirror has finished its first
// list and contains /retired/{inst}. A mirror that has not synced contributes
// nothing, so a missing retirement watch does not end sessions (E12-D034).
func (in *intake) retiredNames() map[string]struct{} {
	out := map[string]struct{}{}
	for region, w := range in.retired {
		view, _, synced := w.observe()
		if !synced {
			continue
		}
		for key := range view.Entries {
			inst, ok := state.RetiredInstance(key)
			if !ok {
				continue
			}
			if in.planes[inst].Region != region {
				continue
			}
			out[inst] = struct{}{}
		}
	}
	return out
}

func (w *watched) observe() (state.View, time.Time, bool) {
	m := w.mirror.Load()
	if m == nil {
		return state.View{}, time.Time{}, false
	}
	select {
	case <-m.Synced():
		w.synced.Store(true)
	default:
	}
	return m.Snapshot(), m.LastContact(), w.synced.Load()
}

func (in *intake) publish(v controller.View, full bool, fails []failure) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.noteFailures(fails, len(v.Undecodable))
	in.noteStale(v)
	in.view = v
	in.stats.Rounds++
	if full {
		in.stats.Full++
	}
	in.stats.Undecodable = len(v.Undecodable)
}

func (in *intake) noteFailures(fails []failure, n int) {
	cur := make(map[string]int64, len(fails))
	for _, f := range fails {
		cur[f.key] = f.mod
		if prev, ok := in.logged[f.key]; ok && prev == f.mod {
			continue
		}
		in.logged[f.key] = f.mod
		in.stats.DecodeFailures++
		in.log.Warn().Str("key", f.key).Int64("mod_revision", f.mod).Err(f.err).
			Int("undecodable", n).Msg("undecodable value")
	}
	for k := range in.logged {
		if _, ok := cur[k]; !ok {
			delete(in.logged, k)
		}
	}
}

// noteDrifts logs each session once per spec revision while its assignment
// stays on an older revision (E13-D022). A later revision logs again; a session
// that stops drifting is forgotten, so a repeat of the same revision logs too.
func (in *intake) noteDrifts(ds []controller.Drift) {
	in.mu.Lock()
	defer in.mu.Unlock()
	cur := make(map[string]int64, len(ds))
	for _, d := range ds {
		ref := controller.SessionRef(d.Instance, d.Session)
		cur[ref] = d.SpecRev
		if prev, ok := in.drifted[ref]; ok && prev == d.SpecRev {
			continue
		}
		in.drifted[ref] = d.SpecRev
		in.log.Warn().Str("instance", d.Instance).Str("session", d.Session).
			Int64("spec_revision", d.SpecRev).Int64("assignment_revision", d.Have).
			Msg("spec revision changed; demands left as stored")
	}
	for ref := range in.drifted {
		if _, ok := cur[ref]; !ok {
			delete(in.drifted, ref)
		}
	}
}

func (in *intake) noteStale(v controller.View) {
	for _, name := range slices.Sorted(maps.Keys(v.Regions)) {
		r := v.Regions[name]
		prev, ok := in.prevStale[name]
		if ok && prev == r.Stale {
			continue
		}
		in.prevStale[name] = r.Stale
		ev := in.log.Info()
		if r.Stale {
			ev = in.log.Warn()
		}
		ev.Str("region", name).Bool("stale", r.Stale).Bool("synced", r.Synced).
			Int64("rev", r.Rev).Msg("region staleness")
	}
}

func (in *intake) kick() {
	in.dirty.Store(true)
	select {
	case in.wake <- struct{}{}:
	default:
	}
}

func (in *intake) runMirror(w *watched) {
	defer in.wg.Done()
	bo := in.minBackoff
	for {
		if in.stopped() {
			return
		}
		r, err := in.cl.Reader(w.region)
		if err != nil {
			if in.stopped() {
				return
			}
			in.log.Warn().Err(err).Str("region", w.region).Str("instance", w.name).Msg("mirror dial")
			if !in.sleep(bo) {
				return
			}
			bo = min(bo*2, in.maxBackoff)
			continue
		}
		m, err := state.NewMirror(r, w.prefix, in.opt)
		if err != nil {
			in.log.Error().Err(err).Str("instance", w.name).Msg("mirror")
			return
		}
		w.mirror.Store(m)
		in.wg.Add(1)
		go in.watchNotify(m)
		_ = m.Run(in.ctx)
		return
	}
}

func (in *intake) watchNotify(m *state.Mirror) {
	defer in.wg.Done()
	for {
		select {
		case <-in.ctx.Done():
			return
		case <-in.term.Done():
			return
		case <-m.Notify():
			in.kick()
		}
	}
}

func (in *intake) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-in.ctx.Done():
		return false
	case <-in.term.Done():
		return false
	}
}
