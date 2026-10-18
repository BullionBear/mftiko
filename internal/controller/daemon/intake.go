package daemon

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Defaults of Config. Zero selects them (D1318, D1310).
const (
	// DefaultStaleAfter is how old a region's LastContact may be before the
	// view marks the region stale (D1306, W9). It matches the agent's default.
	DefaultStaleAfter = 15 * time.Second
	// DefaultResync is how often the reconcile loop runs with nothing else
	// waking it (D1310). A mirror change does not wait for it (D1319).
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

	staleAfter time.Duration
	resync     time.Duration
	regions    []string
	byRegion   map[string][]*watched

	dirty atomic.Bool
	wake  chan struct{}

	wg sync.WaitGroup

	mu         sync.Mutex
	view       controller.View
	stats      Stats
	logged     map[string]int64 // undecodable key -> mod revision last warned
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
		wake:      make(chan struct{}, 1),
		logged:    map[string]int64{},
		prevStale: map[string]bool{},
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
	in.wg.Add(1)
	go in.loop()
	for _, ws := range in.byRegion {
		for _, w := range ws {
			in.wg.Add(1)
			go in.runMirror(w)
		}
	}
	return in
}

// stop ends the mirrors and the reconcile loop and waits until they return.
func (in *intake) stop() {
	in.cancel()
	in.wg.Wait()
}

// writable reports whether a write to region may be attempted. A region with
// no instances has nothing unread to protect. Any other region waits until
// every instance mirror has finished its first list (D1318).
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
	full := true // the election round is a full resync, before any derived write (D1310)
	for {
		if in.stopped() {
			return
		}
		// Cleared before the read, so a notify during the round sets it again
		// and the next round runs at once (D1319).
		in.dirty.Store(false)
		in.reconcile(full)
		full = false
		if in.dirty.Load() {
			select {
			case <-tick.C:
				full = true
			default:
			}
			continue
		}
		select {
		case <-in.ctx.Done():
			return
		case <-in.term.Done():
			return
		case <-in.wake:
		case <-tick.C:
			full = true
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

func (in *intake) reconcile(full bool) {
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
	in.publish(v, full, fails)
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
