package api

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Prometheus text for this process (E12-D038). Host disk, CPU, and memory are not here.
const (
	metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

	helpAgentUp  = "1 after the region's first sync when this instance's AgentRecord key exists."
	helpLeader   = "1 after the controller region's first sync when its AgentRecord key exists."
	helpPhase    = "WorkerStatus phase counts after the region's first sync. BACKOFF is demand PENDING; EXITED after give-up is demand DEAD; LOST keeps the last entry state."
	helpBus      = "1 when WorkerStatus condition bus_disconnected has status false, 0 when it is true. No series when that condition is absent. mftiko does not time it."
	helpRegion   = "1 when the region has finished its first sync and was contacted within unavailable_after."
	helpPostgres = "1 when a scrape-time ping of the API Postgres pool succeeds."
	helpRegistry = "1 when GET /v2/ on the configured registry returns 200."

	// condBusDisconnected matches md agent's BusDisconnected. The API binary does not import that package.
	condBusDisconnected = "bus_disconnected"
)

// knownPhases are the WorkerStatus phases (state.proto). Each is emitted, including zero.
// EntryStatus PENDING and DEAD are not phases; the HELP line records how they correspond.
var knownPhases = []string{"STARTING", "RUNNING", "STOPPING", "BACKOFF", "EXITED", "LOST"}

// regionObs is one cluster at scrape time. view is read once synced is true.
type regionObs struct {
	name    string
	synced  bool
	contact time.Time
	view    state.View
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	obs := make([]regionObs, len(s.clusters))
	for i, c := range s.clusters {
		view, contact, synced := c.observe()
		obs[i] = regionObs{name: c.name, synced: synced, contact: contact, view: view}
	}
	pg, reg := dependencyGauges(r.Context(), s)
	body := renderMetrics(s.cfg, now, obs, pg, reg)
	w.Header().Set("Content-Type", metricsContentType)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// dependencyGauges probes Postgres and the registry together. A failure is 0 and is not logged:
// a Postgres error can carry the DSN (E12-D038).
func dependencyGauges(ctx context.Context, s *Server) (postgres, registry int) {
	pgCh := make(chan int, 1)
	regCh := make(chan int, 1)
	go func() { pgCh <- postgresGauge(ctx, s) }()
	go func() { regCh <- registryGauge(ctx, s.cfg.Catalog) }()
	return <-pgCh, <-regCh
}

func postgresGauge(ctx context.Context, s *Server) int {
	pool, err := s.db()
	if err != nil {
		return 0
	}
	if err := pool.Ping(ctx); err != nil {
		return 0
	}
	return 1
}

func registryGauge(ctx context.Context, cat *artifact.Catalog) int {
	if err := cat.Reachable(ctx); err != nil {
		return 0
	}
	return 1
}

type agentInst struct {
	name   string
	plane  state.Plane
	region string
}

func isAgentPlane(p state.Plane) bool {
	return p == state.PlaneSTS || p == state.PlaneMD || p == state.PlaneTD
}

// renderMetrics is the Prometheus text. Agent, controller, and worker series appear only after
// that region's first sync, and stay after it goes stale. region_available uses the same test as
// /readyz (E12-D040).
func renderMetrics(cfg Config, now time.Time, obs []regionObs, postgresUp, registryUp int) []byte {
	byRegion := make(map[string]regionObs, len(obs))
	for _, o := range obs {
		byRegion[o.name] = o
	}
	var b strings.Builder
	writeFamily(&b, "mftiko_agent_up", helpAgentUp, agentUpSamples(cfg, byRegion))
	writeFamily(&b, "mftiko_controller_leader", helpLeader, leaderSamples(cfg, byRegion))
	writeFamily(&b, "mftiko_worker_phase", helpPhase, phaseSamples(cfg, byRegion))
	writeFamily(&b, "mftiko_worker_bus_up", helpBus, busSamples(cfg, byRegion))
	writeFamily(&b, "mftiko_region_available", helpRegion, regionSamples(cfg, now, obs))
	writeFamily(&b, "mftiko_postgres_up", helpPostgres, []string{gaugeLine("mftiko_postgres_up", nil, postgresUp)})
	writeFamily(&b, "mftiko_registry_up", helpRegistry, []string{gaugeLine("mftiko_registry_up", nil, registryUp)})
	return []byte(b.String())
}

func writeFamily(b *strings.Builder, name, help string, samples []string) {
	if b.Len() > 0 {
		b.WriteByte('\n')
	}
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
	for _, s := range samples {
		b.WriteString(s)
		b.WriteByte('\n')
	}
}

func agentInstances(cfg Config) []agentInst {
	out := make([]agentInst, 0, len(cfg.Planes))
	for name, plane := range cfg.Planes {
		if !isAgentPlane(plane) {
			continue
		}
		out = append(out, agentInst{name: name, plane: plane, region: cfg.Topology.Instances[name]})
	}
	slices.SortFunc(out, func(a, b agentInst) int {
		if c := strings.Compare(a.name, b.name); c != 0 {
			return c
		}
		if c := strings.Compare(string(a.plane), string(b.plane)); c != 0 {
			return c
		}
		return strings.Compare(a.region, b.region)
	})
	return out
}

func regionReady(by map[string]regionObs, region string) (regionObs, bool) {
	o, ok := by[region]
	if !ok || region == "" || !o.synced {
		return regionObs{}, false
	}
	return o, true
}

func agentUpSamples(cfg Config, by map[string]regionObs) []string {
	var lines []string
	for _, inst := range agentInstances(cfg) {
		o, ok := regionReady(by, inst.region)
		if !ok {
			continue
		}
		up := 0
		if _, present := o.view.Entries[state.AgentKey(inst.plane, inst.name)]; present {
			up = 1
		}
		lines = append(lines, gaugeLine("mftiko_agent_up", [][2]string{
			{"instance", inst.name},
			{"plane", string(inst.plane)},
			{"region", inst.region},
		}, up))
	}
	return lines
}

func leaderSamples(cfg Config, by map[string]regionObs) []string {
	region := cfg.Topology.Instances[state.ControllerInstance]
	o, ok := regionReady(by, region)
	if !ok {
		return nil
	}
	up := 0
	key := state.AgentKey(state.PlaneCtl, state.ControllerInstance)
	if _, present := o.view.Entries[key]; present {
		up = 1
	}
	return []string{gaugeLine("mftiko_controller_leader", [][2]string{
		{"instance", state.ControllerInstance},
		{"region", region},
	}, up)}
}

func phaseSamples(cfg Config, by map[string]regionObs) []string {
	known := map[string]struct{}{}
	for _, p := range knownPhases {
		known[p] = struct{}{}
	}
	var lines []string
	for _, inst := range agentInstances(cfg) {
		o, ok := regionReady(by, inst.region)
		if !ok {
			continue
		}
		counts := countPhases(o.view, inst.plane, inst.name)
		for _, phase := range knownPhases {
			lines = append(lines, phaseLine(inst, phase, counts[phase]))
		}
		extra := make([]string, 0)
		for phase := range counts {
			if _, ok := known[phase]; !ok {
				extra = append(extra, phase)
			}
		}
		slices.Sort(extra)
		for _, phase := range extra {
			lines = append(lines, phaseLine(inst, phase, counts[phase]))
		}
	}
	return lines
}

func phaseLine(inst agentInst, phase string, n int) string {
	return gaugeLine("mftiko_worker_phase", [][2]string{
		{"instance", inst.name},
		{"phase", phase},
		{"plane", string(inst.plane)},
		{"region", inst.region},
	}, n)
}

// countPhases counts WorkerStatus values under this instance. A key with a further slash, a value
// that does not unmarshal, and an empty phase are skipped (E12-D038).
func countPhases(view state.View, plane state.Plane, inst string) map[string]int {
	prefix := state.WorkerStatusPrefix(plane, inst)
	counts := map[string]int{}
	for key, ent := range view.Entries {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok || rest == "" || strings.Contains(rest, "/") {
			continue
		}
		var st statev1.WorkerStatus
		if proto.Unmarshal(ent.Value, &st) != nil || st.Phase == "" {
			continue
		}
		counts[st.Phase]++
	}
	return counts
}

type busSample struct {
	inst   agentInst
	worker string
	up     int
}

// busSamples is one gauge per worker that carries condition bus_disconnected.
// status true is 0 (down). status false is 1 (up). An empty worker key, a value
// that does not unmarshal, and a worker without that condition are skipped, so
// TD and STS do not appear as a false 1 (E12-D064).
func busSamples(cfg Config, by map[string]regionObs) []string {
	var samples []busSample
	for _, inst := range agentInstances(cfg) {
		o, ok := regionReady(by, inst.region)
		if !ok {
			continue
		}
		prefix := state.WorkerStatusPrefix(inst.plane, inst.name)
		for key, ent := range o.view.Entries {
			rest, ok := strings.CutPrefix(key, prefix)
			if !ok || rest == "" || strings.Contains(rest, "/") {
				continue
			}
			var st statev1.WorkerStatus
			if proto.Unmarshal(ent.Value, &st) != nil {
				continue
			}
			worker := st.GetWorker().GetKey()
			if worker == "" {
				continue
			}
			up, ok := busGauge(&st)
			if !ok {
				continue
			}
			samples = append(samples, busSample{inst: inst, worker: worker, up: up})
		}
	}
	slices.SortFunc(samples, func(a, b busSample) int {
		if c := strings.Compare(a.inst.name, b.inst.name); c != 0 {
			return c
		}
		if c := strings.Compare(string(a.inst.plane), string(b.inst.plane)); c != 0 {
			return c
		}
		if c := strings.Compare(a.inst.region, b.inst.region); c != 0 {
			return c
		}
		return strings.Compare(a.worker, b.worker)
	})
	lines := make([]string, 0, len(samples))
	for _, s := range samples {
		lines = append(lines, gaugeLine("mftiko_worker_bus_up", [][2]string{
			{"instance", s.inst.name},
			{"plane", string(s.inst.plane)},
			{"region", s.inst.region},
			{"worker", s.worker},
		}, s.up))
	}
	return lines
}

func busGauge(st *statev1.WorkerStatus) (int, bool) {
	for _, c := range st.GetConditions() {
		if c.GetType() != condBusDisconnected {
			continue
		}
		if c.GetStatus() {
			return 0, true
		}
		return 1, true
	}
	return 0, false
}

func regionSamples(cfg Config, now time.Time, obs []regionObs) []string {
	ordered := slices.Clone(obs)
	slices.SortFunc(ordered, func(a, b regionObs) int { return strings.Compare(a.name, b.name) })
	lines := make([]string, 0, len(ordered))
	for _, o := range ordered {
		up := 0
		snap := regionSnap{name: o.name, synced: o.synced, contact: o.contact}
		if snap.available(now, cfg.UnavailableAfter) {
			up = 1
		}
		lines = append(lines, gaugeLine("mftiko_region_available", [][2]string{
			{"region", o.name},
		}, up))
	}
	return lines
}

func gaugeLine(name string, labels [][2]string, value int) string {
	var b strings.Builder
	b.WriteString(name)
	if len(labels) > 0 {
		b.WriteByte('{')
		for i, l := range labels {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(l[0])
			b.WriteString(`="`)
			b.WriteString(escapeLabel(l[1]))
			b.WriteByte('"')
		}
		b.WriteByte('}')
	}
	b.WriteByte(' ')
	b.WriteString(strconv.Itoa(value))
	return b.String()
}

// escapeLabel quotes a Prometheus label value (backslash, newline, double quote).
func escapeLabel(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '"':
			b.WriteString(`\"`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
