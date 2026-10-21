package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
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

	WorkDir string
	Shim    string // mftiko-shim
	DataDir string // ${DATA}: layers, rootfs, session volume and inputs
	ShimEnv []string
	// WorkerEnv is the platform environment every worker gets (MFTIKO_NATS_URL).
	WorkerEnv []string
	// ExtraEnv is appended to one worker's environment at launch. Tests use it.
	ExtraEnv func(agent.WorkerKey) []string
	// Sign issues the session user JWT (D924).
	Sign SessionSigner
	// Applied is told each bindings version a worker reports. Tests use it (D965).
	Applied func(sessionID string, epoch int64, seq uint64)

	// Registry, RegistryCreds, DataRepository, PlainHTTP, and MaxArtifactBytes
	// are the deployment's registry settings (D910, D913, D933, D948, D979).
	// RegistryCreds is a Docker config.json path. DataRepository is the data
	// artifact name without the registry host. Empty leaves a non-empty volume
	// retrying until a push can name a repository.
	Registry         string
	RegistryCreds    string
	DataRepository   string
	PlainHTTP        bool
	MaxArtifactBytes int64
	RecvWindowMS     int
	HookTimeoutS     int
	StartTimeoutS    int
	VolumeMB         int
	// VolumeInterval is how often volumes are measured (D976). 0 takes DefaultVolumeInterval.
	VolumeInterval time.Duration
	Grace          time.Duration
	MaxRestarts    int
	RestartWindowS int
	// BackoffMin and BackoffMax are the restart wait (D919, D971). 0 takes
	// DefaultBackoffMin and DefaultBackoffMax. Tests set a millisecond pair.
	BackoffMin time.Duration
	BackoffMax time.Duration

	Policy     agent.Policy
	Resync     time.Duration
	StaleAfter time.Duration
	Mirror     state.MirrorOptions
	Release    string
	Started    func(pid int)
	Log        zerolog.Logger
	Now        func() time.Time
}

func (cfg *Config) applyDefaults() {
	if cfg.RecvWindowMS <= 0 {
		cfg.RecvWindowMS = DefaultRecvWindowMS
	}
	if cfg.HookTimeoutS <= 0 {
		cfg.HookTimeoutS = DefaultHookTimeoutS
	}
	if cfg.StartTimeoutS <= 0 {
		cfg.StartTimeoutS = DefaultStartTimeoutS
	}
	if cfg.VolumeMB <= 0 {
		cfg.VolumeMB = DefaultVolumeMB
	}
	if cfg.VolumeInterval == 0 {
		cfg.VolumeInterval = DefaultVolumeInterval
	}
	if cfg.Grace <= 0 {
		cfg.Grace = DefaultGrace
	}
	if cfg.MaxRestarts <= 0 {
		cfg.MaxRestarts = DefaultMaxRestarts
	}
	if cfg.RestartWindowS <= 0 {
		cfg.RestartWindowS = DefaultRestartWindowS
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = DefaultBackoffMin
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = DefaultBackoffMax
	}
	if cfg.BackoffMax < cfg.BackoffMin {
		cfg.BackoffMax = cfg.BackoffMin
	}
	if cfg.MaxArtifactBytes <= 0 {
		cfg.MaxArtifactBytes = DefaultMaxArtifactBytes
	}
}

func (cfg Config) validate() error {
	if cfg.Instance == "" || cfg.WorkDir == "" || cfg.Shim == "" || cfg.DataDir == "" || cfg.Registry == "" || cfg.Sign == nil {
		return errors.New("sts: agent config incomplete")
	}
	if cfg.MaxArtifactBytes < 0 {
		return errors.New("sts: max artifact bytes")
	}
	if cfg.VolumeInterval < 0 {
		return errors.New("sts: volume interval")
	}
	return nil
}

// Run is the agent's main: win the term, mirror spec, assignment, and status,
// and reconcile until the term ends or ctx is done. Workers outlive the process
// (D102).
func Run(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
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
		Dur("volume_interval", cfg.VolumeInterval).
		Dur("grace", cfg.Grace).
		Int("max_restarts", cfg.MaxRestarts).
		Int("restart_window_s", cfg.RestartWindowS).
		Dur("backoff_min", cfg.BackoffMin).
		Dur("backoff_max", cfg.BackoffMax).
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
	art, err := artifact.Open(artifact.Registry{
		Address: cfg.Registry, Credentials: cfg.RegistryCreds, PlainHTTP: cfg.PlainHTTP,
	}, cfg.DataDir)
	if err != nil {
		return err
	}
	if err := cleanInputTemps(cfg.DataDir); err != nil {
		return err
	}
	pulls := newPuller(ctx, art, cfg.DataDir, cfg.MaxArtifactBytes, cfg.Log, cfg.Now)
	vols := newVolumer(cfg.DataDir, cfg.VolumeInterval, cfg.Log)
	go vols.run(mctx)
	pol := newPolicy(cfg)
	prog := &Program{
		Env: cfg.WorkerEnv, ExtraEnv: cfg.ExtraEnv, DataDir: cfg.DataDir, Instance: cfg.Instance,
		Epoch: term.Epoch(), Sign: cfg.Sign,
		RecvWindowMS: cfg.RecvWindowMS, HookTimeoutS: cfg.HookTimeoutS, StartTimeoutS: cfg.StartTimeoutS,
		policy: pol,
	}
	f := fleet.New(fleet.Config[Spec, Live]{
		Plane: state.PlaneSTS, Instance: cfg.Instance, Epoch: term.Epoch(), Release: cfg.Release,
		WorkDir: cfg.WorkDir, Shim: cfg.Shim, ShimEnv: cfg.ShimEnv, Numbers: numbers, Program: prog,
		StopGrace: cfg.Grace, Started: cfg.Started, Log: cfg.Log,
	})
	defer f.Close()
	pol.lookup = func(key agent.WorkerKey) (agent.Observed, bool) {
		obs, ok := f.Observe()[key]
		return obs, ok
	}
	var policy agent.Policy = pol
	if cfg.Policy != nil {
		// A test-supplied policy replaces restart decisions. It does not see
		// no_beat marks, and phases stay on the exit itself.
		policy = cfg.Policy
		prog.policy = nil
		pol = nil
	}
	p := &plane{
		inst: cfg.Instance, spec: specM, demand: demandM, status: statusM,
		w: term, log: cfg.Log, now: cfg.Now, prog: prog, pol: pol, pulls: pulls, art: art, fleet: f,
		vols: vols, volumeMB: cfg.VolumeMB, onApplied: cfg.Applied,
		workDir: cfg.WorkDir, dataRepo: cfg.DataRepository, numbers: numbers,
		local: map[string]statusView{}, remembered: map[string]demandView{},
		loggedSpec: map[string]bool{}, loggedDemand: map[string]bool{},
		saved: map[string]*statev1.SessionStatus{}, exits: map[string]exitNote{},
	}
	p.finish = newFinisher(mctx, p)
	go p.finish.loop()
	defer func() {
		stop()
		<-p.finish.done
	}()
	return runner.Run(ctx, runner.Config[Entry, Spec, Live]{
		Term: term, Demand: src, Statuses: reader, Decode: decoder{inst: cfg.Instance}.Decode, Planner: p,
		Policy: policy, Fleet: &releaseFleet{Fleet: f, release: p.release}, Commit: p.commit, Sync: p.sync,
		Release: cfg.Release, Resync: cfg.Resync, StaleAfter: cfg.StaleAfter, Log: cfg.Log, Now: cfg.Now,
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

	prog      *Program
	pol       *Policy
	pulls     *puller
	art       *artifact.Client
	fleet     *fleet.Fleet[Spec, Live]
	vols      *volumer
	volumeMB  int
	onApplied func(sessionID string, epoch int64, seq uint64)

	// mu guards the status mirrors the finisher reads. The runner's commit and
	// sync take it; Release holds it across the fleet call so a push cannot
	// start before the exit note is stored (D978, D982).
	mu       sync.Mutex
	workDir  string
	dataRepo string
	numbers  *state.Incarnations
	finish   *finisher
	saved    map[string]*statev1.SessionStatus
	exits    map[string]exitNote

	// specReady is set on the first commit, which runs only after the first
	// list of spec, demand, and status has synced. specs is the set of session
	// ids whose spec key is in that commit's view (D1019).
	specReady bool
	specs     map[string]struct{}
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
	var wake []string
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()
		if p.finish == nil {
			return
		}
		// Adopt has already run: the first commit is the earliest a resumed push may look.
		// Every commit wakes phase 2; it is level-triggered (D1016, D1019).
		p.finish.adopted()
		p.finish.kick()
		for _, id := range wake {
			p.finish.ensure(id)
		}
	}()
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
	p.stage(sessions, &d)
	p.plans, p.intents, p.hashes = d.Plans, d.Intents, d.Hashes
	p.noteBeat(sessions)
	p.remembered = remember(sessions, p.remembered)
	p.mirrored = map[string]statusView{}
	for id, s := range sessions {
		if s.mirrored != nil {
			p.mirrored[id] = *s.mirrored
		}
		if s.observed != nil && s.observed.State == agent.StateExited && s.observed.Exit != nil {
			p.noteExitLocked(id, s.observed.Incarnation, s.observed.Exit)
		}
		if s.terminal() {
			wake = append(wake, id)
		}
	}
	p.noteSpecsLocked(sessions)
	return nil
}

// noteSpecsLocked records the spec keys of this reconcile. Phase 2 treats
// absence from this set as the spec being gone, and only after specReady
// (D1019). A key that is present but will not decode is still present.
func (p *plane) noteSpecsLocked(sessions map[string]*sess) {
	specs := make(map[string]struct{}, len(sessions))
	for id, s := range sessions {
		if s != nil && s.specPresent {
			specs[id] = struct{}{}
		}
	}
	p.specs = specs
	p.specReady = true
}

// sync writes SessionStatus where the phase or the reason changed (W7). Keys
// are not deleted: the API server removes a terminal session's status (W5).
func (p *plane) sync(ctx context.Context, _ agent.Demand[Entry], observed map[agent.WorkerKey]agent.Observed, _ func(agent.WorkerKey) bool) error {
	var wake []string
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()
		if p.finish == nil {
			return
		}
		for _, id := range wake {
			p.finish.ensure(id)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	now := p.clock()
	for _, id := range slices.Sorted(maps.Keys(p.intents)) {
		in := p.intents[id]
		var obs *agent.Observed
		if o, ok := observed[WorkerKey(id)]; ok {
			obs = &o
		}
		var ev endView
		if p.pol != nil {
			ev = p.pol.statusEnd(id)
		}
		ev.undecided = undecidedExit(obs, ev, p.pol != nil)
		phase, reason, write := phaseOf(in, obs, ev)
		if !write || p.same(id, phase, reason) {
			continue
		}
		rev := in.Rev
		if rev == 0 {
			rev = p.generationLocked(id)
		}
		msg := &statev1.SessionStatus{
			ObservedGeneration: rev, Phase: phase, Reason: reason, UpdatedAtNs: now.UnixNano(),
		}
		if obs != nil {
			msg.Incarnation = obs.Incarnation
			if obs.Exit != nil {
				msg.LastExit = exitForStatus(obs.Exit, ev)
			}
		}
		if prev := p.saved[id]; prev.GetStateOut() != nil {
			msg.StateOut = proto.Clone(prev.GetStateOut()).(*statev1.StateSnapshot)
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return err
		}
		if err := p.w.Put(ctx, state.SessionStatusKey(p.inst, id), string(b)); err != nil {
			return fmt.Errorf("sts: status %s: %w", id, err)
		}
		p.saved[id] = msg
		p.local[id] = statusView{Phase: phase, Reason: reason, Gen: rev}
		if phase == statev1.Phase_PHASE_DONE || phase == statev1.Phase_PHASE_FAILED {
			wake = append(wake, id)
		}
	}
	p.pushBindings(ctx, observed)
	p.noteApplied(observed)
	return nil
}

// stage drops sessions whose rootfs and inputs are not ready from the plan, records
// artifact rejections, and garbage-collects (D958, D959, D973, D974). A worker already
// running this spec stays in the plan so a restart does not stop it while the image is
// fetched again (D102). A volume that is over the limit is the exception: it leaves the
// plan and ends FAILED, so the reconciler's normal Stop runs (D976).
func (p *plane) stage(sessions map[string]*sess, d *decision) {
	for _, id := range slices.Sorted(maps.Keys(sessions)) {
		s := sessions[id]
		if s.spec == nil || s.parsed == nil {
			continue
		}
		var raw []*statev1.Binding
		switch {
		case s.demand != nil:
			raw = s.demand.Bindings
		case s.remembered != nil:
			raw = s.remembered.Bindings
		}
		p.prog.note(id, s.spec.Hash(), s.parsed, toBindings(s.parsed, raw))
	}
	if p.vols != nil {
		p.vols.setTargets(volumeTargets(sessions, p.volumeMB))
	}
	want := map[string]struct{}{}
	wantInputs := map[string]struct{}{}
	for _, id := range slices.Sorted(maps.Keys(d.Intents)) {
		in := d.Intents[id]
		if in.Kind != kindPlan {
			continue
		}
		s := sessions[id]
		if p.vols != nil && p.vols.exceeded(id) {
			delete(d.Plans, WorkerKey(id))
			in.Kind = kindFailed
			in.FailReason = ReasonVolumeOverLimit
			d.Intents[id] = in
			p.log.Warn().Str("session", id).Msg("volume over limit")
			continue
		}
		if err := validateInputs(s.parsed.GetInputs()); err != nil {
			delete(d.Plans, WorkerKey(id))
			in.Kind = kindFailed
			in.FailReason = ReasonInvalidInput
			d.Intents[id] = in
			p.log.Warn().Str("session", id).Err(err).Msg("invalid input")
			continue
		}
		dig := s.parsed.GetStrategy().GetCode()
		want[dig] = struct{}{}
		for _, decl := range s.parsed.GetInputs() {
			wantInputs[inputKey(id, decl.GetName())] = struct{}{}
		}
		snap := p.pulls.ensure(artifact.Request{
			Repository: s.parsed.GetStrategy().GetRepository(), Digest: dig,
		})
		inSnap := p.pulls.ensureInputs(id, s.parsed.GetInputs())
		liveSame := s.observed != nil && !s.observed.State.Ended() && s.observed.SpecHash == in.SpecHash
		switch {
		case snap.ready && inSnap.ready:
			if err := ensureSessionDirs(p.prog.DataDir, id); err != nil {
				p.log.Warn().Str("session", id).Err(err).Msg("session directories")
				if !liveSame {
					delete(d.Plans, WorkerKey(id))
				}
				continue
			}
			p.prog.setImage(id, snap.rootfs, snap.manifest)
		case (snap.perm != "" || inSnap.perm != "") && !liveSame:
			delete(d.Plans, WorkerKey(id))
			in.Kind = kindFailed
			in.FailReason = snap.perm
			if inSnap.perm != "" {
				in.FailReason = inSnap.perm
			}
			d.Intents[id] = in
		case liveSame:
		default:
			delete(d.Plans, WorkerKey(id))
		}
	}
	p.pulls.retain(want)
	p.pulls.retainInputs(wantInputs)
	keep := keepDigests(sessions, p.pulls.inflight())
	if err := p.art.GC(keep, p.pulls.inflightLayers()); err != nil {
		p.log.Warn().Err(err).Msg("artifact gc")
	}
}

// pushBindings sends a Command when a started worker's bindings changed (D961).
// A failure leaves the seq unpublished so the next round retries the same one.
func (p *plane) pushBindings(ctx context.Context, observed map[agent.WorkerKey]agent.Observed) {
	for _, id := range slices.Sorted(maps.Keys(p.intents)) {
		in := p.intents[id]
		if in.Kind != kindPlan {
			continue
		}
		obs, ok := observed[WorkerKey(id)]
		if !ok || obs.SpecHash != in.SpecHash {
			continue
		}
		if obs.State != agent.StateStarting && obs.State != agent.StateRunning {
			continue
		}
		raw, hash, seq, send := p.prog.command(id)
		if !send {
			continue
		}
		err := p.fleet.Relay(ctx, WorkerKey(id), &workerv1.Relay{Contract: workerContract, Payload: raw})
		if err != nil {
			p.log.Warn().Str("session", id).Err(err).Msg("bindings command")
			continue
		}
		p.prog.markSent(id, hash, seq)
	}
}

func (p *plane) noteApplied(observed map[agent.WorkerKey]agent.Observed) {
	if p.onApplied == nil {
		return
	}
	for key, obs := range observed {
		rep := obs.Report
		if rep == nil || rep.GetContract() != workerContract || len(rep.GetPayload()) == 0 {
			continue
		}
		msg := &stsctlv1.Report{}
		if err := proto.Unmarshal(rep.GetPayload(), msg); err != nil || msg.GetBindingsApplied() == nil {
			continue
		}
		id, ok := idFrom("sts/", string(key))
		if !ok {
			continue
		}
		v := msg.GetBindingsApplied()
		p.onApplied(id, v.GetEpoch(), v.GetSeq())
	}
}

// noteBeat publishes each session's resolved limits to the policy before the
// skeleton asks BeatTimeout (D968).
func (p *plane) noteBeat(sessions map[string]*sess) {
	if p.pol == nil {
		return
	}
	for id, s := range sessions {
		if s.parsed == nil {
			continue
		}
		p.pol.note(id, s.parsed, s.observed)
	}
}

// generationLocked is the last observed_generation written or mirrored for id.
// The caller holds p.mu.
func (p *plane) generationLocked(id string) int64 {
	if st, ok := p.local[id]; ok && st.Gen != 0 {
		return st.Gen
	}
	if p.saved[id].GetObservedGeneration() != 0 {
		return p.saved[id].GetObservedGeneration()
	}
	if st, ok := p.mirrored[id]; ok && st.Gen != 0 {
		return st.Gen
	}
	return 0
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
