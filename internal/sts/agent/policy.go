package agent

import (
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// beatMargin is added to the phase's limit (D920, D968). It is not configurable.
const beatMargin = 10 * time.Second

// Policy is the STS restart and beat policy (D919, D920, D966–D971).
// The skeleton asks it from the agent loop; it does no I/O.
type Policy struct {
	backoff       agent.Backoff
	hookDefault   int
	startDefault  int
	maxDefault    int
	windowDefault time.Duration
	now           func() time.Time
	// lookup is the fleet's current observation, used to match a no_beat kill to
	// the incarnation that just ended (D967). Nil in tests that set it themselves.
	lookup func(agent.WorkerKey) (agent.Observed, bool)

	sessions map[string]*sessPol
}

// sessPol is one session's resolved limits and the restarts this process has counted.
type sessPol struct {
	mode        sessionv1.RestartMode
	max         int
	window      time.Duration
	hook        time.Duration
	deadline    time.Duration
	useDeadline bool

	killed map[uint32]struct{} // incarnations SIGKILLed for a missing beat
	times  []time.Time         // when this process restarted it (D969)

	haveEnd        bool
	decided        uint32 // the incarnation whose exit Restart classified last
	restarting     bool
	noBeat         bool // this exit is the SIGKILL for a missing beat (D967)
	reason         string
	terminalReason string
	overrideReason string
}

// endView is how the latest exit should be published. The zero value leaves the
// exit's own outcome in place.
type endView struct {
	// decided is the incarnation the fields below are about. Sync marks an exit of
	// another incarnation undecided: the policy has not seen it yet (D995).
	decided        uint32
	undecided      bool
	restarting     bool
	noBeat         bool
	reason         string
	terminalReason string // FAILED with this reason, restart_limit
	overrideReason string // FAILED with this reason, no_beat when not restarting
}

// newPolicy is the policy Run installs. Zero config fields take the deployment defaults.
func newPolicy(cfg Config) *Policy {
	min, max := cfg.BackoffMin, cfg.BackoffMax
	if min <= 0 {
		min = DefaultBackoffMin
	}
	if max <= 0 {
		max = DefaultBackoffMax
	}
	if max < min {
		max = min
	}
	hook, start := cfg.HookTimeoutS, cfg.StartTimeoutS
	if hook <= 0 {
		hook = DefaultHookTimeoutS
	}
	if start <= 0 {
		start = DefaultStartTimeoutS
	}
	restarts, window := cfg.MaxRestarts, cfg.RestartWindowS
	if restarts <= 0 {
		restarts = DefaultMaxRestarts
	}
	if window <= 0 {
		window = DefaultRestartWindowS
	}
	return &Policy{
		backoff:       agent.Backoff{Min: min, Max: max},
		hookDefault:   hook,
		startDefault:  start,
		maxDefault:    restarts,
		windowDefault: time.Duration(window) * time.Second,
		now:           cfg.Now,
	}
}

func (p *Policy) ensure(id string) *sessPol {
	if p.sessions == nil {
		p.sessions = map[string]*sessPol{}
	}
	s := p.sessions[id]
	if s == nil {
		s = &sessPol{mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE}
		p.sessions[id] = s
	}
	if s.mode == sessionv1.RestartMode_RESTART_MODE_UNSPECIFIED {
		s.mode = sessionv1.RestartMode_RESTART_MODE_ON_FAILURE
	}
	if s.max <= 0 {
		s.max = p.maxDefault
	}
	if s.max <= 0 {
		s.max = DefaultMaxRestarts
	}
	if s.window <= 0 {
		s.window = p.windowDefault
	}
	if s.window <= 0 {
		s.window = time.Duration(DefaultRestartWindowS) * time.Second
	}
	return s
}

// note records the spec's resolved limits and which beat limit the worker is on (D968).
func (p *Policy) note(id string, spec *sessionv1.SessionSpec, obs *agent.Observed) {
	if p == nil || spec == nil {
		return
	}
	s := p.ensure(id)
	s.hook = time.Duration(hookTimeoutS(spec, p.hookDefault)) * time.Second
	s.deadline = time.Duration(startTimeoutS(spec, p.startDefault)) * time.Second
	mode := spec.GetRestart().GetMode()
	if mode == sessionv1.RestartMode_RESTART_MODE_UNSPECIFIED {
		mode = sessionv1.RestartMode_RESTART_MODE_ON_FAILURE
	}
	s.mode = mode
	if max := spec.GetRestart().GetMaxRestarts(); max > 0 {
		s.max = int(max)
	} else {
		s.max = p.maxDefault
	}
	if win := spec.GetRestart().GetWindowS(); win > 0 {
		s.window = time.Duration(win) * time.Second
	} else {
		s.window = p.windowDefault
	}
	s.useDeadline = obs != nil && (obs.State == agent.StateInitializing || obs.State == agent.StateStarting)
}

// noteKill remembers an incarnation this process is killing because it stopped beating (D967).
func (p *Policy) noteKill(key agent.WorkerKey, inc uint32) {
	if p == nil || inc == 0 {
		return
	}
	id, ok := idFrom("sts/", string(key))
	if !ok {
		return
	}
	s := p.ensure(id)
	if s.killed == nil {
		s.killed = map[uint32]struct{}{}
	}
	s.killed[inc] = struct{}{}
}

func (p *Policy) statusEnd(id string) endView {
	if p == nil {
		return endView{}
	}
	s := p.sessions[id]
	if s == nil || !s.haveEnd {
		return endView{}
	}
	return endView{
		decided:    s.decided,
		restarting: s.restarting, noBeat: s.noBeat, reason: s.reason,
		terminalReason: s.terminalReason, overrideReason: s.overrideReason,
	}
}

// Restart says whether a wanted worker runs again, and how long to wait (D919, D969, D970).
// The wait is the skeleton's Ended.Restarts, which goes back to zero after StableAfter.
// The window count is this process's own timestamps and starts at zero after a rotation.
func (p *Policy) Restart(key agent.WorkerKey, end agent.Ended) (bool, time.Duration) {
	id, ok := idFrom("sts/", string(key))
	if !ok {
		return false, 0
	}
	s := p.ensure(id)
	s.haveEnd = true
	s.decided = p.incarnation(key)
	s.restarting = false
	s.noBeat = false
	s.terminalReason = ""
	s.overrideReason = ""
	s.reason = ""
	if end.Exit == nil {
		// The shim is gone and left no record. Restarting could leave the old
		// process placing orders (D970).
		s.reason = ReasonShimLost
		return false, 0
	}
	outcome := end.Exit.GetOutcome()
	reason := end.Exit.GetReason()
	if p.consumeNoBeat(s, key, end.Exit) {
		s.noBeat = true
		outcome = commonv1.Outcome_OUTCOME_HUNG
		reason = ReasonNoBeat
	}
	s.reason = reason
	if s.mode == sessionv1.RestartMode_RESTART_MODE_NEVER || !failureRestart(outcome, end.Exit.GetSignal()) {
		if reason == ReasonNoBeat {
			s.overrideReason = ReasonNoBeat
		}
		return false, 0
	}
	now := p.clock()
	s.times = pruneTimes(s.times, now, s.window)
	if len(s.times) >= s.max {
		s.terminalReason = ReasonRestartLimit
		s.reason = ReasonRestartLimit
		return false, 0
	}
	s.times = append(s.times, now)
	s.restarting = true
	return true, p.backoff.After(end.Restarts)
}

// KeepWhileStale keeps the last plan, and Start still runs, while demand is stale (D109).
func (Policy) KeepWhileStale(agent.WorkerKey) bool { return true }

// BeatTimeout is deadline_ms+10s during Init and Start, and hook_timeout_s+10s
// after that, including WAITING for start (D968). Both are the resolved values.
// 0 means the session is not known yet, so the skeleton does not time it out.
func (p *Policy) BeatTimeout(key agent.WorkerKey) time.Duration {
	id, ok := idFrom("sts/", string(key))
	if !ok {
		return 0
	}
	s := p.sessions[id]
	if s == nil {
		return 0
	}
	base := s.hook
	if s.useDeadline {
		base = s.deadline
	}
	if base <= 0 {
		return 0
	}
	return base + beatMargin
}

// Silent stops a worker that has gone quiet. The fleet turns that into SIGKILL
// for STS (D966); this method only says to stop.
func (Policy) Silent(agent.WorkerKey, time.Duration) bool { return true }

func (p *Policy) clock() time.Time {
	if p != nil && p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *Policy) incarnation(key agent.WorkerKey) uint32 {
	if p == nil || p.lookup == nil {
		return 0
	}
	obs, ok := p.lookup(key)
	if !ok {
		return 0
	}
	return obs.Incarnation
}

// consumeNoBeat reports whether this exit is the SIGKILL noteKill recorded for
// the same incarnation, and forgets that mark either way (D967).
func (p *Policy) consumeNoBeat(s *sessPol, key agent.WorkerKey, ex *commonv1.ExitInfo) bool {
	inc := p.incarnation(key)
	if inc == 0 || s.killed == nil {
		return false
	}
	if _, ok := s.killed[inc]; !ok {
		return false
	}
	delete(s.killed, inc)
	return ex.GetSignal() != 0 && ex.GetOutcome() == commonv1.Outcome_OUTCOME_UNSPECIFIED
}

// failureRestart is the ON_FAILURE set: CRASHED, HUNG, and a signal the agent
// did not ask for (D919). DONE, FAILED, and INIT_FAILED are not in it.
// A signal reaches Restart only when the worker was still wanted; an agent Stop
// either leaves the plan or is a spec replacement, and neither asks Restart.
func failureRestart(outcome commonv1.Outcome, signal int32) bool {
	switch outcome {
	case commonv1.Outcome_OUTCOME_CRASHED, commonv1.Outcome_OUTCOME_HUNG:
		return true
	case commonv1.Outcome_OUTCOME_UNSPECIFIED:
		return signal != 0
	default:
		return false
	}
}

func pruneTimes(times []time.Time, now time.Time, window time.Duration) []time.Time {
	cut := now.Add(-window)
	n := 0
	for _, t := range times {
		if !t.Before(cut) {
			times[n] = t
			n++
		}
	}
	return times[:n]
}
