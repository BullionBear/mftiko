package agent

import (
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
)

// Ended is a worker that is gone, as the Reconciler hands it to Policy.Restart.
type Ended struct {
	Exit     *commonv1.ExitInfo // nil when the worker was lost with its shim
	Restarts uint32             // restarts since it was planned, or since it last ran for StableAfter
	Ran      time.Duration      // from its start to its end; 0 if not known
}

// Policy is a plane's failure semantics: its restart rules (E01-D001).
type Policy interface {
	// Restart says whether a worker the plan still wants runs again after it ended, and how long to wait first.
	// Asked once per incarnation that ended; a worker not restarted stays ended until the plan changes.
	Restart(key WorkerKey, end Ended) (again bool, after time.Duration)
	// KeepWhileStale says whether, while the demand is stale, the agent may still spawn and start this worker to
	// keep the last demand it saw: run again one that ended, start one that was waiting (DECISIONS E01-D009). Nothing
	// is ever stopped or pushed while the demand is stale.
	KeepWhileStale(key WorkerKey) bool
	// BeatTimeout is how long the worker may go without a Beat, counted from its last one or, before the first,
	// from its start; 0 is no limit. The shim passes on at most one Beat a second (PROTOCOL §4.4): a timeout of a
	// few seconds at least.
	BeatTimeout(key WorkerKey) time.Duration
	// Silent tells the Policy that a live worker has sent no Beat for silentFor, past its BeatTimeout: once per
	// silence, again only if it beats and goes silent anew. stop says to stop it, with ReasonSilent; what makes a
	// worker HUNG is the plane's (E9). A worker is stopped for its silence even while the demand is stale: the
	// stop does not come from the demand.
	Silent(key WorkerKey, silentFor time.Duration) (stop bool)
}

// Backoff is an exponential wait: Min for the first retry, doubling up to Max.
type Backoff struct {
	Min, Max time.Duration
}

// After is the wait before retry n, counting from 0.
func (b Backoff) After(n uint32) time.Duration {
	d := b.Min
	for range n {
		if d >= b.Max/2 {
			return b.Max
		}
		d *= 2
	}
	return min(d, b.Max)
}

// Always is a Policy that restarts every worker, with a backoff, and keeps every worker while the demand is
// stale: right for a plane whose workers are harmless to run on old demand, as MD's are. Beat sets the timeout,
// 0 none; a silent worker is stopped when StopSilent is set, and only reported otherwise.
type Always struct {
	Backoff    Backoff
	Beat       time.Duration
	StopSilent bool
}

func (p Always) Restart(_ WorkerKey, end Ended) (bool, time.Duration) {
	return true, p.Backoff.After(end.Restarts)
}

func (Always) KeepWhileStale(WorkerKey) bool { return true }

func (p Always) BeatTimeout(WorkerKey) time.Duration { return p.Beat }

func (p Always) Silent(WorkerKey, time.Duration) bool { return p.StopSilent }
