package agent

import (
	"maps"
	"slices"
	"time"
)

// Kind is what an Action does to a worker.
type Kind int

const (
	ActSpawn   Kind = iota + 1 // a new incarnation with the plan's spec and live config; one that ended is released first (D106)
	ActStart                   // send Start to an initialized worker
	ActPush                    // push the plan's live config to a worker that has another
	ActStop                    // stop a worker: not in the plan, or spawned with another spec
	ActRelease                 // forget a worker that ended and is no longer planned
)

func (k Kind) String() string {
	switch k {
	case ActSpawn:
		return "spawn"
	case ActStart:
		return "start"
	case ActPush:
		return "push"
	case ActStop:
		return "stop"
	case ActRelease:
		return "release"
	default:
		return "unknown"
	}
}

// Action is one thing the Reconciler wants done to one worker.
type Action[S, L Hashed] struct {
	Kind   Kind
	Key    WorkerKey
	Spec   S      // ActSpawn, ActStart
	Live   L      // ActSpawn, ActPush
	Reason string // ActStop
}

// Stop reasons, as the worker's Stop and its ExitInfo carry them.
const (
	ReasonNotPlanned  = "not in demand"
	ReasonSpecChanged = "spec changed"
	ReasonSilent      = "beat timeout"
)

const (
	// DefaultStableAfter is how long a worker must have run for its restarts to count from 0 again.
	DefaultStableAfter = time.Minute
)

// DefaultRetry is the wait after an action that failed, before the Reconciler asks for it again.
var DefaultRetry = Backoff{Min: time.Second, Max: 30 * time.Second}

// Reconciler diffs plans against observations and says what to do (D101). It does no I/O: the caller runs the
// actions, against a Fleet whose observations reflect each action once it returns, and steps again. It keeps what
// the diff alone does not say: restarts and their backoff, which exits it has handled, and which workers it
// stopped to replace. Not safe for concurrent use: the agent has one loop.
type Reconciler[S, L Hashed] struct {
	Policy      Policy
	StableAfter time.Duration // DefaultStableAfter if 0
	Retry       Backoff       // after a failed action; DefaultRetry if zero

	books map[WorkerKey]*book
}

// book is what the Reconciler remembers of one worker key.
type book struct {
	restarts  uint32    // restarts since it was planned, or since it last ran for StableAfter
	failures  uint32    // failed actions in a row
	due       time.Time // no spawn before this: a restart's backoff or a failed action's retry
	handled   uint32    // the incarnation whose end Policy was asked about; 0 for none
	gaveUp    string    // the spec hash the Policy said not to restart; "" if it did not. A new spec tries again
	replacing bool      // stopped for a new spec: spawn again as soon as it has ended, with no backoff
	silentInc uint32    // the incarnation, and the Beat it went silent after, last reported to Policy.Silent
	silentAt  time.Time
}

// Step is one pass. stale says the demand is stale (DECISIONS D109): nothing is stopped or pushed, and a worker is
// spawned or started only if Policy.KeepWhileStale. The actions are in key order. wake is the earliest time a
// worker held back by a backoff becomes due, zero if none is.
func (r *Reconciler[S, L]) Step(now time.Time, stale bool, plans map[WorkerKey]Plan[S, L],
	observed map[WorkerKey]Observed) (acts []Action[S, L], wake time.Time) {
	if r.books == nil {
		r.books = map[WorkerKey]*book{}
	}
	keys := slices.Sorted(maps.Keys(plans))
	for k := range observed {
		if _, ok := plans[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	held := func(t time.Time) {
		if wake.IsZero() || t.Before(wake) {
			wake = t
		}
	}
	for _, k := range keys {
		p, planned := plans[k]
		o, seen := observed[k]
		if !planned {
			delete(r.books, k)
			switch {
			case o.State.Ended():
				acts = append(acts, Action[S, L]{Kind: ActRelease, Key: k})
			case o.State == StateStopping || stale:
			default:
				acts = append(acts, Action[S, L]{Kind: ActStop, Key: k, Reason: ReasonNotPlanned})
			}
			continue
		}
		b := r.books[k]
		if b == nil {
			b = &book{}
			r.books[k] = b
		}
		keep := !stale || r.Policy.KeepWhileStale(k)
		switch {
		case !seen || o.State.Ended():
			if seen {
				r.ended(now, k, b, o, p.Spawn.Hash())
			}
			if b.gaveUp != "" && b.gaveUp != p.Spawn.Hash() {
				b.gaveUp, b.restarts = "", 0
			}
			switch {
			case b.gaveUp != "":
			case now.Before(b.due):
				held(b.due)
			case keep:
				acts = append(acts, Action[S, L]{Kind: ActSpawn, Key: k, Spec: p.Spawn, Live: p.Live})
			}
		case o.SpecHash != p.Spawn.Hash():
			if o.State != StateStopping && !stale {
				b.replacing = true
				acts = append(acts, Action[S, L]{Kind: ActStop, Key: k, Reason: ReasonSpecChanged})
			}
		case o.State == StateStopping:
		default:
			b.failures = 0
			stop, deadline := r.silent(now, k, b, o)
			if stop {
				acts = append(acts, Action[S, L]{Kind: ActStop, Key: k, Reason: ReasonSilent})
				continue
			}
			if !deadline.IsZero() {
				held(deadline)
			}
			if o.LiveHash != p.Live.Hash() && !stale {
				acts = append(acts, Action[S, L]{Kind: ActPush, Key: k, Live: p.Live})
			}
			if p.Start && o.State == StateInitialized && keep {
				acts = append(acts, Action[S, L]{Kind: ActStart, Key: k, Spec: p.Spawn})
			}
		}
	}
	return acts, wake
}

// silent checks a live worker's Beats against the Policy's timeout (E1-18): it reports a silence once, and says
// whether to stop the worker for it; otherwise deadline is when the worker goes silent if no Beat comes.
func (r *Reconciler[S, L]) silent(now time.Time, k WorkerKey, b *book, o Observed) (stop bool, deadline time.Time) {
	timeout := r.Policy.BeatTimeout(k)
	last := o.LastBeat
	if last.IsZero() {
		last = o.StartedAt
	}
	if timeout <= 0 || last.IsZero() {
		return false, time.Time{}
	}
	if deadline = last.Add(timeout); now.Before(deadline) {
		return false, deadline
	}
	if b.silentInc == o.Incarnation && b.silentAt.Equal(last) {
		return false, time.Time{} // reported already
	}
	b.silentInc, b.silentAt = o.Incarnation, last
	return r.Policy.Silent(k, now.Sub(last)), time.Time{}
}

// ended handles a planned worker's end, once per incarnation: a replacement goes at once, anything else as the
// Policy says.
func (r *Reconciler[S, L]) ended(now time.Time, k WorkerKey, b *book, o Observed, spec string) {
	if b.handled == o.Incarnation && o.Incarnation != 0 {
		return
	}
	b.handled = o.Incarnation
	if b.replacing {
		b.replacing, b.gaveUp = false, ""
		b.due = now
		return
	}
	end := Ended{Exit: o.Exit, Ran: ran(now, o)}
	stable := r.StableAfter
	if stable == 0 {
		stable = DefaultStableAfter
	}
	if end.Ran >= stable {
		b.restarts = 0
	}
	end.Restarts = b.restarts
	again, after := r.Policy.Restart(k, end)
	if !again {
		b.gaveUp = spec
		return
	}
	b.restarts++
	b.due = now.Add(after)
}

func ran(now time.Time, o Observed) time.Duration {
	if o.StartedAt.IsZero() {
		return 0
	}
	end := now
	if at := o.Exit.GetAtNs(); at != 0 {
		end = time.Unix(0, at)
	}
	return max(end.Sub(o.StartedAt), 0)
}

// Failed records that an action on key failed at now: the worker is not spawned again before a retry backoff.
// Stops and pushes are asked for again at the next Step, as the diff still shows them.
func (r *Reconciler[S, L]) Failed(now time.Time, key WorkerKey) {
	b := r.books[key]
	if b == nil {
		return
	}
	retry := r.Retry
	if retry == (Backoff{}) {
		retry = DefaultRetry
	}
	b.due = now.Add(retry.After(b.failures))
	b.failures++
}

// Restarts is how many times the planned worker has been restarted: since it was planned, or since it last ran
// for StableAfter.
func (r *Reconciler[S, L]) Restarts(key WorkerKey) uint32 {
	if b := r.books[key]; b != nil {
		return b.restarts
	}
	return 0
}

// GaveUp reports whether the Policy said not to restart the planned worker, which then stays ended until its spec
// changes or it is no longer planned.
func (r *Reconciler[S, L]) GaveUp(key WorkerKey) bool {
	b := r.books[key]
	return b != nil && b.gaveUp != ""
}
