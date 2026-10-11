package agent

import (
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// WorkerKey identifies a worker within a plane: sts/<session_id>, td/<api_id>, md/<venue>/<route>/<n>
// (PROTOCOL §2).
type WorkerKey string

// Hashed is a value the skeleton compares by its hash: a spawn spec or a live config. Equal values hash equal;
// a deterministic encoding of the whole value, hashed, is what the hash should be.
type Hashed interface {
	Hash() string
}

// Plan is what a Planner wants for one worker (D101). Changing Spawn needs a new incarnation; Live is pushed to the
// running worker. Start says that the worker may be sent Start once it is initialized: at once on MD and TD, on
// STS once the controller sets start (D107, PROTOCOL §3.3 N2).
type Plan[S, L Hashed] struct {
	Spawn S
	Live  L
	Start bool
}

// Demand is what Intake read from the agent's demand/ prefix, as of one revision.
type Demand[I any] struct {
	Rev      int64
	Items    map[string]I     // by etcd key
	Rejected map[string]error // keys whose value Intake could not take: an unknown enum, a missing field
	// LastContact is the latest time the copy is known to hold everything committed before (state.Mirror).
	LastContact time.Time
}

// Stale reports whether the demand is older than after at now: no contact with etcd since then, or none yet.
// While it is stale the agent changes nothing by it (DECISIONS D109).
func (d Demand[I]) Stale(now time.Time, after time.Duration) bool {
	return d.LastContact.IsZero() || now.Sub(d.LastContact) > after
}

// Planner is a pure function from demand to per-worker plans. Each plane has its own.
type Planner[I any, S, L Hashed] interface {
	Plan(d Demand[I]) map[WorkerKey]Plan[S, L]
}

// State is where a worker is, as the Fleet sees it through its shim (PROTOCOL §4.3).
type State int

const (
	StateInitializing State = iota + 1 // spawned; Init sent, no Done(INIT) yet
	StateInitialized                   // Done(INIT, ok): waiting for Start
	StateStarting                      // Start sent, no Done(START) yet
	StateRunning                       // Done(START, ok)
	StateStopping                      // Stop sent, or the worker said it is ending
	StateExited                        // reaped and recorded; the shim waits for Release
	StateLost                          // the shim is gone, and left no exit record
)

func (s State) String() string {
	switch s {
	case StateInitializing:
		return "initializing"
	case StateInitialized:
		return "initialized"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateExited:
		return "exited"
	case StateLost:
		return "lost"
	default:
		return "unknown"
	}
}

// Ended reports whether the worker is gone: exited, or lost with its shim.
func (s State) Ended() bool { return s == StateExited || s == StateLost }

// Observed is one worker as the Fleet sees it: what it was spawned with, what was pushed to it, and its state,
// from its shim's Attached and events (D108).
type Observed struct {
	Key         WorkerKey
	Incarnation uint32
	SpecHash    string // the spawn spec's Hash
	LiveHash    string // the Hash of the live config the worker says it has applied (Applied, D111); "" for none yet
	Pushed      string // the Hash of the live config last sent to this incarnation, in Init or Live; "" for none
	PushedAt    time.Time
	State       State
	Stage       string // the worker's last Stage
	Release     string // the worker's, from Hello
	ShimRelease string
	StartedAt   time.Time
	LastBeat    time.Time          // when the shim received the worker's last Beat; zero before the first (E1-18)
	Exit        *commonv1.ExitInfo // once StateExited
	// Report is the worker's latest C5 report. The shim keeps one and sends it ahead of the
	// replay (PROTOCOL §4.4). MD reads mdctl.Report.conn_epoch from it before Release. Nil
	// until a report has arrived.
	Report *workerv1.Report
}
