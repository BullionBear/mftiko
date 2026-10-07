package agent

import "context"

// WorkerKey identifies a worker within a plane: sts/<session_id>, td/<api_id>, md/<venue>/<route>/<n>
// (PROTOCOL §2).
type WorkerKey string

// Plan is what a Planner wants for one worker. Changing Spawn needs a new incarnation;
// Live is pushed to the running worker (DECISIONS D2).
type Plan[S, L any] struct {
	Spawn S
	Live  L
}

// Intake watches desired state in etcd: main resources from the API, lease entries from the STS agent.
type Intake[D any] interface {
	Watch(ctx context.Context) (<-chan D, error)
}

// Planner is a pure function from desired state to per-worker plans.
type Planner[D, S, L any] interface {
	Plan(desired D) map[WorkerKey]Plan[S, L]
}

// Fleet is the set of shim-backed workers: spawn, attach, push live config, observe, release.
type Fleet[S, L, O any] interface {
	Observe(ctx context.Context) (map[WorkerKey]O, error)
	Spawn(ctx context.Context, key WorkerKey, spec S) error
	Push(ctx context.Context, key WorkerKey, live L) error
	Stop(ctx context.Context, key WorkerKey, reason string) error
}

// Reconciler is the single loop that diffs plans against observations and acts.
type Reconciler interface {
	Run(ctx context.Context) error
}
