package state

import "errors"

var (
	// ErrBadKey: the key is not Root/{plane}/{inst}/... with a known plane and a valid instance name.
	ErrBadKey = errors.New("state: bad key")
	// ErrUnknownPlane: a plane other than sts, md, td and ctl.
	ErrUnknownPlane = errors.New("state: unknown plane")
	// ErrBadTopology: Topology.Validate failed.
	ErrBadTopology = errors.New("state: bad topology")
	// ErrUnknownInstance: the instance is not in the topology.
	ErrUnknownInstance = errors.New("state: unknown instance")
	// ErrUnknownRegion: the region is not in the topology.
	ErrUnknownRegion = errors.New("state: unknown region")
	// ErrRegionNotAllowed: the region exists but this process may not connect to it (D005 rule 2).
	ErrRegionNotAllowed = errors.New("state: region not allowed")
	// ErrClustersClosed: Clusters.Close was called.
	ErrClustersClosed = errors.New("state: clusters closed")

	// ErrBadOptions: TermOptions without an ID or with a negative TTL; a Mirror without a reader or prefix, or with
	// a negative option.
	ErrBadOptions = errors.New("state: bad options")
	// ErrBadEpoch: the stored epoch is not a 20-digit decimal, or cannot be incremented.
	ErrBadEpoch = errors.New("state: bad epoch")
	// ErrSessionLost: the election session's lease is gone; this process is no longer the leader. The term has
	// ended: exit and let systemd restart the agent.
	ErrSessionLost = errors.New("state: election session lost")
	// ErrSuperseded: a writer key holds a newer epoch, or changed since this term acquired it (PROTOCOL §3.2 W2).
	// The term has ended: exit and let systemd restart the agent, no retry.
	ErrSuperseded = errors.New("state: superseded by a newer epoch")
	// ErrClosed: Term.Close was called.
	ErrClosed = errors.New("state: term closed")

	// ErrCrossCluster: one write touches keys of more than one cluster, or a range that leaves its instance.
	// Fencing is per cluster (D005 rule 3), so such a write cannot be made atomic.
	ErrCrossCluster = errors.New("state: write spans clusters")
	// ErrBadOp: an option that a put or delete cannot take (clientv3 would panic on it).
	ErrBadOp = errors.New("state: bad op")
	// ErrNotWriter: the write touches a key this agent or controller is not the writer of (PROTOCOL §3.2 W1),
	// including the keys fencing relies on (election, epoch, writer), incarnations outside Incarnations, and the
	// AgentRecord outside PutRecord.
	ErrNotWriter = errors.New("state: not this writer's key")

	// ErrBadIncarnation: a stored incarnation is not a uint32 in canonical decimal form.
	ErrBadIncarnation = errors.New("state: bad incarnation")
	// ErrIncarnationConflict: the stored incarnation kept changing under Next (PROTOCOL §3.2 W6.3).
	ErrIncarnationConflict = errors.New("state: incarnation conflict")
	// ErrIncarnationExhausted: the worker key has taken every uint32.
	ErrIncarnationExhausted = errors.New("state: incarnations exhausted")
	// ErrNotSTS: only the STS agent deletes incarnation keys (PROTOCOL §3.2 W6.5).
	ErrNotSTS = errors.New("state: only STS forgets incarnations")
	// ErrNotController: the controller campaigns only as ControllerInstance (D907).
	ErrNotController = errors.New("state: the controller's instance is " + ControllerInstance)
	// ErrNoWorkers: incarnations belong to the agent planes; the controller has no workers.
	ErrNoWorkers = errors.New("state: plane has no workers")
)
