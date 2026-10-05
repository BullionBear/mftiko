package state

import "errors"

var (
	// ErrBadKey: the key is not Root/{plane}/{inst}/... with a known plane and a valid instance name.
	ErrBadKey = errors.New("state: bad key")
	// ErrUnknownPlane: a plane other than sts, md, td.
	ErrUnknownPlane = errors.New("state: unknown plane")
	// ErrBadTopology: Topology.Validate failed.
	ErrBadTopology = errors.New("state: bad topology")
	// ErrUnknownInstance: the instance is not in the topology.
	ErrUnknownInstance = errors.New("state: unknown instance")
	// ErrUnknownRegion: the region is not in the topology.
	ErrUnknownRegion = errors.New("state: unknown region")
	// ErrRegionNotAllowed: the region exists but this process may not connect to it (D17 rule 2).
	ErrRegionNotAllowed = errors.New("state: region not allowed")
	// ErrClustersClosed: Clusters.Close was called.
	ErrClustersClosed = errors.New("state: clusters closed")

	// ErrBadOptions: TermOptions without an ID or with a negative TTL.
	ErrBadOptions = errors.New("state: bad term options")
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
)
