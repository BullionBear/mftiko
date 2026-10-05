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
)
