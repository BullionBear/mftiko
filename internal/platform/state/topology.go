package state

import (
	"fmt"
	"maps"
	"slices"
)

// Topology is the deployment's etcd layout: one cluster per region, and the region of every instance (D17).
// Loading it from a file is E12.
type Topology struct {
	Regions   map[string]Region `json:"regions"`   // region -> its cluster
	Instances map[string]string `json:"instances"` // instance -> region
}

// Region is one regional etcd cluster.
type Region struct {
	Endpoints []string `json:"endpoints"`
}

// Validate checks that there is at least one region, every region has at least one endpoint, and every instance
// name is valid (PROTOCOL §2) and maps to a defined region. The region is never parsed out of the instance name:
// the {plane}-{region}-{n} convention is not enforced.
func (t Topology) Validate() error {
	if len(t.Regions) == 0 {
		return fmt.Errorf("%w: no regions", ErrBadTopology)
	}
	for _, name := range slices.Sorted(maps.Keys(t.Regions)) {
		if name == "" {
			return fmt.Errorf("%w: empty region name", ErrBadTopology)
		}
		eps := t.Regions[name].Endpoints
		if len(eps) == 0 {
			return fmt.Errorf("%w: region %s has no endpoints", ErrBadTopology, name)
		}
		if slices.Contains(eps, "") {
			return fmt.Errorf("%w: region %s has an empty endpoint", ErrBadTopology, name)
		}
	}
	for _, inst := range slices.Sorted(maps.Keys(t.Instances)) {
		if !instanceName.MatchString(inst) {
			return fmt.Errorf("%w: bad instance name %q", ErrBadTopology, inst)
		}
		if _, ok := t.Regions[t.Instances[inst]]; !ok {
			return fmt.Errorf("%w: instance %s maps to undefined region %q", ErrBadTopology, inst, t.Instances[inst])
		}
	}
	return nil
}

// RegionOf is the region an instance runs in: clusterFor(instance) in PROTOCOL §3.
func (t Topology) RegionOf(inst string) (string, error) {
	r, ok := t.Instances[inst]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownInstance, inst)
	}
	return r, nil
}

// RegionOfKey is the region whose cluster holds key: the region of its Owner (PROTOCOL §3, D17 rule 1).
func (t Topology) RegionOfKey(key string) (string, error) {
	_, inst, err := Owner(key)
	if err != nil {
		return "", err
	}
	return t.RegionOf(inst)
}

func (t Topology) clone() Topology {
	c := Topology{Regions: make(map[string]Region, len(t.Regions)), Instances: maps.Clone(t.Instances)}
	for name, r := range t.Regions {
		c.Regions[name] = Region{Endpoints: slices.Clone(r.Endpoints)}
	}
	if c.Instances == nil {
		c.Instances = map[string]string{}
	}
	return c
}
