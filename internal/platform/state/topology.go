package state

import (
	"fmt"
	"maps"
	"slices"
	"time"
)

// Topology is the deployment's etcd layout: one cluster per region, and the region of every instance (E00-D005).
// Loading it from a file is E12.
type Topology struct {
	Regions   map[string]Region `json:"regions"`   // region -> its cluster
	Instances map[string]string `json:"instances"` // instance -> region
}

// Region is one regional etcd cluster.
//
// DialTimeout, RequestTimeout and StaleAfter are optional Go duration strings
// for the controller (E12-D046). Empty means the caller's default. Agents and
// the API accept the same fields in the shared deployment file and do not
// apply them to their own dials.
type Region struct {
	Endpoints      []string `json:"endpoints"`
	DialTimeout    string   `json:"dial_timeout,omitempty"`
	RequestTimeout string   `json:"request_timeout,omitempty"`
	StaleAfter     string   `json:"stale_after,omitempty"`
}

// Timeouts parses the optional durations (E12-D046). An empty string or a
// zero duration ("0", "0s") is 0, which selects the caller's default: dial is
// DialTimeout, request timeout adds no unary deadline, and the stale threshold
// is the controller's 15s. A value that is not a Go duration, or is negative,
// is ErrBadTopology. The fields are strings, so a JSON number does not decode.
func (r Region) Timeouts() (dial, request, stale time.Duration, err error) {
	dial, err = optionalDuration("dial_timeout", r.DialTimeout)
	if err != nil {
		return 0, 0, 0, err
	}
	request, err = optionalDuration("request_timeout", r.RequestTimeout)
	if err != nil {
		return 0, 0, 0, err
	}
	stale, err = optionalDuration("stale_after", r.StaleAfter)
	if err != nil {
		return 0, 0, 0, err
	}
	return dial, request, stale, nil
}

func optionalDuration(field, raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q", ErrBadTopology, field, raw)
	}
	if d < 0 {
		return 0, fmt.Errorf("%w: %s %s is negative", ErrBadTopology, field, d)
	}
	return d, nil
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
		if _, _, _, err := t.Regions[name].Timeouts(); err != nil {
			return fmt.Errorf("region %s: %w", name, err)
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

// RegionOfKey is the region whose cluster holds key: the region of its Owner (PROTOCOL §3, E00-D005 rule 1).
// A retirement marker is not under /{plane}/{inst}/; it routes to that instance's home region (E12-D033).
func (t Topology) RegionOfKey(key string) (string, error) {
	if inst, ok := RetiredInstance(key); ok {
		return t.RegionOf(inst)
	}
	_, inst, err := Owner(key)
	if err != nil {
		return "", err
	}
	return t.RegionOf(inst)
}

func (t Topology) clone() Topology {
	c := Topology{Regions: make(map[string]Region, len(t.Regions)), Instances: maps.Clone(t.Instances)}
	for name, r := range t.Regions {
		cp := r
		cp.Endpoints = slices.Clone(r.Endpoints)
		c.Regions[name] = cp
	}
	if c.Instances == nil {
		c.Instances = map[string]string{}
	}
	return c
}
