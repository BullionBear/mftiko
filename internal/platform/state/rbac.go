package state

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Users that are not agent instances. An agent's user is its instance name; the controller's is ControllerInstance.
const (
	UserAPI  = "api"  // the API server, every replica (DECISIONS D33)
	UserRoot = "root" // etcd's own administrator, used only to apply the tables; it is not in them
)

// Permission is one key range a role may read, or read and write. RangeEnd is empty for a single key.
type Permission struct {
	Key      string `json:"key"`
	RangeEnd string `json:"range_end,omitempty"`
	Write    bool   `json:"write"` // readwrite; false is read only
}

// Role is an etcd role. Every user has exactly one, named like the user.
type Role struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

// AuthTable is the users and roles of one regional cluster.
type AuthTable struct {
	Region string `json:"region"`
	Roles  []Role `json:"roles"` // sorted by name; user i has role i
}

// Users are the user names of the table: one per role, with the same name.
func (a AuthTable) Users() []string {
	names := make([]string, len(a.Roles))
	for i, r := range a.Roles {
		names[i] = r.Name
	}
	return names
}

// AuthTables derives the users and roles of every regional cluster from the deployment's instances, following the
// writer column of PROTOCOL §3.1, so that etcd itself enforces W1 (E1-08). planes gives the plane of every instance
// in topo except ControllerInstance.
//
//   - An agent exists only in its own region's cluster. It reads its whole instance prefix and writes its own keys:
//     election, epoch, writer, AgentRecord, WorkerStatus, incarnations, and per plane the session status (STS),
//     acks (MD, TD), selector state and placements (MD). It writes no demand and nothing of another instance.
//   - The controller exists in every cluster. It reads everything and writes its own keys under
//     /ctl/controller/ and the demands of every agent instance of that region (PROTOCOL §3.3).
//   - The API server exists in every cluster. It reads everything and writes the session specs of the region's STS
//     instances and the accounts of its TD instances. It may also delete session statuses: W5 lets it remove a
//     terminal session's spec and status once their retention is over, the one exception to the STS agent being
//     the status's only writer.
//
// Granting a lease needs no permission; revoking one needs write permission on every key attached to it, which
// these tables give for the election session and the keys on it.
func AuthTables(topo Topology, planes map[string]Plane) (map[string]AuthTable, error) {
	if err := topo.Validate(); err != nil {
		return nil, err
	}
	if _, ok := topo.Instances[ControllerInstance]; !ok {
		return nil, fmt.Errorf("%w: no %s instance: its home region is needed", ErrBadTopology, ControllerInstance)
	}
	agents := map[string][]string{} // region -> agent instances
	for _, inst := range slices.Sorted(maps.Keys(topo.Instances)) {
		if inst == ControllerInstance {
			continue
		}
		p, ok := planes[inst]
		if !ok || !p.agent() {
			return nil, fmt.Errorf("%w: instance %s has plane %q, want sts, md or td", ErrBadTopology, inst, p)
		}
		if inst == UserAPI || inst == UserRoot {
			return nil, fmt.Errorf("%w: instance name %q is reserved for a user", ErrBadTopology, inst)
		}
		agents[topo.Instances[inst]] = append(agents[topo.Instances[inst]], inst)
	}
	for inst := range planes {
		if _, ok := topo.Instances[inst]; !ok {
			return nil, fmt.Errorf("%w: plane given for %s, which is not in the topology", ErrBadTopology, inst)
		}
	}

	all := prefixPerm(Root+"/", false)
	tables := map[string]AuthTable{}
	for _, region := range slices.Sorted(maps.Keys(topo.Regions)) {
		ctl := Role{Name: ControllerInstance, Permissions: []Permission{all, prefixPerm(instPrefix(PlaneCtl, ControllerInstance), true)}}
		api := Role{Name: UserAPI, Permissions: []Permission{all}}
		roles := []Role{}
		for _, inst := range agents[region] {
			p := planes[inst]
			roles = append(roles, agentRole(p, inst))
			ctl.Permissions = append(ctl.Permissions, prefixPerm(DemandPrefix(p, inst), true))
			switch p {
			case PlaneSTS:
				api.Permissions = append(api.Permissions, prefixPerm(SessionSpecPrefix(inst), true),
					prefixPerm(SessionStatusPrefix(inst), true)) // W5: delete after the retention
			case PlaneTD:
				api.Permissions = append(api.Permissions, prefixPerm(AccountPrefix(inst), true))
			}
		}
		roles = append(roles, ctl, api)
		slices.SortFunc(roles, func(a, b Role) int { return cmp.Compare(a.Name, b.Name) })
		tables[region] = AuthTable{Region: region, Roles: roles}
	}
	return tables, nil
}

// agentRole is what one agent instance may touch in its own cluster.
func agentRole(p Plane, inst string) Role {
	perms := []Permission{
		prefixPerm(instPrefix(p, inst), false),
		prefixPerm(ElectionPrefix(p, inst)+"/", true), // concurrency.Election appends the "/"
		{Key: EpochKey(p, inst), Write: true},
		{Key: WriterKey(p, inst), Write: true},
		{Key: AgentKey(p, inst), Write: true},
		prefixPerm(WorkerStatusPrefix(p, inst), true),
		prefixPerm(IncarnationPrefix(p, inst), true),
	}
	switch p {
	case PlaneSTS:
		perms = append(perms, prefixPerm(SessionStatusPrefix(inst), true))
	case PlaneMD:
		perms = append(perms, prefixPerm(MDAckPrefix(inst), true), prefixPerm(SelectorPrefix(inst), true),
			prefixPerm(PlacementPrefix(inst), true))
	case PlaneTD:
		perms = append(perms, prefixPerm(TDAckPrefix(inst), true))
	}
	return Role{Name: inst, Permissions: perms}
}

func prefixPerm(prefix string, write bool) Permission {
	return Permission{Key: prefix, RangeEnd: clientv3.GetPrefixRangeEnd(prefix), Write: write}
}
