package state

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Users that are not agent instances. An agent's user is its instance name; the controller's is ControllerInstance.
const (
	UserAPI  = "api"  // the API server, every replica (DECISIONS E09-D007)
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
//     instances and the accounts of its TD instances. It also has write on those STS instances' status prefixes.
//     W5 deletes a terminal status, and etcd RBAC does not separate delete from write, so the grant is the whole
//     prefix (E10-D040). etcd therefore cannot refuse an api put of status. W1's "only the STS agent writes status"
//     is kept by the API program, not by etcd. That gap is deliberate: STS does not clear status, which would
//     overturn E10-D017. The API writes no demand, ack, agent, worker or writer key, and nothing under another
//     instance's prefix. It does write each agent instance's exact /retired/{inst} key, and only that key
//     (E12-D033). A prefix grant would let it delete the marker.
//   - An agent reads its own exact /retired/{inst} key and does not write it (E12-D036). The key is outside the
//     instance prefix, so the prefix read does not cover it. EnsureAuth only adds grants: a cluster whose role
//     table predates this key denies the read until the table is applied again.
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
			api.Permissions = append(api.Permissions, Permission{Key: RetiredKey(inst), Write: true})
			switch p {
			case PlaneSTS:
				api.Permissions = append(api.Permissions, prefixPerm(SessionSpecPrefix(inst), true),
					prefixPerm(SessionStatusPrefix(inst), true)) // E10-D040: write is the whole prefix; W5 deletes through it
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
		{Key: RetiredKey(inst), Write: false},
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

// EnsureAuth adds what root's cluster lacks of table: roles, their permissions, users and their roles. A new user's
// password is password(name). What is there already is left alone, so that applying the same table twice changes
// nothing, and does not bump the cluster's auth revision. It neither removes nor revokes anything: a role or a
// permission the table no longer has stays until removed by hand. Tests run every process under RBAC with it;
// deployment applies the tables the same way (mftiko-etcd-roles -apply, E12-D049).
func EnsureAuth(ctx context.Context, root *clientv3.Client, table AuthTable, password func(user string) string) error {
	for _, role := range table.Roles {
		got, err := root.RoleGet(ctx, role.Name)
		if errors.Is(err, rpctypes.ErrRoleNotFound) {
			if _, err = root.RoleAdd(ctx, role.Name); err != nil {
				return err
			}
			got = &clientv3.AuthRoleGetResponse{}
		} else if err != nil {
			return err
		}
		have := map[Permission]bool{}
		for _, p := range got.Perm {
			have[Permission{Key: string(p.Key), RangeEnd: string(p.RangeEnd), Write: p.PermType == authpb.READWRITE}] = true
		}
		for _, p := range role.Permissions {
			if have[p] {
				continue
			}
			kind := clientv3.PermissionType(clientv3.PermRead)
			if p.Write {
				kind = clientv3.PermissionType(clientv3.PermReadWrite)
			}
			if _, err := root.RoleGrantPermission(ctx, role.Name, p.Key, p.RangeEnd, kind); err != nil {
				return err
			}
		}
		user, err := root.UserGet(ctx, role.Name)
		if errors.Is(err, rpctypes.ErrUserNotFound) {
			if _, err := root.UserAdd(ctx, role.Name, password(role.Name)); err != nil {
				return err
			}
			user = &clientv3.AuthUserGetResponse{}
		} else if err != nil {
			return err
		}
		if !slices.Contains(user.Roles, role.Name) {
			if _, err := root.UserGrantRole(ctx, role.Name, role.Name); err != nil {
				return err
			}
		}
	}
	return nil
}
