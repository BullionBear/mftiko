package state

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// testTokenTTL and testSessionTTL are what state tests pass to etcdtest.TokenTTL
// and TermOptions.SessionTTL when the test waits on expiry. Production defaults
// stay etcdtest.DefaultTokenTTL (5m) and DefaultSessionTTL (15s).
const (
	testTokenTTL   = time.Second
	testSessionTTL = 2 * time.Second
)

// testPlanes reads the plane off the test instance names ("md-tw-1" is MD). Names that follow no plane get no user.
func testPlanes(topo Topology) map[string]Plane {
	planes := map[string]Plane{}
	for inst := range topo.Instances {
		for _, p := range []Plane{PlaneSTS, PlaneMD, PlaneTD} {
			if strings.HasPrefix(inst, string(p)+"-") {
				planes[inst] = p
			}
		}
	}
	return planes
}

// testTables are the auth tables of a test topology: its agent instances, plus the controller (at home in the first
// region if the topology has none) and the API server.
func testTables(t *testing.T, topo Topology) map[string]AuthTable {
	t.Helper()
	topo = topo.clone()
	planes := testPlanes(topo)
	for inst := range topo.Instances {
		if _, ok := planes[inst]; !ok && inst != ControllerInstance {
			delete(topo.Instances, inst)
		}
	}
	if _, ok := topo.Instances[ControllerInstance]; !ok {
		topo.Instances[ControllerInstance] = slices.Sorted(maps.Keys(topo.Regions))[0]
	}
	tables, err := AuthTables(topo, planes)
	if err != nil {
		t.Fatalf("AuthTables: %v", err)
	}
	return tables
}

// authMu serialises EnsureAuth: tests that start several processes at once would otherwise race to add the same
// role.
var authMu sync.Mutex

// as is the dial of a process authenticated as user, the way production dials (E1-08). Before dialling a region it
// makes sure, as root, that the region's cluster has the users and roles of topo: every test runs under RBAC.
func as(t *testing.T, topo Topology, user string) DialFunc {
	t.Helper()
	tables := testTables(t, topo)
	userDial := AuthDial(user, etcdtest.Password(user))
	return func(region string, endpoints []string) (*clientv3.Client, error) {
		root, err := AuthDial(UserRoot, etcdtest.RootPassword())(region, endpoints)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		authMu.Lock()
		err = EnsureAuth(ctx, root, tables[region], etcdtest.Password)
		authMu.Unlock()
		cancel()
		root.Close()
		if err != nil {
			return nil, fmt.Errorf("apply the auth table of %s: %w", region, err)
		}
		return userDial(region, endpoints)
	}
}

func TestAuthTables(t *testing.T) {
	topo := twoRegions()
	planes := map[string]Plane{"sts-tw-1": PlaneSTS, "md-tw-1": PlaneMD, "td-tw-1": PlaneTD, "md-jp-1": PlaneMD, "td-jp-1": PlaneTD}
	tables, err := AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	if got := slices.Sorted(maps.Keys(tables)); !slices.Equal(got, []string{"jp", "tw"}) {
		t.Fatalf("regions %v", got)
	}
	if got := tables["tw"].Users(); !slices.Equal(got, []string{"api", "controller", "md-tw-1", "sts-tw-1", "td-tw-1"}) {
		t.Fatalf("tw users %v", got)
	}
	if got := tables["jp"].Users(); !slices.Equal(got, []string{"api", "controller", "md-jp-1", "td-jp-1"}) {
		t.Fatalf("jp users %v (an agent exists only in its own region)", got)
	}

	// What each user may write, checked key by key against the table.
	writes := func(region, user, key string) bool {
		for _, r := range tables[region].Roles {
			if r.Name != user {
				continue
			}
			for _, p := range r.Permissions {
				if p.Write && (key == p.Key || (p.RangeEnd != "" && key >= p.Key && key < p.RangeEnd)) {
					return true
				}
			}
		}
		return false
	}
	reads := func(region, user, key string) bool {
		for _, r := range tables[region].Roles {
			if r.Name != user {
				continue
			}
			for _, p := range r.Permissions {
				if key == p.Key || (p.RangeEnd != "" && key >= p.Key && key < p.RangeEnd) {
					return true
				}
			}
		}
		return false
	}
	const sid, entry = "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT"
	h := WorkerHash("w")
	type check struct {
		region, user, key string
		write             bool
	}
	for _, c := range []check{
		// The STS agent: its own keys, not its spec or its assignments, nothing of others.
		{"tw", "sts-tw-1", ElectionPrefix(PlaneSTS, "sts-tw-1") + "/694d7e8a1c2b3f40", true},
		{"tw", "sts-tw-1", EpochKey(PlaneSTS, "sts-tw-1"), true},
		{"tw", "sts-tw-1", WriterKey(PlaneSTS, "sts-tw-1"), true},
		{"tw", "sts-tw-1", AgentKey(PlaneSTS, "sts-tw-1"), true},
		{"tw", "sts-tw-1", WorkerStatusKey(PlaneSTS, "sts-tw-1", h), true},
		{"tw", "sts-tw-1", IncarnationKey(PlaneSTS, "sts-tw-1", h), true},
		{"tw", "sts-tw-1", SessionStatusKey("sts-tw-1", sid), true},
		{"tw", "sts-tw-1", SessionSpecKey("sts-tw-1", sid), false},
		{"tw", "sts-tw-1", SessionDemandKey("sts-tw-1", sid), false},
		{"tw", "sts-tw-1", AgentKey(PlaneMD, "md-tw-1"), false},
		{"tw", "sts-tw-1", instPrefix(PlaneSTS, "sts-tw-1") + "electionX", false}, // not under election/
		// MD and TD: acks yes, demands no.
		{"tw", "md-tw-1", MDAckKey("md-tw-1", sid, entry), true},
		{"tw", "md-tw-1", SelectorKey("md-tw-1", "s"), true},
		{"tw", "md-tw-1", PlacementKey("md-tw-1", "a"), true},
		{"tw", "md-tw-1", MDDemandKey("md-tw-1", sid, entry), false},
		{"tw", "td-tw-1", TDAckKey("td-tw-1", sid, 42), true},
		{"tw", "td-tw-1", TDDemandKey("td-tw-1", sid, 42), false},
		{"tw", "td-tw-1", AccountKey("td-tw-1", 42), false},
		// The controller: every demand of the region, its own keys, nothing the agents or the API write.
		{"tw", "controller", SessionDemandKey("sts-tw-1", sid), true},
		{"tw", "controller", MDDemandKey("md-tw-1", sid, entry), true},
		{"tw", "controller", TDDemandKey("td-tw-1", sid, 42), true},
		{"jp", "controller", MDDemandKey("md-jp-1", sid, entry), true},
		{"jp", "controller", WriterKey(PlaneCtl, ControllerInstance), true},
		{"tw", "controller", AgentKey(PlaneCtl, ControllerInstance), true},
		{"tw", "controller", MDAckKey("md-tw-1", sid, entry), false},
		{"tw", "controller", SessionStatusKey("sts-tw-1", sid), false},
		{"tw", "controller", SessionSpecKey("sts-tw-1", sid), false},
		{"tw", "controller", WriterKey(PlaneMD, "md-tw-1"), false},
		// The API server: spec, account, and the whole status prefix (E10-D040). Nothing else, and
		// nothing under an instance this region's role was not given.
		{"tw", "api", SessionSpecKey("sts-tw-1", sid), true},
		{"tw", "api", AccountKey("td-tw-1", 42), true},
		{"jp", "api", AccountKey("td-jp-1", 42), true},
		{"tw", "api", SessionStatusKey("sts-tw-1", sid), true}, // E10-D040: write is the whole status/ prefix
		{"tw", "api", SessionDemandKey("sts-tw-1", sid), false},
		{"tw", "api", MDDemandKey("md-tw-1", sid, entry), false},
		{"tw", "api", TDDemandKey("td-tw-1", sid, 42), false},
		{"tw", "api", MDAckKey("md-tw-1", sid, entry), false},
		{"tw", "api", TDAckKey("td-tw-1", sid, 42), false},
		{"tw", "api", AgentKey(PlaneSTS, "sts-tw-1"), false},
		{"tw", "api", AgentKey(PlaneCtl, ControllerInstance), false},
		{"tw", "api", WorkerStatusKey(PlaneSTS, "sts-tw-1", h), false},
		{"tw", "api", WriterKey(PlaneSTS, "sts-tw-1"), false},
		{"tw", "api", WriterKey(PlaneCtl, ControllerInstance), false},
		{"jp", "api", SessionSpecKey("sts-tw-1", sid), false},   // another region's instance
		{"jp", "api", SessionStatusKey("sts-tw-1", sid), false}, // jp has no STS, so no status grant
		{"tw", "api", AccountKey("td-jp-1", 42), false},         // td-jp-1's account is granted only in jp
		{"tw", "api", SessionSpecKey("sts-jp-1", sid), false},   // an instance this cluster does not grant
		{"tw", "api", SessionStatusKey("sts-jp-1", sid), false},
	} {
		if got := writes(c.region, c.user, c.key); got != c.write {
			t.Errorf("%s in %s writes %s: %v, want %v", c.user, c.region, c.key, got, c.write)
		}
	}
	for _, c := range []check{
		{"tw", "md-tw-1", MDDemandKey("md-tw-1", sid, entry), true}, // its own demands, read only
		{"tw", "td-tw-1", AccountKey("td-tw-1", 42), true},
		{"tw", "sts-tw-1", SessionSpecKey("sts-tw-1", sid), true},
		{"tw", "md-tw-1", AgentKey(PlaneTD, "td-tw-1"), false}, // nothing of other instances
		{"tw", "controller", MDAckKey("md-tw-1", sid, entry), true},
		{"tw", "api", WorkerStatusKey(PlaneMD, "md-tw-1", h), true},
		{"tw", "api", SessionDemandKey("sts-tw-1", sid), true},
		{"tw", "api", MDAckKey("md-tw-1", sid, entry), true},
		{"tw", "api", WriterKey(PlaneCtl, ControllerInstance), true},
		{"tw", "api", AgentKey(PlaneTD, "td-tw-1"), true},
		{"tw", "api", SessionSpecKey("sts-jp-1", sid), true}, // read covers another instance's prefix
		{"jp", "api", AccountKey("td-tw-1", 42), true},
	} {
		if got := reads(c.region, c.user, c.key); got != c.write {
			t.Errorf("%s in %s reads %s: %v, want %v", c.user, c.region, c.key, got, c.write)
		}
	}

	bad := []struct {
		name   string
		topo   Topology
		planes map[string]Plane
	}{
		{"no controller", Topology{Regions: topo.Regions, Instances: map[string]string{"md-tw-1": "tw"}}, map[string]Plane{"md-tw-1": PlaneMD}},
		{"no plane", topo, map[string]Plane{"sts-tw-1": PlaneSTS}},
		{"controller plane", topo, map[string]Plane{"sts-tw-1": PlaneCtl}},
		{"reserved name", Topology{Regions: topo.Regions, Instances: map[string]string{ControllerInstance: "tw", "api": "tw"}},
			map[string]Plane{"api": PlaneMD}},
		{"plane of an unknown instance", Topology{Regions: topo.Regions, Instances: map[string]string{ControllerInstance: "tw"}},
			map[string]Plane{"md-kr-1": PlaneMD}},
		{"bad topology", Topology{}, nil},
	}
	for _, c := range bad {
		if _, err := AuthTables(c.topo, c.planes); !errors.Is(err, ErrBadTopology) {
			t.Errorf("%s: %v, want ErrBadTopology", c.name, err)
		}
	}
}

// etcd itself refuses what the tables do not grant, whatever the client: writes outside a user's keys, demands for
// an agent, watches of another instance, and revoking a lease that holds a key the user may not write.
func TestRBACEnforced(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1", "md-tw-1", "md-tw-2", "td-tw-1")
	raw := s.Client(t)
	if _, err := as(t, topo, "md-tw-1")("tw", s.Endpoints()); err != nil { // applies the tables
		t.Fatal(err)
	}
	md := etcdtest.ClientAs(t, s.Endpoints(), "md-tw-1")
	ctl := etcdtest.ClientAs(t, s.Endpoints(), ControllerInstance)
	api := etcdtest.ClientAs(t, s.Endpoints(), UserAPI)
	const sid, entry = "a1b2c3", "feed/x"
	denied := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, rpctypes.ErrPermissionDenied) {
			t.Errorf("%s: %v, want permission denied", what, err)
		}
	}
	allowed := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: %v", what, err)
		}
	}
	put := func(c *clientv3.Client, key string) error { _, err := c.Put(ctx, key, "v"); return err }

	allowed("md writes its ack", put(md, MDAckKey("md-tw-1", sid, entry)))
	denied("md writes another instance's ack", put(md, MDAckKey("md-tw-2", sid, entry)))
	denied("md writes its own demand", put(md, MDDemandKey("md-tw-1", sid, entry)))
	denied("md writes the STS agent's status", put(md, SessionStatusKey("sts-tw-1", sid)))
	_, err := md.Delete(ctx, instPrefix(PlaneMD, "md-tw-1"), clientv3.WithPrefix())
	denied("md wipes its whole instance", err)
	_, err = md.Get(ctx, instPrefix(PlaneMD, "md-tw-2"), clientv3.WithPrefix())
	denied("md reads another instance", err)

	allowed("controller writes a demand", put(ctl, MDDemandKey("md-tw-1", sid, entry)))
	denied("controller writes an ack", put(ctl, MDAckKey("md-tw-1", sid, entry)))
	denied("controller writes a spec", put(ctl, SessionSpecKey("sts-tw-1", sid)))
	// api on a cluster with auth on (E10-D041). Positive: read the keyspace, write spec
	// and account, delete a status the STS agent could have written, and write
	// status. The status put is deliberate (E10-D040): etcd cannot tell it from the
	// delete W5 needs. Negative: demand, ack, agent, worker, writer, and prefixes
	// of instances this role was not given.
	h := WorkerHash("w")
	const otherSTS, otherTD = "sts-tw-2", "td-tw-2"
	planted := []string{
		SessionSpecKey("sts-tw-1", sid),
		SessionStatusKey("sts-tw-1", sid),
		SessionDemandKey("sts-tw-1", sid),
		MDDemandKey("md-tw-1", sid, entry),
		TDDemandKey("td-tw-1", sid, 42),
		MDAckKey("md-tw-1", sid, entry),
		MDAckKey("md-tw-2", sid, entry),
		TDAckKey("td-tw-1", sid, 42),
		AgentKey(PlaneSTS, "sts-tw-1"),
		AgentKey(PlaneMD, "md-tw-1"),
		AgentKey(PlaneMD, "md-tw-2"),
		AgentKey(PlaneTD, "td-tw-1"),
		AgentKey(PlaneCtl, ControllerInstance),
		WorkerStatusKey(PlaneSTS, "sts-tw-1", h),
		WorkerStatusKey(PlaneMD, "md-tw-2", h),
		WorkerStatusKey(PlaneTD, "td-tw-1", h),
		WriterKey(PlaneSTS, "sts-tw-1"),
		WriterKey(PlaneMD, "md-tw-1"),
		WriterKey(PlaneMD, "md-tw-2"),
		WriterKey(PlaneTD, "td-tw-1"),
		WriterKey(PlaneCtl, ControllerInstance),
		AccountKey("td-tw-1", 42),
		EpochKey(PlaneMD, "md-tw-2"),
		SelectorKey("md-tw-1", "s"),
		PlacementKey("md-tw-2", "a"),
		IncarnationKey(PlaneSTS, "sts-tw-1", h),
		SessionSpecKey(otherSTS, sid),
		SessionStatusKey(otherSTS, sid),
		AccountKey(otherTD, 42),
	}
	for _, key := range planted {
		if _, err := raw.Put(ctx, key, "planted"); err != nil {
			t.Fatalf("plant %s: %v", key, err)
		}
	}
	read := map[string]string{}
	resp, err := api.Get(ctx, Root+"/", clientv3.WithPrefix())
	allowed("api reads the keyspace", err)
	if err == nil {
		for _, kv := range resp.Kvs {
			read[string(kv.Key)] = string(kv.Value)
		}
	}
	for _, key := range planted {
		if read[key] != "planted" {
			t.Errorf("api reads %s: %q", key, read[key])
		}
	}
	allowed("api writes a spec", put(api, SessionSpecKey("sts-tw-1", sid)))
	allowed("api writes an account", put(api, AccountKey("td-tw-1", 42)))
	statusKey := SessionStatusKey("sts-tw-1", "d1040")
	allowed("api writes a status", put(api, statusKey))
	if val, _, ok := kvOf(t, ctx, raw, statusKey); !ok || val != "v" {
		t.Errorf("api status write did not land: ok=%v val=%q", ok, val)
	}
	if _, err := api.Delete(ctx, SessionStatusKey("sts-tw-1", sid)); err != nil {
		t.Errorf("api deletes a status: %v", err)
	} else if _, _, ok := kvOf(t, ctx, raw, SessionStatusKey("sts-tw-1", sid)); ok {
		t.Error("api deletes a status: key still present")
	}
	for _, c := range []struct{ what, key string }{
		{"api writes an sts demand", SessionDemandKey("sts-tw-1", sid)},
		{"api writes an md demand", MDDemandKey("md-tw-1", sid, entry)},
		{"api writes a td demand", TDDemandKey("td-tw-1", sid, 42)},
		{"api writes an md ack", MDAckKey("md-tw-1", sid, entry)},
		{"api writes another instance's ack", MDAckKey("md-tw-2", sid, entry)},
		{"api writes a td ack", TDAckKey("td-tw-1", sid, 42)},
		{"api writes an agent record", AgentKey(PlaneSTS, "sts-tw-1")},
		{"api writes another instance's agent", AgentKey(PlaneMD, "md-tw-2")},
		{"api writes the controller's agent", AgentKey(PlaneCtl, ControllerInstance)},
		{"api writes a worker status", WorkerStatusKey(PlaneSTS, "sts-tw-1", h)},
		{"api writes another instance's worker", WorkerStatusKey(PlaneMD, "md-tw-2", h)},
		{"api writes a writer key", WriterKey(PlaneSTS, "sts-tw-1")},
		{"api writes another instance's writer", WriterKey(PlaneMD, "md-tw-2")},
		{"api writes the controller's writer", WriterKey(PlaneCtl, ControllerInstance)},
		{"api writes another instance's spec", SessionSpecKey(otherSTS, sid)},
		{"api writes another instance's account", AccountKey(otherTD, 42)},
		{"api writes another instance's status", SessionStatusKey(otherSTS, sid)},
		{"api writes under another instance", EpochKey(PlaneMD, "md-tw-2")},
	} {
		denied(c.what, put(api, c.key))
	}
	_, err = api.Delete(ctx, instPrefix(PlaneMD, "md-tw-2"), clientv3.WithPrefix())
	denied("api deletes another instance's prefix", err)

	// A txn needs every key it names: one foreign compare is enough to refuse it.
	_, err = md.Txn(ctx).If(clientv3.Compare(clientv3.Version(AgentKey(PlaneMD, "md-tw-2")), "=", 0)).
		Then(clientv3.OpPut(MDAckKey("md-tw-1", sid, entry), "v")).Commit()
	denied("md txn comparing another instance's key", err)

	// Watch: allowed on its own prefix, refused on another's.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// A refused watch comes back canceled, its reason the server's error as text.
	if resp := <-md.Watch(wctx, instPrefix(PlaneMD, "md-tw-2"), clientv3.WithPrefix()); !resp.Canceled ||
		resp.Err() == nil || !strings.Contains(resp.Err().Error(), rpctypes.ErrPermissionDenied.Error()) {
		t.Errorf("md watches another instance: %v, %v", resp.Err(), resp.Canceled)
	}
	own := md.Watch(wctx, MDAckPrefix("md-tw-1"), clientv3.WithPrefix())
	allowed("md writes its ack again", put(md, MDAckKey("md-tw-1", sid, "feed/y")))
	select {
	case resp := <-own:
		if resp.Err() != nil || len(resp.Events) != 1 {
			t.Errorf("md watching its acks: %v %v", resp.Err(), resp.Events)
		}
	case <-ctx.Done():
		t.Fatal("no event on md's own watch")
	}

	// Lease revoke needs write permission on every key attached to the lease.
	lease, err := md.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := md.Put(ctx, WorkerStatusKey(PlaneMD, "md-tw-1", "h"), "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Put(ctx, AgentKey(PlaneMD, "md-tw-2"), "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	_, err = md.Revoke(ctx, lease.ID)
	denied("md revokes a lease holding md-tw-2's key", err)
	if _, err := raw.Delete(ctx, AgentKey(PlaneMD, "md-tw-2")); err != nil {
		t.Fatal(err)
	}
	_, err = md.Revoke(ctx, lease.ID)
	allowed("md revokes a lease holding only its keys", err)
}

// Tokens expire and auth changes; a long-running process keeps working: its session stays alive with nothing but
// keepalives, its watch keeps delivering, and its writes authenticate again.
func TestTokenExpiry(t *testing.T) {
	ctx := testCtx(t)
	s := etcdtest.Start(t, etcdtest.TokenTTL(testTokenTTL))
	raw := s.Client(t)
	topo := topologyOf(map[string]*etcdtest.Server{"tw": s}, map[string]string{"md-tw-1": "tw"})
	cl := newClusters(t, topo, nil, as(t, topo, "md-tw-1"))
	term := campaign(t, ctx, cl, PlaneMD, "md-tw-1", TermOptions{SessionTTL: testSessionTTL})
	if err := term.PutRecord(ctx, "record"); err != nil {
		t.Fatal(err)
	}
	r, err := cl.Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wch := r.Watch(wctx, DemandPrefix(PlaneMD, "md-tw-1"), clientv3.WithPrefix())

	// A token issued now is newer than the one the watch and the session were
	// opened with, so when it is rejected those have expired too. The bound
	// stays several times testTokenTTL.
	waitRejected(t, ctx, raw, s.Endpoints(), "md-tw-1", AgentKey(PlaneMD, "md-tw-1"))
	if _, err := raw.Put(ctx, MDDemandKey("md-tw-1", "a1b2c3", "feed/x"), "d"); err != nil {
		t.Fatal(err)
	}
	select {
	case resp := <-wch:
		if resp.Err() != nil || len(resp.Events) != 1 {
			t.Fatalf("watch after the token expired: %v %v", resp.Err(), resp.Events)
		}
	case <-ctx.Done():
		t.Fatal("watch silent after the token expired")
	}
	if err := term.Put(ctx, MDAckKey("md-tw-1", "a1b2c3", "feed/x"), "READY"); err != nil {
		t.Fatalf("write after the token expired: %v", err)
	}

	// A change to users and roles makes every token older than the auth revision: the client authenticates again.
	if _, err := raw.RoleAdd(ctx, "unrelated"); err != nil {
		t.Fatal(err)
	}
	if err := term.Put(ctx, MDAckKey("md-tw-1", "a1b2c3", "feed/x"), "PENDING"); err != nil {
		t.Fatalf("write after an auth change: %v", err)
	}
	// An unrenewed lease of the session TTL has to actually disappear before the
	// record's survival means the keepalives held it. etcd sweeps expired leases
	// on its own ticker and later under load. The bound stays wider than testSessionTTL.
	canary := "/canary/session-ttl"
	lease, err := raw.Grant(ctx, int64(testSessionTTL/time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Put(ctx, canary, "x", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	until(t, 30*time.Second, "an unrenewed lease of the session TTL is gone", func() error {
		if _, _, ok := kvOf(t, ctx, raw, canary); ok {
			return fmt.Errorf("canary %s still present on lease %x", canary, lease.ID)
		}
		return nil
	})
	if !running(term) {
		t.Fatalf("term ended: %v", term.Err())
	}
	if kvs := mustGet(t, ctx, raw, AgentKey(PlaneMD, "md-tw-1")).Kvs; len(kvs) != 1 {
		t.Fatal("record gone: the session was not kept alive")
	}
}

// A dial with the wrong credentials fails, and the next use dials again.
func TestDialWrongPassword(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	if _, err := as(t, topo, "md-tw-1")("tw", s.Endpoints()); err != nil {
		t.Fatal(err)
	}
	user, other := "md-tw-1", "md-tw-2"
	wrong := 0
	dial := func(region string, eps []string) (*clientv3.Client, error) {
		wrong++
		if wrong == 1 {
			return AuthDial(user, etcdtest.Password(other))(region, eps) // another user's
		}
		return AuthDial(user, etcdtest.Password(user))(region, eps)
	}
	cl := newClusters(t, topo, nil, dial)
	if _, err := cl.client(ctx, "tw"); !errors.Is(err, rpctypes.ErrAuthFailed) {
		t.Fatalf("first dial: %v, want auth failed", err)
	}
	if _, err := Campaign(ctx, cl, PlaneMD, "md-tw-1", TermOptions{ID: "x"}); err != nil {
		t.Fatalf("Campaign after a failed dial: %v", err)
	}
	if wrong != 2 {
		t.Fatalf("%d dials, want 2", wrong)
	}
}

// Mirrors on one client after its token has expired: a mirror started late, and one that reopens its watch after a
// compaction and a change to users and roles, both keep converging. etcd checks a new watch against the token its
// gRPC stream was opened with; if mirrors shared a stream, the late one's watch would land on a stream opened before
// the expiry and be refused for good. Each mirror has a stream of its own (Mirror.stream).
func TestMirrorAfterTokenExpiry(t *testing.T) {
	ctx := testCtx(t)
	s := etcdtest.Start(t, etcdtest.TokenTTL(testTokenTTL))
	raw := s.Client(t)
	topo := topologyOf(map[string]*etcdtest.Server{"tw": s}, map[string]string{"td-tw-1": "tw"})
	r, err := newClusters(t, topo, nil, as(t, topo, "td-tw-1")).Reader("tw") // the agent's own, narrow, rights
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, ctx, raw, ackKey(0), "v0")
	m := startMirror(t, r, TDAckPrefix("td-tw-1"), MirrorOptions{MaxBackoff: 200 * time.Millisecond})

	// m's watch keeps the token it was opened with. A token issued after that
	// watch expires later, so rejection means the watch's token is already
	// dead. The bound stays wider than testTokenTTL.
	waitRejected(t, ctx, raw, s.Endpoints(), "td-tw-1", ackKey(0))
	late := startMirror(t, r, TDAckPrefix("td-tw-1"), MirrorOptions{MaxBackoff: 200 * time.Millisecond})
	mustPut(t, ctx, raw, ackKey(1), "v1")
	waitConverged(t, ctx, late, raw, 20*time.Second)
	waitConverged(t, ctx, m, raw, 20*time.Second)

	rev := mustPut(t, ctx, raw, ackKey(2), "v2")
	if _, err := raw.Compact(ctx, rev); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.RoleAdd(ctx, "unrelated"); err != nil { // every token is now older than the auth revision
		t.Fatal(err)
	}
	m.askRelist()
	mustPut(t, ctx, raw, ackKey(3), "v3")
	waitConverged(t, ctx, m, raw, 20*time.Second)
	waitConverged(t, ctx, late, raw, 20*time.Second)
}

func TestAuthRefused(t *testing.T) {
	for _, err := range []error{
		errors.New("rpc error: code = Unauthenticated desc = " + rpctypes.ErrInvalidAuthToken.Error()),
		rpctypes.ErrAuthOldRevision,
	} {
		if !authRefused(err) {
			t.Errorf("%v: not recognised", err)
		}
	}
	for _, err := range []error{nil, rpctypes.ErrPermissionDenied, errors.New("watch channel closed")} {
		if authRefused(err) {
			t.Errorf("%v: taken for a refused token", err)
		}
	}
}

// waitRejected waits until a token issued now is refused. That token is newer
// than one the test's long-running client already presented, so the older one
// has expired as well. The bound is several times a test token TTL.
func waitRejected(t *testing.T, ctx context.Context, raw *clientv3.Client, endpoints []string, user, key string) {
	t.Helper()
	issued := time.Now()
	probe := fixedTokenClient(t, endpoints, issueToken(t, ctx, raw, user))
	until(t, 20*time.Second, "a token issued for "+user+" is rejected", func() error {
		rejected, err := probe.rejected(ctx, key)
		if err != nil {
			return err
		}
		if !rejected {
			return fmt.Errorf("token still accepted, %s after it was issued", time.Since(issued).Round(time.Millisecond))
		}
		return nil
	})
}

// issueToken logs user in and returns the token etcd just signed. It is newer
// than a token the test's long-running client already presented.
func issueToken(t *testing.T, ctx context.Context, raw *clientv3.Client, user string) string {
	t.Helper()
	resp, err := raw.Authenticate(ctx, user, etcdtest.Password(user))
	if err != nil {
		t.Fatalf("authenticate %s: %v", user, err)
	}
	if resp.Token == "" {
		t.Fatalf("authenticate %s: empty token", user)
	}
	return resp.Token
}

// fixedToken is a client that presents one token and cannot refresh it.
type fixedToken struct {
	cli *clientv3.Client
}

func fixedTokenClient(t *testing.T, endpoints []string, token string) *fixedToken {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: DialTimeout,
		Token:       token,
	})
	if err != nil {
		t.Fatalf("client for a fixed token: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return &fixedToken{cli: cli}
}

// rejected reports whether the cluster refuses the fixed token. A permission
// failure still means the token was accepted. Any other error is returned so
// the caller can show it.
func (f *fixedToken) rejected(ctx context.Context, key string) (bool, error) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := f.cli.Get(cctx, key)
	if err == nil || strings.Contains(err.Error(), rpctypes.ErrPermissionDenied.Error()) {
		return false, nil
	}
	if authRefused(err) {
		return true, nil
	}
	return false, err
}
