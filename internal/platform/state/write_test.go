package state

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRouteTxn(t *testing.T) {
	topo := twoRegions()
	ctl := writeScope{plane: PlaneCtl, inst: ControllerInstance}
	const sid, entry = "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT"
	spec := SessionSpecKey("sts-tw-1", sid)
	demand := SessionDemandKey("sts-tw-1", sid)
	jpDemand := MDDemandKey("md-jp-1", sid, entry)
	jpAck := MDAckKey("md-jp-1", sid, entry)
	twAgent := AgentKey(PlaneMD, "md-tw-1") // another instance, same region
	put := func(k string) clientv3.Op { return clientv3.OpPut(k, "v") }
	get := func(k string) clientv3.Op { return clientv3.OpGet(k) }

	ok := []struct {
		name string
		cmps []clientv3.Cmp
		then []clientv3.Op
		els  []clientv3.Op
		want string
	}{
		{"one put", nil, []clientv3.Op{put(jpDemand)}, nil, "jp"},
		{"same region, two instances", []clientv3.Cmp{clientv3.Compare(clientv3.Version(spec), ">", 0)},
			[]clientv3.Op{put(demand)}, []clientv3.Op{get(twAgent)}, "tw"},
		{"demand and ack of one provider", []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(jpAck), "=", 0)},
			[]clientv3.Op{put(jpDemand)}, nil, "jp"},
		{"prefix delete inside the instance", nil,
			[]clientv3.Op{clientv3.OpDelete(DemandPrefix(PlaneMD, "md-jp-1")+sid+"/", clientv3.WithPrefix())}, nil, "jp"},
		{"whole instance", nil,
			[]clientv3.Op{clientv3.OpGet(instPrefix(PlaneMD, "md-jp-1"), clientv3.WithPrefix())}, nil, "jp"},
		{"nested txn", nil,
			[]clientv3.Op{clientv3.OpTxn(nil, []clientv3.Op{put(demand)}, []clientv3.Op{get(spec)})}, nil, "tw"},
	}
	for _, c := range ok {
		if got, err := routeTxn(topo, ctl, c.cmps, c.then, c.els); err != nil || got != c.want {
			t.Errorf("%s: %s, %v; want %s", c.name, got, err, c.want)
		}
	}

	bad := []struct {
		name string
		cmps []clientv3.Cmp
		then []clientv3.Op
		els  []clientv3.Op
		want error
	}{
		{"compare tw, write jp", []clientv3.Cmp{clientv3.Compare(clientv3.Version(spec), ">", 0)},
			[]clientv3.Op{put(jpDemand)}, nil, ErrCrossCluster},
		{"then tw, else jp", nil, []clientv3.Op{put(demand)}, []clientv3.Op{put(jpDemand)}, ErrCrossCluster},
		{"jp inside a nested txn", nil,
			[]clientv3.Op{clientv3.OpTxn(nil, []clientv3.Op{put(demand)}, []clientv3.Op{put(jpDemand)})}, nil, ErrCrossCluster},
		{"range compare leaving the instance", []clientv3.Cmp{clientv3.Compare(clientv3.Version(spec), ">", 0).WithRange(Root + "/td/")},
			[]clientv3.Op{put(demand)}, nil, ErrCrossCluster},
		{"from-key delete", nil, []clientv3.Op{clientv3.OpDelete(demand, clientv3.WithFromKey())}, nil, ErrCrossCluster},
		{"range past the instance", nil,
			[]clientv3.Op{clientv3.OpDelete(demand, clientv3.WithRange(Root+"/sts/sts-tw-2/"))}, nil, ErrCrossCluster},
		{"no keys", nil, nil, nil, ErrBadKey},
		{"key outside Root", nil, []clientv3.Op{put("/elsewhere")}, nil, ErrBadKey},
		{"unknown instance", nil, []clientv3.Op{put(AgentKey(PlaneMD, "md-kr-1"))}, nil, ErrUnknownInstance},
	}
	for _, c := range bad {
		if got, err := routeTxn(topo, ctl, c.cmps, c.then, c.els); !errors.Is(err, c.want) {
			t.Errorf("%s: %s, %v; want %v", c.name, got, err, c.want)
		}
	}
}

// W1: each agent, and the controller, writes only the rows of PROTOCOL §3.1 whose writer it is.
func TestWriteScope(t *testing.T) {
	topo := twoRegions()
	for _, inst := range []string{"sts-tw-2", "md-tw-2", "td-tw-2", "ctl-tw-2"} {
		topo.Instances[inst] = "tw"
	}
	const sid, entry = "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT"
	h := WorkerHash("w")

	// reserved are the keys no process writes through the write API: what fencing relies on, the API server's
	// declarations, and incarnations outside Incarnations.
	reserved := func(p Plane, inst string) []string {
		return []string{
			ElectionPrefix(p, inst) + "/694d7e8a1c2b3f40",
			EpochKey(p, inst),
			WriterKey(p, inst),
			IncarnationKey(p, inst, h),
			instPrefix(p, inst) + "other",
			AgentKey(p, inst) + "/sub", // an exact key is not a prefix
		}
	}
	cases := []struct {
		scope    writeScope
		writable []string
		denied   []string
	}{
		{
			scope: writeScope{plane: PlaneSTS, inst: "sts-tw-1"},
			writable: []string{
				SessionStatusKey("sts-tw-1", sid),
				WorkerStatusKey(PlaneSTS, "sts-tw-1", h),
			},
			denied: append(reserved(PlaneSTS, "sts-tw-1"),
				AgentKey(PlaneSTS, "sts-tw-1"),    // bound to the election session: PutRecord only (W7)
				SessionSpecKey("sts-tw-1", sid),   // the API server's
				SessionDemandKey("sts-tw-1", sid), // the controller's: STS only reads its assignments
				SessionStatusKey("sts-tw-2", sid),
				AgentKey(PlaneSTS, "sts-tw-2"),
				MDDemandKey("md-jp-1", sid, entry), // no longer the STS agent's to write (D9)
				TDDemandKey("td-tw-1", sid, 42),
				MDAckKey("md-jp-1", sid, entry), // the provider's
				TDAckKey("td-jp-1", sid, 42),
				WriterKey(PlaneMD, "md-tw-1"),
				AgentKey(PlaneMD, "md-tw-1"),
				AgentKey(PlaneCtl, ControllerInstance),
			),
		},
		{
			scope: writeScope{plane: PlaneMD, inst: "md-tw-1"},
			writable: []string{
				MDAckKey("md-tw-1", sid, entry),
				SelectorKey("md-tw-1", "s1"),
				PlacementKey("md-tw-1", "a1"),
				WorkerStatusKey(PlaneMD, "md-tw-1", h),
			},
			denied: append(reserved(PlaneMD, "md-tw-1"),
				AgentKey(PlaneMD, "md-tw-1"),
				MDDemandKey("md-tw-1", sid, entry), // providers only read demands (PROTOCOL §3.3 N7)
				MDAckKey("md-tw-2", sid, entry),
				SelectorKey("md-tw-2", "s1"),
				AgentKey(PlaneMD, "md-tw-2"),
				AgentKey(PlaneTD, "td-tw-1"),
				SessionStatusKey("sts-tw-1", sid),
			),
		},
		{
			scope: writeScope{plane: PlaneTD, inst: "td-tw-1"},
			writable: []string{
				TDAckKey("td-tw-1", sid, 42),
				WorkerStatusKey(PlaneTD, "td-tw-1", h),
			},
			denied: append(reserved(PlaneTD, "td-tw-1"),
				AgentKey(PlaneTD, "td-tw-1"),
				AccountKey("td-tw-1", 42), // the API server's
				TDDemandKey("td-tw-1", sid, 42),
				TDAckKey("td-tw-2", sid, 42),
				WorkerStatusKey(PlaneTD, "td-tw-2", h),
				MDAckKey("md-tw-1", sid, entry),
			),
		},
		{
			scope: writeScope{plane: PlaneCtl, inst: ControllerInstance},
			writable: []string{
				SessionDemandKey("sts-tw-1", sid),
				SessionDemandKey("sts-tw-2", sid),
				MDDemandKey("md-jp-1", sid, entry), // another region's MD instance
				MDDemandKey("md-tw-1", sid, "select/top10"),
				TDDemandKey("td-jp-1", sid, 42),
			},
			denied: append(reserved(PlaneCtl, ControllerInstance),
				AgentKey(PlaneCtl, ControllerInstance), // PutRecord only
				SessionSpecKey("sts-tw-1", sid),        // the API server's
				AccountKey("td-jp-1", 42),
				SessionStatusKey("sts-tw-1", sid), // the agents' own keys
				MDAckKey("md-jp-1", sid, entry),
				TDAckKey("td-tw-1", sid, 42),
				AgentKey(PlaneMD, "md-tw-1"),
				WorkerStatusKey(PlaneSTS, "sts-tw-1", h),
				SelectorKey("md-tw-1", "s1"),
				IncarnationKey(PlaneTD, "td-tw-1", h),
				WriterKey(PlaneMD, "md-jp-1"),
				WorkerStatusKey(PlaneCtl, ControllerInstance, h), // the controller has no workers
				DemandPrefix(PlaneCtl, ControllerInstance)+"x",   // nor demands of its own
				AgentKey(PlaneCtl, "ctl-tw-2"),                   // a name that is not the controller's
				EpochKey(PlaneCtl, "ctl-tw-2"),
				DemandPrefix(PlaneCtl, "ctl-tw-2")+"x",
			),
		},
		{
			// A controller scope under any other name writes nothing (Campaign refuses the name in the first place).
			scope: writeScope{plane: PlaneCtl, inst: "ctl-tw-2"},
			denied: []string{
				SessionDemandKey("sts-tw-1", sid),
				MDDemandKey("md-jp-1", sid, entry),
				TDDemandKey("td-tw-1", sid, 42),
				AgentKey(PlaneCtl, "ctl-tw-2"),
				AgentKey(PlaneCtl, ControllerInstance),
			},
		},
		// The AgentRecord, written by PutRecord alone (W7).
		{
			scope:    writeScope{plane: PlaneMD, inst: "md-tw-1", record: true},
			writable: []string{AgentKey(PlaneMD, "md-tw-1")},
			denied: append(reserved(PlaneMD, "md-tw-1"),
				WorkerStatusKey(PlaneMD, "md-tw-1", h),
				MDAckKey("md-tw-1", sid, entry),
				PlacementKey("md-tw-1", "a1"),
				AgentKey(PlaneMD, "md-tw-2"),
				AgentKey(PlaneCtl, ControllerInstance),
			),
		},
		{
			scope:    writeScope{plane: PlaneCtl, inst: ControllerInstance, record: true},
			writable: []string{AgentKey(PlaneCtl, ControllerInstance)},
			denied: append(reserved(PlaneCtl, ControllerInstance),
				SessionDemandKey("sts-tw-1", sid),
				MDDemandKey("md-jp-1", sid, entry),
				AgentKey(PlaneMD, "md-tw-1"),
				AgentKey(PlaneCtl, "ctl-tw-2"),
			),
		},
		{
			scope:  writeScope{plane: PlaneCtl, inst: "ctl-tw-2", record: true},
			denied: []string{AgentKey(PlaneCtl, "ctl-tw-2"), AgentKey(PlaneCtl, ControllerInstance)},
		},
		{
			// The controller has no workers, hence no incarnations (LoadIncarnations refuses it too).
			scope:  writeScope{plane: PlaneCtl, inst: ControllerInstance, incarnations: true},
			denied: []string{IncarnationKey(PlaneCtl, ControllerInstance, h), AgentKey(PlaneCtl, ControllerInstance)},
		},
		{
			scope:    writeScope{plane: PlaneTD, inst: "td-tw-1", incarnations: true},
			writable: []string{IncarnationKey(PlaneTD, "td-tw-1", h)},
			denied: []string{
				AgentKey(PlaneTD, "td-tw-1"),
				WriterKey(PlaneTD, "td-tw-1"),
				IncarnationKey(PlaneTD, "td-tw-2", h),
			},
		},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%s/%s(incarnations=%v, record=%v)", c.scope.plane, c.scope.inst, c.scope.incarnations, c.scope.record)
		for _, k := range c.writable {
			for _, op := range []clientv3.Op{clientv3.OpPut(k, "v"), clientv3.OpDelete(k)} {
				if _, err := routeTxn(topo, c.scope, nil, []clientv3.Op{op}, nil); err != nil {
					t.Errorf("%s: %s refused: %v", name, k, err)
				}
			}
		}
		for _, k := range c.denied {
			ops := map[string]clientv3.Op{
				"put":    clientv3.OpPut(k, "v"),
				"delete": clientv3.OpDelete(k),
				"nested": clientv3.OpTxn(nil, nil, []clientv3.Op{clientv3.OpPut(k, "v")}),
			}
			for kind, op := range ops {
				if _, err := routeTxn(topo, c.scope, nil, []clientv3.Op{op}, nil); !errors.Is(err, ErrNotWriter) {
					t.Errorf("%s: %s %s: %v, want ErrNotWriter", name, kind, k, err)
				}
			}
			// Reading it is fine.
			if _, err := routeTxn(topo, c.scope, []clientv3.Cmp{clientv3.Compare(clientv3.Version(k), ">", 0)},
				[]clientv3.Op{clientv3.OpGet(k)}, nil); err != nil {
				t.Errorf("%s: reading %s: %v", name, k, err)
			}
		}
	}

	// Ranged deletes: within one writable prefix only; exact keys take no range.
	rangeDel := func(scope writeScope) func(key string, opts ...clientv3.OpOption) error {
		return func(key string, opts ...clientv3.OpOption) error {
			_, err := routeTxn(topo, scope, nil, []clientv3.Op{clientv3.OpDelete(key, opts...)}, nil)
			return err
		}
	}
	sts := rangeDel(writeScope{plane: PlaneSTS, inst: "sts-tw-1"})
	ctl := rangeDel(writeScope{plane: PlaneCtl, inst: ControllerInstance})
	for name, err := range map[string]error{
		"STS: all statuses":                    sts(SessionStatusPrefix("sts-tw-1"), clientv3.WithPrefix()),
		"STS: range up to the prefix's end":    sts(SessionStatusKey("sts-tw-1", "a"), clientv3.WithRange(clientv3.GetPrefixRangeEnd(SessionStatusPrefix("sts-tw-1")))),
		"STS: workers between two hashes":      sts(WorkerStatusKey(PlaneSTS, "sts-tw-1", "0"), clientv3.WithRange(WorkerStatusKey(PlaneSTS, "sts-tw-1", "8"))),
		"controller: one session's MD demands": ctl(DemandPrefix(PlaneMD, "md-jp-1")+sid+"/", clientv3.WithPrefix()),
		"controller: every demand of md-jp-1":  ctl(DemandPrefix(PlaneMD, "md-jp-1"), clientv3.WithPrefix()),
		"controller: one session's TD demands": ctl(DemandPrefix(PlaneTD, "td-jp-1")+sid+"/", clientv3.WithPrefix()),
	} {
		if err != nil {
			t.Errorf("ranged delete, %s: %v", name, err)
		}
	}
	for name, err := range map[string]error{
		"STS: whole instance":                   sts(instPrefix(PlaneSTS, "sts-tw-1"), clientv3.WithPrefix()),
		"STS: status into worker":               sts(SessionStatusKey("sts-tw-1", "a"), clientv3.WithRange(WorkerStatusPrefix(PlaneSTS, "sts-tw-1")+"x")),
		"STS: past the status prefix":           sts(SessionStatusKey("sts-tw-1", "a"), clientv3.WithRange(instPrefix(PlaneSTS, "sts-tw-1")+"statut")),
		"STS: agent as a prefix":                sts(AgentKey(PlaneSTS, "sts-tw-1"), clientv3.WithPrefix()),
		"STS: its own demands":                  sts(DemandPrefix(PlaneSTS, "sts-tw-1"), clientv3.WithPrefix()),
		"controller: whole MD instance":         ctl(instPrefix(PlaneMD, "md-jp-1"), clientv3.WithPrefix()),
		"controller: demand prefix, no slash":   ctl(instPrefix(PlaneMD, "md-jp-1")+"demand", clientv3.WithPrefix()), // covers demandX
		"controller: demands into incarnations": ctl(DemandPrefix(PlaneMD, "md-jp-1"), clientv3.WithRange(PlacementPrefix("md-jp-1"))),
		"controller: its own record as prefix":  ctl(AgentKey(PlaneCtl, ControllerInstance), clientv3.WithPrefix()),
	} {
		if !errors.Is(err, ErrNotWriter) {
			t.Errorf("ranged delete, %s: %v, want ErrNotWriter", name, err)
		}
	}
}

// Every kind of write from a superseded agent fails, ends the term, and changes nothing; after the first failure
// the rest fail without I/O.
func TestSupersededWritesFail(t *testing.T) {
	type write func(ctx context.Context, term *Term, key string, lease clientv3.LeaseID) error
	writes := []struct {
		name string
		do   write
	}{
		{"put", func(ctx context.Context, term *Term, key string, _ clientv3.LeaseID) error {
			return term.Put(ctx, key, "new")
		}},
		{"delete", func(ctx context.Context, term *Term, key string, _ clientv3.LeaseID) error {
			return term.Delete(ctx, key)
		}},
		{"put with lease", func(ctx context.Context, term *Term, key string, lease clientv3.LeaseID) error {
			return term.Put(ctx, key, "new", clientv3.WithLease(lease))
		}},
		{"record", func(ctx context.Context, term *Term, _ string, _ clientv3.LeaseID) error {
			return term.PutRecord(ctx, "new")
		}},
		{"txn", func(ctx context.Context, term *Term, key string, _ clientv3.LeaseID) error {
			_, err := term.Txn(ctx, []clientv3.Cmp{clientv3.Compare(clientv3.Version(key), ">", 0)},
				[]clientv3.Op{clientv3.OpPut(key, "new")}, []clientv3.Op{clientv3.OpPut(key, "else")})
			return err
		}},
	}
	insts := []string{"md-tw-1", "md-tw-2", "md-tw-3", "md-tw-4", "md-tw-5"}
	s, topo := singleRegion(t, insts...)
	raw := s.Client(t)

	for i, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			ctx := testCtx(t)
			inst := insts[i] // a fresh instance per case: its writer key is bumped for good
			cl := newClusters(t, topo, nil, nil)
			term := campaign(t, ctx, cl, PlaneMD, inst, TermOptions{})
			key := WorkerStatusKey(PlaneMD, inst, WorkerHash("md/BinanceUM/default/1"))
			if _, err := raw.Put(ctx, key, "orig"); err != nil {
				t.Fatal(err)
			}
			lease, err := raw.Grant(ctx, 60)
			if err != nil {
				t.Fatal(err)
			}
			before := mustGet(t, ctx, raw, key).Kvs[0]

			// A newer agent took the writer key.
			if _, err := raw.Put(ctx, WriterKey(PlaneMD, inst), formatEpoch(term.Epoch()+1)); err != nil {
				t.Fatal(err)
			}
			if err := w.do(ctx, term, key, lease.ID); !errors.Is(err, ErrSuperseded) {
				t.Fatalf("write after being superseded: %v", err)
			}
			waitDone(t, term, time.Second)
			if !errors.Is(term.Err(), ErrSuperseded) {
				t.Fatalf("Err = %v", term.Err())
			}
			after := mustGet(t, ctx, raw, key).Kvs
			if len(after) != 1 || string(after[0].Value) != "orig" || after[0].ModRevision != before.ModRevision || after[0].Lease != 0 {
				t.Fatalf("key changed: %v", after)
			}

			// No more I/O: with the clients closed, any call to etcd would fail with something else.
			cl.Close()
			for _, again := range writes {
				if err := again.do(ctx, term, key, lease.ID); !errors.Is(err, ErrSuperseded) {
					t.Fatalf("%s after the term ended: %v", again.name, err)
				}
			}
			if _, err := term.Fence(ctx, "tw"); !errors.Is(err, ErrSuperseded) {
				t.Fatalf("Fence after the term ended: %v", err)
			}
		})
	}
}

// A takeover A has not noticed: A's election key goes while its session stays alive (as when A is cut off from
// etcd but its lease has not run out in A's view), B is elected with the next epoch and writes. A's next write
// reaches etcd and is refused by the server-side fence alone.
func TestSupersededByNewLeader(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	key := WorkerStatusKey(PlaneTD, "td-tw-1", WorkerHash("td/42"))

	a := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "a"})
	if err := a.Put(ctx, key, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Delete(ctx, a.election.Key()); err != nil {
		t.Fatal(err)
	}
	b := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "b"})
	if b.Epoch() != a.Epoch()+1 {
		t.Fatalf("epochs a=%d b=%d", a.Epoch(), b.Epoch())
	}
	if err := b.Put(ctx, key, "b"); err != nil {
		t.Fatal(err)
	}

	if !running(a) {
		t.Fatalf("A noticed before writing (%v): the fence path would go untested", a.Err())
	}
	if err := a.Put(ctx, key, "a again"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("old leader's write: %v, want ErrSuperseded from the fence", err)
	}
	waitDone(t, a, time.Second)
	if !errors.Is(a.Err(), ErrSuperseded) {
		t.Fatalf("A ended with %v", a.Err())
	}
	if kvs := mustGet(t, ctx, raw, key).Kvs; string(kvs[0].Value) != "b" {
		t.Fatalf("value %q, want b", kvs[0].Value)
	}
	if !running(b) {
		t.Fatalf("new leader's term ended: %v", b.Err())
	}
}

// The other order: A's lease goes and A notices first. A stops writing on its own, before etcd is asked.
func TestSessionLostThenNewLeader(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	key := WorkerStatusKey(PlaneTD, "td-tw-1", WorkerHash("td/42"))

	a := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "a", SessionTTL: 2 * time.Second})
	if err := a.Put(ctx, key, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Revoke(ctx, a.session.Lease()); err != nil {
		t.Fatal(err)
	}
	waitDone(t, a, 15*time.Second)
	b := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "b"})
	if err := b.Put(ctx, key, "b"); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, key, "a again"); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("old leader's write: %v, want ErrSessionLost", err)
	}
	if kvs := mustGet(t, ctx, raw, key).Kvs; string(kvs[0].Value) != "b" {
		t.Fatalf("value %q, want b", kvs[0].Value)
	}
}

func TestWrites(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	status := SessionStatusKey("sts-tw-1", "a1b2c3")
	other := WorkerStatusKey(PlaneSTS, "sts-tw-1", WorkerHash("a1b2c3"))

	// Put, bound to no lease (W7), then Delete.
	if err := term.Put(ctx, other, "record"); err != nil {
		t.Fatal(err)
	}
	if kvs := mustGet(t, ctx, raw, other).Kvs; len(kvs) != 1 || string(kvs[0].Value) != "record" || kvs[0].Lease != 0 {
		t.Fatalf("after Put: %v", kvs)
	}
	if err := term.Delete(ctx, other); err != nil {
		t.Fatal(err)
	}
	if kvs := mustGet(t, ctx, raw, other).Kvs; len(kvs) != 0 {
		t.Fatalf("after Delete: %v", kvs)
	}

	// Caller's compare false: Else runs, the term goes on.
	resp, err := term.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(status), "=", "nope")},
		[]clientv3.Op{clientv3.OpPut(status, "then")},
		[]clientv3.Op{clientv3.OpPut(other, "else")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Succeeded || resp.Header == nil || resp.Header.Revision == 0 {
		t.Fatalf("false compare: %+v", resp)
	}
	if !running(term) {
		t.Fatalf("a false caller compare ended the term: %v", term.Err())
	}
	if kvs := mustGet(t, ctx, raw, status).Kvs; len(kvs) != 0 {
		t.Fatal("Then ran on a false compare")
	}
	if kvs := mustGet(t, ctx, raw, other).Kvs; len(kvs) != 1 || string(kvs[0].Value) != "else" {
		t.Fatalf("Else did not run: %v", kvs)
	}

	// Caller's compare true: Then runs, its responses come back.
	resp, err = term.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(status), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(status, "then"), clientv3.OpGet(other)},
		nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Succeeded || len(resp.Responses) != 2 {
		t.Fatalf("true compare: %+v", resp)
	}
	if kvs := resp.Responses[1].GetResponseRange().GetKvs(); len(kvs) != 1 || string(kvs[0].Value) != "else" {
		t.Fatalf("Get inside Then: %v", kvs)
	}
	if kvs := mustGet(t, ctx, raw, status).Kvs; len(kvs) != 1 || string(kvs[0].Value) != "then" || kvs[0].ModRevision != resp.Header.Revision {
		t.Fatalf("Then: %v at rev %d", kvs, resp.Header.Revision)
	}

	// Prefix delete inside the instance.
	if err := term.Delete(ctx, SessionStatusPrefix("sts-tw-1"), clientv3.WithPrefix()); err != nil {
		t.Fatal(err)
	}
	if kvs := mustGet(t, ctx, raw, status).Kvs; len(kvs) != 0 {
		t.Fatal("prefix delete left the key")
	}

	// The AgentRecord is bound to the election session: PutRecord writes it, not Put (W7).
	err = term.Put(ctx, AgentKey(PlaneSTS, "sts-tw-1"), "record")
	if !errors.Is(err, ErrNotWriter) || !strings.Contains(err.Error(), "PutRecord") {
		t.Fatalf("Put of the AgentRecord: %v", err)
	}

	// Bad options and unroutable writes are refused before any I/O and do not end the term.
	lease, err := raw.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"put with a revision":   term.Put(ctx, status, "v", clientv3.WithRev(5)),
		"put with a prefix":     term.Put(ctx, status, "v", clientv3.WithPrefix()),
		"delete with a lease":   term.Delete(ctx, status, clientv3.WithLease(lease.ID)),
		"delete from key":       term.Delete(ctx, status, clientv3.WithFromKey()),
		"put outside Root":      term.Put(ctx, "/elsewhere", "v"),
		"put, unknown instance": term.Put(ctx, AgentKey(PlaneSTS, "sts-kr-1"), "v"),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if !running(term) {
		t.Fatalf("a refused write ended the term: %v", term.Err())
	}
}

func TestWritesTwoRegions(t *testing.T) {
	ctx := testCtx(t)
	tw, jp, topo := twoServers(t)
	rawTW, rawJP := tw.Client(t), jp.Client(t)
	spec := SessionSpecKey("sts-tw-1", "a1b2c3")
	jpDemand := MDDemandKey("md-jp-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")

	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})

	// Cross-cluster: refused before any I/O, nothing written, jp not even fenced, term goes on.
	_, err := term.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(spec), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(jpDemand, "demand")}, nil)
	if !errors.Is(err, ErrCrossCluster) {
		t.Fatalf("cross-cluster Txn: %v", err)
	}
	for _, key := range []string{spec, jpDemand} {
		if kvs := mustGet(t, ctx, rawTW, key).Kvs; len(kvs) != 0 {
			t.Fatalf("%s written in tw", key)
		}
	}
	for _, key := range []string{spec, jpDemand, WriterKey(PlaneCtl, ControllerInstance)} {
		if kvs := mustGet(t, ctx, rawJP, key).Kvs; len(kvs) != 0 {
			t.Fatalf("%s written in jp", key)
		}
	}
	if !running(term) {
		t.Fatalf("a cross-cluster refusal ended the term: %v", term.Err())
	}

	// The controller writes a TW session's JP demand into JP's cluster, bound to no lease (PROTOCOL §3.3).
	if err := term.Put(ctx, jpDemand, "demand"); err != nil {
		t.Fatal(err)
	}
	kvs := mustGet(t, ctx, rawJP, jpDemand).Kvs
	if len(kvs) != 1 || kvs[0].Lease != 0 {
		t.Fatalf("jp demand entry: %v", kvs)
	}
	if kvs := mustGet(t, ctx, rawTW, jpDemand).Kvs; len(kvs) != 0 {
		t.Fatal("jp demand entry also in tw")
	}
	if kvs := mustGet(t, ctx, rawJP, WriterKey(PlaneCtl, ControllerInstance)).Kvs; len(kvs) != 1 {
		t.Fatal("jp writer key not acquired by the first jp write")
	}

	// An MD agent in tw writes nothing in jp: no key there is its to write (W1).
	allowed, _ := AgentRegions(topo, PlaneMD, "md-tw-1")
	md := campaign(t, ctx, newClusters(t, topo, allowed, nil), PlaneMD, "md-tw-1", TermOptions{})
	if err := md.Put(ctx, MDAckKey("md-jp-1", "a1b2c3", "feed/x"), "READY"); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("MD agent in tw writing jp's ack: %v", err)
	}
	if !running(md) {
		t.Fatalf("a refused write ended the term: %v", md.Err())
	}

	// A key the controller may write, in a region its Clusters may not reach: refused by Clusters. The next
	// controller campaigns once this one is gone.
	if err := term.Close(ctx); err != nil {
		t.Fatal(err)
	}
	twOnly := campaign(t, ctx, newClusters(t, topo, []string{"tw"}, nil), PlaneCtl, ControllerInstance, TermOptions{})
	if err := twOnly.Put(ctx, MDDemandKey("md-jp-1", "a1b2c3", "feed/x"), "demand"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("controller limited to tw writing a jp demand: %v", err)
	}
	if !running(twOnly) {
		t.Fatalf("a refused region ended the term: %v", twOnly.Err())
	}
}

// The reviewer's reproductions: a term can neither forge another agent's writer key nor wipe its own fencing
// and incarnation keys.
func TestWriteAPICannotTouchFencing(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1", "md-tw-1", "td-tw-1")
	raw := s.Client(t)
	snapshot := func(prefix string) map[string]int64 {
		t.Helper()
		resp, err := raw.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]int64{}
		for _, kv := range resp.Kvs {
			m[string(kv.Key)] = kv.ModRevision
		}
		return m
	}

	md := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	sts := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	td := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	inc := loadIncarnations(t, ctx, td)
	next(t, ctx, inc, "td/42", 1)

	mdBefore := snapshot(instPrefix(PlaneMD, "md-tw-1"))
	if err := sts.Put(ctx, WriterKey(PlaneMD, "md-tw-1"), formatEpoch(math.MaxInt64)); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("STS forging MD's writer key: %v", err)
	}
	if got := snapshot(instPrefix(PlaneMD, "md-tw-1")); !maps.Equal(got, mdBefore) {
		t.Fatalf("md-tw-1 changed: %v -> %v", mdBefore, got)
	}

	tdBefore := snapshot(instPrefix(PlaneTD, "td-tw-1"))
	for _, k := range []string{EpochKey(PlaneTD, "td-tw-1"), WriterKey(PlaneTD, "td-tw-1"), IncarnationKey(PlaneTD, "td-tw-1", WorkerHash("td/42"))} {
		if _, ok := tdBefore[k]; !ok {
			t.Fatalf("%s missing before the delete", k)
		}
	}
	if err := td.Delete(ctx, instPrefix(PlaneTD, "td-tw-1"), clientv3.WithPrefix()); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("TD wiping its own instance: %v", err)
	}
	if err := td.Put(ctx, IncarnationKey(PlaneTD, "td-tw-1", WorkerHash("td/42")), "1"); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("TD resetting an incarnation through Put: %v", err)
	}
	if got := snapshot(instPrefix(PlaneTD, "td-tw-1")); !maps.Equal(got, tdBefore) {
		t.Fatalf("td-tw-1 changed: %v -> %v", tdBefore, got)
	}
	for _, term := range []*Term{md, sts, td} {
		if !running(term) {
			t.Fatalf("%s/%s ended: %v", term.Plane(), term.Instance(), term.Err())
		}
	}
	next(t, ctx, inc, "td/42", 2)
}
