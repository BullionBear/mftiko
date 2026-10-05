package state

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRouteTxn(t *testing.T) {
	topo := twoRegions()
	sts := writeScope{plane: PlaneSTS, inst: "sts-tw-1"}
	spec := SessionSpecKey("sts-tw-1", "a1b2c3")
	status := SessionStatusKey("sts-tw-1", "a1b2c3")
	jpLease := MDLeaseKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
	jpAck := MDAckKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
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
		{"one put", nil, []clientv3.Op{put(jpLease)}, nil, "jp"},
		{"same region, two instances", []clientv3.Cmp{clientv3.Compare(clientv3.Version(spec), ">", 0)},
			[]clientv3.Op{put(status)}, []clientv3.Op{get(twAgent)}, "tw"},
		{"lease and ack of one provider", []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(jpAck), "=", 0)},
			[]clientv3.Op{put(jpLease)}, nil, "jp"},
		{"prefix delete inside the instance", nil,
			[]clientv3.Op{clientv3.OpDelete(MDLeasePrefix("md-jp-1")+"sts-tw-1/a1b2c3/", clientv3.WithPrefix())}, nil, "jp"},
		{"whole instance", nil,
			[]clientv3.Op{clientv3.OpGet(instPrefix(PlaneMD, "md-jp-1"), clientv3.WithPrefix())}, nil, "jp"},
		{"nested txn", nil,
			[]clientv3.Op{clientv3.OpTxn(nil, []clientv3.Op{put(status)}, []clientv3.Op{get(spec)})}, nil, "tw"},
	}
	for _, c := range ok {
		if got, err := routeTxn(topo, sts, c.cmps, c.then, c.els); err != nil || got != c.want {
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
			[]clientv3.Op{put(jpLease)}, nil, ErrCrossCluster},
		{"then tw, else jp", nil, []clientv3.Op{put(status)}, []clientv3.Op{put(jpLease)}, ErrCrossCluster},
		{"jp inside a nested txn", nil,
			[]clientv3.Op{clientv3.OpTxn(nil, []clientv3.Op{put(status)}, []clientv3.Op{put(jpLease)})}, nil, ErrCrossCluster},
		{"range compare leaving the instance", []clientv3.Cmp{clientv3.Compare(clientv3.Version(spec), ">", 0).WithRange(Root + "/td/")},
			[]clientv3.Op{put(status)}, nil, ErrCrossCluster},
		{"from-key delete", nil, []clientv3.Op{clientv3.OpDelete(status, clientv3.WithFromKey())}, nil, ErrCrossCluster},
		{"range past the instance", nil,
			[]clientv3.Op{clientv3.OpDelete(status, clientv3.WithRange(Root+"/sts/sts-tw-2/"))}, nil, ErrCrossCluster},
		{"no keys", nil, nil, nil, ErrBadKey},
		{"key outside Root", nil, []clientv3.Op{put("/elsewhere")}, nil, ErrBadKey},
		{"unknown instance", nil, []clientv3.Op{put(AgentKey(PlaneMD, "md-kr-1"))}, nil, ErrUnknownInstance},
	}
	for _, c := range bad {
		if got, err := routeTxn(topo, sts, c.cmps, c.then, c.els); !errors.Is(err, c.want) {
			t.Errorf("%s: %s, %v; want %v", c.name, got, err, c.want)
		}
	}
}

// W1: each agent writes only the rows of PROTOCOL §3.1 whose writer it is.
func TestWriteScope(t *testing.T) {
	topo := twoRegions()
	for _, inst := range []string{"sts-tw-2", "md-tw-2", "td-tw-2"} {
		topo.Instances[inst] = "tw"
	}
	const sid, entry = "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT"
	h := WorkerHash("w")

	// reserved are the keys no agent writes through the write API: what fencing relies on, the API's
	// declarations, and incarnations outside Incarnations.
	reserved := func(p Plane, inst string) []string {
		return []string{
			ElectionPrefix(p, inst) + "/694d7e8a1c2b3f40",
			EpochKey(p, inst),
			WriterKey(p, inst),
			IncarnationKey(p, inst, h),
			instPrefix(p, inst) + "other",
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
				AgentKey(PlaneSTS, "sts-tw-1"),
				WorkerStatusKey(PlaneSTS, "sts-tw-1", h),
				MDLeaseKey("md-jp-1", "sts-tw-1", sid, entry), // another region's MD instance
				MDLeaseKey("md-tw-1", "sts-tw-1", sid, "select/top10"),
				TDLeaseKey("td-jp-1", "sts-tw-1", sid, 42),
			},
			denied: append(reserved(PlaneSTS, "sts-tw-1"),
				SessionSpecKey("sts-tw-1", sid), // the API's
				SessionStatusKey("sts-tw-2", sid),
				AgentKey(PlaneSTS, "sts-tw-2"),
				MDLeaseKey("md-jp-1", "sts-tw-2", sid, entry), // another STS's entries
				MDAckKey("md-jp-1", "sts-tw-1", sid, entry),   // the provider's
				TDAckKey("td-jp-1", "sts-tw-1", sid, 42),
				WriterKey(PlaneMD, "md-tw-1"),
				AgentKey(PlaneMD, "md-tw-1"),
			),
		},
		{
			scope: writeScope{plane: PlaneMD, inst: "md-tw-1"},
			writable: []string{
				MDAckKey("md-tw-1", "sts-tw-1", sid, entry),
				SelectorKey("md-tw-1", "s1"),
				PlacementKey("md-tw-1", "a1"),
				WorkerStatusKey(PlaneMD, "md-tw-1", h),
				AgentKey(PlaneMD, "md-tw-1"),
			},
			denied: append(reserved(PlaneMD, "md-tw-1"),
				MDLeaseKey("md-tw-1", "sts-tw-1", sid, entry), // providers only read lease entries (L5)
				MDAckKey("md-tw-2", "sts-tw-1", sid, entry),
				SelectorKey("md-tw-2", "s1"),
				AgentKey(PlaneMD, "md-tw-2"),
				AgentKey(PlaneTD, "td-tw-1"),
				SessionStatusKey("sts-tw-1", sid),
			),
		},
		{
			scope: writeScope{plane: PlaneTD, inst: "td-tw-1"},
			writable: []string{
				TDAckKey("td-tw-1", "sts-tw-1", sid, 42),
				WorkerStatusKey(PlaneTD, "td-tw-1", h),
				AgentKey(PlaneTD, "td-tw-1"),
			},
			denied: append(reserved(PlaneTD, "td-tw-1"),
				AccountKey("td-tw-1", 42), // the API's
				TDLeaseKey("td-tw-1", "sts-tw-1", sid, 42),
				TDAckKey("td-tw-2", "sts-tw-1", sid, 42),
				WorkerStatusKey(PlaneTD, "td-tw-2", h),
				MDAckKey("md-tw-1", "sts-tw-1", sid, entry),
			),
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
		name := fmt.Sprintf("%s/%s(incarnations=%v)", c.scope.plane, c.scope.inst, c.scope.incarnations)
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
	sts := writeScope{plane: PlaneSTS, inst: "sts-tw-1"}
	del := func(key string, opts ...clientv3.OpOption) error {
		_, err := routeTxn(topo, sts, nil, []clientv3.Op{clientv3.OpDelete(key, opts...)}, nil)
		return err
	}
	for name, err := range map[string]error{
		"all statuses":                  del(SessionStatusPrefix("sts-tw-1"), clientv3.WithPrefix()),
		"this STS's leases at md-jp-1":  del(MDLeasePrefix("md-jp-1")+"sts-tw-1/", clientv3.WithPrefix()),
		"one session's leases":          del(MDLeasePrefix("md-jp-1")+"sts-tw-1/"+sid+"/", clientv3.WithPrefix()),
		"range up to the prefix's end":  del(SessionStatusKey("sts-tw-1", "a"), clientv3.WithRange(clientv3.GetPrefixRangeEnd(SessionStatusPrefix("sts-tw-1")))),
		"workers between two hashes":    del(WorkerStatusKey(PlaneSTS, "sts-tw-1", "0"), clientv3.WithRange(WorkerStatusKey(PlaneSTS, "sts-tw-1", "8"))),
		"one TD session's lease prefix": del(TDLeasePrefix("td-jp-1")+"sts-tw-1/"+sid+"/", clientv3.WithPrefix()),
	} {
		if err != nil {
			t.Errorf("STS ranged delete %s: %v", name, err)
		}
	}
	for name, err := range map[string]error{
		"whole instance":                del(instPrefix(PlaneSTS, "sts-tw-1"), clientv3.WithPrefix()),
		"status into worker":            del(SessionStatusKey("sts-tw-1", "a"), clientv3.WithRange(WorkerStatusPrefix(PlaneSTS, "sts-tw-1")+"x")),
		"past the status prefix":        del(SessionStatusKey("sts-tw-1", "a"), clientv3.WithRange(instPrefix(PlaneSTS, "sts-tw-1")+"statut")),
		"agent as a prefix":             del(AgentKey(PlaneSTS, "sts-tw-1"), clientv3.WithPrefix()),
		"every STS's leases at md-jp-1": del(MDLeasePrefix("md-jp-1"), clientv3.WithPrefix()),
		"this STS's prefix, no slash":   del(MDLeasePrefix("md-jp-1")+"sts-tw-1", clientv3.WithPrefix()), // covers sts-tw-10
	} {
		if !errors.Is(err, ErrNotWriter) {
			t.Errorf("STS ranged delete %s: %v, want ErrNotWriter", name, err)
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
		{"txn", func(ctx context.Context, term *Term, key string, _ clientv3.LeaseID) error {
			_, err := term.Txn(ctx, []clientv3.Cmp{clientv3.Compare(clientv3.Version(key), ">", 0)},
				[]clientv3.Op{clientv3.OpPut(key, "new")}, []clientv3.Op{clientv3.OpPut(key, "else")})
			return err
		}},
	}
	insts := []string{"md-tw-1", "md-tw-2", "md-tw-3", "md-tw-4"}
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

// The realistic takeover: A's session goes, B is elected with the next epoch and writes; A's next write never lands.
func TestSupersededByNewLeader(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	key := WorkerStatusKey(PlaneTD, "td-tw-1", WorkerHash("td/42"))

	a := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "a"})
	if err := a.Put(ctx, key, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Revoke(ctx, a.session.Lease()); err != nil {
		t.Fatal(err)
	}
	b := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "b"})
	if b.Epoch() != a.Epoch()+1 {
		t.Fatalf("epochs a=%d b=%d", a.Epoch(), b.Epoch())
	}
	if err := b.Put(ctx, key, "b"); err != nil {
		t.Fatal(err)
	}

	err := a.Put(ctx, key, "a again")
	if !errors.Is(err, ErrSuperseded) && !errors.Is(err, ErrSessionLost) {
		t.Fatalf("old leader's write: %v", err)
	}
	t.Logf("old leader saw: %v", err)
	waitDone(t, a, time.Second)
	if kvs := mustGet(t, ctx, raw, key).Kvs; string(kvs[0].Value) != "b" {
		t.Fatalf("value %q, want b", kvs[0].Value)
	}
	if !running(b) {
		t.Fatalf("new leader's term ended: %v", b.Err())
	}
}

func TestWrites(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	status := SessionStatusKey("sts-tw-1", "a1b2c3")
	agent := AgentKey(PlaneSTS, "sts-tw-1")

	// Put with a lease, then Delete.
	lease, err := raw.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if err := term.Put(ctx, agent, "record", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	if kvs := mustGet(t, ctx, raw, agent).Kvs; len(kvs) != 1 || string(kvs[0].Value) != "record" || clientv3.LeaseID(kvs[0].Lease) != lease.ID {
		t.Fatalf("after Put with lease: %v", kvs)
	}
	if err := term.Delete(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if kvs := mustGet(t, ctx, raw, agent).Kvs; len(kvs) != 0 {
		t.Fatalf("after Delete: %v", kvs)
	}

	// Caller's compare false: Else runs, the term goes on.
	resp, err := term.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(status), "=", "nope")},
		[]clientv3.Op{clientv3.OpPut(status, "then")},
		[]clientv3.Op{clientv3.OpPut(agent, "else")})
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
	if kvs := mustGet(t, ctx, raw, agent).Kvs; len(kvs) != 1 || string(kvs[0].Value) != "else" {
		t.Fatalf("Else did not run: %v", kvs)
	}

	// Caller's compare true: Then runs, its responses come back.
	resp, err = term.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(status), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(status, "then"), clientv3.OpGet(agent)},
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

	// Bad options and unroutable writes are refused before any I/O and do not end the term.
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
	jpLease := MDLeaseKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")

	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})

	// Cross-cluster: refused before any I/O, nothing written, jp not even fenced, term goes on.
	_, err := term.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(spec), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(jpLease, "lease")}, nil)
	if !errors.Is(err, ErrCrossCluster) {
		t.Fatalf("cross-cluster Txn: %v", err)
	}
	for _, key := range []string{spec, jpLease} {
		if kvs := mustGet(t, ctx, rawTW, key).Kvs; len(kvs) != 0 {
			t.Fatalf("%s written in tw", key)
		}
	}
	for _, key := range []string{spec, jpLease, WriterKey(PlaneSTS, "sts-tw-1")} {
		if kvs := mustGet(t, ctx, rawJP, key).Kvs; len(kvs) != 0 {
			t.Fatalf("%s written in jp", key)
		}
	}
	if !running(term) {
		t.Fatalf("a cross-cluster refusal ended the term: %v", term.Err())
	}

	// The TW STS writes its JP lease entry, bound to its JP etcd lease, into JP's cluster (PROTOCOL §3.3 L1).
	jpEtcdLease, err := rawJP.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if err := term.Put(ctx, jpLease, "lease", clientv3.WithLease(jpEtcdLease.ID)); err != nil {
		t.Fatal(err)
	}
	kvs := mustGet(t, ctx, rawJP, jpLease).Kvs
	if len(kvs) != 1 || clientv3.LeaseID(kvs[0].Lease) != jpEtcdLease.ID {
		t.Fatalf("jp lease entry: %v", kvs)
	}
	if kvs := mustGet(t, ctx, rawTW, jpLease).Kvs; len(kvs) != 0 {
		t.Fatal("jp lease entry also in tw")
	}
	if kvs := mustGet(t, ctx, rawJP, WriterKey(PlaneSTS, "sts-tw-1")).Kvs; len(kvs) != 1 {
		t.Fatal("jp writer key not acquired by the first jp write")
	}

	// An MD agent in tw writes nothing in jp: no key there is its to write (W1).
	allowed, _ := AgentRegions(topo, PlaneMD, "md-tw-1")
	md := campaign(t, ctx, newClusters(t, topo, allowed, nil), PlaneMD, "md-tw-1", TermOptions{})
	if err := md.Put(ctx, MDAckKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/x"), "READY"); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("MD agent in tw writing jp's ack: %v", err)
	}
	if !running(md) {
		t.Fatalf("a refused write ended the term: %v", md.Err())
	}

	// A key the agent may write, in a region its Clusters may not reach: refused by Clusters.
	topo.Instances["sts-tw-2"] = "tw"
	twOnly := campaign(t, ctx, newClusters(t, topo, []string{"tw"}, nil), PlaneSTS, "sts-tw-2", TermOptions{})
	if err := twOnly.Put(ctx, MDLeaseKey("md-jp-1", "sts-tw-2", "a1b2c3", "feed/x"), "lease"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("STS limited to tw writing a jp lease: %v", err)
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
