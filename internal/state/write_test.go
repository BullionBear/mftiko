package state

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRouteTxn(t *testing.T) {
	topo := twoRegions()
	spec := SessionSpecKey("sts-tw-1", "a1b2c3")
	status := SessionStatusKey("sts-tw-1", "a1b2c3")
	jpLease := MDLeaseKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
	jpAck := MDAckKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
	twAgent := AgentKey(PlaneMD, "md-tw-1") // another instance, same region
	put := func(k string) clientv3.Op { return clientv3.OpPut(k, "v") }

	ok := []struct {
		name string
		cmps []clientv3.Cmp
		then []clientv3.Op
		els  []clientv3.Op
		want string
	}{
		{"one put", nil, []clientv3.Op{put(jpLease)}, nil, "jp"},
		{"same region, two instances", []clientv3.Cmp{clientv3.Compare(clientv3.Version(spec), ">", 0)},
			[]clientv3.Op{put(status)}, []clientv3.Op{put(twAgent)}, "tw"},
		{"lease and ack of one provider", []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(jpAck), "=", 0)},
			[]clientv3.Op{put(jpLease)}, nil, "jp"},
		{"prefix delete inside the instance", nil,
			[]clientv3.Op{clientv3.OpDelete(MDLeasePrefix("md-jp-1")+"sts-tw-1/a1b2c3/", clientv3.WithPrefix())}, nil, "jp"},
		{"whole instance", nil,
			[]clientv3.Op{clientv3.OpGet(instPrefix(PlaneMD, "md-jp-1"), clientv3.WithPrefix())}, nil, "jp"},
		{"nested txn", nil,
			[]clientv3.Op{clientv3.OpTxn(nil, []clientv3.Op{put(status)}, []clientv3.Op{put(spec)})}, nil, "tw"},
	}
	for _, c := range ok {
		if got, err := routeTxn(topo, c.cmps, c.then, c.els); err != nil || got != c.want {
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
		if got, err := routeTxn(topo, c.cmps, c.then, c.els); !errors.Is(err, c.want) {
			t.Errorf("%s: %s, %v; want %v", c.name, got, err, c.want)
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

	// An MD agent in tw may not write to jp at all.
	allowed, _ := AgentRegions(topo, PlaneMD, "md-tw-1")
	md := campaign(t, ctx, newClusters(t, topo, allowed, nil), PlaneMD, "md-tw-1", TermOptions{})
	if err := md.Put(ctx, MDAckKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/x"), "READY"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("MD agent in tw writing to jp: %v", err)
	}
	if !running(md) {
		t.Fatalf("a refused region ended the term: %v", md.Err())
	}
}
