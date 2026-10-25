package state

import (
	"errors"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// The API's scope (W10, E10-D042): specs and accounts to put, statuses only to delete (E10-D040), nothing of an agent's
// or the controller's. Checked before any I/O.
func TestAPIScope(t *testing.T) {
	topo := twoRegions()
	spec := SessionSpecKey("sts-tw-1", "a1b2c3")
	status := SessionStatusKey("sts-tw-1", "a1b2c3")
	acct := AccountKey("td-jp-1", 42)
	put := func(k string) clientv3.Op { return clientv3.OpPut(k, "v") }
	del := func(k string, opts ...clientv3.OpOption) clientv3.Op { return clientv3.OpDelete(k, opts...) }
	allowed := []struct {
		name string
		op   clientv3.Op
	}{
		{"put spec", put(spec)},
		{"put account", put(acct)},
		{"delete spec", del(spec)},
		{"delete status", del(status)},
		{"delete account", del(acct)},
		{"delete specs by prefix", del(SessionSpecPrefix("sts-tw-1"), clientv3.WithPrefix())},
		{"put retirement", put(RetiredKey("sts-tw-1"))},
	}
	for _, c := range allowed {
		if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{c.op}, nil); err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	refused := []struct {
		name string
		op   clientv3.Op
	}{
		{"put status", put(status)},
		{"put session demand", put(SessionDemandKey("sts-tw-1", "a1b2c3"))},
		{"delete session demand", del(SessionDemandKey("sts-tw-1", "a1b2c3"))},
		{"put md demand", put(MDDemandKey("md-tw-1", "a1b2c3", "feed/trade.Binance_Spot_BTCUSDT"))},
		{"put td ack", put(TDAckKey("td-jp-1", "a1b2c3", 42))},
		{"put agent record", put(AgentKey(PlaneSTS, "sts-tw-1"))},
		{"put writer", put(WriterKey(PlaneTD, "td-jp-1"))},
		{"put worker status", put(WorkerStatusKey(PlaneSTS, "sts-tw-1", "h"))},
		{"put controller key", put(AgentKey(PlaneCtl, ControllerInstance))},
		{"delete the whole sts subtree", del(instPrefix(PlaneSTS, "sts-tw-1"), clientv3.WithPrefix())},
		{"delete retirement", del(RetiredKey("sts-tw-1"))},
	}
	// A range on the retirement key is not a key the router can place (E12-D033).
	if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{del(RetiredKey("sts-tw-1"), clientv3.WithPrefix())}, nil); !errors.Is(err, ErrBadKey) {
		t.Errorf("delete retirement by prefix: %v, want ErrBadKey", err)
	}
	for _, c := range refused {
		if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{c.op}, nil); !errors.Is(err, ErrNotWriter) {
			t.Errorf("%s: %v, want ErrNotWriter", c.name, err)
		}
	}
	// A spec in TW and an account in JP cannot share a txn.
	if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{put(spec), put(acct)}, nil); !errors.Is(err, ErrCrossCluster) {
		t.Errorf("cross-cluster: %v", err)
	}
	// Compares may read any key of the cluster.
	cmp := clientv3.Compare(clientv3.ModRevision(status), "=", 3)
	if _, err := routeTxn(topo, apiScope{}, []clientv3.Cmp{cmp}, []clientv3.Op{del(spec), del(status)}, nil); err != nil {
		t.Errorf("delete spec and status on a status compare: %v", err)
	}
}

// Against etcd as the api user: a create-only txn creates once and refuses the second, and a status put is refused
// by the scope even though etcd would take it (E10-D040).
func TestAPIWriterTxn(t *testing.T) {
	ctx := testCtx(t)
	_, topo := singleRegion(t, "sts-tw-1", "td-tw-1")
	w := newClusters(t, topo, nil, as(t, topo, UserAPI)).APIWriter()
	key := SessionSpecKey("sts-tw-1", "a1b2c3")
	create := func(val string) (*clientv3.TxnResponse, error) {
		return w.Txn(ctx, []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)},
			[]clientv3.Op{clientv3.OpPut(key, val)}, []clientv3.Op{clientv3.OpGet(key)})
	}
	resp, err := create("first")
	if err != nil || !resp.Succeeded {
		t.Fatalf("first create: %v %+v", err, resp)
	}
	resp, err = create("second")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Succeeded {
		t.Fatal("second create succeeded")
	}
	if kvs := resp.Responses[0].GetResponseRange().GetKvs(); len(kvs) != 1 || string(kvs[0].Value) != "first" {
		t.Fatalf("value after the refused create: %+v", kvs)
	}
	status := SessionStatusKey("sts-tw-1", "a1b2c3")
	if _, err := w.Txn(ctx, nil, []clientv3.Op{clientv3.OpPut(status, "x")}, nil); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("status put: %v", err)
	}
}

func TestAPIRetireTxn(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	w := newClusters(t, topo, nil, as(t, topo, UserAPI)).APIWriter()
	agent := AgentKey(PlaneSTS, "sts-tw-1")
	key := RetiredKey("sts-tw-1")
	raw := s.Client(t)
	if _, err := raw.Put(ctx, agent, "rec"); err != nil {
		t.Fatal(err)
	}
	retire := func() (*clientv3.TxnResponse, error) {
		return w.Txn(ctx,
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(agent), "=", 0)},
			[]clientv3.Op{clientv3.OpPut(key, "sts-tw-1")}, nil)
	}
	resp, err := retire()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Succeeded {
		t.Fatal("agent present, txn succeeded")
	}
	if val, _, ok := kvOf(t, ctx, raw, key); ok {
		t.Fatalf("marker written %q", val)
	}
	if _, err := raw.Delete(ctx, agent); err != nil {
		t.Fatal(err)
	}
	resp, err = retire()
	if err != nil || !resp.Succeeded {
		t.Fatalf("retire: %v succeeded=%v", err, resp.Succeeded)
	}
	if val, _, ok := kvOf(t, ctx, raw, key); !ok || val != "sts-tw-1" {
		t.Fatalf("marker %q ok=%v", val, ok)
	}
	if _, err := w.Txn(ctx, nil, []clientv3.Op{clientv3.OpDelete(key)}, nil); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("delete: %v", err)
	}
}
