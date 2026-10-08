package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// kvOf is the key's value and lease, or ok=false if it is absent.
func kvOf(t *testing.T, ctx context.Context, raw *clientv3.Client, key string) (val string, lease clientv3.LeaseID, ok bool) {
	t.Helper()
	kvs := mustGet(t, ctx, raw, key).Kvs
	if len(kvs) == 0 {
		return "", 0, false
	}
	return string(kvs[0].Value), clientv3.LeaseID(kvs[0].Lease), true
}

// eventually polls cond until it returns nil, or fails the test with its last error.
func eventually(t *testing.T, within time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s: %v", within, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func mdKeys(inst string) (record, worker, ack string) {
	return AgentKey(PlaneMD, inst), WorkerStatusKey(PlaneMD, inst, WorkerHash("md/BinanceUM/default/1")),
		MDAckKey(inst, "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
}

// The AgentRecord lives on the election session's lease; the acks and WorkerStatus the agent writes live on no
// lease. Closing the term revokes the session and the record goes with it; the rest stays (W7).
func TestPutRecord(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	record, worker, ack := mdKeys("md-tw-1")

	if err := term.PutRecord(ctx, "record"); err != nil {
		t.Fatal(err)
	}
	if err := term.Put(ctx, worker, "RUNNING"); err != nil {
		t.Fatal(err)
	}
	if err := term.Put(ctx, ack, "READY"); err != nil {
		t.Fatal(err)
	}
	if v, lease, ok := kvOf(t, ctx, raw, record); !ok || v != "record" || lease != term.session.Lease() {
		t.Fatalf("record %q on lease %x (present %v), want it on the session's %x", v, lease, ok, term.session.Lease())
	}
	for _, k := range []string{worker, ack} {
		if _, lease, ok := kvOf(t, ctx, raw, k); !ok || lease != 0 {
			t.Fatalf("%s on lease %x (present %v), want no lease", k, lease, ok)
		}
	}

	if err := term.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := kvOf(t, ctx, raw, record); ok {
		t.Fatal("the record outlived the term's Close")
	}
	for k, want := range map[string]string{worker: "RUNNING", ack: "READY"} {
		if v, _, ok := kvOf(t, ctx, raw, k); !ok || v != want {
			t.Fatalf("%s = %q (present %v) after Close, want %q kept", k, v, ok, want)
		}
	}
	if err := term.PutRecord(ctx, "again"); !errors.Is(err, ErrClosed) {
		t.Fatalf("PutRecord after Close: %v", err)
	}
}

// The agent is cut off from etcd for longer than its session's TTL, as when its host goes down: the term ends,
// etcd drops the record, and the ack and WorkerStatus stay as last written. Readers see no agent and the last
// known state; nothing is reclaimed.
func TestRecordGoesWithTheAgent(t *testing.T) {
	ctx := testCtx(t)
	s := etcdtest.Start(t)
	proxy := etcdtest.NewProxy(t, s)
	raw := s.Client(t)
	topo := Topology{Regions: map[string]Region{"tw": {Endpoints: proxy.Endpoints()}}, Instances: map[string]string{"td-tw-1": "tw"}}
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{SessionTTL: 2 * time.Second})
	record := AgentKey(PlaneTD, "td-tw-1")
	worker := WorkerStatusKey(PlaneTD, "td-tw-1", WorkerHash("td/42"))
	ack := TDAckKey("td-tw-1", "a1b2c3", 42)
	if err := term.PutRecord(ctx, "record"); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{worker: "RUNNING", ack: "READY"} {
		if err := term.Put(ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}

	proxy.Cut()
	defer proxy.Restore() // before the cleanups, which talk to etcd, even if the test fails here
	eventually(t, 10*time.Second, func() error {
		if _, _, ok := kvOf(t, ctx, raw, record); ok {
			return errors.New("record still there")
		}
		return nil
	})
	waitDone(t, term, 10*time.Second)
	if !errors.Is(term.Err(), ErrSessionLost) {
		t.Fatalf("term ended with %v", term.Err())
	}
	for k, want := range map[string]string{worker: "RUNNING", ack: "READY"} {
		if v, _, ok := kvOf(t, ctx, raw, k); !ok || v != want {
			t.Fatalf("%s = %q (present %v) with the agent gone, want %q kept", k, v, ok, want)
		}
	}
}

// A new agent takes over while the old one still holds its session (it was cut off and has not noticed): the new
// term's record replaces the old one on its own session, and the old term closing later does not remove it. The
// acks the old agent wrote are still there.
func TestRecordHandover(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	record, _, ack := mdKeys("md-tw-1")

	a := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "a"})
	if err := a.PutRecord(ctx, fmt.Sprint(a.Epoch())); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, ack, "READY"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Delete(ctx, a.election.Key()); err != nil { // as in TestSupersededByNewLeader
		t.Fatal(err)
	}
	b := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "b"})
	if err := b.PutRecord(ctx, fmt.Sprint(b.Epoch())); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if v, lease, _ := kvOf(t, ctx, raw, record); v != fmt.Sprint(b.Epoch()) || lease != b.session.Lease() {
		t.Fatalf("record %q on %x, want b's epoch %d on b's session %x", v, lease, b.Epoch(), b.session.Lease())
	}
	if v, _, ok := kvOf(t, ctx, raw, ack); !ok || v != "READY" {
		t.Fatalf("ack = %q (present %v) across the handover", v, ok)
	}
}

// A superseded term's record goes within the session TTL, though the process has not closed the term: the session
// is no longer renewed once the term ends.
func TestRecordGoesWithSupersededTerm(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{SessionTTL: 2 * time.Second})
	record := AgentKey(PlaneSTS, "sts-tw-1")
	if err := term.PutRecord(ctx, "record"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Put(ctx, WriterKey(PlaneSTS, "sts-tw-1"), formatEpoch(term.Epoch()+1)); err != nil {
		t.Fatal(err)
	}
	if err := term.Put(ctx, SessionStatusKey("sts-tw-1", "a1b2c3"), "x"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("write after being superseded: %v", err)
	}
	eventually(t, 10*time.Second, func() error {
		if _, _, ok := kvOf(t, ctx, raw, record); ok {
			return errors.New("record still there")
		}
		return nil
	})
}

// The session's lease is gone before the session notices: PutRecord ends the term as the session's loss would.
func TestPutRecordLeaseGone(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	if _, err := raw.Revoke(ctx, term.session.Lease()); err != nil {
		t.Fatal(err)
	}
	if err := term.PutRecord(ctx, "record"); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("PutRecord on a revoked session: %v", err)
	}
	if !errors.Is(term.Err(), ErrSessionLost) {
		t.Fatalf("term: %v", term.Err())
	}
}

// The controller's record lives in its home region, on its session there.
func TestPutRecordController(t *testing.T) {
	ctx := testCtx(t)
	tw, jp, topo := twoServers(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})
	if err := term.PutRecord(ctx, "record"); err != nil {
		t.Fatal(err)
	}
	record := AgentKey(PlaneCtl, ControllerInstance)
	if v, lease, ok := kvOf(t, ctx, tw.Client(t), record); !ok || v != "record" || lease != term.session.Lease() {
		t.Fatalf("record in tw: %q on %x (present %v)", v, lease, ok)
	}
	if _, _, ok := kvOf(t, ctx, jp.Client(t), record); ok {
		t.Fatal("record also in jp")
	}
}
