package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// testTTL is the shortest lease etcd grants with its default election timeout.
const testTTL = 2 * time.Second

func newPresence(t *testing.T, ctx context.Context, term *Term, opt PresenceOptions) *Presence {
	t.Helper()
	p, err := NewPresence(ctx, term, opt)
	if err != nil {
		t.Fatalf("NewPresence(%s/%s): %v", term.Plane(), term.Instance(), err)
	}
	t.Cleanup(p.Close)
	return p
}

func put(t *testing.T, ctx context.Context, p *Presence, key, val string) {
	t.Helper()
	if err := p.Put(ctx, key, val); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

// bound fails the test unless every key in want holds its value under lease, the same lease for all.
func bound(t *testing.T, ctx context.Context, raw *clientv3.Client, want map[string]string, lease clientv3.LeaseID) {
	t.Helper()
	if err := boundErr(ctx, raw, want, lease); err != nil {
		t.Fatal(err)
	}
}

func boundErr(ctx context.Context, raw *clientv3.Client, want map[string]string, lease clientv3.LeaseID) error {
	for k, v := range want {
		resp, err := raw.Get(ctx, k)
		if err != nil {
			return err
		}
		if len(resp.Kvs) != 1 {
			return fmt.Errorf("%s missing", k)
		}
		if kv := resp.Kvs[0]; string(kv.Value) != v || clientv3.LeaseID(kv.Lease) != lease {
			return fmt.Errorf("%s = %q on lease %x, want %q on %x", k, kv.Value, kv.Lease, v, lease)
		}
	}
	return nil
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

func absent(ctx context.Context, raw *clientv3.Client, keys ...string) error {
	for _, k := range keys {
		resp, err := raw.Get(ctx, k)
		if err != nil {
			return err
		}
		if len(resp.Kvs) != 0 {
			return fmt.Errorf("%s still there, on lease %x", k, resp.Kvs[0].Lease)
		}
	}
	return nil
}

func revision(t *testing.T, ctx context.Context, raw *clientv3.Client) int64 {
	t.Helper()
	resp, err := raw.Get(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	return resp.Header.Revision
}

func mdKeys(inst string) (record, worker, ack string) {
	return AgentKey(PlaneMD, inst), WorkerStatusKey(PlaneMD, inst, WorkerHash("md/BinanceUM/default/1")),
		MDAckKey(inst, "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
}

// The keys are bound to the lease, which is kept alive well past its TTL.
func TestPresenceKeepsKeys(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL})
	record, worker, ack := mdKeys("md-tw-1")
	want := map[string]string{record: "record", worker: "RUNNING", ack: "READY"}
	for k, v := range want {
		put(t, ctx, p, k, v)
	}
	bound(t, ctx, raw, want, p.Lease())
	ttl, err := raw.TimeToLive(ctx, p.Lease())
	if err != nil || ttl.GrantedTTL != int64(testTTL/time.Second) {
		t.Fatalf("lease: %+v, %v", ttl, err)
	}

	lease := p.Lease()
	time.Sleep(3 * testTTL)
	if p.Lease() != lease {
		t.Fatalf("lease replaced: %x -> %x", lease, p.Lease())
	}
	bound(t, ctx, raw, want, lease)
	if ttl, err := raw.TimeToLive(ctx, lease); err != nil || ttl.TTL <= 0 {
		t.Fatalf("lease after 3 TTLs: %+v, %v", ttl, err)
	}
	if !running(term) {
		t.Fatalf("term ended: %v", term.Err())
	}
}

// A value equal to the one etcd holds is not written again, however often it is put (W7: Beat and Report never
// reach etcd, and an unchanged WorkerStatus costs nothing).
func TestPresenceWritesChangesOnly(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	p := newPresence(t, ctx, term, PresenceOptions{})
	worker := WorkerStatusKey(PlaneTD, "td-tw-1", WorkerHash("td/42"))

	put(t, ctx, p, worker, "STARTING")
	rev := revision(t, ctx, raw)
	for range 100 {
		put(t, ctx, p, worker, "STARTING")
	}
	if got := revision(t, ctx, raw); got != rev {
		t.Fatalf("100 unchanged puts moved the revision %d -> %d", rev, got)
	}
	put(t, ctx, p, worker, "RUNNING")
	if got := revision(t, ctx, raw); got != rev+1 {
		t.Fatalf("a change: revision %d -> %d, want one write", rev, got)
	}

	if err := p.Delete(ctx, worker); err != nil {
		t.Fatal(err)
	}
	rev = revision(t, ctx, raw)
	if err := p.Delete(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if got := revision(t, ctx, raw); got != rev {
		t.Fatalf("deleting a deleted key wrote: %d -> %d", rev, got)
	}
	put(t, ctx, p, worker, "RUNNING")
	bound(t, ctx, raw, map[string]string{worker: "RUNNING"}, p.Lease())
}

// The lease is lost and etcd drops every key with it: a new lease is granted and every wanted key comes back on
// it, the AgentRecord first in a txn of its own (an MD agent's acks sort before it), the rest in batches; a deleted
// key does not.
func TestPresenceRegrantsAndRewrites(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL})

	record := AgentKey(PlaneMD, "md-tw-1")
	want := map[string]string{record: "record"}
	put(t, ctx, p, record, "record")
	for i := range 2*presenceBatchOps + 10 {
		k := MDAckKey("md-tw-1", fmt.Sprintf("s%03d", i), "feed/trade.BinanceUM_Perp_BTCUSDT")
		want[k] = "READY"
		put(t, ctx, p, k, "PENDING")
		put(t, ctx, p, k, "READY")
	}
	worker := WorkerStatusKey(PlaneMD, "md-tw-1", WorkerHash("md/BinanceUM/default/1"))
	want[worker] = "RUNNING"
	put(t, ctx, p, worker, "RUNNING")
	gone := WorkerStatusKey(PlaneMD, "md-tw-1", WorkerHash("md/BinanceUM/default/2"))
	put(t, ctx, p, gone, "EXITED")
	if err := p.Delete(ctx, gone); err != nil {
		t.Fatal(err)
	}
	old := p.Lease()
	bound(t, ctx, raw, want, old)

	if _, err := raw.Revoke(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := absent(ctx, raw, record); err != nil {
		t.Fatalf("revoke left the keys: %v", err)
	}
	eventually(t, 10*time.Second, func() error {
		if p.Lease() == old {
			return errors.New("no new lease yet")
		}
		return boundErr(ctx, raw, want, p.Lease())
	})
	if err := absent(ctx, raw, gone); err != nil {
		t.Fatalf("a deleted key came back: %v", err)
	}
	// The record's txn, then one per batch.
	first := mustGet(t, ctx, raw, record).Kvs[0].ModRevision
	resp, err := raw.Get(ctx, instPrefix(PlaneMD, "md-tw-1"), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	revs := map[int64]int{}
	for _, kv := range resp.Kvs {
		if _, ok := want[string(kv.Key)]; !ok {
			continue
		}
		revs[kv.ModRevision]++
		if kv.ModRevision < first {
			t.Fatalf("%s rewritten at %d, before the AgentRecord at %d", kv.Key, kv.ModRevision, first)
		}
	}
	if len(revs) != 4 || revs[first] != 1 {
		t.Fatalf("%d keys rewritten in %d txns, want the record alone and 3 batches: %v", len(want), len(revs), revs)
	}
	// And the new lease is kept alive.
	time.Sleep(2 * testTTL)
	bound(t, ctx, raw, want, p.Lease())
}

// A Put that finds the lease gone before the keepalive notices grants the new lease itself and rewrites every
// wanted key before its own; the keepalive then moves to the new lease.
func TestPresencePutFindsLeaseGone(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	keeping, checked := make(chan clientv3.LeaseID, 8), make(chan bool, 8)
	// The longest TTL: after its first round, the keepalive is 20 s from noticing anything.
	p := newPresence(t, ctx, term, PresenceOptions{hooks: &presenceHooks{
		keeping: func(l clientv3.LeaseID) { send(keeping, l) },
		checked: func(_ clientv3.LeaseID, alive bool) { send(checked, alive) },
	}})
	record, worker, ack := mdKeys("md-tw-1")
	put(t, ctx, p, record, "record")
	put(t, ctx, p, worker, "RUNNING")
	old := p.Lease()
	if got := <-keeping; got != old {
		t.Fatalf("keepalive started on %x, the lease is %x", got, old)
	}
	time.Sleep(time.Second) // past the keepalive's first round, which would notice the revoke

	if _, err := raw.Revoke(ctx, old); err != nil {
		t.Fatal(err)
	}
	put(t, ctx, p, ack, "READY")
	if p.Lease() == old {
		t.Fatal("Put succeeded on the revoked lease")
	}
	want := map[string]string{record: "record", worker: "RUNNING", ack: "READY"}
	bound(t, ctx, raw, want, p.Lease())
	// The Put's own key went with the rewrite, which starts with the AgentRecord: nothing before it.
	if r, a := mustGet(t, ctx, raw, record).Kvs[0].ModRevision, mustGet(t, ctx, raw, ack).Kvs[0].ModRevision; r > a {
		t.Fatalf("AgentRecord at %d, after the new ack at %d", r, a)
	}
	select {
	case got := <-keeping:
		if got != p.Lease() {
			t.Fatalf("keepalive moved to %x, the lease is %x", got, p.Lease())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the keepalive stayed on the revoked lease")
	}
	if len(checked) != 0 {
		t.Fatal("the keepalive noticed first: the Put's own path went untested")
	}
}

// send hands v to a test hook's channel without ever blocking the presence's goroutine.
func send[T any](c chan T, v T) {
	select {
	case c <- v:
	default:
	}
}

// Cut off from etcd for longer than the TTL: etcd drops the lease and the keys. Once etcd is back, they return on
// a new lease. The term survives: its election session has the longer TTL.
func TestPresencePartition(t *testing.T) {
	ctx := testCtx(t)
	s := etcdtest.Start(t)
	proxy := etcdtest.NewProxy(t, s)
	raw := s.Client(t)
	topo := Topology{Regions: map[string]Region{"tw": {Endpoints: proxy.Endpoints()}}, Instances: map[string]string{"td-tw-1": "tw"}}
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{SessionTTL: 30 * time.Second})
	p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL})
	record := AgentKey(PlaneTD, "td-tw-1")
	ack := TDAckKey("td-tw-1", "a1b2c3", 42)
	want := map[string]string{record: "record", ack: "READY"}
	for k, v := range want {
		put(t, ctx, p, k, v)
	}
	old := p.Lease()

	proxy.Cut()
	eventually(t, 10*time.Second, func() error { return absent(ctx, raw, record, ack) })
	time.Sleep(testTTL) // the client gives up on the keepalive and starts checking, against a cut-off etcd
	proxy.Restore()

	eventually(t, 15*time.Second, func() error {
		if p.Lease() == old {
			return errors.New("no new lease yet")
		}
		return boundErr(ctx, raw, want, p.Lease())
	})
	if !running(term) {
		t.Fatalf("term ended: %v", term.Err())
	}
}

// The keepalive ends while the lease still exists (as when the client stops waiting for a reply that is merely
// late): the keepalive resumes on the same lease, and nothing is written.
func TestPresenceLeaseStillAlive(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	drop := make(chan struct{})
	checked := make(chan bool, 8)
	p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL, hooks: &presenceHooks{
		drop:    drop,
		checked: func(_ clientv3.LeaseID, alive bool) { send(checked, alive) },
	}})
	record, _, _ := mdKeys("md-tw-1")
	put(t, ctx, p, record, "record")
	lease, rev := p.Lease(), revision(t, ctx, raw)

	drop <- struct{}{}
	select {
	case alive := <-checked:
		if !alive {
			t.Fatal("the live lease was found gone")
		}
	case <-ctx.Done():
		t.Fatal("no check after the keepalive ended")
	}
	time.Sleep(3 * testTTL)
	if p.Lease() != lease {
		t.Fatalf("lease replaced: %x -> %x", lease, p.Lease())
	}
	bound(t, ctx, raw, map[string]string{record: "record"}, lease)
	if got := revision(t, ctx, raw); got != rev {
		t.Fatalf("revision %d -> %d: something was written", rev, got)
	}
}

// A value etcd refuses is not kept for the rewrite, so the other keys still come back after a lease loss; values
// that each fit but not together are rewritten in batches small enough for etcd.
func TestPresenceRefusedValue(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL})
	record, worker, ack := mdKeys("md-tw-1")
	want := map[string]string{record: "record", worker: "RUNNING"}
	for k, v := range want {
		put(t, ctx, p, k, v)
	}
	big := string(make([]byte, 30<<10)) // 70 of them are 2 MiB, more than one request may carry
	for i := range 70 {
		k := MDAckKey("md-tw-1", fmt.Sprintf("s%02d", i), "feed/x")
		want[k] = big
		put(t, ctx, p, k, big)
	}
	put(t, ctx, p, ack, "PENDING")
	if err := p.Put(ctx, ack, string(make([]byte, 2<<20))); err == nil {
		t.Fatal("etcd took a 2 MiB value")
	}
	want[ack] = "PENDING" // the last value etcd accepted
	old := p.Lease()
	if _, err := raw.Revoke(ctx, old); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() error {
		if p.Lease() == old {
			return errors.New("no new lease yet")
		}
		return boundErr(ctx, raw, want, p.Lease())
	})
	put(t, ctx, p, record, "record 2")
}

// Puts from many goroutines while the lease is lost again and again: every key ends up on the current lease with
// its last value.
func TestPresenceConcurrentPutsAndLosses(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL})
	const writers, puts = 8, 40
	errs := make(chan error, writers)
	for w := range writers {
		go func() {
			key := WorkerStatusKey(PlaneSTS, "sts-tw-1", WorkerHash(fmt.Sprintf("s%d", w)))
			for i := range puts {
				if err := p.Put(ctx, key, fmt.Sprint(i)); err != nil {
					errs <- fmt.Errorf("%s: %w", key, err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			errs <- nil
		}()
	}
	for range 5 {
		time.Sleep(30 * time.Millisecond)
		if _, err := raw.Revoke(ctx, p.Lease()); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
			t.Fatal(err)
		}
	}
	for range writers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	want := map[string]string{}
	for w := range writers {
		want[WorkerStatusKey(PlaneSTS, "sts-tw-1", WorkerHash(fmt.Sprintf("s%d", w)))] = fmt.Sprint(puts - 1)
	}
	eventually(t, 10*time.Second, func() error { return boundErr(ctx, raw, want, p.Lease()) })
}

// The term ends: the keepalive stops and nothing is revoked, so the keys live out the TTL and then go, and nothing
// brings them back. Close does the same with the term still running.
func TestPresenceStopsWithoutRevoke(t *testing.T) {
	for _, how := range []string{"term ends", "Close"} {
		t.Run(how, func(t *testing.T) {
			ctx := testCtx(t)
			s, topo := singleRegion(t, "md-tw-1")
			raw := s.Client(t)
			term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
			p := newPresence(t, ctx, term, PresenceOptions{TTL: testTTL})
			record, worker, _ := mdKeys("md-tw-1")
			want := map[string]string{record: "record", worker: "RUNNING"}
			for k, v := range want {
				put(t, ctx, p, k, v)
			}
			lease := p.Lease()

			wantErr := ErrPresenceClosed
			if how == "Close" {
				p.Close()
			} else {
				if err := term.Close(ctx); err != nil {
					t.Fatal(err)
				}
				wantErr = ErrClosed
				select {
				case <-p.done:
				case <-time.After(5 * time.Second):
					t.Fatal("the loop outlived the term")
				}
			}
			bound(t, ctx, raw, want, lease) // not revoked
			if err := p.Put(ctx, worker, "STOPPING"); !errors.Is(err, wantErr) {
				t.Fatalf("Put after stopping: %v, want %v", err, wantErr)
			}
			if err := p.Delete(ctx, worker); !errors.Is(err, wantErr) {
				t.Fatalf("Delete after stopping: %v, want %v", err, wantErr)
			}
			eventually(t, 3*testTTL, func() error { return absent(ctx, raw, record, worker) })
			time.Sleep(testTTL)
			if err := absent(ctx, raw, record, worker); err != nil {
				t.Fatalf("came back after the presence stopped: %v", err)
			}
			p.Close() // idempotent
		})
	}
}

// An agent hands over: the old one stops without revoking, the new one rewrites what it wants on its own lease.
// What it does not rewrite goes when the old lease runs out, or at once if it deletes it.
func TestPresenceHandover(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	record, worker, ack := mdKeys("md-tw-1")
	stale := WorkerStatusKey(PlaneMD, "md-tw-1", WorkerHash("md/BinanceUM/default/2"))
	dead := WorkerStatusKey(PlaneMD, "md-tw-1", WorkerHash("md/BinanceUM/default/3"))

	a := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "a"})
	pa := newPresence(t, ctx, a, PresenceOptions{TTL: testTTL})
	for k, v := range map[string]string{record: "a", worker: "RUNNING", ack: "READY", stale: "RUNNING", dead: "RUNNING"} {
		put(t, ctx, pa, k, v)
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}

	b := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "b"})
	pb := newPresence(t, ctx, b, PresenceOptions{TTL: testTTL})
	want := map[string]string{record: "b", worker: "RUNNING", ack: "READY"} // the same values: still written, to move them
	for k, v := range want {
		put(t, ctx, pb, k, v)
	}
	bound(t, ctx, raw, want, pb.Lease())
	// B never wrote dead, but deleting it reaches etcd: A's lease still holds it.
	if err := pb.Delete(ctx, dead); err != nil {
		t.Fatal(err)
	}
	if err := absent(ctx, raw, dead); err != nil {
		t.Fatalf("Delete of the old term's key: %v", err)
	}
	eventually(t, 3*testTTL, func() error { return absent(ctx, raw, stale) })
	time.Sleep(testTTL)
	bound(t, ctx, raw, want, pb.Lease())
}

// The controller's presence holds its AgentRecord, and nothing else.
func TestPresenceController(t *testing.T) {
	ctx := testCtx(t)
	tw, _, topo := twoServers(t)
	raw := tw.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})
	p := newPresence(t, ctx, term, PresenceOptions{})
	record := AgentKey(PlaneCtl, ControllerInstance)
	put(t, ctx, p, record, "record")
	bound(t, ctx, raw, map[string]string{record: "record"}, p.Lease())

	for _, k := range []string{
		MDDemandKey("md-jp-1", "a1b2c3", "feed/x"), // demands are bound to no lease (PROTOCOL §3.3)
		SessionDemandKey("sts-tw-1", "a1b2c3"),
		WorkerStatusKey(PlaneCtl, ControllerInstance, WorkerHash("w")),
		AgentKey(PlaneMD, "md-tw-1"),
	} {
		if err := p.Put(ctx, k, "v"); !errors.Is(err, ErrNotWriter) {
			t.Errorf("controller presence Put %s: %v, want ErrNotWriter", k, err)
		}
	}
}

func TestPresenceArgs(t *testing.T) {
	ctx := testCtx(t)
	_, topo := singleRegion(t, "sts-tw-1", "sts-tw-2", "md-tw-1")
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	if _, err := NewPresence(ctx, term, PresenceOptions{TTL: -time.Second}); !errors.Is(err, ErrBadOptions) {
		t.Fatalf("negative TTL: %v", err)
	}
	p := newPresence(t, ctx, term, PresenceOptions{})
	if _, err := NewPresence(ctx, term, PresenceOptions{}); !errors.Is(err, ErrPresenceExists) {
		t.Fatalf("second presence: %v", err)
	}
	for _, k := range []string{
		SessionStatusKey("sts-tw-1", "a1b2c3"), // the agent's, but not bound to its lease
		IncarnationKey(PlaneSTS, "sts-tw-1", WorkerHash("w")),
		WriterKey(PlaneSTS, "sts-tw-1"),
		AgentKey(PlaneSTS, "sts-tw-2"),
		MDAckKey("md-tw-1", "a1b2c3", "feed/x"),
	} {
		if err := p.Put(ctx, k, "v"); !errors.Is(err, ErrNotWriter) {
			t.Errorf("Put %s: %v, want ErrNotWriter", k, err)
		}
		if err := p.Delete(ctx, k); !errors.Is(err, ErrNotWriter) {
			t.Errorf("Delete %s: %v, want ErrNotWriter", k, err)
		}
	}
	if err := term.Close(ctx); err != nil {
		t.Fatal(err)
	}
	other := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	if err := other.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPresence(ctx, other, PresenceOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("presence of an ended term: %v", err)
	}
}
