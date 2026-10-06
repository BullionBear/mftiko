package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/state/etcdtest"
)

// startMirror runs a mirror until the test ends and waits for its first list.
func startMirror(t *testing.T, r Reader, prefix string, opt MirrorOptions) *Mirror {
	t.Helper()
	m, err := NewMirror(r, prefix, opt)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Run after cancel: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	select {
	case <-m.Synced():
	case <-time.After(30 * time.Second):
		t.Fatal("mirror never synced")
	}
	return m
}

// etcdEntries is the prefix in etcd now.
func etcdEntries(t *testing.T, ctx context.Context, raw *clientv3.Client, prefix string, opts ...clientv3.OpOption) (map[string]Entry, int64) {
	t.Helper()
	resp, err := raw.Get(ctx, prefix, append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
	if err != nil {
		t.Fatalf("get %s: %v", prefix, err)
	}
	m := map[string]Entry{}
	for _, kv := range resp.Kvs {
		m[string(kv.Key)] = entryOf(kv)
	}
	return m, resp.Header.Revision
}

// diffEntries describes how got differs from want; "" if they are the same.
func diffEntries(got, want map[string]Entry) string {
	var d []string
	for k, w := range want {
		g, ok := got[k]
		switch {
		case !ok:
			d = append(d, "missing "+k)
		case g.ModRevision != w.ModRevision || g.CreateRevision != w.CreateRevision || g.Version != w.Version ||
			g.Lease != w.Lease || !bytes.Equal(g.Value, w.Value):
			d = append(d, fmt.Sprintf("stale %s: mod %d, want %d", k, g.ModRevision, w.ModRevision))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			d = append(d, "extra "+k)
		}
	}
	slices.Sort(d)
	return strings.Join(d, "; ")
}

// waitConverged waits until the mirror holds exactly what etcd holds under its prefix.
func waitConverged(t *testing.T, ctx context.Context, m *Mirror, raw *clientv3.Client, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		want, _ := etcdEntries(t, ctx, raw, m.Prefix())
		d := diffEntries(m.Snapshot().Entries, want)
		if d == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mirror not converged after %s (stats %+v): %s", within, m.Stats(), d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitNotify(t *testing.T, m *Mirror) {
	t.Helper()
	select {
	case <-m.Notify():
	case <-time.After(20 * time.Second):
		t.Fatal("no notification")
	}
}

func noPendingNotify(t *testing.T, m *Mirror) {
	t.Helper()
	select {
	case <-m.Notify():
		t.Fatal("a notification is pending with nothing changed")
	default:
	}
}

// gatedReader wraps a Reader: beforeGet runs before the n-th Get (from 1), and Watch waits for gate to close.
type gatedReader struct {
	Reader
	beforeGet func(n int)
	gate      chan struct{}

	mu   sync.Mutex
	gets int
}

func (g *gatedReader) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	g.mu.Lock()
	g.gets++
	n := g.gets
	g.mu.Unlock()
	if g.beforeGet != nil {
		g.beforeGet(n)
	}
	return g.Reader.Get(ctx, key, opts...)
}

func (g *gatedReader) Done() <-chan struct{} {
	if g.Reader == nil {
		return nil // never closed
	}
	return g.Reader.Done()
}

func (g *gatedReader) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if g.gate != nil {
		select {
		case <-g.gate:
		case <-ctx.Done():
			ch := make(chan clientv3.WatchResponse)
			close(ch)
			return ch
		}
	}
	return g.Reader.Watch(ctx, key, opts...)
}

func mustPut(t *testing.T, ctx context.Context, raw *clientv3.Client, key, val string, opts ...clientv3.OpOption) int64 {
	t.Helper()
	resp, err := raw.Put(ctx, key, val, opts...)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return resp.Header.Revision
}

func mustCompact(t *testing.T, ctx context.Context, raw *clientv3.Client) int64 {
	t.Helper()
	rev := mustGet(t, ctx, raw, "compact").Header.Revision
	if _, err := raw.Compact(ctx, rev, clientv3.WithCompactPhysical()); err != nil {
		t.Fatalf("compact %d: %v", rev, err)
	}
	return rev
}

func errOf[T any](_ T, err error) error { return err }

func leaseKey(i int) string {
	return TDLeaseKey("td-tw-1", "sts-tw-1", fmt.Sprintf("%06x", i), int64(i+1))
}

func TestNewMirrorArgs(t *testing.T) {
	_, topo := singleRegion(t, "td-tw-1")
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func() (*Mirror, error){
		"nil reader":       func() (*Mirror, error) { return NewMirror(nil, "/p/", MirrorOptions{}) },
		"empty prefix":     func() (*Mirror, error) { return NewMirror(r, "", MirrorOptions{}) },
		"negative page":    func() (*Mirror, error) { return NewMirror(r, "/p/", MirrorOptions{PageSize: -1}) },
		"negative backoff": func() (*Mirror, error) { return NewMirror(r, "/p/", MirrorOptions{MaxBackoff: -1}) },
	} {
		if _, err := f(); !errors.Is(err, ErrBadOptions) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestMirrorListThenWatch(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	prefix := TDLeasePrefix("td-tw-1")
	noise := AccountKey("td-tw-1", 7) // same instance, another prefix
	for i := range 5 {
		mustPut(t, ctx, raw, leaseKey(i), fmt.Sprint(i))
	}
	listed := mustPut(t, ctx, raw, noise, "account")

	m := startMirror(t, r, prefix, MirrorOptions{PageSize: 2}) // three pages
	v, changed := m.Take()
	want, _ := etcdEntries(t, ctx, raw, prefix)
	if d := diffEntries(v.Entries, want); d != "" {
		t.Fatalf("first view: %s", d)
	}
	if v.Rev < listed {
		t.Fatalf("view rev %d, before the last write %d", v.Rev, listed)
	}
	if !slices.Equal(changed, slices.Sorted(maps.Keys(want))) {
		t.Fatalf("first Take changed %v, want every key", changed)
	}
	waitNotify(t, m) // the first list signals
	noPendingNotify(t, m)

	// One txn: two keys of the prefix and one outside it. The view takes it whole.
	resp, err := raw.Txn(ctx).Then(
		clientv3.OpPut(leaseKey(9), "nine"),
		clientv3.OpDelete(leaseKey(0)),
		clientv3.OpPut(noise, "changed"),
	).Commit()
	if err != nil {
		t.Fatal(err)
	}
	waitNotify(t, m)
	v, changed = m.Take()
	want, _ = etcdEntries(t, ctx, raw, prefix)
	if d := diffEntries(v.Entries, want); d != "" {
		t.Fatalf("after txn: %s", d)
	}
	if v.Rev != resp.Header.Revision {
		t.Fatalf("view rev %d, txn at %d", v.Rev, resp.Header.Revision)
	}
	if !slices.Equal(changed, []string{leaseKey(0), leaseKey(9)}) {
		t.Fatalf("changed %v", changed)
	}
	if _, ok := m.Get(leaseKey(0)); ok {
		t.Fatal("deleted key still in the mirror")
	}
	if e, ok := m.Get(leaseKey(9)); !ok || string(e.Value) != "nine" {
		t.Fatalf("Get = %+v, %v", e, ok)
	}
	if st := m.Stats(); st.Lists != 1 || st.Watches != 1 || st.Errors != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// The list's pages are read at the first page's revision: the first view is one revision's state, even when the
// prefix changes between pages.
func TestMirrorListIsOneRevision(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	direct, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		mustPut(t, ctx, raw, leaseKey(i), "v1")
	}
	g := &gatedReader{Reader: direct, gate: make(chan struct{})}
	g.beforeGet = func(n int) {
		if n == 2 { // between the first and second page; on Run's goroutine, so t.Error, not t.Fatal
			for _, err := range []error{
				errOf(raw.Put(ctx, leaseKey(1), "v2")),
				errOf(raw.Put(ctx, leaseKey(8), "late")),
				errOf(raw.Delete(ctx, leaseKey(5))),
			} {
				if err != nil {
					t.Error(err)
				}
			}
		}
	}
	m := startMirror(t, g, TDLeasePrefix("td-tw-1"), MirrorOptions{PageSize: 2})
	v := m.Snapshot()
	atRev, _ := etcdEntries(t, ctx, raw, m.Prefix(), clientv3.WithRev(v.Rev))
	if d := diffEntries(v.Entries, atRev); d != "" {
		t.Fatalf("first view is not etcd at rev %d: %s", v.Rev, d)
	}
	if len(v.Entries) != 6 {
		t.Fatalf("first view has %d keys, want the 6 listed before the changes", len(v.Entries))
	}

	close(g.gate) // the watch from rev+1 brings the changes made during the list
	waitConverged(t, ctx, m, raw, 20*time.Second)
	if st := m.Stats(); st.Lists != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// A compaction between pages makes the next page's revision unavailable: the list starts over.
func TestMirrorCompactedMidList(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	direct, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		mustPut(t, ctx, raw, leaseKey(i), "v1")
	}
	g := &gatedReader{Reader: direct}
	g.beforeGet = func(n int) {
		if n == 2 { // on Run's goroutine, so t.Error, not t.Fatal
			resp, err := raw.Delete(ctx, leaseKey(4))
			if err == nil {
				_, err = raw.Compact(ctx, resp.Header.Revision, clientv3.WithCompactPhysical())
			}
			if err != nil {
				t.Error(err)
			}
		}
	}
	m := startMirror(t, g, TDLeasePrefix("td-tw-1"), MirrorOptions{PageSize: 2})
	waitConverged(t, ctx, m, raw, 20*time.Second)
	if _, ok := m.Get(leaseKey(4)); ok {
		t.Fatal("a key deleted before the restarted list is in the view")
	}
	if st := m.Stats(); st.Lists != 1 || st.Errors != 1 {
		t.Fatalf("stats %+v, want one complete list after one compacted page", st)
	}
}

// proxied is a mirror whose cluster is reached through a proxy that the test can cut, and a raw client that is
// not behind it.
func proxied(t *testing.T) (*etcdtest.Server, *etcdtest.Proxy, *clientv3.Client, Reader) {
	t.Helper()
	s := etcdtest.Start(t)
	p := etcdtest.NewProxy(t, s)
	topo := Topology{Regions: map[string]Region{"tw": {Endpoints: p.Endpoints()}}, Instances: map[string]string{"td-tw-1": "tw"}}
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	return s, p, s.Client(t), r
}

// While the mirror is cut off, keys change and the history is compacted, deletes included. On reconnect the
// watch cannot resume, so the mirror lists again; the deleted key must be gone.
func TestMirrorCompactedWhileCut(t *testing.T) {
	ctx := testCtx(t)
	_, p, raw, r := proxied(t)
	for i := range 4 {
		mustPut(t, ctx, raw, leaseKey(i), "v1")
	}
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 500 * time.Millisecond})
	m.Take()

	p.Cut()
	mustPut(t, ctx, raw, leaseKey(1), "v2")
	mustPut(t, ctx, raw, leaseKey(7), "new")
	if _, err := raw.Delete(ctx, leaseKey(2)); err != nil {
		t.Fatal(err)
	}
	compacted := mustCompact(t, ctx, raw)
	p.Restore()

	waitConverged(t, ctx, m, raw, 30*time.Second)
	if _, ok := m.Get(leaseKey(2)); ok {
		t.Fatal("a key deleted while cut off survived the compaction")
	}
	v, changed := m.Take()
	if v.Rev < compacted {
		t.Fatalf("view rev %d, before the compaction at %d", v.Rev, compacted)
	}
	if !slices.Equal(changed, []string{leaseKey(1), leaseKey(2), leaseKey(7)}) {
		t.Fatalf("changed %v, want exactly the keys changed while cut off", changed)
	}
	if st := m.Stats(); st.Lists != 2 {
		t.Fatalf("stats %+v, want a relist", st)
	}
}

// Cut off and back without a compaction: the watch resumes where it left off, no relist.
func TestMirrorReconnects(t *testing.T) {
	ctx := testCtx(t)
	_, p, raw, r := proxied(t)
	mustPut(t, ctx, raw, leaseKey(0), "v1")
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 500 * time.Millisecond})

	for round := range 3 {
		p.Cut()
		mustPut(t, ctx, raw, leaseKey(round+1), "v1")
		if _, err := raw.Delete(ctx, leaseKey(round)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)
		if _, ok := m.Get(leaseKey(round + 1)); ok { // the cut is real: nothing gets through
			t.Fatalf("round %d: a write made while cut off reached the mirror", round)
		}
		p.Restore()
		waitConverged(t, ctx, m, raw, 30*time.Second)
	}
	if st := m.Stats(); st.Lists != 1 {
		t.Fatalf("stats %+v, want no relist without a compaction", st)
	}
}

// etcd itself restarts: same data, same endpoint. The mirror carries on and sees writes made afterwards.
func TestMirrorServerRestart(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, ctx, raw, leaseKey(0), "v1")
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 500 * time.Millisecond})

	s.Restart(t)
	mustPut(t, ctx, raw, leaseKey(1), "after restart")
	if _, err := raw.Delete(ctx, leaseKey(0)); err != nil {
		t.Fatal(err)
	}
	waitConverged(t, ctx, m, raw, 30*time.Second)
}

// Keys bound to a lease disappear when it expires: a delete the mirror must see like any other.
func TestMirrorLeaseExpiry(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := raw.Grant(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, ctx, raw, leaseKey(0), "leased", clientv3.WithLease(lease.ID))
	mustPut(t, ctx, raw, leaseKey(1), "kept")
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{})
	if e, ok := m.Get(leaseKey(0)); !ok || e.Lease != int64(lease.ID) {
		t.Fatalf("leased key = %+v, %v", e, ok)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, ok := m.Get(leaseKey(0)); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the leased key outlived its lease in the mirror")
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitConverged(t, ctx, m, raw, 10*time.Second)
}

func TestMirrorRunExits(t *testing.T) {
	ctx := testCtx(t)
	_, topo := singleRegion(t, "td-tw-1")
	cl, err := NewClusters(topo, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	r, err := cl.Reader("tw")
	if err != nil {
		t.Fatal(err)
	}

	// ctx done: Run returns its error promptly.
	m, _ := NewMirror(r, TDLeasePrefix("td-tw-1"), MirrorOptions{})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- m.Run(runCtx) }()
	<-m.Synced()
	if err := m.Run(ctx); err == nil {
		t.Fatal("a second Run was accepted")
	}
	start := time.Now()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after cancel: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Run took %s to return after cancel", d)
	}

	// The client closed under a live ctx: Run gives up with ErrClustersClosed instead of retrying forever.
	m2, _ := NewMirror(r, TDLeasePrefix("td-tw-1"), MirrorOptions{})
	go func() { done <- m2.Run(ctx) }()
	<-m2.Synced()
	cl.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClustersClosed) {
			t.Fatalf("Run after Clusters.Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run kept retrying a closed client")
	}
}

// The resume point is the last event's revision, never a header that may be ahead of undelivered events; only a
// progress notification vouches for its header.
func TestMirrorResumePoint(t *testing.T) {
	m, err := NewMirror(&gatedReader{}, "/p/", MirrorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m.replace(&etcdserverpb.ResponseHeader{Revision: 10}, map[string]Entry{})
	m.apply(clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 100},
		Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/p/a"), ModRevision: 50, CreateRevision: 50, Version: 1}}},
	})
	if got := m.Rev(); got != 50 {
		t.Fatalf("rev after events = %d, want the last event's 50", got)
	}
	// Replayed events, at or below the copy's revision, are already in it.
	m.Take()
	m.apply(clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 100},
		Events: []*clientv3.Event{{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/p/a"), ModRevision: 50}}},
	})
	if _, ok := m.Get("/p/a"); !ok {
		t.Fatal("a replayed event at the copy's revision was applied again")
	}
	if _, changed := m.Take(); len(changed) != 0 {
		t.Fatalf("a replayed event marked %v changed", changed)
	}
	select {
	case <-m.Notify():
	default:
	}
	m.apply(clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 100},
		Events: []*clientv3.Event{{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/p/a"), ModRevision: 40}}},
	})
	select {
	case <-m.Notify():
		t.Fatal("a response with only replayed events signalled")
	default:
	}
	m.apply(clientv3.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 120}}) // progress notification
	if got := m.Rev(); got != 120 {
		t.Fatalf("rev after a progress notification = %d, want 120", got)
	}
}

// Writes, deletes, multi-key txns, leases that expire, keys outside the prefix, compactions, network cuts, broken
// and stalled watches, all at once. Every view sampled meanwhile must be exactly etcd at the view's revision, the
// revision never goes back, and afterwards the mirror holds exactly what etcd holds.
func TestMirrorRandomized(t *testing.T) {
	ctx := testCtx(t)
	_, p, raw, r := proxied(t)
	b := &breakableReader{Reader: r}
	prefix := TDLeasePrefix("td-tw-1")
	m := startMirror(t, b, prefix, MirrorOptions{PageSize: 7, MaxBackoff: 300 * time.Millisecond, ProbeInterval: 200 * time.Millisecond})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	report := func(err error) {
		if err != nil && ctx.Err() == nil {
			select {
			case errs <- err:
			default:
			}
		}
	}
	const keys = 25
	for w := range 3 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				k := leaseKey(rng.IntN(keys))
				var err error
				switch op := rng.IntN(10); {
				case op < 4:
					_, err = raw.Put(ctx, k, fmt.Sprintf("w%d-%d", w, n))
				case op < 6:
					_, err = raw.Delete(ctx, k)
				case op < 8:
					a, b := rng.IntN(keys), rng.IntN(keys)
					if a == b {
						continue
					}
					_, err = raw.Txn(ctx).Then(clientv3.OpPut(leaseKey(a), "txn"), clientv3.OpDelete(leaseKey(b))).Commit()
				case op < 9:
					_, err = raw.Put(ctx, AccountKey("td-tw-1", int64(rng.IntN(keys))), "noise")
				default:
					var l *clientv3.LeaseGrantResponse
					if l, err = raw.Grant(ctx, 1); err == nil {
						_, err = raw.Put(ctx, k, "leased", clientv3.WithLease(l.ID))
					}
				}
				report(err)
				time.Sleep(time.Duration(rng.IntN(3)) * time.Millisecond)
			}
		})
	}
	wg.Go(func() { // compactor
		for {
			select {
			case <-stop:
				return
			case <-time.After(150 * time.Millisecond):
			}
			resp, err := raw.Get(ctx, "compact")
			if err != nil {
				report(err)
				continue
			}
			if _, err := raw.Compact(ctx, resp.Header.Revision); err != nil && !errors.Is(err, rpctypes.ErrCompacted) {
				report(err)
			}
		}
	})
	wg.Go(func() { // cutter
		rng := rand.New(rand.NewPCG(99, 1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(200+rng.IntN(300)) * time.Millisecond):
			}
			p.Cut()
			time.Sleep(time.Duration(100+rng.IntN(300)) * time.Millisecond)
			p.Restore()
		}
	})
	wg.Go(func() { // breaks the mirror's watch (with a compaction while it is down) or freezes it
		rng := rand.New(rand.NewPCG(7, 1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(300+rng.IntN(400)) * time.Millisecond):
			}
			if rng.IntN(3) == 0 {
				b.freeze() // the probe must notice the silence and reopen the watch
				continue
			}
			release := b.breakAndHold()
			time.Sleep(time.Duration(50+rng.IntN(150)) * time.Millisecond)
			if resp, err := raw.Get(ctx, "compact"); err == nil { // the resume point is gone: a relist follows
				if _, err := raw.Compact(ctx, resp.Header.Revision); err != nil && !errors.Is(err, rpctypes.ErrCompacted) {
					report(err)
				}
			}
			release()
		}
	})
	wg.Go(func() { // every view is etcd at its revision; the revision never goes back
		var last int64
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			v := m.Snapshot()
			if v.Rev < last {
				report(fmt.Errorf("view rev went back from %d to %d", last, v.Rev))
			}
			last = v.Rev
			for k := range v.Entries {
				if !strings.HasPrefix(k, prefix) {
					report(fmt.Errorf("%s is outside the prefix", k))
				}
			}
			resp, err := raw.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(v.Rev))
			if errors.Is(err, rpctypes.ErrCompacted) {
				continue // that revision is gone: nothing to compare with
			}
			if err != nil {
				report(err)
				continue
			}
			want := map[string]Entry{}
			for _, kv := range resp.Kvs {
				want[string(kv.Key)] = entryOf(kv)
			}
			if d := diffEntries(v.Entries, want); d != "" {
				report(fmt.Errorf("view at rev %d is not etcd at that revision: %s", v.Rev, d))
			}
		}
	})

	time.Sleep(4 * time.Second)
	close(stop)
	wg.Wait()
	p.Restore()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	waitConverged(t, ctx, m, raw, 60*time.Second) // leases granted at the end expire meanwhile: polled until stable
	st := m.Stats()
	t.Logf("stats %+v", st)
	if st.Lists < 2 || st.Errors < 1 {
		t.Fatalf("stats %+v: the relist and rewatch paths were not exercised", st)
	}
}

// breakableReader forwards watches and can end them all at once, as a cancelled watch or a closed stream would;
// while held, new watches wait.
type breakableReader struct {
	Reader

	mu     sync.Mutex
	stops  []context.CancelFunc
	frozen []*atomic.Bool // per open watch: drop every response, keep the channel open
	held   chan struct{}  // non-nil: new watches wait for it to close
	starts []int64        // the start revision of every watch
}

func (b *breakableReader) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	op := clientv3.OpGet(key, opts...)
	b.mu.Lock()
	b.starts = append(b.starts, op.Rev())
	held := b.held
	b.mu.Unlock()
	if held != nil {
		select {
		case <-held:
		case <-ctx.Done():
		}
	}
	wctx, cancel := context.WithCancel(ctx)
	frozen := &atomic.Bool{}
	b.mu.Lock()
	b.stops = append(b.stops, cancel)
	b.frozen = append(b.frozen, frozen)
	b.mu.Unlock()
	in := b.Reader.Watch(wctx, key, opts...)
	out := make(chan clientv3.WatchResponse)
	go func() {
		defer close(out)
		for {
			select {
			case resp, ok := <-in:
				if !ok {
					return
				}
				if frozen.Load() {
					continue
				}
				select {
				case out <- resp:
				case <-wctx.Done():
					return
				}
			case <-wctx.Done():
				return
			}
		}
	}()
	return out
}

// breakAndHold ends every open watch and makes new ones wait until release.
func (b *breakableReader) breakAndHold() (release func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	held := make(chan struct{})
	b.held = held
	for _, stop := range b.stops {
		stop()
	}
	b.stops = nil
	return func() {
		b.mu.Lock()
		b.held = nil
		b.mu.Unlock()
		close(held)
	}
}

// freeze makes every open watch go silent without ending, as a stuck stream would; later watches work.
func (b *breakableReader) freeze() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, f := range b.frozen {
		f.Store(true)
	}
	b.frozen = nil
}

func (b *breakableReader) watchStarts() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.starts)
}

// A watch that ends without a compaction is reopened from the revision after the last event applied, not relisted;
// what changed in between arrives through the new watch.
func TestMirrorRewatchesAfterBrokenWatch(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	direct, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	b := &breakableReader{Reader: direct}
	mustPut(t, ctx, raw, leaseKey(0), "v1")
	m := startMirror(t, b, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 200 * time.Millisecond})
	last := mustPut(t, ctx, raw, leaseKey(1), "v1")
	waitConverged(t, ctx, m, raw, 20*time.Second)
	mustPut(t, ctx, raw, AccountKey("td-tw-1", 1), "noise") // advances the cluster, not the prefix

	release := b.breakAndHold()
	mustPut(t, ctx, raw, leaseKey(2), "while broken")
	if _, err := raw.Delete(ctx, leaseKey(0)); err != nil {
		t.Fatal(err)
	}
	release()

	waitConverged(t, ctx, m, raw, 20*time.Second)
	starts := b.watchStarts()
	if len(starts) < 2 || starts[len(starts)-1] != last {
		t.Fatalf("watch starts %v, want the last one at %d (the last event applied)", starts, last)
	}
	if st := m.Stats(); st.Lists != 1 || st.Errors < 1 {
		t.Fatalf("stats %+v, want a rewatch without a relist", st)
	}
}

// The proxy now leads to a different cluster, with fewer revisions than the mirror has seen. Its watch would wait
// for a revision that cluster has not reached; the probe sees another cluster ID and the mirror lists again.
func TestMirrorClusterReplaced(t *testing.T) {
	ctx := testCtx(t)
	_, p, rawA, r := proxied(t)
	b := etcdtest.Start(t)
	rawB := b.Client(t)
	for i := range 20 {
		mustPut(t, ctx, rawA, leaseKey(i), "from A")
	}
	mustPut(t, ctx, rawB, leaseKey(0), "from B")
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 200 * time.Millisecond, ProbeInterval: 200 * time.Millisecond})
	m.Take()

	p.Cut()
	p.SetTarget(b)
	p.Restore()
	waitConverged(t, ctx, m, rawB, 30*time.Second)
	v, changed := m.Take()
	if len(v.Entries) != 1 || len(changed) != 20 {
		t.Fatalf("view %d keys, changed %d; want B's one key and all 20 marked", len(v.Entries), len(changed))
	}
	if st := m.Stats(); st.Lists < 2 {
		t.Fatalf("stats %+v, want a relist", st)
	}
}

// The cluster is rebuilt from nothing: same cluster ID, revisions start over. The probe sees the revision go back.
func TestMirrorClusterRebuilt(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		mustPut(t, ctx, raw, leaseKey(i), "old")
	}
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 200 * time.Millisecond, ProbeInterval: 200 * time.Millisecond})
	before := m.Rev()

	s.Rebuild(t)
	mustPut(t, ctx, raw, leaseKey(3), "new")
	waitConverged(t, ctx, m, raw, 30*time.Second)
	if got := m.Rev(); got >= before {
		t.Fatalf("rev %d after the rebuild, the old cluster was at %d", got, before)
	}
	if st := m.Stats(); st.Lists < 2 {
		t.Fatalf("stats %+v, want a relist", st)
	}
}

// With nothing written, progress notifications keep LastContact fresh; cut off, it stops; back, it resumes.
func TestMirrorLastContact(t *testing.T) {
	ctx := testCtx(t)
	_, p, raw, r := proxied(t)
	mustPut(t, ctx, raw, leaseKey(0), "v1")
	const probe = 200 * time.Millisecond
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{MaxBackoff: 200 * time.Millisecond, ProbeInterval: probe})
	fresh := func(within time.Duration) bool { return time.Since(m.LastContact()) < within }
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: last contact %s ago, stats %+v", what, time.Since(m.LastContact()), m.Stats())
			}
		}
	}

	time.Sleep(5 * probe) // longer than the list's contact lasts
	waitFor("quiet but connected", func() bool { return fresh(3 * probe) })
	p.Cut()
	time.Sleep(6 * probe)
	if fresh(4 * probe) {
		t.Fatalf("cut off for %s, yet last contact %s ago", 6*probe, time.Since(m.LastContact()))
	}
	p.Restore()
	waitFor("reconnected", func() bool { return fresh(3 * probe) })
}

func TestMirrorWaitRev(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	m := startMirror(t, r, TDLeasePrefix("td-tw-1"), MirrorOptions{ProbeInterval: 200 * time.Millisecond})

	rev := mustPut(t, ctx, raw, leaseKey(1), "mine")
	if err := m.WaitRev(ctx, rev); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get(leaseKey(1)); !ok {
		t.Fatal("WaitRev returned before the write it waited for was in the view")
	}
	// A write outside the prefix is reached through a progress notification.
	rev = mustPut(t, ctx, raw, AccountKey("td-tw-1", 1), "elsewhere")
	if err := m.WaitRev(ctx, rev); err != nil {
		t.Fatalf("WaitRev past a write outside the prefix: %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := m.WaitRev(short, rev+1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitRev for a revision nobody wrote: %v", err)
	}
}

// A watch that goes silent without ending, while the cluster answers: the probe reopens it from the last applied
// revision, and what was written meanwhile arrives.
func TestMirrorStalledWatch(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	direct, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	b := &breakableReader{Reader: direct}
	m := startMirror(t, b, TDLeasePrefix("td-tw-1"), MirrorOptions{ProbeInterval: 200 * time.Millisecond})
	mustPut(t, ctx, raw, leaseKey(0), "v1")
	waitConverged(t, ctx, m, raw, 20*time.Second)

	b.freeze()
	mustPut(t, ctx, raw, leaseKey(1), "while stalled")
	if _, err := raw.Delete(ctx, leaseKey(0)); err != nil {
		t.Fatal(err)
	}
	waitConverged(t, ctx, m, raw, 20*time.Second)
	if st := m.Stats(); st.Lists != 1 || st.Watches < 2 {
		t.Fatalf("stats %+v, want the watch reopened without a relist", st)
	}
}

// Two mirrors on one client share its watch stream, opened on a quiet cluster and left quiet. A watch that started
// beyond the cluster's revision would make etcd withhold progress notifications from the whole stream; both mirrors
// must stay fresh, and neither watch may be reopened as stalled.
func TestMirrorQuietSharedStream(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	r, err := newClusters(t, topo, nil, nil).Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	rev := mustPut(t, ctx, raw, leaseKey(0), "the last write") // before the mirrors: they open at the cluster's rev
	const probe = 200 * time.Millisecond
	opt := MirrorOptions{ProbeInterval: probe}
	leases := startMirror(t, r, TDLeasePrefix("td-tw-1"), opt)
	acks := startMirror(t, r, TDAckPrefix("td-tw-1"), opt)

	time.Sleep(15 * probe)
	for name, m := range map[string]*Mirror{"leases": leases, "acks": acks} {
		if ago := time.Since(m.LastContact()); ago > 3*probe {
			t.Errorf("%s: last contact %s ago on a connected, quiet cluster", name, ago)
		}
		if st := m.Stats(); st.Watches != 1 || st.Errors != 0 {
			t.Errorf("%s: stats %+v, want one watch and no reopen", name, st)
		}
		if got := m.Rev(); got != rev {
			t.Errorf("%s: rev %d, want the cluster's %d", name, got, rev)
		}
	}
}
