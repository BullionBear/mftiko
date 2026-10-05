package state

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/state/etcdtest"
)

// testTimeout bounds every integration test; generous so a slow -race run does not flake.
const testTimeout = 60 * time.Second

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

// topologyOf maps each region to its test server and each instance to a region.
func topologyOf(servers map[string]*etcdtest.Server, instances map[string]string) Topology {
	t := Topology{Regions: map[string]Region{}, Instances: instances}
	for region, s := range servers {
		t.Regions[region] = Region{Endpoints: s.Endpoints()}
	}
	return t
}

// dialCounter is a DialFunc that counts dials per region.
type dialCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func newDialCounter() *dialCounter { return &dialCounter{n: map[string]int{}} }

func (d *dialCounter) dial(region string, eps []string) (*clientv3.Client, error) {
	d.mu.Lock()
	d.n[region]++
	d.mu.Unlock()
	return DefaultDial(region, eps)
}

func (d *dialCounter) count(region string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n[region]
}

func newClusters(t *testing.T, topo Topology, allowed []string, dial DialFunc) *Clusters {
	t.Helper()
	cl, err := NewClusters(topo, allowed, dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func mustGet(t *testing.T, ctx context.Context, c *clientv3.Client, key string) *clientv3.GetResponse {
	t.Helper()
	resp, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return resp
}

// No etcd needed: nothing is dialled until a region is used.
func TestClustersAllowedAndLazy(t *testing.T) {
	topo := twoRegions()
	d := newDialCounter()
	allowed, err := AgentRegions(topo, PlaneMD, "md-tw-1")
	if err != nil {
		t.Fatal(err)
	}
	cl := newClusters(t, topo, allowed, d.dial)

	if _, err := cl.Reader("jp"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("Reader(jp) for an MD agent in tw: %v", err)
	}
	if _, err := cl.ReaderFor(MDAckKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/x")); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("ReaderFor(jp key) for an MD agent in tw: %v", err)
	}
	if _, err := cl.Reader("kr"); !errors.Is(err, ErrUnknownRegion) {
		t.Fatalf("Reader(kr): %v", err)
	}
	if _, err := cl.ReaderFor("/nope"); !errors.Is(err, ErrBadKey) {
		t.Fatalf("ReaderFor(bad key): %v", err)
	}
	if n := d.count("tw") + d.count("jp"); n != 0 {
		t.Fatalf("%d dials before any allowed use", n)
	}
	for range 3 {
		if _, err := cl.Reader("tw"); err != nil {
			t.Fatal(err)
		}
	}
	if d.count("tw") != 1 || d.count("jp") != 0 {
		t.Fatalf("dials tw=%d jp=%d, want 1 and 0", d.count("tw"), d.count("jp"))
	}

	// Topology returns a copy.
	cp := cl.Topology()
	cp.Instances["md-jp-1"] = "tw"
	if r, _ := cl.Topology().RegionOf("md-jp-1"); r != "jp" {
		t.Fatal("Topology() exposed the internal map")
	}

	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := cl.Reader("tw"); !errors.Is(err, ErrClustersClosed) {
		t.Fatalf("Reader after Close: %v", err)
	}
}

func TestNewClustersRejects(t *testing.T) {
	if _, err := NewClusters(Topology{}, nil, nil); !errors.Is(err, ErrBadTopology) {
		t.Errorf("empty topology: %v", err)
	}
	if _, err := NewClusters(twoRegions(), []string{"kr"}, nil); !errors.Is(err, ErrUnknownRegion) {
		t.Errorf("unknown allowed region: %v", err)
	}
	if _, err := NewClusters(twoRegions(), []string{}, nil); err == nil {
		t.Errorf("empty allowed list accepted")
	}
}

func TestClustersSingleRegion(t *testing.T) {
	ctx := testCtx(t)
	s := etcdtest.Start(t)
	topo := topologyOf(map[string]*etcdtest.Server{"tw": s}, map[string]string{"sts-tw-1": "tw", "md-tw-1": "tw"})
	cl := newClusters(t, topo, nil, nil)

	key := AgentKey(PlaneMD, "md-tw-1")
	region, err := cl.Topology().RegionOfKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := cl.client(region)
	if err != nil {
		t.Fatal(err)
	}
	r, err := cl.ReaderFor(key)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wch := r.Watch(wctx, key)

	if _, err := cli.Put(ctx, key, "v1"); err != nil {
		t.Fatal(err)
	}
	resp, err := r.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v1" {
		t.Fatalf("read back %v", resp.Kvs)
	}
	select {
	case w := <-wch:
		if err := w.Err(); err != nil || len(w.Events) != 1 || string(w.Events[0].Kv.Value) != "v1" {
			t.Fatalf("watch: %v %v", err, w.Events)
		}
	case <-ctx.Done():
		t.Fatal("no watch event")
	}
}

func TestClustersTwoRegions(t *testing.T) {
	ctx := testCtx(t)
	tw, jp := etcdtest.Start(t), etcdtest.Start(t)
	topo := topologyOf(map[string]*etcdtest.Server{"tw": tw, "jp": jp}, twoRegions().Instances)
	rawTW, rawJP := tw.Client(t), jp.Client(t)

	// STS agent in tw: every region allowed.
	d := newDialCounter()
	allowed, err := AgentRegions(topo, PlaneSTS, "sts-tw-1")
	if err != nil {
		t.Fatal(err)
	}
	cl := newClusters(t, topo, allowed, d.dial)

	// The TW STS writes its JP market-data lease into JP's cluster (D17 rule 1).
	key := MDLeaseKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
	region, err := topo.RegionOfKey(key)
	if err != nil || region != "jp" {
		t.Fatalf("RegionOfKey = %s, %v", region, err)
	}
	cli, err := cl.client(region)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, key, "lease"); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, ctx, rawJP, key); len(got.Kvs) != 1 {
		t.Fatalf("jp has %d copies", len(got.Kvs))
	}
	if got := mustGet(t, ctx, rawTW, key); len(got.Kvs) != 0 {
		t.Fatal("jp's key landed in tw")
	}
	if d.count("jp") != 1 || d.count("tw") != 0 {
		t.Fatalf("dials jp=%d tw=%d, want 1 and 0", d.count("jp"), d.count("tw"))
	}

	// Reads route the same way; tw is dialled on first use.
	r, err := cl.ReaderFor(key)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := r.Get(ctx, key); err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("ReaderFor(jp key).Get: %v %v", resp, err)
	}
	spec := SessionSpecKey("sts-tw-1", "a1b2c3")
	if _, err := rawTW.Put(ctx, spec, "spec"); err != nil {
		t.Fatal(err)
	}
	r, err = cl.ReaderFor(spec)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := r.Get(ctx, spec); err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("ReaderFor(tw spec).Get: %v %v", resp, err)
	}
	if d.count("jp") != 1 || d.count("tw") != 1 {
		t.Fatalf("dials jp=%d tw=%d, want 1 and 1", d.count("jp"), d.count("tw"))
	}

	// MD agent in tw: only tw.
	md := newDialCounter()
	allowed, err = AgentRegions(topo, PlaneMD, "md-tw-1")
	if err != nil {
		t.Fatal(err)
	}
	mdcl := newClusters(t, topo, allowed, md.dial)
	if _, err := mdcl.Reader("jp"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("MD agent in tw, Reader(jp): %v", err)
	}
	if _, err := mdcl.ReaderFor(key); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("MD agent in tw, ReaderFor(jp key): %v", err)
	}
	if md.count("jp") != 0 {
		t.Fatal("dialled a region that is not allowed")
	}
}
