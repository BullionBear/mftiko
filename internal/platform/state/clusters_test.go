package state

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
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

// dialCounter is a DialFunc that counts dials per region, then dials with inner (DefaultDial if nil).
type dialCounter struct {
	inner DialFunc
	mu    sync.Mutex
	n     map[string]int
}

func newDialCounter(inner DialFunc) *dialCounter {
	if inner == nil {
		inner = DefaultDial
	}
	return &dialCounter{inner: inner, n: map[string]int{}}
}

func (d *dialCounter) dial(region string, eps []string) (*clientv3.Client, error) {
	d.mu.Lock()
	d.n[region]++
	d.mu.Unlock()
	return d.inner(region, eps)
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
	d := newDialCounter(nil) // nothing listens: a dial without credentials does not wait
	allowed, err := AgentRegions(topo, PlaneMD, "md-tw-1")
	if err != nil {
		t.Fatal(err)
	}
	cl := newClusters(t, topo, allowed, d.dial)

	if _, err := cl.Reader("jp"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("Reader(jp) for an MD agent in tw: %v", err)
	}
	if _, err := cl.ReaderFor(MDAckKey("md-jp-1", "a1b2c3", "feed/x")); !errors.Is(err, ErrRegionNotAllowed) {
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
	s := shared.Take(t)
	topo := topologyOf(map[string]*etcdtest.Server{"tw": s}, map[string]string{"sts-tw-1": "tw", "md-tw-1": "tw"})
	cl := newClusters(t, topo, nil, as(t, topo, "md-tw-1"))

	key := AgentKey(PlaneMD, "md-tw-1")
	region, err := cl.Topology().RegionOfKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := cl.client(ctx, region)
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
	tw, jp := shared.Take(t), etcdtest.Start(t)
	topo := topologyOf(map[string]*etcdtest.Server{"tw": tw, "jp": jp}, twoRegions().Instances)
	rawTW, rawJP := tw.Client(t), jp.Client(t)

	// The controller, at home in tw: every region allowed (E00-D005 rule 2).
	d := newDialCounter(as(t, topo, ControllerInstance))
	allowed, err := AgentRegions(topo, PlaneCtl, ControllerInstance)
	if err != nil || allowed != nil {
		t.Fatalf("controller's regions: %v, %v; want all", allowed, err)
	}
	cl := newClusters(t, topo, allowed, d.dial)

	// It writes a TW session's JP market-data demand into JP's cluster (E00-D005 rule 1).
	key := MDDemandKey("md-jp-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT")
	region, err := topo.RegionOfKey(key)
	if err != nil || region != "jp" {
		t.Fatalf("RegionOfKey = %s, %v", region, err)
	}
	cli, err := cl.client(ctx, region)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, key, "demand"); err != nil {
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
	md := newDialCounter(as(t, topo, "md-tw-1"))
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

func TestDialWaitUsesTheLongestRegion(t *testing.T) {
	topo := twoRegions()
	if got := DialWait(topo); got != 2*DialTimeout {
		t.Fatalf("omitted: %s", got)
	}
	tw := topo.Regions["tw"]
	tw.DialTimeout = "8s"
	topo.Regions["tw"] = tw
	jp := topo.Regions["jp"]
	jp.DialTimeout = "0s"
	topo.Regions["jp"] = jp
	if got := DialWait(topo); got != 16*time.Second {
		t.Fatalf("longest: %s", got)
	}
}

func TestCloneKeepsRegionTimeouts(t *testing.T) {
	topo := twoRegions()
	tw := topo.Regions["tw"]
	tw.DialTimeout = "8s"
	tw.RequestTimeout = "2s"
	tw.StaleAfter = "20s"
	topo.Regions["tw"] = tw
	cl := newClusters(t, topo, nil, nil)
	got := cl.Topology().Regions["tw"]
	if got.DialTimeout != "8s" || got.RequestTimeout != "2s" || got.StaleAfter != "20s" {
		t.Fatalf("%+v", got)
	}
	if &got.Endpoints[0] == &tw.Endpoints[0] {
		t.Fatal("clone shares the endpoint slice")
	}
}

func TestReaderHonorsDialWait(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	dial := func(region string, eps []string) (*clientv3.Client, error) {
		close(started)
		<-release
		return nil, errors.New("still dialing")
	}
	topo := Topology{
		Regions:   map[string]Region{"tw": {Endpoints: []string{"http://127.0.0.1:1"}}},
		Instances: map[string]string{},
	}
	cl := newClusters(t, topo, nil, dial)
	cl.SetDialWait(200 * time.Millisecond)
	t.Cleanup(func() { close(release) })
	start := time.Now()
	_, err := cl.Reader("tw")
	if err == nil {
		t.Fatal("reader succeeded while the dial was blocked")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("reader waited %s: %v", time.Since(start), err)
	}
	select {
	case <-started:
	default:
		t.Fatal("dial was not started")
	}
}

// hangListener accepts TCP and never writes, so an etcd authenticate call waits
// until its own deadline. closeConns unblocks every dial.
func hangListener(t *testing.T) (endpoint string, closeConns func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	stopped := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			select {
			case <-stopped:
				mu.Unlock()
				c.Close()
				continue
			default:
				conns = append(conns, c)
			}
			mu.Unlock()
			go func() {
				io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	var once sync.Once
	closeConns = func() {
		once.Do(func() {
			close(stopped)
			ln.Close()
			mu.Lock()
			defer mu.Unlock()
			for _, c := range conns {
				c.Close()
			}
		})
	}
	t.Cleanup(closeConns)
	return "http://" + ln.Addr().String(), closeConns
}

func TestHungPeerHonorsDialAndRequestTimeouts(t *testing.T) {
	ep, closeConns := hangListener(t)
	const user, pw = "controller", "pw"

	longDial := make(chan struct{})
	go func() {
		defer close(longDial)
		_, _ = Dial(DialConfig{User: user, Password: pw, DialTimeout: 2 * time.Second})("hang", []string{ep})
	}()
	start := time.Now()
	_, err := Dial(DialConfig{User: user, Password: pw, DialTimeout: 200 * time.Millisecond})("hang", []string{ep})
	if err == nil {
		t.Fatal("short dial succeeded")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("dial timeout took %s: %v", time.Since(start), err)
	}
	select {
	case <-longDial:
		t.Fatal("2s dial finished with the 200ms dial")
	default:
	}

	longRequest := make(chan struct{})
	go func() {
		defer close(longRequest)
		_, _ = Dial(DialConfig{User: user, Password: pw, DialTimeout: 2 * time.Second})("hang", []string{ep})
	}()
	start = time.Now()
	_, err = Dial(DialConfig{
		User: user, Password: pw,
		DialTimeout: 5 * time.Second, RequestTimeout: 200 * time.Millisecond,
	})("hang", []string{ep})
	if err == nil {
		t.Fatal("request timeout dial succeeded")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("request timeout took %s: %v", time.Since(start), err)
	}
	if !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("request timeout error %v", err)
	}
	select {
	case <-longRequest:
		t.Fatal("dial without a request timeout finished with the bounded one")
	default:
	}

	closeConns()
	// Closing the peer does not cancel a dial that is waiting on its own
	// deadline, so wait that deadline out.
	deadline := time.Now().Add(4 * time.Second)
	for {
		select {
		case <-longDial:
			select {
			case <-longRequest:
				return
			default:
			}
		default:
		}
		if !time.Now().Before(deadline) {
			t.Fatal("hung dials did not return after the peer closed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
