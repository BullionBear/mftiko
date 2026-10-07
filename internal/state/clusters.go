package state

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// DialFunc opens a client to one regional cluster. It is called at most once per region, on first use, with the
// Clusters lock held, so it must not wait for the cluster to answer (clientv3.New without auth does not).
type DialFunc func(region string, endpoints []string) (*clientv3.Client, error)

// DefaultDial is clientv3.New with a 5s dial timeout and gRPC keepalive: a connection whose peer stops answering
// (a hung member, a dropped route) is closed within about 15s and reconnected, instead of being trusted forever.
// 10s is above etcd's default minimum ping interval (--grpc-keepalive-min-time, 5s).
func DefaultDial(region string, endpoints []string) (*clientv3.Client, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("state: dial %s: %w", region, err)
	}
	return c, nil
}

// AgentRegions is the set of regions an agent may connect to (D17 rule 2): MD and TD only their own;
// STS (nil = every region in the topology).
func AgentRegions(t Topology, p Plane, inst string) ([]string, error) {
	if !p.valid() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPlane, p)
	}
	home, err := t.RegionOf(inst)
	if err != nil {
		return nil, err
	}
	if p == PlaneSTS {
		return nil, nil
	}
	return []string{home}, nil
}

// Reader is the read side of one cluster. There is deliberately no exported way to get a raw client or KV:
// writes go through Term (E1-05) so none can skip the writer condition.
type Reader interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	// Done is closed once the client is closed (Clusters.Close). A closed client fails Get only after its retries
	// and ends a watch without an error, so a long-running reader such as Mirror checks this instead.
	Done() <-chan struct{}
	// RequestProgress asks the watch stream that ctx's metadata selects for a progress notification: once every
	// watch on it has caught up, and none starts beyond the current revision, each receives that revision
	// (clientv3.Watcher.RequestProgress). Every watch on the stream gets the notification, so N mirrors probing one
	// client make N² of them per interval. With no stream for that metadata, clientv3 opens one that lasts until
	// the client closes; while the stream reconnects, the call waits for ctx.
	RequestProgress(ctx context.Context) error
}

// Clusters holds one client per regional cluster, dialled lazily on first use (PROTOCOL §3, D17). With a single
// region it is a single cluster and nothing else changes (D17 rule 7). Safe for concurrent use.
type Clusters struct {
	topo    Topology
	allowed map[string]bool
	dial    DialFunc

	mu      sync.Mutex
	clients map[string]*clientv3.Client
	closed  bool
}

// NewClusters validates t and returns a Clusters that may connect to the regions in allowed (nil = all regions,
// see AgentRegions). A nil dial is DefaultDial. Nothing is dialled yet.
func NewClusters(t Topology, allowed []string, dial DialFunc) (*Clusters, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	t = t.clone()
	if allowed == nil {
		allowed = slices.Collect(maps.Keys(t.Regions))
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("%w: empty allowed region list (nil means all)", ErrRegionNotAllowed)
	}
	c := &Clusters{topo: t, allowed: map[string]bool{}, dial: dial, clients: map[string]*clientv3.Client{}}
	for _, r := range allowed {
		if _, ok := t.Regions[r]; !ok {
			return nil, fmt.Errorf("%w: allowed region %q", ErrUnknownRegion, r)
		}
		c.allowed[r] = true
	}
	if c.dial == nil {
		c.dial = DefaultDial
	}
	return c, nil
}

// Topology returns a copy of the topology.
func (c *Clusters) Topology() Topology { return c.topo.clone() }

// Reader is the read side of region's cluster. ErrRegionNotAllowed for regions outside the allowed set.
func (c *Clusters) Reader(region string) (Reader, error) {
	cli, err := c.client(region)
	if err != nil {
		return nil, err
	}
	return reader{cli}, nil
}

// ReaderFor is the read side of the cluster that holds key (Topology.RegionOfKey).
func (c *Clusters) ReaderFor(key string) (Reader, error) {
	region, err := c.topo.RegionOfKey(key)
	if err != nil {
		return nil, err
	}
	return c.Reader(region)
}

// Close closes every client dialled so far. Later calls return ErrClustersClosed; Close itself is idempotent.
func (c *Clusters) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var errs []error
	for _, region := range slices.Sorted(maps.Keys(c.clients)) {
		if err := c.clients[region].Close(); err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, fmt.Errorf("state: close %s: %w", region, err))
		}
	}
	clear(c.clients)
	return errors.Join(errs...)
}

// client is the raw client of region's cluster, dialled on first use. Only Term writes through it.
func (c *Clusters) client(region string) (*clientv3.Client, error) {
	r, ok := c.topo.Regions[region]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRegion, region)
	}
	if !c.allowed[region] {
		return nil, fmt.Errorf("%w: %s", ErrRegionNotAllowed, region)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClustersClosed
	}
	if cli, ok := c.clients[region]; ok {
		return cli, nil
	}
	cli, err := c.dial(region, slices.Clone(r.Endpoints))
	if err != nil {
		return nil, err
	}
	if cli == nil {
		return nil, fmt.Errorf("state: dial %s returned no client", region)
	}
	c.clients[region] = cli
	return cli, nil
}

// reader hides the client behind Reader, so a type assertion cannot recover it.
type reader struct{ c *clientv3.Client }

func (r reader) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return r.c.Get(ctx, key, opts...)
}

func (r reader) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return r.c.Watch(ctx, key, opts...)
}

func (r reader) Done() <-chan struct{} { return r.c.Ctx().Done() }

func (r reader) RequestProgress(ctx context.Context) error { return r.c.RequestProgress(ctx) }
