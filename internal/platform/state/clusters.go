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

// DialFunc opens a client to one regional cluster. Clusters calls it on first use of the region, outside its lock
// and on a goroutine of its own, so it may wait for the cluster; it should give up within a bounded time (the dial
// timeout). A failed dial is not remembered: the next use of the region dials again.
type DialFunc func(region string, endpoints []string) (*clientv3.Client, error)

// DialTimeout bounds a dial: with credentials, connecting and authenticating.
const DialTimeout = 5 * time.Second

// DefaultDial is a dial without credentials, for clusters with auth off: AuthDial with no user.
func DefaultDial(region string, endpoints []string) (*clientv3.Client, error) {
	return AuthDial("", "")(region, endpoints)
}

// AuthDial returns a dial that authenticates as user (E1-08, PROTOCOL §3.2 W1). Every process has a user of its
// own in every cluster it connects to: an agent its instance name, the controller "controller", the API server
// "api" (AuthTables). The cluster issues tokens (JWT in production); the client authenticates when it is created,
// and again whenever the cluster rejects its token as expired or as older than the cluster's last change to users
// and roles. With no user it is a plain client.
//
// The client has gRPC keepalive: a connection whose peer stops answering (a hung member, a dropped route) is closed
// within about 15s and reconnected, instead of being trusted forever. 10s is above etcd's default minimum ping
// interval (--grpc-keepalive-min-time, 5s).
func AuthDial(user, password string) DialFunc {
	return func(region string, endpoints []string) (*clientv3.Client, error) {
		if (user == "") != (password == "") {
			// clientv3 would silently not authenticate, and every request would fail as from an unknown user.
			return nil, fmt.Errorf("state: dial %s: a user needs a password, and a password a user", region)
		}
		c, err := clientv3.New(clientv3.Config{
			Endpoints:            endpoints,
			DialTimeout:          DialTimeout,
			DialKeepAliveTime:    10 * time.Second,
			DialKeepAliveTimeout: 5 * time.Second,
			Username:             user,
			Password:             password,
		})
		if err != nil {
			return nil, fmt.Errorf("state: dial %s: %w", region, err)
		}
		return c, nil
	}
}

// AgentRegions is the set of regions a process of (p, inst) may connect to (D17 rule 2): an agent only its own;
// the controller every region in the topology (nil).
func AgentRegions(t Topology, p Plane, inst string) ([]string, error) {
	if !p.valid() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPlane, p)
	}
	home, err := t.RegionOf(inst)
	if err != nil {
		return nil, err
	}
	if p == PlaneCtl {
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
	// (clientv3.Watcher.RequestProgress). Every watch on the stream gets the notification; Mirror gives each mirror
	// a stream of its own. With no stream for that metadata, clientv3 opens one that lasts until the client closes;
	// while the stream reconnects, the call waits for ctx.
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
	dialing map[string]*dialing // regions being dialled now
	closed  bool
}

// dialing is one dial in progress; done is closed when cli and err are set.
type dialing struct {
	done chan struct{}
	cli  *clientv3.Client
	err  error
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
	c := &Clusters{
		topo: t, allowed: map[string]bool{}, dial: dial,
		clients: map[string]*clientv3.Client{}, dialing: map[string]*dialing{},
	}
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

// Reader is the read side of region's cluster. ErrRegionNotAllowed for regions outside the allowed set. The first
// use of a region dials it, which with credentials waits up to DialTimeout for the cluster to authenticate the
// client; an unreachable cluster then fails the call, and the next one dials again.
func (c *Clusters) Reader(region string) (Reader, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*DialTimeout)
	defer cancel()
	cli, err := c.client(ctx, region)
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

// client is the raw client of region's cluster, dialled on first use. Only Term writes through it. The dial runs on
// its own goroutine: a caller waits for it only as long as ctx allows, and a dial in progress is shared by every
// caller of that region.
func (c *Clusters) client(ctx context.Context, region string) (*clientv3.Client, error) {
	r, ok := c.topo.Regions[region]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRegion, region)
	}
	if !c.allowed[region] {
		return nil, fmt.Errorf("%w: %s", ErrRegionNotAllowed, region)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClustersClosed
	}
	if cli, ok := c.clients[region]; ok {
		c.mu.Unlock()
		return cli, nil
	}
	d, ok := c.dialing[region]
	if !ok {
		d = &dialing{done: make(chan struct{})}
		c.dialing[region] = d
		go c.dialRegion(region, slices.Clone(r.Endpoints), d)
	}
	c.mu.Unlock()
	select {
	case <-d.done:
		return d.cli, d.err
	case <-ctx.Done():
		return nil, fmt.Errorf("state: waiting for the dial of %s: %w", region, ctx.Err())
	}
}

// dialRegion dials region and publishes the outcome: a client is kept unless Clusters closed meanwhile, an error is
// handed to the callers waiting now and then forgotten.
func (c *Clusters) dialRegion(region string, endpoints []string, d *dialing) {
	cli, err := c.dial(region, endpoints)
	if err == nil && cli == nil {
		err = fmt.Errorf("state: dial %s returned no client", region)
	}
	c.mu.Lock()
	delete(c.dialing, region)
	switch {
	case err != nil:
	case c.closed:
		cli.Close()
		cli, err = nil, ErrClustersClosed
	default:
		c.clients[region] = cli
	}
	c.mu.Unlock()
	d.cli, d.err = cli, err
	close(d.done)
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
