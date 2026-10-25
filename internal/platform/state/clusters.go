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
	"google.golang.org/grpc"
)

// DialFunc opens a client to one regional cluster. Clusters calls it on first use of the region, outside its lock
// and on a goroutine of its own, so it may wait for the cluster; it should give up within a bounded time (the dial
// timeout). A failed dial is not remembered: the next use of the region dials again.
type DialFunc func(region string, endpoints []string) (*clientv3.Client, error)

// DialTimeout bounds a dial: with credentials, connecting and authenticating.
const DialTimeout = 5 * time.Second

// DialConfig is one authenticated dial (E12-D046). A zero DialTimeout is the
// DialTimeout constant. A zero RequestTimeout adds no unary deadline, which is
// what every dial did before that field existed.
type DialConfig struct {
	User, Password string
	DialTimeout    time.Duration
	RequestTimeout time.Duration
}

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
// interval (--grpc-keepalive-min-time, 5s). Timeouts are the defaults (E12-D046): Dial with only the user set.
func AuthDial(user, password string) DialFunc {
	return Dial(DialConfig{User: user, Password: password})
}

// Dial returns a dial for cfg. Keepalive matches AuthDial. A user without a password, or a password without a
// user, is refused: clientv3 would otherwise not authenticate, and every request would fail as from an unknown user.
// A positive RequestTimeout limits unary RPCs, including the authenticate call made while dialling. Watches and
// lease keepalive are streams and are not limited. A zero RequestTimeout sets no interceptor, so the client matches
// AuthDial.
func Dial(cfg DialConfig) DialFunc {
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = DialTimeout
	}
	return func(region string, endpoints []string) (*clientv3.Client, error) {
		if (cfg.User == "") != (cfg.Password == "") {
			return nil, fmt.Errorf("state: dial %s: a user needs a password, and a password a user", region)
		}
		ccfg := clientv3.Config{
			Endpoints:            endpoints,
			DialTimeout:          dialTimeout,
			DialKeepAliveTime:    10 * time.Second,
			DialKeepAliveTimeout: 5 * time.Second,
			Username:             cfg.User,
			Password:             cfg.Password,
		}
		if cfg.RequestTimeout > 0 {
			ccfg.DialOptions = []grpc.DialOption{
				grpc.WithChainUnaryInterceptor(unaryRequestTimeout(cfg.RequestTimeout)),
			}
		}
		c, err := clientv3.New(ccfg)
		if err != nil {
			return nil, fmt.Errorf("state: dial %s: %w", region, err)
		}
		return c, nil
	}
}

// unaryRequestTimeout bounds one unary RPC (E12-D046). It is chained inside etcd's retry interceptor, so a timeout
// is returned as an ordinary error and is not retried against a fresh budget. An auth failure is returned unchanged,
// and token refresh on that failure still runs. The error is not wrapped: a wrapped context deadline would look like
// the call's own deadline and etcd would retry it. Zero or negative is a no-op.
func unaryRequestTimeout(limit time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if limit <= 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		if deadline, ok := ctx.Deadline(); ok && !deadline.After(time.Now().Add(limit)) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		tctx, cancel := context.WithTimeout(ctx, limit)
		defer cancel()
		err := invoker(tctx, method, req, reply, cc, opts...)
		if err != nil && tctx.Err() != nil && ctx.Err() == nil {
			return fmt.Errorf("state: etcd request exceeded %s", limit)
		}
		return err
	}
}

// DialWait is how long Clusters.Reader waits for a dial in progress (E12-D046). It is twice DialTimeout, or twice
// the longest positive region dial timeout when that is longer. A region that omits dial_timeout does not shorten
// the wait. The dial itself still fails at its own timeout.
func DialWait(t Topology) time.Duration {
	wait := 2 * DialTimeout
	for _, r := range t.Regions {
		dial, _, _, err := r.Timeouts()
		if err != nil || dial <= 0 {
			continue
		}
		if twice := 2 * dial; twice > wait {
			wait = twice
		}
	}
	return wait
}

// AgentRegions is the set of regions a process of (p, inst) may connect to (E00-D005 rule 2): an agent only its own;
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
// writes go through Term (E1-05), or for the API server through APIWriter (E10-D042), so none can skip the writer
// condition or the scope.
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

// Clusters holds one client per regional cluster, dialled lazily on first use (PROTOCOL §3, E00-D005). With a single
// region it is a single cluster and nothing else changes (E00-D005 rule 7). Safe for concurrent use.
type Clusters struct {
	topo    Topology
	allowed map[string]bool
	dial    DialFunc

	mu       sync.Mutex
	clients  map[string]*clientv3.Client
	dialing  map[string]*dialing // regions being dialled now
	closed   bool
	dialWait time.Duration // Reader's wait for a dial in progress; 0 is 2*DialTimeout
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

// SetDialWait sets how long Reader waits for a dial that is already running (E12-D046). Zero and negative are
// ignored, leaving the default of twice DialTimeout. Call it before the first Reader. A later call is safe and
// applies to the next wait.
func (c *Clusters) SetDialWait(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.dialWait = d
	c.mu.Unlock()
}

func (c *Clusters) readerWait() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dialWait > 0 {
		return c.dialWait
	}
	return 2 * DialTimeout
}

// Reader is the read side of region's cluster. ErrRegionNotAllowed for regions outside the allowed set. The first
// use of a region dials it. The caller waits up to readerWait (twice DialTimeout, unless SetDialWait says longer)
// for that dial; an unreachable cluster then fails the call, and the next one dials again. The dial's own timeout
// can fail it sooner.
func (c *Clusters) Reader(region string) (Reader, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.readerWait())
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
