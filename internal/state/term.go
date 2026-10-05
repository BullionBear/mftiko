package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DefaultSessionTTL is the election session's TTL when TermOptions leaves it 0.
const DefaultSessionTTL = 15 * time.Second

const (
	epochAttempts  = 5               // epoch bumps tried before giving up (PROTOCOL §3.2 W2 step 2)
	writerAttempts = 3               // writer acquisitions tried when the key vanishes in between
	cleanupTimeout = 5 * time.Second // resign and revoke after a failed Campaign
)

// TermOptions configure Campaign.
type TermOptions struct {
	ID         string        // this candidate, stored as the election value (e.g. host:pid); required
	SessionTTL time.Duration // election session TTL, rounded up to whole seconds; 0 = DefaultSessionTTL
}

// Term is this process's tenure as the agent of one (plane, instance): the election it won, the epoch E it
// got, and the writer key it holds in each cluster it writes to (PROTOCOL §3.2 W2). Every write goes through
// a Term and is conditioned on that cluster's writer key.
//
// A term ends once, for the first of: the election session is lost (ErrSessionLost), a writer key shows a
// newer epoch (ErrSuperseded), or Close (ErrClosed). From then on Done is closed and every write returns Err
// without contacting etcd; in-flight calls are cancelled. An ended term never resumes: the agent exits and
// systemd restarts it, and the new process campaigns for a new epoch.
//
// Safe for concurrent use.
type Term struct {
	cl    *Clusters
	plane Plane
	inst  string
	home  string // the region of inst: where the election and the epoch live
	epoch int64
	val   string // formatEpoch(epoch), the writer key's value

	session  *concurrency.Session
	election *concurrency.Election

	ctx    context.Context // cancelled when the term ends, to abort in-flight calls
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	err    error             // the first cause; set under mu before done is closed
	fences map[string]*fence // region -> writer key held there

	closeOnce sync.Once
	closeErr  error
}

// fence is the writer key this term holds in one cluster.
type fence struct {
	mu  sync.Mutex // held while acquiring, so one term never races itself for the key
	rev int64      // R, the writer key's mod_revision after our put; 0 until acquired
}

// Campaign blocks until this process leads (plane, inst) in its home region (PROTOCOL §3.2 W2 steps 1-2), then
// increments the persisted epoch and returns the Term. The home region is Topology.RegionOf(inst) and must be
// allowed. The writer key in the home region is acquired before Campaign returns (W2 step 3); other regions on
// first use (Fence).
//
// ctx bounds the campaign only; the term outlives it. On error nothing is held: the candidacy is withdrawn and
// the session's lease revoked.
func Campaign(ctx context.Context, cl *Clusters, p Plane, inst string, opt TermOptions) (*Term, error) {
	if !p.valid() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPlane, p)
	}
	if opt.ID == "" {
		return nil, fmt.Errorf("%w: ID is required", ErrBadOptions)
	}
	if opt.SessionTTL < 0 {
		return nil, fmt.Errorf("%w: negative SessionTTL %s", ErrBadOptions, opt.SessionTTL)
	}
	ttl := opt.SessionTTL
	if ttl == 0 {
		ttl = DefaultSessionTTL
	}
	secs := int64((ttl + time.Second - 1) / time.Second)
	home, err := cl.topo.RegionOf(inst)
	if err != nil {
		return nil, err
	}
	cli, err := cl.client(home)
	if err != nil {
		return nil, err
	}

	// Grant here rather than inside NewSession so that ctx can cancel it: the session itself must outlive ctx.
	lease, err := cli.Grant(ctx, secs)
	if err != nil {
		return nil, fmt.Errorf("state: campaign %s/%s: grant: %w", p, inst, err)
	}
	sess, err := concurrency.NewSession(cli, concurrency.WithLease(lease.ID), concurrency.WithTTL(int(secs)))
	if err == nil && sess == nil {
		err = errors.New("no session")
	}
	if err != nil {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		cli.Revoke(rctx, lease.ID)
		cancel()
		return nil, fmt.Errorf("state: campaign %s/%s: session: %w", p, inst, err)
	}

	t := &Term{
		cl: cl, plane: p, inst: inst, home: home,
		session:  sess,
		election: concurrency.NewElection(sess, ElectionPrefix(p, inst)),
		done:     make(chan struct{}),
		fences:   map[string]*fence{},
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	go t.watchSession()

	if err := t.start(ctx, cli, opt.ID); err != nil {
		t.end(err)
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		t.Close(cctx)
		cancel()
		return nil, err
	}
	return t, nil
}

// start runs W2 steps 1-3 in the home region.
func (t *Term) start(ctx context.Context, cli *clientv3.Client, id string) error {
	bctx, release := t.bind(ctx)
	defer release()
	if err := t.election.Campaign(bctx, id); err != nil {
		return t.cause(fmt.Errorf("state: campaign %s/%s: %w", t.plane, t.inst, err))
	}
	if err := t.bumpEpoch(bctx, cli); err != nil {
		return t.cause(err)
	}
	if _, err := t.Fence(ctx, t.home); err != nil {
		return err
	}
	return t.Err()
}

// bumpEpoch is W2 step 2: E = stored epoch + 1, written only while this candidate still leads and the epoch is
// unchanged since it was read.
func (t *Term) bumpEpoch(ctx context.Context, cli *clientv3.Client) error {
	key := EpochKey(t.plane, t.inst)
	for range epochAttempts {
		get, err := cli.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("state: read epoch %s: %w", key, err)
		}
		var prev int64
		unchanged := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		if len(get.Kvs) > 0 {
			if prev, err = parseEpoch(get.Kvs[0].Value); err != nil {
				return fmt.Errorf("%w: %s holds %q", ErrBadEpoch, key, get.Kvs[0].Value)
			}
			unchanged = clientv3.Compare(clientv3.ModRevision(key), "=", get.Kvs[0].ModRevision)
		}
		if prev == math.MaxInt64 {
			return fmt.Errorf("%w: %s is at its maximum", ErrBadEpoch, key)
		}
		e := prev + 1
		leader := clientv3.Compare(clientv3.CreateRevision(t.election.Key()), "=", t.election.Rev())
		resp, err := cli.Txn(ctx).
			If(leader, unchanged).
			Then(clientv3.OpPut(key, formatEpoch(e))).
			Else(clientv3.OpGet(t.election.Key())).
			Commit()
		if err != nil {
			return fmt.Errorf("state: bump epoch %s: %w", key, err)
		}
		if resp.Succeeded {
			t.epoch, t.val = e, formatEpoch(e)
			return nil
		}
		if kvs := resp.Responses[0].GetResponseRange().GetKvs(); len(kvs) == 0 || kvs[0].CreateRevision != t.election.Rev() {
			return fmt.Errorf("%w: no longer leads %s/%s", ErrSessionLost, t.plane, t.inst)
		}
		// Still the leader, so only the epoch moved: read it again.
	}
	return fmt.Errorf("%w: %s changed %d times while being bumped", ErrSuperseded, key, epochAttempts)
}

// Fence acquires (once per region, cached) the writer key in that region's cluster (W2 step 3) and returns its rev R.
// If the writer key already holds an epoch at least E, the term ends with ErrSuperseded.
func (t *Term) Fence(ctx context.Context, region string) (int64, error) {
	if err := t.Err(); err != nil {
		return 0, err
	}
	cli, err := t.cl.client(region)
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	f := t.fences[region]
	if f == nil {
		f = &fence{}
		t.fences[region] = f
	}
	t.mu.Unlock()

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rev != 0 {
		return f.rev, nil
	}
	if err := t.Err(); err != nil {
		return 0, err
	}
	bctx, release := t.bind(ctx)
	defer release()
	rev, err := t.acquire(bctx, cli, region)
	if errors.Is(err, ErrSuperseded) {
		t.end(err)
		return 0, t.Err() // the first cause, if the term had ended meanwhile
	}
	if err != nil {
		return 0, t.cause(err)
	}
	f.rev = rev
	return rev, nil
}

// acquire puts E into the writer key if it is absent or holds a smaller epoch. etcd fails a Value compare on a
// missing key, hence two txns.
func (t *Term) acquire(ctx context.Context, cli *clientv3.Client, region string) (int64, error) {
	key := WriterKey(t.plane, t.inst)
	for range writerAttempts {
		resp, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, t.val)).
			Commit()
		if err != nil {
			return 0, fmt.Errorf("state: fence %s: %w", region, err)
		}
		if resp.Succeeded {
			return resp.Header.Revision, nil
		}
		resp, err = cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(key), "<", t.val)).
			Then(clientv3.OpPut(key, t.val)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return 0, fmt.Errorf("state: fence %s: %w", region, err)
		}
		if resp.Succeeded {
			return resp.Header.Revision, nil
		}
		kvs := resp.Responses[0].GetResponseRange().GetKvs()
		if len(kvs) == 0 {
			continue // deleted between the two txns: start over
		}
		if string(kvs[0].Value) == t.val {
			// Epochs are unique, so this is our own put from an earlier attempt whose reply was lost.
			return kvs[0].ModRevision, nil
		}
		return 0, fmt.Errorf("%w: writer key in %s holds epoch %q, this term has %d", ErrSuperseded, region, kvs[0].Value, t.epoch)
	}
	return 0, fmt.Errorf("state: fence %s: %s disappeared %d times", region, key, writerAttempts)
}

// Plane is the plane this term leads.
func (t *Term) Plane() Plane { return t.plane }

// Instance is the instance this term leads.
func (t *Term) Instance() string { return t.inst }

// Epoch is E, the epoch this term got (PROTOCOL §2); C3 Attach carries it (D6).
func (t *Term) Epoch() int64 { return t.epoch }

// Done is closed when the term ends: session lost, superseded, or Close.
func (t *Term) Done() <-chan struct{} { return t.done }

// Err is nil while the term runs; then the cause it ended with, matching ErrSessionLost, ErrSuperseded or ErrClosed.
func (t *Term) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// Close ends the term with ErrClosed (unless it had already ended), resigns and revokes the session's lease.
// Errors from resigning and revoking are ignored once the session is lost; with the cluster client already
// closed (Clusters.Close) neither is tried. Idempotent: later calls return the first call's result.
func (t *Term) Close(ctx context.Context) error {
	t.closeOnce.Do(func() {
		t.end(ErrClosed)
		lost := false
		select {
		case <-t.session.Done():
			lost = true
		default:
		}
		cli := t.session.Client()
		if cli.Ctx().Err() != nil {
			t.session.Orphan()
			return
		}
		var errs []error
		if err := t.election.Resign(ctx); err != nil && !lost {
			errs = append(errs, fmt.Errorf("state: resign %s/%s: %w", t.plane, t.inst, err))
		}
		t.session.Orphan()
		if _, err := cli.Revoke(ctx, t.session.Lease()); err != nil && !lost {
			errs = append(errs, fmt.Errorf("state: revoke session of %s/%s: %w", t.plane, t.inst, err))
		}
		t.closeErr = errors.Join(errs...)
	})
	return t.closeErr
}

// end ends the term with cause, unless it has already ended. Only the first cause is kept.
func (t *Term) end(cause error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return
	}
	t.err = cause
	close(t.done)
	t.cancel()
}

// cause is the term's error if it has ended, else err: a call cut short by the term ending reports why it ended.
func (t *Term) cause(err error) error {
	if e := t.Err(); e != nil {
		return e
	}
	return err
}

// bind derives a context that is also cancelled when the term ends. If it has ended already, the context is
// cancelled before bind returns (AfterFunc alone would cancel it from another goroutine, a little later).
func (t *Term) bind(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(t.ctx, cancel)
	if t.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

// watchSession ends the term when the election session's keepalive stops: from then on this process may no
// longer be the leader, so it stops writing at once (E1-04).
func (t *Term) watchSession() {
	select {
	case <-t.session.Done():
		t.end(ErrSessionLost)
	case <-t.done:
	}
}

// formatEpoch is the 20-digit zero-padded decimal form, so byte order equals numeric order (PROTOCOL §3.1).
func formatEpoch(e int64) string { return fmt.Sprintf("%020d", e) }

func parseEpoch(v []byte) (int64, error) {
	if len(v) != 20 {
		return 0, ErrBadEpoch
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, ErrBadEpoch
		}
	}
	return strconv.ParseInt(string(v), 10, 64) // fails past MaxInt64
}
