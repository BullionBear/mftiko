package state

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DefaultPresenceTTL is the TTL of the agent's own lease when PresenceOptions leaves it 0 (DECISIONS D28).
const DefaultPresenceTTL = 60 * time.Second

const (
	presenceBatchOps   = 64        // puts per txn when rewriting; etcd allows 128 ops per txn by default
	presenceBatchBytes = 512 << 10 // keys and values per txn when rewriting; etcd takes requests up to 1.5 MiB by default
	presenceCheck      = 5 * time.Second
	presenceRetryMin   = 100 * time.Millisecond // first wait after a failed check, grant or rewrite
	presenceRetryMax   = 2 * time.Second
	fixAttempts        = 3 // grants tried by one fix, when each new lease is gone again before the rewrite ends
)

// PresenceOptions configure NewPresence.
type PresenceOptions struct {
	TTL time.Duration // rounded up to whole seconds; 0 = DefaultPresenceTTL. etcd raises a TTL below its minimum

	hooks *presenceHooks // tests only
}

// presenceHooks let tests reach the paths no real failure reaches on cue. Nil outside tests. The functions run on
// the presence's own goroutine and must not block.
type presenceHooks struct {
	drop    <-chan struct{}                          // each receive ends the keepalive, as the client's deadline would
	checked func(lease clientv3.LeaseID, alive bool) // after a check of a lease whose keepalive ended
	keeping func(lease clientv3.LeaseID)             // a keepalive of lease started
}

// Presence is the agent's own etcd lease and the keys bound to it (PROTOCOL §3.1, §3.2 W7; DECISIONS D14, D28): the
// AgentRecord, every WorkerStatus and, for MD and TD, every ack. The controller has one too, for its AgentRecord.
// These keys say "this process is here": when it has been gone for longer than the TTL, etcd deletes them, and the
// controller reads the silence as "unknown", never as a nack (D9).
//
// The keys are written only through a Presence (W1): Term.Put, Delete and Txn refuse them, so none is ever left
// unbound.
//
//   - Put writes the value under the lease, unless etcd already holds exactly that value there: callers put on state
//     changes only, and an unchanged value costs no write. A WorkerStatus changes with its phase, not with each Beat
//     or Report (W7).
//   - The lease is kept alive at a third of its TTL. When the keepalive ends, the lease is checked: if it still
//     exists, the keepalive resumes; if etcd has dropped it, and with it the keys, a new lease is granted and every
//     wanted key rewritten under it, the AgentRecord first. Puts wait for that rewrite and run after it. A put that
//     finds the lease gone does the same at once.
//   - Nothing is revoked when the term ends or on Close: the keepalive stops and the keys live out the TTL. An agent
//     that restarts or hands over therefore does not make its acks vanish at once; the next term rewrites what it
//     wants under its own lease, and etcd drops the rest when this lease expires.
//
// One per Term, in the term's home region. Safe for concurrent use.
type Presence struct {
	t     *Term
	cli   *clientv3.Client
	ttl   int64 // seconds
	hooks *presenceHooks

	ctx    context.Context // ends with the term or on Close; bounds every call this presence makes
	cancel context.CancelFunc
	done   chan struct{} // closed when the loop has exited
	wake   chan struct{} // one slot: a new lease was granted, the loop moves the keepalive to it

	sem chan struct{} // one slot, held across every etcd call that writes the keys or replaces the lease
	// The rest is guarded by sem.
	lease   clientv3.LeaseID
	expired bool              // lease is known to be gone: grant a new one before the next write
	stale   bool              // keys in want may be missing under lease: rewrite them before any other put
	want    map[string]string // key -> the value last put and accepted by etcd, to rewrite after a lease loss
	held    map[string]kvHeld // what etcd is known to hold for a key under lease; absent = unknown

	current atomic.Int64 // lease, for readers that do not hold sem
}

type kvHeld struct {
	val     string
	present bool
}

// NewPresence grants the agent's lease in the term's home region and starts keeping it alive. Call it once per
// Term, right after Campaign, then put the AgentRecord; a second call returns ErrPresenceExists. ctx bounds the
// grant only.
func NewPresence(ctx context.Context, t *Term, opt PresenceOptions) (*Presence, error) {
	if opt.TTL < 0 {
		return nil, fmt.Errorf("%w: negative TTL %s", ErrBadOptions, opt.TTL)
	}
	ttl := opt.TTL
	if ttl == 0 {
		ttl = DefaultPresenceTTL
	}
	if err := t.Err(); err != nil {
		return nil, err
	}
	cli, err := t.cl.client(t.home)
	if err != nil {
		return nil, err
	}
	if !t.presence.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("%w: %s/%s", ErrPresenceExists, t.plane, t.inst)
	}
	p := &Presence{
		t: t, cli: cli, ttl: int64((ttl + time.Second - 1) / time.Second), hooks: opt.hooks,
		done: make(chan struct{}),
		wake: make(chan struct{}, 1),
		sem:  make(chan struct{}, 1),
		want: map[string]string{},
		held: map[string]kvHeld{},
	}
	p.ctx, p.cancel = context.WithCancel(t.ctx)
	bctx, release := p.bind(ctx)
	err = p.grant(bctx)
	release()
	if err != nil {
		p.cancel()
		t.presence.Store(false)
		return nil, err
	}
	p.stale = false // nothing is wanted yet
	go p.run()
	return p, nil
}

// Lease is the lease the keys are bound to now. It changes when the lease is lost and granted again.
func (p *Presence) Lease() clientv3.LeaseID { return clientv3.LeaseID(p.current.Load()) }

// Put binds key to the lease with val, writing it only if etcd does not already hold val there under the lease.
// key must be the agent's AgentRecord, one of its WorkerStatus keys, or (MD, TD) one of its acks; the controller's
// only key is its AgentRecord. Anything else is ErrNotWriter, before any I/O.
//
// A failed Put is not retried here; callers reconcile level-triggered and put again. Until then a rewrite after a
// lease loss writes the key's last accepted value: a value etcd refused (too large, say) is never rewritten, so it
// cannot block the rewrite of the others.
func (p *Presence) Put(ctx context.Context, key, val string) error {
	if err := p.check(clientv3.OpPut(key, val)); err != nil {
		return err
	}
	if err := p.lock(ctx); err != nil {
		return err
	}
	defer p.unlock()
	ctx, release := p.bind(ctx)
	defer release()
	prev, had := p.want[key]
	p.want[key] = val
	err := p.putWanted(ctx, key, val)
	if err != nil {
		if had {
			p.want[key] = prev
		} else {
			delete(p.want, key)
		}
	}
	return err
}

// putWanted writes want[key], which is val, after any pending grant and rewrite. Under sem.
func (p *Presence) putWanted(ctx context.Context, key, val string) error {
	for range fixAttempts {
		if err := p.fix(ctx); err != nil {
			return err
		}
		if p.held[key] == (kvHeld{val: val, present: true}) {
			return nil
		}
		err := p.put(ctx, []string{key})
		if !errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return err
		}
		p.expired = true // the next fix grants a new lease and rewrites every wanted key, this one included
	}
	return fmt.Errorf("state: presence of %s/%s: lease lost %d times in one put", p.t.plane, p.t.inst, fixAttempts)
}

// Delete removes key from etcd and stops wanting it. The same keys as Put. A key this presence never wrote is still
// deleted: the previous term's lease may hold it.
func (p *Presence) Delete(ctx context.Context, key string) error {
	if err := p.check(clientv3.OpDelete(key)); err != nil {
		return err
	}
	if err := p.lock(ctx); err != nil {
		return err
	}
	defer p.unlock()
	ctx, release := p.bind(ctx)
	defer release()
	delete(p.want, key)
	if kv, ok := p.held[key]; ok && !kv.present {
		return nil
	}
	if _, err := p.t.txn(ctx, p.scope(), nil, []clientv3.Op{clientv3.OpDelete(key)}, nil); err != nil {
		delete(p.held, key) // it may or may not be gone
		return err
	}
	p.held[key] = kvHeld{}
	return nil
}

// Close stops the keepalive, waits for a Put or Delete in progress and for the background loop, then returns. It
// revokes nothing: the keys stay until the TTL runs out, or until the next term rewrites them under its own lease.
// Puts and Deletes after Close fail with ErrPresenceClosed; so they do once the client is closed. The term ending
// stops the keepalive the same way. Idempotent.
func (p *Presence) Close() {
	p.cancel()
	p.sem <- struct{}{} // the call holding it was cut short by the cancel
	<-p.sem
	<-p.done
}

// check refuses, before any I/O, a key outside the presence scope. Every key in it belongs to the term's own
// instance, so it lives in the home region, where the lease is.
func (p *Presence) check(op clientv3.Op) error {
	_, err := routeTxn(p.t.cl.topo, p.scope(), nil, []clientv3.Op{op}, nil)
	return err
}

// lock takes sem, unless ctx is done, the term has ended or the presence is closed.
func (p *Presence) lock(ctx context.Context) error {
	if err := p.t.Err(); err != nil {
		return err
	}
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("state: waiting for the presence of %s/%s: %w", p.t.plane, p.t.inst, ctx.Err())
	case <-p.ctx.Done():
		return p.closedErr()
	}
	if p.ctx.Err() != nil {
		<-p.sem
		return p.closedErr()
	}
	return nil
}

func (p *Presence) unlock() { <-p.sem }

// bind derives a context that also ends when the presence stops, so no call outlives Close or the term.
func (p *Presence) bind(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// closedErr is why p.ctx ended: the term's cause, or Close.
func (p *Presence) closedErr() error {
	if err := p.t.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s/%s", ErrPresenceClosed, p.t.plane, p.t.inst)
}

// fix makes etcd hold every wanted key under a live lease, as far as is known: a new lease if the current one is
// gone, then the rewrite. Under sem.
func (p *Presence) fix(ctx context.Context) error {
	for range fixAttempts {
		if p.expired {
			if err := p.grant(ctx); err != nil {
				return err
			}
		}
		if !p.stale {
			return nil
		}
		err := p.rewrite(ctx)
		if err == nil {
			p.stale = false
			return nil
		}
		if !errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return err
		}
		p.expired = true
	}
	return fmt.Errorf("state: presence of %s/%s: lease lost %d times while rewriting", p.t.plane, p.t.inst, fixAttempts)
}

// grant replaces the lease with a new one. Every key is then assumed missing: etcd dropped them with the old lease.
// Under sem.
func (p *Presence) grant(ctx context.Context) error {
	if err := p.t.Err(); err != nil {
		return err
	}
	resp, err := p.cli.Grant(ctx, p.ttl)
	if err != nil {
		return p.t.cause(fmt.Errorf("state: grant the lease of %s/%s: %w", p.t.plane, p.t.inst, err))
	}
	p.lease, p.expired, p.stale = resp.ID, false, true
	clear(p.held)
	p.current.Store(int64(resp.ID))
	p.signal()
	return nil
}

// signal tells the loop a new lease was granted.
func (p *Presence) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// rewrite puts every wanted key that etcd is not known to hold: the AgentRecord in a txn of its own, then the rest
// in batches bounded in ops and bytes. Under sem.
func (p *Presence) rewrite(ctx context.Context) error {
	record := AgentKey(p.t.plane, p.t.inst)
	var keys []string
	for k, v := range p.want {
		if k != record && p.held[k] != (kvHeld{val: v, present: true}) {
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, cmp.Compare)
	if v, ok := p.want[record]; ok && p.held[record] != (kvHeld{val: v, present: true}) {
		if err := p.put(ctx, []string{record}); err != nil {
			return err
		}
	}
	for len(keys) > 0 {
		n, size := 0, 0
		for n < len(keys) && n < presenceBatchOps {
			kv := len(keys[n]) + len(p.want[keys[n]])
			if n > 0 && size+kv > presenceBatchBytes {
				break
			}
			n, size = n+1, size+kv
		}
		if err := p.put(ctx, keys[:n]); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// put writes the wanted values of keys under the lease, in one fenced txn. Under sem.
func (p *Presence) put(ctx context.Context, keys []string) error {
	ops := make([]clientv3.Op, len(keys))
	for i, k := range keys {
		ops[i] = clientv3.OpPut(k, p.want[k], clientv3.WithLease(p.lease))
	}
	if _, err := p.t.txn(ctx, p.scope(), nil, ops, nil); err != nil {
		for _, k := range keys {
			delete(p.held, k)
		}
		return err
	}
	for _, k := range keys {
		p.held[k] = kvHeld{val: p.want[k], present: true}
	}
	return nil
}

// scope is the keys bound to the agent's lease, and only those (W1, W7).
func (p *Presence) scope() writeScope {
	return writeScope{plane: p.t.plane, inst: p.t.inst, presence: true}
}

// run keeps the current lease alive. When a keepalive ends without a new lease to move to, it checks the lease and
// grants a new one if etcd has dropped it; once a keepalive runs, it rewrites the keys if they may be missing.
func (p *Presence) run() {
	defer close(p.done)
	for p.ctx.Err() == nil {
		select {
		case <-p.wake: // the lease about to be read is the newest
		default:
		}
		lease := p.Lease()
		ended, stop, err := p.keepAlive(lease)
		if err != nil {
			var halted clientv3.ErrKeepAliveHalted
			if errors.As(err, &halted) {
				p.cancel() // the client is closed: nothing here can reach etcd again
				return
			}
			p.sleep(presenceRetryMin)
			continue
		}
		var drop <-chan struct{}
		if p.hooks != nil {
			drop = p.hooks.drop
			if p.hooks.keeping != nil {
				p.hooks.keeping(lease)
			}
		}
		p.repair(ended)
		select {
		case <-ended:
		case <-p.wake:
		case <-drop:
		case <-p.ctx.Done():
		}
		stop()
		if p.ctx.Err() != nil {
			return
		}
		if p.Lease() == lease {
			p.recover(lease)
		}
	}
}

// keepAlive starts keeping lease alive. ended is closed when the keepalive ends: the lease expired, the client gave
// up waiting for a reply, or stop was called. stop returns once ended is closed.
func (p *Presence) keepAlive(lease clientv3.LeaseID) (ended <-chan struct{}, stop func(), err error) {
	kctx, cancel := context.WithCancel(p.ctx)
	ch, err := p.cli.KeepAlive(kctx, lease)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	c := make(chan struct{})
	go func() {
		for range ch { // drain: the client drops replies, with a warning, when the channel is full
		}
		close(c)
	}()
	return c, func() { cancel(); <-c }, nil
}

// repair grants and rewrites if a put left that undone (a lease was granted since the last rewrite, or found gone),
// retrying until it succeeds, the keepalive ends, a newer lease is granted or the presence stops. A Put that comes
// first does it itself.
func (p *Presence) repair(ended <-chan struct{}) {
	wait := presenceRetryMin
	for {
		if p.lock(p.ctx) != nil {
			return
		}
		if !p.stale && !p.expired {
			p.unlock()
			return
		}
		err := p.fix(p.ctx)
		p.unlock()
		if err == nil {
			return
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			wait = min(2*wait, presenceRetryMax)
			continue
		case <-p.wake:
			p.signal() // leave it for run, which moves the keepalive to the new lease
		case <-ended:
		case <-p.ctx.Done():
		}
		timer.Stop()
		return
	}
}

// recover runs after the keepalive of lease ended: if the lease still exists the keepalive simply resumes;
// otherwise a new lease is granted, and the next round rewrites the keys under it. Retries until one of the two
// happens, someone else replaces the lease, or the presence stops. The check is made without sem, so puts that
// need no I/O go on meanwhile.
func (p *Presence) recover(lease clientv3.LeaseID) {
	wait := presenceRetryMin
	for p.Lease() == lease {
		alive, err := p.alive(lease)
		if err == nil {
			if p.lock(p.ctx) != nil {
				return
			}
			if p.lease != lease {
				p.unlock() // a Put found it gone and granted another meanwhile
				return
			}
			if !alive {
				p.expired = true
				err = p.grant(p.ctx)
			}
			p.unlock()
			if err == nil {
				if p.hooks != nil && p.hooks.checked != nil {
					p.hooks.checked(lease, alive)
				}
				return
			}
		}
		if !p.sleep(wait) {
			return
		}
		wait = min(2*wait, presenceRetryMax)
	}
}

// alive asks etcd whether lease still exists. A lease with less than a second left reads as gone: replacing it
// costs a rewrite, nothing more.
func (p *Presence) alive(lease clientv3.LeaseID) (bool, error) {
	ctx, cancel := context.WithTimeout(p.ctx, presenceCheck)
	defer cancel()
	resp, err := p.cli.TimeToLive(ctx, lease)
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("state: check the lease of %s/%s: %w", p.t.plane, p.t.inst, err)
	}
	return resp.TTL > 0, nil
}

// sleep waits d unless the presence stops first; false if it stopped.
func (p *Presence) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-p.ctx.Done():
		return false
	}
}
