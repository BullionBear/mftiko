package daemon

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// ErrUnsynced is a write to a region whose instance mirrors have not finished
// their first list. The write is not attempted, and the region is not fenced
// (D1318, D1302).
var ErrUnsynced = errors.New("controller: region is not synced")

// queues is the per-region write path (D1306, D1313). A write runs on its
// region's goroutine, one at a time, in the order accepted. A region that is
// stuck or unreachable holds none of the others. The first write to a region
// fences it, inside Term.Put; regions with no write are not fenced (D1305).
// A region that has not synced is not written at all (D1318).
//
// submit waits for the write's result. post does not: the reconcile loop
// posts what a round derived and goes on, so a region whose write hangs holds
// neither the loop nor the other regions (D1337). A posted key that is still
// queued is replaced by the newer value in place, and one equal to the write
// in flight is dropped, so a stuck region queues at most one write per key.
// One equal to what this term last wrote is dropped too until the view has
// read the region up to that write's revision: before then the round is
// deciding from a view that does not show it yet (D1308, D1337).
type queues struct {
	term   *state.Term
	topo   state.Topology
	log    zerolog.Logger
	synced func(region string) bool

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex
	wg sync.WaitGroup
	qs map[string]*regionQueue
}

type regionQueue struct {
	ch   chan request
	kick chan struct{} // a post is waiting; capacity 1

	mu      sync.Mutex
	order   []string           // posted keys, oldest first
	posted  map[string]request // by key; result is nil
	running *request           // the posted write in flight
	wrote   map[string]written // by key: this term's last write the view may not show yet
}

type written struct {
	req request
	rev int64 // the revision the write committed at
}

type request struct {
	ctx    context.Context
	key    string
	val    string
	del    bool
	result chan error
}

func newQueues(term *state.Term, topo state.Topology, log zerolog.Logger, synced func(string) bool) *queues {
	ctx, cancel := context.WithCancel(context.Background())
	return &queues{
		term: term, topo: topo, log: log, synced: synced,
		ctx: ctx, cancel: cancel,
		qs: map[string]*regionQueue{},
	}
}

// stop rejects new writes and waits until every region's goroutine has
// returned. In-flight calls unblock because the term is closed first.
func (q *queues) stop() {
	q.mu.Lock()
	q.cancel()
	q.mu.Unlock()
	q.wg.Wait()
}

func (q *queues) submit(ctx context.Context, key, val string, del bool) error {
	if err := q.term.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	region, err := q.topo.RegionOfKey(key)
	if err != nil {
		return err
	}
	rq, err := q.region(region)
	if err != nil {
		if terr := q.term.Err(); terr != nil {
			return terr
		}
		return err
	}
	req := request{ctx: ctx, key: key, val: val, del: del, result: make(chan error, 1)}
	select {
	case rq.ch <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-q.ctx.Done():
		return q.doneErr()
	case <-q.term.Done():
		return q.term.Err()
	}
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return q.prefer(ctx.Err(), req.result)
	case <-q.ctx.Done():
		return q.prefer(q.doneErr(), req.result)
	case <-q.term.Done():
		return q.prefer(q.term.Err(), req.result)
	}
}

// post queues a write on its region and returns without waiting for it. The
// write's error is logged by the region's goroutine, not returned. Rounds
// derive again from the view, so a failed write is posted again by a later
// round that still wants it (D1337).
//
// seen is the revision the round's view has read the region up to.
func (q *queues) post(ctx context.Context, key, val string, del bool, seen int64) {
	if q.term.Err() != nil || ctx.Err() != nil {
		return
	}
	region, err := q.topo.RegionOfKey(key)
	if err != nil {
		q.log.Warn().Err(err).Str("key", key).Msg("write")
		return
	}
	rq, err := q.region(region)
	if err != nil {
		return // stopped
	}
	req := request{ctx: ctx, key: key, val: val, del: del}
	rq.mu.Lock()
	for k, w := range rq.wrote {
		if w.rev <= seen {
			delete(rq.wrote, k)
		}
	}
	last, unseen := rq.wrote[key]
	switch _, queued := rq.posted[key]; {
	case queued:
		rq.posted[key] = req
	case rq.running != nil && rq.running.same(req):
	case unseen && last.req.same(req):
	default:
		rq.order = append(rq.order, key)
		rq.posted[key] = req
	}
	rq.mu.Unlock()
	select {
	case rq.kick <- struct{}{}:
	default:
	}
}

func (r request) same(o request) bool { return r.key == o.key && r.val == o.val && r.del == o.del }

// next takes the oldest posted write and marks it in flight.
func (rq *regionQueue) next() (request, bool) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if len(rq.order) == 0 {
		return request{}, false
	}
	key := rq.order[0]
	rq.order = rq.order[1:]
	req := rq.posted[key]
	delete(rq.posted, key)
	rq.running = &req
	return req, true
}

// finished clears the write in flight. rev is where it committed, or 0 when
// it failed.
func (rq *regionQueue) finished(rev int64) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if rev != 0 {
		rq.wrote[rq.running.key] = written{req: *rq.running, rev: rev}
	} else {
		delete(rq.wrote, rq.running.key)
	}
	rq.running = nil
}

// prefer returns a finished write's error when the worker already has one.
func (q *queues) prefer(fallback error, result <-chan error) error {
	select {
	case err := <-result:
		return err
	default:
		return fallback
	}
}

func (q *queues) doneErr() error {
	if err := q.term.Err(); err != nil {
		return err
	}
	return context.Canceled
}

// region is the queue for name, started on first use. After stop it refuses.
func (q *queues) region(name string) (*regionQueue, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ctx.Err() != nil {
		return nil, q.ctx.Err()
	}
	if rq := q.qs[name]; rq != nil {
		return rq, nil
	}
	rq := &regionQueue{ch: make(chan request), kick: make(chan struct{}, 1), posted: map[string]request{}, wrote: map[string]written{}}
	q.qs[name] = rq
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.loop(name, rq)
	}()
	return rq, nil
}

// loop runs one region's writes. Posted writes go first, oldest first; a
// submit waits for at most one round's worth of them. Posted writes still
// queued when the queues stop are dropped: no caller waits for them.
func (q *queues) loop(region string, rq *regionQueue) {
	for {
		if q.ctx.Err() == nil {
			if req, ok := rq.next(); ok {
				rev, _ := q.exec(region, req)
				rq.finished(rev)
				continue
			}
		}
		select {
		case <-q.ctx.Done():
			q.abandon(rq.ch)
			return
		case req := <-rq.ch:
			_, err := q.exec(region, req)
			req.result <- err
		case <-rq.kick:
		}
	}
}

// exec runs one write behind the writer condition and returns the revision
// it committed at.
func (q *queues) exec(region string, req request) (int64, error) {
	if err := req.ctx.Err(); err != nil {
		return 0, err
	}
	if err := q.term.Err(); err != nil {
		return 0, err
	}
	if q.synced != nil && !q.synced(region) {
		return 0, ErrUnsynced
	}
	op := clientv3.OpPut(req.key, req.val)
	if req.del {
		op = clientv3.OpDelete(req.key)
	}
	resp, err := q.term.Txn(req.ctx, nil, []clientv3.Op{op}, nil)
	if err != nil {
		if !quiet(err) {
			q.log.Warn().Err(err).Str("region", region).Str("key", req.key).Msg("write")
		}
		return 0, err
	}
	return resp.Header.Revision, nil
}

// quiet is an error the term ending or the caller canceling already explains.
func quiet(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, state.ErrSuperseded) ||
		errors.Is(err, state.ErrSessionLost) || errors.Is(err, state.ErrClosed)
}

// abandon fails everything still queued once the queue has stopped. A send
// that races with this is unblocked by submit watching q.ctx.
func (q *queues) abandon(ch <-chan request) {
	err := q.doneErr()
	for {
		select {
		case req := <-ch:
			req.result <- err
		default:
			return
		}
	}
}
