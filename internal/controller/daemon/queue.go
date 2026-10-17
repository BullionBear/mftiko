package daemon

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// queues is the per-region write path (D1306, D1313). A write runs on its
// region's goroutine, one at a time, in the order accepted. A region that is
// stuck or unreachable holds none of the others. The first write to a region
// fences it, inside Term.Put; regions with no write are not fenced (D1305).
type queues struct {
	term *state.Term
	topo state.Topology
	log  zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex
	wg sync.WaitGroup
	qs map[string]*regionQueue
}

type regionQueue struct {
	ch chan request
}

type request struct {
	ctx    context.Context
	key    string
	val    string
	del    bool
	result chan error
}

func newQueues(term *state.Term, topo state.Topology, log zerolog.Logger) *queues {
	ctx, cancel := context.WithCancel(context.Background())
	return &queues{
		term: term, topo: topo, log: log,
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
	rq := &regionQueue{ch: make(chan request)}
	q.qs[name] = rq
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.loop(name, rq.ch)
	}()
	return rq, nil
}

func (q *queues) loop(region string, ch <-chan request) {
	for {
		select {
		case <-q.ctx.Done():
			q.abandon(ch)
			return
		case req := <-ch:
			err := q.exec(region, req)
			req.result <- err
		}
	}
}

func (q *queues) exec(region string, req request) error {
	if err := req.ctx.Err(); err != nil {
		return err
	}
	if err := q.term.Err(); err != nil {
		return err
	}
	var err error
	if req.del {
		err = q.term.Delete(req.ctx, req.key)
	} else {
		err = q.term.Put(req.ctx, req.key, req.val)
	}
	if err != nil && !quiet(err) {
		q.log.Warn().Err(err).Str("region", region).Str("key", req.key).Msg("write")
	}
	return err
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
