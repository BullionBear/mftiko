package state

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// incarnationRetries is how often Next refreshes a wrong cache entry and tries again (PROTOCOL §3.2 W6.3).
const incarnationRetries = 3

// Incarnations hands out incarnation numbers for one agent's workers (PROTOCOL §3.2 W6).
//
// A worker key's counter is IncarnationKey(plane, inst, WorkerHash(worker key)) in the home region: decimal, not
// bound to any lease, written only by this agent and only behind its writer key (D8). Safe for concurrent use.
type Incarnations struct {
	t   *Term
	sem chan struct{} // one slot: one Next or Forget at a time; waiting for it honours ctx and the term

	mu  sync.Mutex        // guards cur only, never held across etcd calls
	cur map[string]uint32 // WorkerHash -> last number taken or loaded
}

// LoadIncarnations lists IncarnationPrefix(plane, inst) in the home region into memory (W6.1). Call once per Term,
// before the first spawn. The prefix has a single writer (this agent), so no watch. A stored value that is not a
// uint32 in canonical decimal form fails the load with ErrBadIncarnation.
func LoadIncarnations(ctx context.Context, t *Term) (*Incarnations, error) {
	if !t.plane.agent() {
		return nil, fmt.Errorf("%w: %s", ErrNoWorkers, t.plane)
	}
	if err := t.Err(); err != nil {
		return nil, err
	}
	cli, err := t.cl.client(ctx, t.home)
	if err != nil {
		return nil, err
	}
	prefix := IncarnationPrefix(t.plane, t.inst)
	bctx, release := t.bind(ctx)
	defer release()
	resp, err := cli.Get(bctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, t.cause(fmt.Errorf("state: load incarnations %s: %w", prefix, err))
	}
	c := &Incarnations{t: t, sem: make(chan struct{}, 1), cur: make(map[string]uint32, len(resp.Kvs))}
	for _, kv := range resp.Kvs {
		n, err := parseIncarnation(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: %s holds %q", ErrBadIncarnation, kv.Key, kv.Value)
		}
		c.cur[strings.TrimPrefix(string(kv.Key), prefix)] = n
	}
	return c, nil
}

// Next takes the next number for workerKey (W6.2-W6.4): one fenced txn, like Term.Txn, with compares
//
//	Value(IncarnationKey) == current n   (or CreateRevision == 0 when the cache has none)
//
// then Put(n+1), else Get(IncarnationKey). On success, cache n+1 and return it; the caller spawns with it.
// ErrSuperseded/ErrSessionLost from the Term pass through: the caller must not spawn.
// If only the incarnation compare failed, the cache was wrong: refresh it from the Else Get and retry, at most 3
// times, then return ErrIncarnationConflict.
//
// Numbers only increase and may skip: a number taken before a failed spawn is never reused (W6.4), and nothing
// here reuses a lower one.
func (c *Incarnations) Next(ctx context.Context, workerKey string) (uint32, error) {
	h := WorkerHash(workerKey)
	key := IncarnationKey(c.t.plane, c.t.inst, h)
	if err := c.t.lock(ctx, c.sem, "incarnations"); err != nil {
		return 0, err
	}
	defer func() { <-c.sem }()
	for range 1 + incarnationRetries {
		n, ok := c.get(h)
		held := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		if ok {
			held = clientv3.Compare(clientv3.Value(key), "=", formatIncarnation(n))
		}
		if n == math.MaxUint32 {
			return 0, fmt.Errorf("%w: %s", ErrIncarnationExhausted, workerKey)
		}
		resp, err := c.t.txn(ctx, c.scope(),
			[]clientv3.Cmp{held},
			[]clientv3.Op{clientv3.OpPut(key, formatIncarnation(n+1))},
			[]clientv3.Op{clientv3.OpGet(key)})
		if err != nil {
			return 0, err
		}
		if resp.Succeeded {
			c.set(h, n+1, true)
			return n + 1, nil
		}
		// The writer condition held, so only the cache was wrong: take what etcd has and try again.
		kvs := resp.Responses[0].GetResponseRange().GetKvs()
		if len(kvs) == 0 {
			c.set(h, 0, false)
			continue
		}
		v, err := parseIncarnation(kvs[0].Value)
		if err != nil {
			return 0, fmt.Errorf("%w: %s (%s) holds %q", ErrBadIncarnation, key, workerKey, kvs[0].Value)
		}
		c.set(h, v, true)
	}
	return 0, fmt.Errorf("%w: %s changed %d times", ErrIncarnationConflict, workerKey, 1+incarnationRetries)
}

// Current is the last number taken or loaded for workerKey; ok=false if none.
func (c *Incarnations) Current(workerKey string) (n uint32, ok bool) {
	return c.get(WorkerHash(workerKey))
}

// Forget deletes the key (W6.5). Only for STS, once the session is terminal and its last incarnation is confirmed gone;
// returns ErrNotSTS for other planes. Fenced like every write.
//
// MD and TD keys stay forever: their worker keys come back, and a counter restarting at 1 would make the new
// process's messages look older than the last one's (D8).
func (c *Incarnations) Forget(ctx context.Context, workerKey string) error {
	if c.t.plane != PlaneSTS {
		return fmt.Errorf("%w: %s agent, worker %s", ErrNotSTS, c.t.plane, workerKey)
	}
	h := WorkerHash(workerKey)
	if err := c.t.lock(ctx, c.sem, "incarnations"); err != nil {
		return err
	}
	defer func() { <-c.sem }()
	del := []clientv3.Op{clientv3.OpDelete(IncarnationKey(c.t.plane, c.t.inst, h))}
	if _, err := c.t.txn(ctx, c.scope(), nil, del, nil); err != nil {
		return err
	}
	c.set(h, 0, false)
	return nil
}

func (c *Incarnations) get(h string) (uint32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.cur[h]
	return n, ok
}

// set caches n for h, or forgets h when ok is false.
func (c *Incarnations) set(h string, n uint32, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok {
		c.cur[h] = n
	} else {
		delete(c.cur, h)
	}
}

// scope lets the incarnation keys, and only those, be written: Term.Put, Delete and Txn cannot reach them (W1).
func (c *Incarnations) scope() writeScope {
	return writeScope{plane: c.t.plane, inst: c.t.inst, incarnations: true}
}

func formatIncarnation(n uint32) string { return strconv.FormatUint(uint64(n), 10) }

// parseIncarnation accepts only what formatIncarnation writes. Next compares the stored bytes against
// formatIncarnation(n), so a value such as "07" or "+7" would fail that compare forever.
func parseIncarnation(v []byte) (uint32, error) {
	n, err := strconv.ParseUint(string(v), 10, 32)
	if err != nil {
		return 0, err
	}
	if formatIncarnation(uint32(n)) != string(v) {
		return 0, fmt.Errorf("state: %q is not in canonical decimal form", v)
	}
	return uint32(n), nil
}
