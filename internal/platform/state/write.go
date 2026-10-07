package state

import (
	"context"
	"fmt"
	"slices"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Put, Delete and Txn route by key to the owning region (RegionOfKey), acquire that region's Fence if needed, and
// make the write one txn conditioned on ModRevision(WriterKey) == R (PROTOCOL §3.2 W2 step 4). If the condition
// fails, another agent has taken over: the term ends with ErrSuperseded and the write is not retried (the agent
// exits and systemd restarts it). There is no other write path: Clusters hands out readers only.
//
// They also enforce W1 (PROTOCOL §3.2, the writer column of §3.1): a term puts and deletes only keys its agent is
// the writer of, so it can neither touch the keys fencing relies on nor another agent's or the API's keys.
// Reads (gets and compares) may name any key in the same cluster.

// Put writes key behind the writer condition. opts are put options, e.g. clientv3.WithLease.
func (t *Term) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) error {
	if err := t.Err(); err != nil {
		return err
	}
	op, err := buildOp(func() clientv3.Op { return clientv3.OpPut(key, val, opts...) })
	if err != nil {
		return err
	}
	_, err = t.Txn(ctx, nil, []clientv3.Op{op}, nil)
	return err
}

// Delete deletes key behind the writer condition. opts are delete options, e.g. clientv3.WithPrefix; a range
// must lie within one of the agent's writable prefixes.
func (t *Term) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error {
	if err := t.Err(); err != nil {
		return err
	}
	op, err := buildOp(func() clientv3.Op { return clientv3.OpDelete(key, opts...) })
	if err != nil {
		return err
	}
	_, err = t.Txn(ctx, nil, []clientv3.Op{op}, nil)
	return err
}

// Txn runs the caller's compares and ops behind the writer condition, as a nested txn:
//
//	If(writer rev == R) Then(Txn(If(cmps) Then(then) Else(els))) Else(Get WriterKey)
//
// It returns the inner response (Succeeded says whether the caller's compares held; Header is the outer txn's).
// All keys in cmps, then and els, nested txns included, must route to the same region, else ErrCrossCluster;
// every put and delete must be the agent's to write (W1), else ErrNotWriter. Both are checked before any I/O.
func (t *Term) Txn(ctx context.Context, cmps []clientv3.Cmp, then, els []clientv3.Op) (*clientv3.TxnResponse, error) {
	return t.txn(ctx, writeScope{plane: t.plane, inst: t.inst}, cmps, then, els)
}

// txn is Txn with the writable areas given by scope.
func (t *Term) txn(ctx context.Context, scope writeScope, cmps []clientv3.Cmp, then, els []clientv3.Op) (*clientv3.TxnResponse, error) {
	if err := t.Err(); err != nil {
		return nil, err
	}
	region, err := routeTxn(t.cl.topo, scope, cmps, then, els)
	if err != nil {
		return nil, err
	}
	rev, err := t.Fence(ctx, region)
	if err != nil {
		return nil, err
	}
	cli, err := t.cl.client(region)
	if err != nil {
		return nil, err
	}
	bctx, release := t.bind(ctx)
	defer release()
	writer := WriterKey(t.plane, t.inst)
	resp, err := cli.Txn(bctx).
		If(clientv3.Compare(clientv3.ModRevision(writer), "=", rev)).
		Then(clientv3.OpTxn(cmps, then, els)).
		Else(clientv3.OpGet(writer)).
		Commit()
	if err != nil {
		return nil, t.cause(fmt.Errorf("state: txn in %s: %w", region, err))
	}
	if !resp.Succeeded {
		held := "nothing"
		if kvs := resp.Responses[0].GetResponseRange().GetKvs(); len(kvs) > 0 {
			held = fmt.Sprintf("epoch %q at rev %d", kvs[0].Value, kvs[0].ModRevision)
		}
		t.end(fmt.Errorf("%w: writer key in %s holds %s, this term has epoch %d at rev %d",
			ErrSuperseded, region, held, t.epoch, rev))
		return nil, t.Err() // the first cause, if the term had ended meanwhile
	}
	inner := resp.Responses[0].GetResponseTxn()
	if inner == nil {
		return nil, fmt.Errorf("state: txn in %s: no inner response", region)
	}
	inner.Header = resp.Header // etcd leaves the nested header empty
	return (*clientv3.TxnResponse)(inner), nil
}

// buildOp turns clientv3's panics on options a put or delete cannot take into ErrBadOp.
func buildOp(f func() clientv3.Op) (op clientv3.Op, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrBadOp, r)
		}
	}()
	return f(), nil
}

// routeTxn is the one region every key of a txn routes to; every put and delete must also lie in scope.
func routeTxn(topo Topology, scope writeScope, cmps []clientv3.Cmp, then, els []clientv3.Op) (string, error) {
	r := router{topo: topo, scope: scope}
	if err := r.txn(cmps, then, els); err != nil {
		return "", err
	}
	if r.region == "" {
		return "", fmt.Errorf("%w: txn touches no key", ErrBadKey)
	}
	return r.region, nil
}

type router struct {
	topo   Topology
	scope  writeScope
	region string
	first  string // the key that fixed region, for the error message
}

func (r *router) txn(cmps []clientv3.Cmp, then, els []clientv3.Op) error {
	for _, c := range cmps {
		if err := r.key(c.KeyBytes(), c.GetCompare().GetRangeEnd()); err != nil {
			return err
		}
	}
	for _, ops := range [][]clientv3.Op{then, els} {
		for _, op := range ops {
			if op.IsTxn() {
				if err := r.txn(op.Txn()); err != nil {
					return err
				}
				continue
			}
			if err := r.key(op.KeyBytes(), op.RangeBytes()); err != nil {
				return err
			}
			if op.IsPut() || op.IsDelete() {
				if err := r.scope.check(op.KeyBytes(), op.RangeBytes()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// key routes one key, or the range [key, end) when end is set. A range must stay inside its instance's
// subtree: past it lie other instances, possibly in other clusters.
func (r *router) key(key, end []byte) error {
	k := string(key)
	p, inst, err := Owner(k)
	if err != nil {
		return err
	}
	region, err := r.topo.RegionOf(inst)
	if err != nil {
		return err
	}
	if len(end) > 0 {
		limit := clientv3.GetPrefixRangeEnd(instPrefix(p, inst))
		if string(end) == "\x00" || string(end) > limit {
			return fmt.Errorf("%w: range [%q, %q) leaves %s", ErrCrossCluster, k, end, instPrefix(p, inst))
		}
	}
	switch r.region {
	case "":
		r.region, r.first = region, k
	case region:
	default:
		return fmt.Errorf("%w: %q is in %s, %q in %s", ErrCrossCluster, r.first, r.region, k, region)
	}
	return nil
}

// writeScope is what a term of (plane, inst) may put and delete: the rows of PROTOCOL §3.1 whose writer is that
// agent or the controller (W1). The election, epoch and writer keys are not among them; Term writes those itself.
// Incarnation keys are written only through Incarnations, whose scope is incarnations alone.
type writeScope struct {
	plane        Plane
	inst         string
	incarnations bool
}

// areas are the writable prefixes and exact keys of the scope that fall under the instance (p, inst) a key
// belongs to.
func (w writeScope) areas(p Plane, inst string) (prefixes, exact []string) {
	switch {
	case w.incarnations:
		if p == w.plane && inst == w.inst {
			prefixes = []string{IncarnationPrefix(w.plane, w.inst)}
		}
	case w.plane == PlaneCtl:
		switch {
		case p == PlaneCtl && inst == w.inst:
			exact = []string{AgentKey(PlaneCtl, w.inst)}
		case p.agent():
			// Every agent instance's demands, in every region: the controller is their only writer (PROTOCOL §3.3).
			prefixes = []string{DemandPrefix(p, inst)}
		}
	case p == w.plane && inst == w.inst:
		exact = []string{AgentKey(w.plane, w.inst)}
		prefixes = []string{WorkerStatusPrefix(w.plane, w.inst)}
		switch w.plane {
		case PlaneSTS:
			prefixes = append(prefixes, SessionStatusPrefix(w.inst))
		case PlaneMD:
			prefixes = append(prefixes, MDAckPrefix(w.inst), SelectorPrefix(w.inst), PlacementPrefix(w.inst))
		case PlaneTD:
			prefixes = append(prefixes, TDAckPrefix(w.inst))
		}
	}
	return prefixes, exact
}

// check is nil if the put or delete of key, or of the range [key, end), lies within one writable area. Exact
// keys take no range.
func (w writeScope) check(key, end []byte) error {
	k := string(key)
	p, inst, err := Owner(k)
	if err != nil {
		return err
	}
	prefixes, exact := w.areas(p, inst)
	if len(end) == 0 && slices.Contains(exact, k) {
		return nil
	}
	for _, pre := range prefixes {
		if !strings.HasPrefix(k, pre) {
			continue
		}
		if len(end) == 0 || (string(end) != "\x00" && string(end) <= clientv3.GetPrefixRangeEnd(pre)) {
			return nil
		}
	}
	if len(end) > 0 {
		return fmt.Errorf("%w: %s/%s may not delete [%q, %q)", ErrNotWriter, w.plane, w.inst, k, end)
	}
	return fmt.Errorf("%w: %s/%s may not write %q", ErrNotWriter, w.plane, w.inst, k)
}
