package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// The write primitives of PROTOCOL §3.2 W10 (E10-D006, E10-D042). Every one is a single txn in one cluster, guarded by its
// own compares; none takes a lock or an election, so any number of replicas may run them at once. A failure is one of:
//
//   - errConflict: the compares did not hold. Nothing was written; the caller reads again (409 conflict).
//   - errRegionUnavailable: etcd could not be reached or did not answer in time (503 region_unavailable). The write
//     may or may not have been applied: a timeout after the request left says nothing about the commit.
//   - anything else: a bug or a misconfiguration (500 internal), such as a key outside the API's scope.
var (
	errConflict          = errors.New("api: conditions did not hold")
	errRegionUnavailable = errors.New("api: region unavailable")
	errNotFound          = errors.New("api: not found")
	// errIDsExhausted is an allocation whose every candidate was taken (E10-D007, E10-D008). At the expected sizes it does
	// not happen by chance; it means the id source or the keyspace is wrong.
	errIDsExhausted = errors.New("api: no free id")
)

// txner is the write path the primitives run on: state.APIWriter.
type txner interface {
	Txn(ctx context.Context, cmps []clientv3.Cmp, then, els []clientv3.Op) (*clientv3.TxnResponse, error)
}

// txn runs one txn and sorts a failure into errRegionUnavailable or the error as is. A txn whose compares failed is not
// an error here; the primitives decide what that means.
func txn(ctx context.Context, w txner, cmps []clientv3.Cmp, then, els []clientv3.Op) (*clientv3.TxnResponse, error) {
	// DialTimeout is how long a dial waits for etcd. A region whose connection is blackholed does not
	// answer, so the txn uses that same bound. The caller's deadline, when shorter, still ends the call.
	// Either deadline is region_unavailable (E10-D042).
	ctx, cancel := context.WithTimeout(ctx, state.DialTimeout)
	defer cancel()
	resp, err := w.Txn(ctx, cmps, then, els)
	if err != nil {
		return nil, classify(err)
	}
	return resp, nil
}

// classify wraps an error from etcd or from dialling it in errRegionUnavailable when it says the cluster was not
// reached or did not answer: a gRPC Unavailable, DeadlineExceeded or ResourceExhausted (no leader, request timed out,
// too many requests, no space), a context deadline, or Clusters already closed by shutdown. Other errors are returned
// as they are.
func classify(err error) error {
	if err == nil || !unavailable(err) {
		return err
	}
	return fmt.Errorf("%w: %w", errRegionUnavailable, err)
}

func unavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, state.ErrClustersClosed) {
		return true
	}
	var ee rpctypes.EtcdError
	if errors.As(err, &ee) {
		return transient(ee.Code())
	}
	if s, ok := status.FromError(err); ok {
		return transient(s.Code())
	}
	return false
}

func transient(c codes.Code) bool {
	return c == codes.Unavailable || c == codes.DeadlineExceeded || c == codes.ResourceExhausted
}

// create puts key = val if the key does not exist (create_revision == 0) and returns the revision it was created at.
// errConflict if it exists.
func create(ctx context.Context, w txner, key string, val []byte) (int64, error) {
	resp, err := txn(ctx, w,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(key, string(val))}, nil)
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, fmt.Errorf("%w: %s exists", errConflict, key)
	}
	return resp.Header.Revision, nil
}

// casPut puts key = val if its mod_revision is still rev, and returns the new mod_revision. errConflict if the key
// changed or is gone.
func casPut(ctx context.Context, w txner, key string, val []byte, rev int64) (int64, error) {
	if rev <= 0 {
		return 0, fmt.Errorf("%w: %s: revision %d", errConflict, key, rev)
	}
	resp, err := txn(ctx, w,
		[]clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(key), "=", rev)},
		[]clientv3.Op{clientv3.OpPut(key, string(val))}, nil)
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, fmt.Errorf("%w: %s is no longer at revision %d", errConflict, key, rev)
	}
	return resp.Header.Revision, nil
}

// cond is one key a delete is conditioned on: its mod_revision when it was read.
type cond struct {
	key string
	rev int64
}

// deleteIf deletes every key of conds in one txn if each is still at its revision, and returns the revision of the
// delete. errConflict if any changed or is gone; then none is deleted. W5 deletes a spec and its status this way: the
// caller has read the status as terminal, and terminal is absorbing, so a status still at that revision is still
// terminal (E10-D006).
func deleteIf(ctx context.Context, w txner, conds ...cond) (int64, error) {
	if len(conds) == 0 {
		return 0, errors.New("api: delete of nothing")
	}
	cmps := make([]clientv3.Cmp, 0, len(conds))
	ops := make([]clientv3.Op, 0, len(conds))
	for _, c := range conds {
		if c.rev <= 0 {
			return 0, fmt.Errorf("%w: %s: revision %d", errConflict, c.key, c.rev)
		}
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(c.key), "=", c.rev))
		ops = append(ops, clientv3.OpDelete(c.key))
	}
	resp, err := txn(ctx, w, cmps, ops, nil)
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, fmt.Errorf("%w: %s changed since it was read", errConflict, keysOf(conds))
	}
	return resp.Header.Revision, nil
}

func keysOf(conds []cond) []string {
	out := make([]string, len(conds))
	for i, c := range conds {
		out[i] = c.key
	}
	return out
}

// update is read, change, CAS, retried by the server (E10-D037: ending a session). It reads key from etcd, gives the
// value to f, and writes what f returns with a mod_revision CAS. When another write got there first, the failed txn
// reads the key back and f runs again on that, up to attempts times in all; past that it is errConflict.
//
// f gets nil when the key does not exist and decides what that is, typically errNotFound. f returning write false
// means no change: nothing is written and update returns the revision it read, so a repeated request is idempotent.
// An error from f is returned as is.
func update(ctx context.Context, w txner, key string, attempts int, f func(cur *mvccpb.KeyValue) (next []byte, write bool, err error)) (int64, error) {
	resp, err := txn(ctx, w, nil, []clientv3.Op{clientv3.OpGet(key)}, nil)
	if err != nil {
		return 0, err
	}
	cur := firstKV(resp, 0)
	for i := 1; ; i++ {
		next, write, err := f(cur)
		if err != nil {
			return 0, err
		}
		if cur == nil {
			// update changes what is there: a key that is gone is f's to report.
			return 0, fmt.Errorf("api: update of %s: the key does not exist and f did not say so", key)
		}
		if !write {
			return cur.ModRevision, nil
		}
		resp, err := txn(ctx, w,
			[]clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(key), "=", cur.ModRevision)},
			[]clientv3.Op{clientv3.OpPut(key, string(next))},
			[]clientv3.Op{clientv3.OpGet(key)})
		if err != nil {
			return 0, err
		}
		if resp.Succeeded {
			return resp.Header.Revision, nil
		}
		if i >= attempts {
			return 0, fmt.Errorf("%w: %s kept changing (%d attempts)", errConflict, key, attempts)
		}
		cur = firstKV(resp, 0)
	}
}

// firstKV is the single key a Get at Responses[i] returned, or nil if it found none.
func firstKV(resp *clientv3.TxnResponse, i int) *mvccpb.KeyValue {
	if i >= len(resp.Responses) {
		return nil
	}
	kvs := resp.Responses[i].GetResponseRange().GetKvs()
	if len(kvs) == 0 {
		return nil
	}
	return kvs[0]
}

// problemOf is the HTTP status and the ErrorCode of the contract (E10-D036) for an error of the write path.
func problemOf(err error) (int, string) {
	switch {
	case errors.Is(err, errConflict):
		return http.StatusConflict, "conflict"
	case errors.Is(err, errRegionUnavailable):
		return http.StatusServiceUnavailable, "region_unavailable"
	case errors.Is(err, errNotFound):
		return http.StatusNotFound, "not_found"
	default:
		return http.StatusInternalServerError, "internal"
	}
}
