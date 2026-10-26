package state

import (
	"context"
	"fmt"
	"math"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// RestoreMargin is the margin of a restore's bump (E12-D055): bump = R_old − R_s + RestoreMargin, where R_s is the
// snapshot's revision and R_old the cluster's revision before the restore, or the newest snapshot's when the cluster
// cannot be read. The same bump goes to etcdutl snapshot restore --bump-revision and to RaiseEpoch (E12-D056). The
// margin covers writes R_old does not count: those after the newest snapshot, or within a probe interval.
const RestoreMargin = 1_000_000_000

// RaiseEpoch adds by to the epoch at key, as root after a restore and before the instance's agent starts again
// (E12-D056). The snapshot holds an older epoch than the shims have accepted, and an agent whose next epoch is below
// theirs is refused with stale_epoch. by is the restore's revision bump: every epoch increment is a write, so an epoch
// grows no faster than the revision, and the raised epoch is above any used since the snapshot, earlier restores
// included. A missing key is raised from 0: the instance may first have been elected after the snapshot. It returns
// the epoch before and after.
func RaiseEpoch(ctx context.Context, c *clientv3.Client, key string, by int64) (from, to int64, err error) {
	if by < 1 {
		return 0, 0, fmt.Errorf("%w: raise by %d", ErrBadOptions, by)
	}
	for range epochAttempts {
		get, err := c.Get(ctx, key)
		if err != nil {
			return 0, 0, fmt.Errorf("state: read epoch %s: %w", key, err)
		}
		from = 0
		unchanged := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		if len(get.Kvs) > 0 {
			if from, err = parseEpoch(get.Kvs[0].Value); err != nil {
				return 0, 0, fmt.Errorf("%w: %s holds %q", ErrBadEpoch, key, get.Kvs[0].Value)
			}
			unchanged = clientv3.Compare(clientv3.ModRevision(key), "=", get.Kvs[0].ModRevision)
		}
		if from > math.MaxInt64-by {
			return 0, 0, fmt.Errorf("%w: %s at %d cannot be raised by %d", ErrBadEpoch, key, from, by)
		}
		resp, err := c.Txn(ctx).If(unchanged).Then(clientv3.OpPut(key, formatEpoch(from+by))).Commit()
		if err != nil {
			return 0, 0, fmt.Errorf("state: raise epoch %s: %w", key, err)
		}
		if resp.Succeeded {
			return from, from + by, nil
		}
		// An agent was elected meanwhile: it should not be running yet, but read again rather than overwrite it.
	}
	return 0, 0, fmt.Errorf("%w: %s changed %d times while being raised", ErrSuperseded, key, epochAttempts)
}
