package state

import (
	"context"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InstanceRetired reports whether inst's home region holds /retired/{inst} (E12-D036).
//
// A permission-denied read means this process's role was granted before that key existed.
// EnsureAuth only adds grants, so an upgraded agent on a cluster that has not had the new
// table applied cannot see the marker. Treating that as retired would stop every agent
// until the table is applied. The read is therefore "not retired", and campaign proceeds.
// Any other read error is returned: the agent must not register while it cannot tell.
func InstanceRetired(ctx context.Context, cl *Clusters, inst string) (bool, error) {
	if cl == nil {
		return false, fmt.Errorf("state: retired %s: nil clusters", inst)
	}
	region, err := cl.topo.RegionOf(inst)
	if err != nil {
		return false, err
	}
	cli, err := cl.client(ctx, region)
	if err != nil {
		return false, err
	}
	resp, err := cli.Get(ctx, RetiredKey(inst))
	if err != nil {
		if permissionDenied(err) {
			return false, nil
		}
		return false, fmt.Errorf("state: retired %s: %w", inst, err)
	}
	return len(resp.Kvs) > 0, nil
}

func permissionDenied(err error) bool {
	if errors.Is(err, rpctypes.ErrPermissionDenied) {
		return true
	}
	if s, ok := status.FromError(err); ok && s.Code() == codes.PermissionDenied {
		return true
	}
	return false
}
