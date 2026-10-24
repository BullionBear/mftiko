package etcdtest

import (
	"errors"
	"fmt"
	"testing"
)

// StartRegions starts one throwaway etcd per region name, in the order given,
// and returns the servers keyed by name. Each server's Endpoints are that
// region's client URLs in a deployment topology; the caller fills which
// instance lives where.
//
// This package does not return state.Topology. The state tests import etcdtest,
// so etcdtest importing state would cycle (E13-D016).
//
// Names are non-empty and unique. A missing etcd binary skips t, as Start does.
func StartRegions(t testing.TB, regions ...string) map[string]*Server {
	t.Helper()
	if err := validRegions(regions); err != nil {
		t.Fatalf("%v", err)
		return nil
	}
	out := make(map[string]*Server, len(regions))
	for _, region := range regions {
		out[region] = Start(t)
	}
	return out
}

func validRegions(regions []string) error {
	if len(regions) == 0 {
		return errors.New("etcdtest: no regions")
	}
	seen := map[string]bool{}
	for _, region := range regions {
		if region == "" {
			return errors.New("etcdtest: empty region name")
		}
		if seen[region] {
			return fmt.Errorf("etcdtest: duplicate region %q", region)
		}
		seen[region] = true
	}
	return nil
}
