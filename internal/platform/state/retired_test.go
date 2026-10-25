package state

import (
	"errors"
	"slices"
	"strings"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRetiredRouting(t *testing.T) {
	topo := twoRegions()
	key := RetiredKey("sts-tw-1")
	region, err := routeTxn(topo, apiScope{},
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(AgentKey(PlaneSTS, "sts-tw-1")), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(key, "sts-tw-1")}, nil)
	if err != nil || region != "tw" {
		t.Fatalf("region %s err %v", region, err)
	}
	if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{clientv3.OpDelete(key)}, nil); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("delete %v", err)
	}
	if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{clientv3.OpDelete(key, clientv3.WithPrefix())}, nil); !errors.Is(err, ErrBadKey) {
		t.Fatalf("range %v", err)
	}
	if _, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{clientv3.OpPut(RetiredKey("sts-nope"), "sts-nope")}, nil); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("unknown %v", err)
	}
	got, err := routeTxn(topo, apiScope{}, nil, []clientv3.Op{clientv3.OpPut(RetiredKey("td-jp-1"), "td-jp-1")}, nil)
	if err != nil || got != "jp" {
		t.Fatalf("jp region %s err %v", got, err)
	}
	if _, ok := SessionOf(key); ok {
		t.Fatal("a retirement key named a session")
	}
	if home, err := topo.RegionOfKey(key); err != nil || home != "tw" {
		t.Fatalf("RegionOfKey %s %v", home, err)
	}
	if _, ok := RetiredInstance(RetiredPrefix); ok {
		t.Fatal("the prefix is an instance")
	}
	if _, ok := RetiredInstance(key + "/extra"); ok {
		t.Fatal("a longer key is an instance")
	}

	prefixes, exact := RetireCleanTargets(PlaneSTS, "sts-tw-1")
	if !slices.Contains(prefixes, SessionStatusPrefix("sts-tw-1")) || !slices.Contains(exact, EpochKey(PlaneSTS, "sts-tw-1")) {
		t.Fatalf("sts targets %v %v", prefixes, exact)
	}
	for _, key := range append(append([]string{}, prefixes...), exact...) {
		if strings.Contains(key, "/spec/") || strings.Contains(key, "/demand/") || strings.Contains(key, "/account/") || strings.Contains(key, "/retired/") {
			t.Fatalf("sts clean includes %s", key)
		}
	}
	mdPre, _ := RetireCleanTargets(PlaneMD, "md-tw-1")
	if !slices.Contains(mdPre, MDAckPrefix("md-tw-1")) || slices.Contains(mdPre, DemandPrefix(PlaneMD, "md-tw-1")) {
		t.Fatalf("md targets %v", mdPre)
	}
	tdPre, _ := RetireCleanTargets(PlaneTD, "td-tw-1")
	if !slices.Contains(tdPre, TDAckPrefix("td-tw-1")) || slices.Contains(tdPre, AccountPrefix("td-tw-1")) {
		t.Fatalf("td targets %v", tdPre)
	}
	if pre, exact := RetireCleanTargets(PlaneCtl, ControllerInstance); pre != nil || exact != nil {
		t.Fatalf("controller %v %v", pre, exact)
	}
}
