package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func TestConnectRefusesRetiredName(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	const inst = "sts-tw-1"
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{state.ControllerInstance: "tw", inst: "tw"},
	}
	planes := map[string]state.Plane{inst: state.PlaneSTS}
	tables, err := state.AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Put(ctx, state.RetiredKey(inst), inst); err != nil {
		t.Fatal(err)
	}
	cl, term, err := Connect(ctx, topo, state.PlaneSTS, inst, state.AuthDial(inst, etcdtest.Password(inst)), state.TermOptions{ID: "retire-a"})
	if !errors.Is(err, state.ErrRetired) || cl != nil || term != nil {
		t.Fatalf("connect cl=%v term=%v err=%v", cl != nil, term != nil, err)
	}
	for _, key := range []string{state.EpochKey(state.PlaneSTS, inst), state.AgentKey(state.PlaneSTS, inst)} {
		resp, err := root.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) != 0 {
			t.Fatalf("%s was written", key)
		}
	}
}

func TestConnectTreatsDeniedRetirementReadAsAbsent(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	const inst = "sts-tw-1"
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{state.ControllerInstance: "tw", inst: "tw"},
	}
	planes := map[string]state.Plane{inst: state.PlaneSTS}
	tables, err := state.AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	if _, err := root.RoleRevokePermission(ctx, inst, state.RetiredKey(inst), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Put(ctx, state.RetiredKey(inst), inst); err != nil {
		t.Fatal(err)
	}
	cl, term, err := Connect(ctx, topo, state.PlaneSTS, inst, state.AuthDial(inst, etcdtest.Password(inst)), state.TermOptions{ID: "retire-b"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = term.Close(context.Background())
		_ = cl.Close()
	})
	resp, err := root.Get(ctx, state.EpochKey(state.PlaneSTS, inst))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("permission denied on the retirement key stopped the campaign")
	}
}
