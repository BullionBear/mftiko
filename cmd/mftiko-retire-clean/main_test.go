package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func TestCleanRefusesThenDeletesOwnKeys(t *testing.T) {
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
	raw := s.Client(t)
	if err := state.EnsureAuth(ctx, raw, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	dep := writeDeploy(t, topo, planes)
	pass := etcdtest.Password(inst)
	worker := state.WorkerStatusKey(state.PlaneSTS, inst, state.WorkerHash("sts/abc123"))
	status := state.SessionStatusKey(inst, "abc123")
	epoch := state.EpochKey(state.PlaneSTS, inst)
	spec := state.SessionSpecKey(inst, "abc123")
	demand := state.SessionDemandKey(inst, "abc123")
	retired := state.RetiredKey(inst)
	agent := state.AgentKey(state.PlaneSTS, inst)

	if _, err := raw.Put(ctx, worker, "w"); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, dep, inst, pass, nil, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "not retired") {
		t.Fatalf("missing marker: %v", err)
	}
	if !exists(t, ctx, raw, worker) {
		t.Fatal("refused clean deleted a worker")
	}

	for _, key := range []string{retired, agent, status, epoch, spec, demand} {
		if _, err := raw.Put(ctx, key, inst); err != nil {
			t.Fatal(err)
		}
	}
	if err := run(ctx, dep, inst, pass, nil, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "agent record") {
		t.Fatalf("agent present: %v", err)
	}
	if !exists(t, ctx, raw, worker) || !exists(t, ctx, raw, status) {
		t.Fatal("refused clean deleted a key")
	}

	if _, err := raw.Delete(ctx, agent); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(ctx, dep, inst, pass, nil, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "cleaned "+inst+"\nnext: mftiko-etcd-roles -drop "+inst+"\n" {
		t.Fatalf("stdout %q", out.String())
	}
	for _, key := range []string{worker, status, epoch} {
		if exists(t, ctx, raw, key) {
			t.Fatalf("still present %s", key)
		}
	}
	for _, key := range []string{spec, demand, retired} {
		if !exists(t, ctx, raw, key) {
			t.Fatalf("deleted %s", key)
		}
	}
}

func TestCleanRejects(t *testing.T) {
	ctx := t.Context()
	if err := run(ctx, "-", "", "pw", strings.NewReader(`{}`), &bytes.Buffer{}); err == nil {
		t.Fatal("empty instance accepted")
	}
	if err := run(ctx, "-", "sts-tw-1", "", strings.NewReader(`{}`), &bytes.Buffer{}); err == nil {
		t.Fatal("empty password accepted")
	}
}

func writeDeploy(t *testing.T, topo state.Topology, planes map[string]state.Plane) string {
	t.Helper()
	b, err := json.Marshal(struct {
		state.Topology
		Planes map[string]state.Plane `json:"planes"`
	}{Topology: topo, Planes: planes})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "deploy.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func exists(t *testing.T, ctx context.Context, raw *clientv3.Client, key string) bool {
	t.Helper()
	resp, err := raw.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs) != 0
}
