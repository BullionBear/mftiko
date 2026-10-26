package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func writeDeployment(t *testing.T, endpoints []string) string {
	t.Helper()
	d := map[string]any{
		"regions":   map[string]any{"tw": map[string]any{"endpoints": endpoints}, "jp": map[string]any{"endpoints": []string{"http://127.0.0.1:1"}}},
		"instances": map[string]string{"controller": "tw", "md-tw-1": "tw", "sts-tw-1": "tw", "sts-jp-1": "jp"},
		"planes":    map[string]string{"md-tw-1": "md", "sts-tw-1": "sts", "sts-jp-1": "sts"},
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "deploy.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func rootSecrets(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "root"), []byte(etcdtest.RootPassword()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func snapshots(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// Saves, prunes to -keep, and the saved file restores: after which -raise-epochs puts every epoch of the region
// above any it reached after the snapshot.
func TestSaveRestoreRaise(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	raw := s.Client(t)
	dep, secrets := writeDeployment(t, s.Endpoints()), rootSecrets(t)
	dir := t.TempDir()
	md, sts, ctl := state.EpochKey(state.PlaneMD, "md-tw-1"), state.EpochKey(state.PlaneSTS, "sts-tw-1"), state.EpochKey(state.PlaneCtl, "controller")
	put := func(k, v string) {
		t.Helper()
		if _, err := raw.Put(ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	put(md, fmt.Sprintf("%020d", 5))
	put(ctl, fmt.Sprintf("%020d", 3))
	put("/mftiko/v1/md/md-tw-1/demand/s1/feed/x", "before")

	t0 := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	for i := range 3 {
		at := t0.Add(time.Duration(i) * time.Hour)
		var out bytes.Buffer
		if err := runSave(ctx, dep, secrets, "tw", dir, 2, func() time.Time { return at }, nil, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "region tw: saved "+filepath.Join(dir, "tw", snapshotName(at))) {
			t.Fatalf("save %d printed:\n%s", i, out.String())
		}
		savedRevs[filepath.Join(dir, "tw", snapshotName(at))] = revision(t, raw)
	}
	if err := os.WriteFile(filepath.Join(dir, "tw", "notes.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prune(filepath.Join(dir, "tw"), 2, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"20261026T020000Z.db", "20261026T030000Z.db", "notes.txt"}
	if got := snapshots(t, filepath.Join(dir, "tw")); !slices.Equal(got, want) {
		t.Fatalf("snapshots %v, want %v", got, want)
	}

	// After the newest snapshot: md is elected four more times, sts for the first time.
	put(md, fmt.Sprintf("%020d", 9))
	put(sts, fmt.Sprintf("%020d", 1))
	put("/mftiko/v1/md/md-tw-1/demand/s1/feed/x", "after")

	snap := filepath.Join(dir, "tw", "20261026T030000Z.db")
	bump := restoreBump(t, raw, savedRevs[snap])
	s.Restore(t, snap, bump)
	if got := string(mustGet(t, raw, "/mftiko/v1/md/md-tw-1/demand/s1/feed/x")); got != "before" {
		t.Fatalf("restored demand %q", got)
	}
	var out bytes.Buffer
	if err := runRaise(ctx, dep, secrets, "tw", bump, nil, &out); err != nil {
		t.Fatal(err)
	}
	wantOut := fmt.Sprintf("%s: 3 -> %d\n%s: 5 -> %d\n%s: 0 -> %d\n", ctl, 3+bump, md, 5+bump, sts, bump)
	if out.String() != wantOut {
		t.Fatalf("raise printed:\n%s\nwant:\n%s", out.String(), wantOut)
	}
	for k, e := range map[string]int64{md: 5 + bump, sts: bump, ctl: 3 + bump} {
		if got := string(mustGet(t, raw, k)); got != fmt.Sprintf("%020d", e) {
			t.Errorf("%s = %q, want %d", k, got, e)
		}
	}
}

// revision is the cluster's revision now.
func revision(t *testing.T, raw *clientv3.Client) int64 {
	t.Helper()
	resp, err := raw.Get(t.Context(), "/mftiko/v1/", clientv3.WithLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Header.Revision
}

// savedRevs is the cluster's revision when each snapshot of a test was saved: no write runs during a save, so it is
// the snapshot's revision (what etcdutl snapshot status prints).
var savedRevs = map[string]int64{}

// restoreBump is the runbook's bump (E12-D055): R_old − R_s + RestoreMargin.
func restoreBump(t *testing.T, raw *clientv3.Client, rs int64) int64 {
	t.Helper()
	return revision(t, raw) - rs + state.RestoreMargin
}

func mustGet(t *testing.T, raw *clientv3.Client, key string) []byte {
	t.Helper()
	resp, err := raw.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatalf("%s is missing", key)
	}
	return resp.Kvs[0].Value
}

// The same snapshot restored twice, as on testnet (E12-D058): the second restore starts from a cluster already
// bumped by the first. A bump computed from R_old keeps the revision going up and the epochs above every epoch used
// in between; the fixed margin alone would put both back below.
func TestRepeatedRestore(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	raw := s.Client(t)
	dep, secrets := writeDeployment(t, s.Endpoints()), rootSecrets(t)
	dir := t.TempDir()
	ctl := state.EpochKey(state.PlaneCtl, "controller")
	if _, err := raw.Put(ctx, ctl, fmt.Sprintf("%020d", 3)); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 26, 0, 5, 14, 0, time.UTC)
	if err := runSave(ctx, dep, secrets, "tw", dir, 1, func() time.Time { return at }, nil, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(dir, "tw", snapshotName(at))
	rs := revision(t, raw)

	highest := int64(5) // elections after the snapshot
	if _, err := raw.Put(ctx, ctl, fmt.Sprintf("%020d", highest)); err != nil {
		t.Fatal(err)
	}
	for round := range 2 {
		rOld := revision(t, raw)
		bump := restoreBump(t, raw, rs)
		s.Restore(t, snap, bump)
		if got := revision(t, raw); got <= rOld {
			t.Fatalf("round %d: revision %d after the restore, %d before", round, got, rOld)
		}
		if err := runRaise(ctx, dep, secrets, "tw", bump, nil, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		e, err := strconv.ParseInt(string(mustGet(t, raw, ctl)), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if e <= highest {
			t.Fatalf("round %d: epoch raised to %d, but %d was used before", round, e, highest)
		}
		highest = e + 1 // one more election after the restore
		if _, err := raw.Put(ctx, ctl, fmt.Sprintf("%020d", highest)); err != nil {
			t.Fatal(err)
		}
	}
	if 3+state.RestoreMargin+1 > highest-1 { // the second round's epoch had to clear the first round's
		t.Fatalf("the snapshot's epoch plus the margin alone, %d, clears %d: the test shows nothing", 3+state.RestoreMargin+1, highest-1)
	}
}

func TestArgs(t *testing.T) {
	ctx := t.Context()
	dep, secrets := writeDeployment(t, []string{"http://127.0.0.1:1"}), rootSecrets(t)
	now := time.Now
	for name, err := range map[string]error{
		"no dir":       runSave(ctx, dep, secrets, "", "", 1, now, nil, &bytes.Buffer{}),
		"keep 0":       runSave(ctx, dep, secrets, "", t.TempDir(), 0, now, nil, &bytes.Buffer{}),
		"no secrets":   runSave(ctx, dep, "", "", t.TempDir(), 1, now, nil, &bytes.Buffer{}),
		"bad region":   runSave(ctx, dep, secrets, "eu", t.TempDir(), 1, now, nil, &bytes.Buffer{}),
		"raise all":    runRaise(ctx, dep, secrets, "", 1, nil, &bytes.Buffer{}),
		"raise no by":  runRaise(ctx, dep, secrets, "jp", 0, nil, &bytes.Buffer{}),
		"raise region": runRaise(ctx, dep, secrets, "eu", 1, nil, &bytes.Buffer{}),
	} {
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	d := input{Topology: state.Topology{Instances: map[string]string{"x-tw-1": "tw"}}}
	if _, err := epochKeys(d, "tw"); err == nil {
		t.Error("instance without a plane: no error")
	}
}

func TestSnapshotName(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := snapshotName(at); got != "20260102T030405Z.db" {
		t.Fatalf("name %q", got)
	}
	for name, want := range map[string]bool{
		"20260102T030405Z.db": true, "20260102T030405Z.db.part": false, "2026-01-02.db": false, "x.db": false,
	} {
		if got := isSnapshotName(name); got != want {
			t.Errorf("isSnapshotName(%q) = %v", name, got)
		}
	}
}
