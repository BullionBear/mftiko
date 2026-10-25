package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func TestApply(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	raw := s.Client(t)
	dep := writeDeployment(t, s.Endpoints())
	secrets := t.TempDir()
	writeFile(t, filepath.Join(secrets, "root"), etcdtest.RootPassword()+"\n")
	writeFile(t, filepath.Join(secrets, "sts-tw-1"), "given-by-hand\n")

	var out bytes.Buffer
	if err := runApply(ctx, dep, "", secrets, nil, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# region tw: add user md-tw-1, password generated into " + filepath.Join(secrets, "md-tw-1") + "\n",
		"# region tw: add user sts-tw-1, password from " + filepath.Join(secrets, "sts-tw-1") + "\n",
		"# region tw: add user api, password generated into ",
		"# region tw: add user controller, password generated into ",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "not in the table") {
		t.Errorf("extras on a fresh cluster:\n%s", out.String())
	}
	info, err := os.Stat(filepath.Join(secrets, "md-tw-1"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("generated secret mode %v", info.Mode().Perm())
	}

	// Each user signs in with the password in its file and holds only its own role.
	md := clientAs(t, s.Endpoints(), "md-tw-1", secret(t, secrets, "md-tw-1"))
	if _, err := md.Put(ctx, state.EpochKey(state.PlaneMD, "md-tw-1"), "1"); err != nil {
		t.Fatalf("md writes its epoch: %v", err)
	}
	if _, err := md.Put(ctx, state.EpochKey(state.PlaneSTS, "sts-tw-1"), "1"); !errors.Is(err, rpctypes.ErrPermissionDenied) {
		t.Fatalf("md writes sts's epoch: %v", err)
	}
	clientAs(t, s.Endpoints(), "sts-tw-1", "given-by-hand")

	// Twice changes nothing: no user is added, no password changes, the auth revision stays.
	before, err := raw.AuthStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mdPass := secret(t, secrets, "md-tw-1")
	out.Reset()
	if err := runApply(ctx, dep, "tw", secrets, nil, &out); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Errorf("second apply printed:\n%s", out.String())
	}
	after, err := raw.AuthStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.AuthRevision != before.AuthRevision {
		t.Errorf("auth revision %d -> %d", before.AuthRevision, after.AuthRevision)
	}
	if got := secret(t, secrets, "md-tw-1"); got != mdPass {
		t.Error("second apply rewrote a secret")
	}

	// What the table lacks is listed and left alone.
	if _, err := raw.RoleAdd(ctx, "stray"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.UserAdd(ctx, "stray", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.RoleGrantPermission(ctx, "md-tw-1", "/other/", "/other0", clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runApply(ctx, dep, "", secrets, nil, &out); err != nil {
		t.Fatal(err)
	}
	want := "# region tw: role md-tw-1 has readwrite /other/ /other0, not in the table\n" +
		"# region tw: role stray is not in the table\n" +
		"# region tw: user stray is not in the table\n"
	if out.String() != want {
		t.Errorf("extras:\n%s\nwant\n%s", out.String(), want)
	}
	if _, err := raw.RoleGet(ctx, "stray"); err != nil {
		t.Errorf("apply removed a role: %v", err)
	}
}

func TestApplyRejects(t *testing.T) {
	ctx := t.Context()
	dep := writeDeployment(t, []string{"http://127.0.0.1:1"})
	empty := t.TempDir()
	if err := runApply(ctx, dep, "", "", nil, &bytes.Buffer{}); err == nil {
		t.Error("no -secrets accepted")
	}
	if err := runApply(ctx, dep, "", empty, nil, &bytes.Buffer{}); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("no root secret: %v", err)
	}
	writeFile(t, filepath.Join(empty, "root"), "x")
	if err := runApply(ctx, dep, "jp", empty, nil, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), `region "jp"`) {
		t.Errorf("unknown region: %v", err)
	}
}

func TestApplyWrongRootPassword(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	secrets := t.TempDir()
	writeFile(t, filepath.Join(secrets, "root"), "wrong")
	err := runApply(ctx, writeDeployment(t, s.Endpoints()), "", secrets, nil, &bytes.Buffer{})
	if !errors.Is(err, rpctypes.ErrAuthFailed) {
		t.Fatalf("wrong root password: %v", err)
	}
	if entries, _ := os.ReadDir(secrets); len(entries) != 1 {
		t.Fatalf("secrets written before root signed in: %v", entries)
	}
}

func writeDeployment(t *testing.T, endpoints []string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"regions":   map[string]any{"tw": map[string]any{"endpoints": endpoints}},
		"instances": map[string]string{"controller": "tw", "md-tw-1": "tw", "sts-tw-1": "tw"},
		"planes":    map[string]string{"md-tw-1": "md", "sts-tw-1": "sts"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "deploy.json")
	writeFile(t, p, string(b))
	return p
}

func writeFile(t *testing.T, path, s string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
}

func secret(t *testing.T, dir, user string) string {
	t.Helper()
	p, err := readSecret(dir, user)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func clientAs(t *testing.T, endpoints []string, user, password string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second, Username: user, Password: password})
	if err != nil {
		t.Fatalf("%s signs in: %v", user, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
