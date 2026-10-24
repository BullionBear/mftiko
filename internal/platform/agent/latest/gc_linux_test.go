//go:build linux

package latest

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/artifact/artifacttest"
)

func TestSourceGCKeepsVerifiedRunningAndInflight(t *testing.T) {
	reg := artifacttest.New(t)
	s := newSource(t, reg)
	ctx := context.Background()
	old := push(t, reg, artifact.KindMDConn, "old")
	if err := s.step(ctx); err != nil {
		t.Fatal(err)
	}
	cur := push(t, reg, artifact.KindMDConn, "cur")
	if err := s.step(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := filepath.EvalSymlinks(s.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, data)
	inflight := plantDigest(t, data, "inflight")
	extra := plantDigest(t, data, "extra")
	s.mu.Lock()
	s.inflight = inflight
	s.mu.Unlock()

	reg.SetDown(true)
	if err := s.GC(); err != nil {
		t.Fatal(err)
	}
	if s.Digest() != cur {
		t.Fatalf("digest %s, want %s", s.Digest(), cur)
	}
	assertRoot(t, data, cur, true)
	assertRoot(t, data, running, true)
	assertRoot(t, data, inflight, true)
	assertRoot(t, data, old, false)
	assertRoot(t, data, extra, false)
}

func TestSourceGCDropsFailedDigest(t *testing.T) {
	reg := artifacttest.New(t)
	s := newSource(t, reg)
	ctx := context.Background()
	good := push(t, reg, artifact.KindMDConn, "good")
	if err := s.step(ctx); err != nil {
		t.Fatal(err)
	}
	bad := push(t, reg, artifact.KindTDAccount, "bad")
	if err := s.step(ctx); err == nil {
		t.Fatal("wrong kind unpacked")
	}
	extra := plantDigest(t, s.cfg.DataDir, "extra")
	if _, err := os.Stat(filepath.Join(s.cfg.DataDir, "artifacts", bad)); !os.IsNotExist(err) {
		t.Fatalf("failed digest on disk: %v", err)
	}
	reg.SetDown(true)
	if err := s.GC(); err != nil {
		t.Fatal(err)
	}
	assertRoot(t, s.cfg.DataDir, good, true)
	assertRoot(t, s.cfg.DataDir, extra, false)
}

func plantDigest(t *testing.T, data, label string) string {
	t.Helper()
	dig := digest.FromString(label).String()
	if err := os.MkdirAll(filepath.Join(data, "artifacts", dig, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dig
}

func assertRoot(t *testing.T, data, dig string, want bool) {
	t.Helper()
	_, err := os.Stat(filepath.Join(data, "artifacts", dig, "rootfs"))
	got := err == nil
	if got != want {
		t.Fatalf("rootfs %s present %v, want %v (%v)", dig, got, want, err)
	}
}

func startWorker(t *testing.T, data string) string {
	t.Helper()
	dig := digest.FromString("running-worker").String()
	bin := filepath.Join(data, "artifacts", dig, "rootfs", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(bin, "worker")
	in, err := os.Open("/bin/sleep")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(dst, "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := artifact.RunningDigests(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range got {
			if d == dig {
				return dig
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("process not visible: %v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
