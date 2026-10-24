//go:build linux

package artifact

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

func TestRunningDigests(t *testing.T) {
	data := t.TempDir()
	resolved, err := filepath.EvalSymlinks(data)
	if err != nil {
		t.Fatal(err)
	}
	dig := digest.FromString("running").String()
	bin := filepath.Join(resolved, "artifacts", dig, "rootfs", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	worker := filepath.Join(bin, "worker")
	copyFile(t, "/bin/sleep", worker)
	cmd := exec.Command(worker, "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	waitRunning(t, resolved, dig, true)
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	_ = cmd.Wait()
	waitRunning(t, resolved, dig, false)
}

func waitRunning(t *testing.T, data, dig string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := RunningDigests(data)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(got, dig) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("running %v includes %s: %v", got, dig, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
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
}
