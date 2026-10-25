package shim

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestShimLogRotatesStderr(t *testing.T) {
	restoreStdio(t)
	dir := t.TempDir()
	path := filepath.Join(LogDir(dir, "md"), workerHash("md/a")+".shim.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o640); err != nil {
		t.Fatal(err)
	}
	sl, err := openShimLogLimits(dir, "md", "md/a", 4, ShimLogKeep)
	if err != nil || sl == nil {
		t.Fatal(err)
	}
	defer sl.Close()
	for _, chunk := range []string{"aaaa", "bbbb", "cccc", "dddd"} {
		if _, err := sl.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stderr.Write([]byte("Q")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf(".3: %v", err)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(current, []byte("dddd")) || !bytes.Contains(current, []byte("Q")) {
		t.Fatalf("current %q", current)
	}
	older, err := os.ReadFile(path + ".2")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(older, []byte("Q")) {
		t.Fatalf("stderr followed the rotated file: %q", older)
	}
}

func restoreStdio(t *testing.T) {
	t.Helper()
	out, err := unix.Dup(1)
	if err != nil {
		t.Fatal(err)
	}
	errFd, err := unix.Dup(2)
	if err != nil {
		unix.Close(out)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = unix.Dup3(out, 1, 0)
		_ = unix.Dup3(errFd, 2, 0)
		unix.Close(out)
		unix.Close(errFd)
	})
}
