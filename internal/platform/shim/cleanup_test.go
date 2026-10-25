package shim

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanAbsentLogs(t *testing.T) {
	dir := t.TempDir()
	retain := 7 * 24 * time.Hour
	kept := "0123456789abcdef"
	gone := "fedcba9876543210"

	oldGone := []string{
		gone + ".stdout",
		gone + ".stderr",
		gone + ".shim.log",
		gone + ".shim.log.1",
		gone + ".stdout.2",
	}
	for _, name := range oldGone {
		plantLog(t, dir, name, time.Unix(1_600_000_000, 0))
	}
	plantLog(t, dir, gone+".stdout.3", time.Unix(1_800_000_000, 0)) // recent: stays
	plantLog(t, dir, kept+".stdout", time.Unix(1_600_000_000, 0))   // hash still appears
	plantLog(t, dir, "notes.txt", time.Unix(1_600_000_000, 0))
	plantLog(t, dir, gone+".stdout.01", time.Unix(1_600_000_000, 0)) // leading zero is not a rotation
	if err := os.Mkdir(filepath.Join(dir, gone+".stderr.1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "notes.txt"), filepath.Join(dir, gone+".shim.log.2")); err != nil {
		t.Fatal(err)
	}

	exact := filepath.Join(dir, gone+".stderr.9")
	plantLog(t, dir, gone+".stderr.9", time.Unix(1_700_000_000, 0))
	exactInfo, err := os.Lstat(exact)
	if err != nil {
		t.Fatal(err)
	}
	now := exactInfo.ModTime().Add(retain)
	older := filepath.Join(dir, gone+".stderr.8")
	plantLog(t, dir, gone+".stderr.8", exactInfo.ModTime().Add(-time.Second))

	if err := CleanAbsentLogs(dir, retain, now, map[string]struct{}{kept: {}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range oldGone {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := os.Lstat(older); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("older than the cutoff: %v", err)
	}
	for _, name := range []string{
		gone + ".stdout.3",
		kept + ".stdout",
		"notes.txt",
		gone + ".stdout.01",
		gone + ".stderr.9",
		gone + ".shim.log.2",
	} {
		if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(dir, gone+".stderr.1")); err != nil {
		t.Fatalf("directory: %v", err)
	}
	if err := CleanAbsentLogs(filepath.Join(dir, "missing"), retain, now, nil); err != nil {
		t.Fatal(err)
	}
	if err := CleanAbsentLogs(dir, 0, now, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, kept+".stdout")); err != nil {
		t.Fatal(err)
	}
}

func TestSocketHashes(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"0123456789abcdef.sock", "fedcba9876543210.sock", "notes.sock", "abcd.sock"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := SocketHashes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("hashes %v", got)
	}
	if _, ok := got["0123456789abcdef"]; !ok {
		t.Fatalf("missing live hash: %v", got)
	}
	if _, ok := got["fedcba9876543210"]; !ok {
		t.Fatalf("missing live hash: %v", got)
	}
	empty, err := SocketHashes(filepath.Join(dir, "missing"))
	if err != nil || len(empty) != 0 {
		t.Fatalf("missing dir: %v %v", empty, err)
	}
	file := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := SocketHashes(file); err == nil {
		t.Fatal("read of a file succeeded")
	}
}

func plantLog(t *testing.T, dir, name string, mtime time.Time) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}
