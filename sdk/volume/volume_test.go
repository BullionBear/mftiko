package volume

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("MFTIKO_VOLUME_CRASH") == "1" {
		beforeRename = func() { os.Exit(9) }
		_ = Write(os.Getenv("MFTIKO_VOLUME_DIR"), os.Getenv("MFTIKO_VOLUME_NAME"), []byte(os.Getenv("MFTIKO_VOLUME_DATA")))
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestWriteReadAndOrder(t *testing.T) {
	vol := t.TempDir()
	in := t.TempDir()
	name := "state.bin"

	data, ok, err := Read(vol, in, name)
	if err != nil || ok || data != nil {
		t.Fatalf("missing: %q %v %v", data, ok, err)
	}
	if err := os.WriteFile(filepath.Join(in, name), []byte("from-input"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, ok, err = Read(vol, in, name)
	if err != nil || !ok || string(data) != "from-input" {
		t.Fatalf("input: %q %v %v", data, ok, err)
	}
	if err := Write(vol, name, []byte("from-volume")); err != nil {
		t.Fatal(err)
	}
	data, ok, err = Read(vol, in, name)
	if err != nil || !ok || string(data) != "from-volume" {
		t.Fatalf("volume: %q %v %v", data, ok, err)
	}
	if err := Write(vol, name, []byte("replaced")); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(vol, name))
	if err != nil || string(got) != "replaced" {
		t.Fatalf("replaced: %q %v", got, err)
	}
	entries, err := os.ReadDir(vol)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != name {
			t.Fatalf("temp left behind: %s", e.Name())
		}
	}
}

func TestNestedWriteStaysInThatDirectory(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join("nested", "state.bin")
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Write(dir, name, []byte("nested")); err != nil {
		t.Fatal(err)
	}
	data, ok, err := Read(dir, "", name)
	if err != nil || !ok || string(data) != "nested" {
		t.Fatalf("read %q %v %v", data, ok, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "nested" {
			t.Fatalf("temp in parent: %s", e.Name())
		}
	}
	entries, err = os.ReadDir(filepath.Join(dir, "nested"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "state.bin" {
			t.Fatalf("temp left behind: %s", e.Name())
		}
	}
}

func TestReadEmptyDirDoesNotUseCwd(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	name := "state.bin"
	if err := os.WriteFile(name, []byte("cwd"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, ok, err := Read("", "", name)
	if err != nil || ok || data != nil {
		t.Fatalf("cwd: %q %v %v", data, ok, err)
	}
	missing := filepath.Join(dir, "no-such-volume")
	data, ok, err = Read(missing, "", name)
	if err != nil || ok || data != nil {
		t.Fatalf("missing dir: %q %v %v", data, ok, err)
	}
}

func TestRejectsEscapingNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"", ".", "..", "/tmp/x", "a/../../b", "a/../b"} {
		if _, _, err := Read(dir, dir, name); err == nil {
			t.Fatalf("Read %q", name)
		}
		if err := Write(dir, name, []byte("x")); err == nil {
			t.Fatalf("Write %q", name)
		}
	}
	if err := Write("", "a", []byte("x")); err == nil {
		t.Fatal("empty dir")
	}
}

func TestCrashLeavesDestinationWhole(t *testing.T) {
	t.Run("previous", func(t *testing.T) {
		dir := t.TempDir()
		name := "state.bin"
		if err := os.WriteFile(filepath.Join(dir, name), []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
		crash(t, dir, name, "new-bytes-that-must-not-land")
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != "old" {
			t.Fatalf("dest %q %v", got, err)
		}
	})
	t.Run("absent", func(t *testing.T) {
		dir := t.TempDir()
		name := "state.bin"
		crash(t, dir, name, "new-bytes")
		_, err := os.ReadFile(filepath.Join(dir, name))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dest err %v", err)
		}
	})
}

func TestConcurrentReadersSeeWholeFiles(t *testing.T) {
	dir := t.TempDir()
	name := "state.bin"
	a := bytes.Repeat([]byte("A"), 64<<10)
	b := bytes.Repeat([]byte("B"), 64<<10)
	if err := Write(dir, name, a); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	errc := make(chan error, 1)
	go func() {
		for !stop.Load() {
			if err := Write(dir, name, a); err != nil {
				errc <- err
				return
			}
			if err := Write(dir, name, b); err != nil {
				errc <- err
				return
			}
		}
		errc <- nil
	}()
	seenA, seenB := false, false
	deadline := time.Now().Add(5 * time.Second)
	for !seenA || !seenB {
		if time.Now().After(deadline) {
			t.Fatal("did not observe both complete versions")
		}
		select {
		case err := <-errc:
			t.Fatal(err)
		default:
		}
		data, ok, err := Read(dir, "", name)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}
		switch {
		case bytes.Equal(data, a):
			seenA = true
		case bytes.Equal(data, b):
			seenB = true
		default:
			t.Fatalf("partial file len %d", len(data))
		}
	}
	stop.Store(true)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func crash(t *testing.T, dir, name, data string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"MFTIKO_VOLUME_CRASH=1",
		"MFTIKO_VOLUME_DIR="+dir,
		"MFTIKO_VOLUME_NAME="+name,
		"MFTIKO_VOLUME_DATA="+data,
	)
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 9 {
		t.Fatalf("child exit: %v", err)
	}
}
