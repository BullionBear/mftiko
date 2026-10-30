package shim

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const envLogMode = "MFTIKO_TEST_LOGMODE"

// New log files are 0640 and the plane directory is 0750 even when the process
// umask would hide the group bits. An existing file keeps its mode. setgid on
// the directory is kept, so a new file takes the directory's group (E10-D021, E10-D022, E10-D015).
func TestLogModeIgnoresUmask(t *testing.T) {
	if os.Getenv(envLogMode) == "1" {
		runLogModeChild()
		return
	}
	work := t.TempDir()
	dir := LogDir(work, "sts")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(work, "log"), 0o750|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
	gid, haveGroup := otherGroup()
	if haveGroup {
		if err := os.Chown(dir, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(dir, 0o700|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(dir, "old.stdout")
	if err := os.WriteFile(old, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(old, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLogModeIgnoresUmask$")
	cmd.Env = append(os.Environ(), envLogMode+"=1", "MFTIKO_TEST_LOGDIR="+work)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o750 || fi.Mode()&os.ModeSetgid == 0 {
		t.Fatalf("dir mode %v, want 0750 with setgid", fi.Mode())
	}
	fi, err = os.Stat(old)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("existing file mode %v, want 0600", fi.Mode())
	}
	created := filepath.Join(dir, "new.stderr")
	fi, err = os.Stat(created)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("new file mode %v, want 0640", fi.Mode())
	}
	if fileGID(t, created) != fileGID(t, dir) {
		t.Fatalf("new file gid %d, dir gid %d", fileGID(t, created), fileGID(t, dir))
	}
}

// A plane directory that is 0750 already gets no chmod. Linux clears setgid on a
// chmod by a process outside the directory's group, and the agent is not in
// mftiko-ops, so even a chmod that keeps the bit would drop it (E16-D036). A chmod
// changes ctime, so an unchanged ctime means none was made.
func TestEnsureLogDirLeavesModeAlone(t *testing.T) {
	work := t.TempDir()
	dir := LogDir(work, "sts")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o750|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
	before := fileCtime(t, dir)
	time.Sleep(10 * time.Millisecond)
	if err := EnsureLogDir(work, "sts"); err != nil {
		t.Fatal(err)
	}
	if after := fileCtime(t, dir); after != before {
		t.Fatalf("ctime %v, was %v: EnsureLogDir chmodded a 0750 directory", after, before)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o750 || fi.Mode()&os.ModeSetgid == 0 {
		t.Fatalf("dir mode %v, want 0750 with setgid", fi.Mode())
	}
}

func runLogModeChild() {
	syscall.Umask(0o077)
	work := os.Getenv("MFTIKO_TEST_LOGDIR")
	if err := EnsureLogDir(work, "sts"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	old := filepath.Join(LogDir(work, "sts"), "old.stdout")
	f, err := OpenLogFile(old)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	f.Close()
	created := filepath.Join(LogDir(work, "sts"), "new.stderr")
	f, err = OpenLogFile(created)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	if _, err := f.Write([]byte("n")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		f.Close()
		os.Exit(5)
	}
	f.Close()
	os.Exit(0)
}

func otherGroup() (int, bool) {
	egid := os.Getegid()
	groups, err := os.Getgroups()
	if err != nil {
		return 0, false
	}
	for _, g := range groups {
		if g != egid && g != 0 {
			return g, true
		}
	}
	return 0, false
}

func fileGID(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s: stat %T", path, fi.Sys())
	}
	return st.Gid
}

func fileCtime(t *testing.T, path string) time.Time {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s: stat %T", path, fi.Sys())
	}
	return time.Unix(st.Ctim.Sec, st.Ctim.Nsec)
}
