package shim

import (
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The socket is at SocketPath, 0600, in a run dir that is 0700; closing the listener removes it.
func TestListen(t *testing.T) {
	l := testLaunch(workDir(t))
	ln, err := listen(l)
	if err != nil {
		t.Fatal(err)
	}
	path := SocketPath(l.GetWorkDir(), testPlane, "td/42")
	if ln.Addr().String() != path {
		t.Errorf("listening on %s, want %s", ln.Addr(), path)
	}
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Type() != fs.ModeSocket || fi.Mode().Perm() != 0o600 {
		t.Errorf("socket mode %v, want a socket, 0600", fi.Mode())
	}
	for _, dir := range []string{filepath.Dir(path), filepath.Dir(filepath.Dir(path))} {
		if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: mode %v (%v), want 0700", dir, fi.Mode(), err)
		}
	}
	ln.Close()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Errorf("socket still there after close: %v", err)
	}
}

// A socket no one listens on, left by a shim that was killed, is replaced.
func TestListenStale(t *testing.T) {
	l := testLaunch(workDir(t))
	old, err := listen(l)
	if err != nil {
		t.Fatal(err)
	}
	old.SetUnlinkOnClose(false) // as when the shim is killed
	old.Close()
	ln, err := listen(l)
	if err != nil {
		t.Fatalf("a stale socket: %v", err)
	}
	defer ln.Close()
	c, err := net.Dial("unix", ln.Addr().String())
	if err != nil {
		t.Fatalf("the new socket: %v", err)
	}
	c.Close()
}

// A shim still serving the worker key keeps its socket, and a second one does not start (§4.1, E01-D006).
func TestListenBusy(t *testing.T) {
	l := testLaunch(workDir(t))
	first, err := listen(l)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := listen(l); err == nil || !strings.Contains(err.Error(), "another shim") {
		t.Fatalf("a second listen: %v", err)
	}
	c, err := net.Dial("unix", first.Addr().String()) // the first is untouched
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

// Something other than a socket at the path is left alone, and the shim does not start.
func TestListenNotSocket(t *testing.T) {
	l := testLaunch(workDir(t))
	path := SocketPath(l.GetWorkDir(), testPlane, "td/42")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listen(l); err == nil {
		t.Fatal("listened over a regular file")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "keep" {
		t.Fatalf("the file was touched: %q, %v", b, err)
	}
}

// A run dir someone else may enter, or that is not a directory of the shim's uid, is refused (§4.1).
func TestListenRunDir(t *testing.T) {
	cases := map[string]func(dir string) error{
		"group may read": func(dir string) error { return mkdirMode(dir, 0o750) },
		"others may use": func(dir string) error { return mkdirMode(dir, 0o701) },
		"symlink": func(dir string) error {
			target := dir + "-real"
			if err := os.Mkdir(target, 0o700); err != nil {
				return err
			}
			return os.Symlink(target, dir)
		},
	}
	if os.Geteuid() == 0 {
		cases["another uid's"] = func(dir string) error {
			if err := os.Mkdir(dir, 0o700); err != nil {
				return err
			}
			return os.Chown(dir, 65534, 65534)
		}
	}
	for name, setup := range cases {
		l := testLaunch(workDir(t))
		dir := RunDir(l.GetWorkDir(), testPlane)
		if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := setup(dir); err != nil {
			t.Fatal(err)
		}
		if ln, err := listen(l); err == nil {
			ln.Close()
			t.Errorf("%s: listened", name)
		}
	}
}

// mkdirMode makes dir with exactly mode, whatever the umask.
func mkdirMode(dir string, mode os.FileMode) error {
	if err := os.Mkdir(dir, mode); err != nil {
		return err
	}
	return os.Chmod(dir, mode)
}
