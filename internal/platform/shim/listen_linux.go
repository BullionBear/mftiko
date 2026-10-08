package shim

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
)

// probeTimeout bounds the connect to a socket already at the shim's path: a live shim answers at once.
const probeTimeout = time.Second

// listen opens the C3 socket of l's worker (PROTOCOL §4.1, §4.6 step 4). Only the shim's uid, which is the agent's,
// can reach it: the directory is the uid's alone and the socket is 0600. A socket already at the path is either a
// shim still serving the same worker key, and listen fails, or one left behind by a shim that was killed, and listen
// replaces it. Closing the listener removes the socket.
//
// l must have passed checkIdentity.
func listen(l *shimv1.Launch) (*net.UnixListener, error) {
	dir := RunDir(l.GetWorkDir(), l.GetPlane())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("shim: run dir: %w", err)
	}
	if err := checkRunDir(dir); err != nil {
		return nil, err
	}
	path := SocketPath(l.GetWorkDir(), l.GetPlane(), l.GetWorker().GetKey())
	if err := clearStale(path); err != nil {
		return nil, err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("shim: listen: %w", err)
	}
	// The directory already keeps everyone else out; the mode says so too.
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("shim: socket mode: %w", err)
	}
	return ln, nil
}

// checkRunDir requires dir to be a directory of the shim's uid that no one else may enter.
func checkRunDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("shim: run dir: %w", err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	switch {
	case !fi.IsDir():
		return fmt.Errorf("shim: run dir %s is not a directory (%s)", dir, fi.Mode().Type())
	case !ok || int(st.Uid) != os.Geteuid():
		return fmt.Errorf("shim: run dir %s is not owned by the shim's uid %d", dir, os.Geteuid())
	case fi.Mode().Perm()&0o077 != 0:
		return fmt.Errorf("shim: run dir %s has mode %04o: only its owner may have access", dir, fi.Mode().Perm())
	}
	return nil
}

// clearStale makes way for the socket at path: nothing there, or a socket no one listens on any more, which it
// removes. Anything else stays, and the shim does not start.
func clearStale(path string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("shim: socket path: %w", err)
	}
	if fi.Mode().Type() != fs.ModeSocket {
		return fmt.Errorf("shim: %s exists and is not a socket", path)
	}
	c, err := net.DialTimeout("unix", path, probeTimeout)
	if err == nil {
		c.Close()
		return fmt.Errorf("shim: another shim serves %s: release it before spawning the worker again", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("shim: probe %s: %w", path, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("shim: remove stale socket: %w", err)
	}
	return nil
}

// peerCred is who is at the other end of c: its pid, uid and gid when it connected.
func peerCred(c *net.UnixConn) (*unix.Ucred, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *unix.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return nil, err
	}
	return cred, cerr
}
