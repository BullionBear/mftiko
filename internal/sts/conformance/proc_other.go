//go:build unix && !linux

package conformance

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// ctlPair is the C4 socket pair. Without SOCK_CLOEXEC, close-on-exec is set right after.
func ctlPair() ([2]int, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return fds, err
	}
	unix.CloseOnExec(fds[0])
	unix.CloseOnExec(fds[1])
	return fds, nil
}

// procAttr puts the worker in its own group. There is no Pdeathsig here, so a worker outlives a
// runner that is killed before stop.
func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
