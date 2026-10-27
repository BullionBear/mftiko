package conformance

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// ctlPair is the C4 socket pair, close-on-exec from the start.
func ctlPair() ([2]int, error) {
	return unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
}

// procAttr puts the worker in its own group and kills it when this process dies.
func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}
