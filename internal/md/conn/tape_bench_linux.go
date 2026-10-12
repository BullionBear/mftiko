//go:build linux

package conn

import (
	"syscall"
	"time"
	"unsafe"
)

// threadCPU is the calling OS thread's user+system time. The bench locks the
// publisher goroutine to this thread first, so the figure is the publisher.
func threadCPU() time.Duration {
	var ru syscall.Rusage
	_, _, errno := syscall.RawSyscall(syscall.SYS_GETRUSAGE, 1, uintptr(unsafe.Pointer(&ru)), 0)
	if errno != 0 {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
