package shim

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// dupStdio makes fd 1 and fd 2 aliases of fd. The destination flags are not close-on-exec.
func dupStdio(fd int) error {
	if err := unix.Dup3(fd, 1, 0); err != nil {
		return fmt.Errorf("shim: dup stdout: %w", err)
	}
	if err := unix.Dup3(fd, 2, 0); err != nil {
		return fmt.Errorf("shim: dup stderr: %w", err)
	}
	return nil
}
