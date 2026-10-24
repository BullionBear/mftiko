//go:build linux

package cli

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func stdinIsTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	_, err := unix.IoctlGetTermios(int(f.Fd()), unix.TCGETS)
	return err == nil
}
