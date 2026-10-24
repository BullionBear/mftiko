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

// withoutEcho clears ECHO for the duration of a credential prompt (E11-D014).
// The previous termios is restored by the returned function.
func withoutEcho(f *os.File) (func(), error) {
	fd := int(f.Fd())
	term, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, err
	}
	old := *term
	next := *term
	next.Lflag &^= unix.ECHO | unix.ECHONL
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &next); err != nil {
		return nil, err
	}
	return func() {
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, &old)
	}, nil
}
