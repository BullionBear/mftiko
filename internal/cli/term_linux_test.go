//go:build linux

package cli

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCredentialEchoOff(t *testing.T) {
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	// Fd clears O_NONBLOCK, so remember the descriptor and set the flag after it.
	mfd := int(master.Fd())
	if err := unix.SetNonblock(mfd, true); err != nil {
		t.Fatal(err)
	}
	if err := unix.IoctlSetPointerInt(mfd, unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	n, err := unix.IoctlGetInt(mfd, unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	slave, err := os.OpenFile("/dev/pts/"+itoa(n), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	term, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	term.Lflag |= unix.ECHO | unix.ECHONL
	if err := unix.IoctlSetTermios(int(slave.Fd()), unix.TCSETS, term); err != nil {
		t.Fatal(err)
	}
	restore, err := withoutEcho(slave)
	if err != nil {
		t.Fatal(err)
	}
	if termiosFlags(t, slave)&unix.ECHO != 0 {
		t.Fatal("echo stayed on")
	}
	if _, err := unix.Write(mfd, []byte("test-secret-a\n")); err != nil {
		t.Fatal(err)
	}
	// The line discipline echoes asynchronously. Wait long enough that an echo
	// would have shown up, and require that the secret never does.
	hidden, err := awaitMaster(mfd, 200*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(hidden), "test-secret-a") {
		t.Fatal("credential was echoed")
	}
	if err := slave.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := readHidden(slave, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "test-secret-a\n" {
		t.Fatal("hidden read mismatch")
	}
	restore()
	if termiosFlags(t, slave)&unix.ECHO == 0 {
		t.Fatal("echo flag was not restored")
	}
	if _, err := unix.Write(mfd, []byte("test-secret-a\n")); err != nil {
		t.Fatal(err)
	}
	echoed, err := awaitMaster(mfd, 200*time.Millisecond, func(b []byte) bool {
		return strings.Contains(string(b), "test-secret-a")
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(echoed), "test-secret-a") {
		t.Fatal("echo was not restored")
	}
}

func termiosFlags(t *testing.T, f *os.File) uint32 {
	t.Helper()
	term, err := unix.IoctlGetTermios(int(f.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	return term.Lflag
}

// awaitMaster collects bytes the pty master receives until stop reports done
// or wait elapses. stop == nil always waits out the full interval so a test
// can observe that nothing was echoed.
func awaitMaster(mfd int, wait time.Duration, stop func([]byte) bool) ([]byte, error) {
	deadline := time.Now().Add(wait)
	var got []byte
	for {
		if stop != nil && stop(got) {
			return got, nil
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return got, nil
		}
		ms := int((remain + time.Millisecond - 1) / time.Millisecond)
		fds := []unix.PollFd{{Fd: int32(mfd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, ms)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return got, err
		}
		if n == 0 {
			return got, nil
		}
		_, buf, err := readMaster(mfd)
		got = append(got, buf...)
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EWOULDBLOCK) {
			return got, err
		}
	}
}

func readMaster(mfd int) (int, []byte, error) {
	if err := unix.SetNonblock(mfd, true); err != nil {
		return 0, nil, err
	}
	buf := make([]byte, 128)
	n, err := unix.Read(mfd, buf)
	if n < 0 {
		n = 0
	}
	return n, buf[:n], err
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
