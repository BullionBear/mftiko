package shim

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// CtlFD is the worker's end of the socketpair (PROTOCOL §5.1), and EnvCtlFD the variable that says so.
const (
	CtlFD    = 3
	EnvCtlFD = "MFTIKO_CTL_FD"
)

// ErrBadLaunch is a Launch the shim refuses to spawn from.
var ErrBadLaunch = errors.New("shim: bad launch")

// ReadLaunch reads the one Launch frame the agent writes on the shim's stdin (PROTOCOL §4.6) and checks it. It
// reads nothing past that frame.
func ReadLaunch(r io.Reader) (*shimv1.Launch, error) {
	l := &shimv1.Launch{}
	// Unbuffered on purpose: a buffered reader could take bytes past the frame, and there must be none to take.
	if err := frame.NewReader(r).Read(l); err != nil {
		return nil, fmt.Errorf("shim: read launch: %w", err)
	}
	if err := CheckLaunch(l); err != nil {
		return nil, err
	}
	return l, nil
}

// CheckLaunch is step 3 of PROTOCOL §4.6.
func CheckLaunch(l *shimv1.Launch) error {
	if !filepath.IsAbs(l.GetDir()) {
		return fmt.Errorf("%w: dir %q is not absolute", ErrBadLaunch, l.GetDir())
	}
	// The rootfs and its entrypoint (PROTOCOL §4.6): the executable is in the directory, compared as written.
	dir := filepath.Clean(l.GetDir())
	if !filepath.IsAbs(l.GetPath()) || l.GetPath() != filepath.Clean(l.GetPath()) ||
		!strings.HasPrefix(l.GetPath(), strings.TrimSuffix(dir, "/")+"/") {
		return fmt.Errorf("%w: path %q is not a clean absolute path under dir %q", ErrBadLaunch, l.GetPath(), dir)
	}
	seen := map[string]bool{}
	for _, kv := range l.GetEnv() {
		k, _, ok := strings.Cut(kv, "=")
		switch {
		case !ok || k == "":
			return fmt.Errorf("%w: env entry %q is not KEY=VALUE", ErrBadLaunch, kv)
		case k == EnvCtlFD:
			return fmt.Errorf("%w: env sets %s, which is the shim's", ErrBadLaunch, EnvCtlFD)
		case seen[k]:
			return fmt.Errorf("%w: env sets %s twice", ErrBadLaunch, k)
		}
		seen[k] = true
	}
	return nil
}

// workerEnv is the worker's whole environment: the Launch's, and the control fd. Nothing of the shim's own.
func workerEnv(l *shimv1.Launch) []string {
	return append(append([]string{}, l.GetEnv()...), fmt.Sprintf("%s=%d", EnvCtlFD, CtlFD))
}

// newFrameReader reads WorkerUp frames off the shim's end of the socketpair.
func newFrameReader(r io.Reader) *frame.Reader { return frame.NewReader(bufio.NewReader(r)) }
