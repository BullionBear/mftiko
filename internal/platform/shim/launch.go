package shim

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
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

// ReadLaunch reads the one Launch frame the agent writes on the shim's stdin (PROTOCOL §4.6) and checks all of it
// (step 3). It reads nothing past that frame.
func ReadLaunch(r io.Reader) (*shimv1.Launch, error) {
	l := &shimv1.Launch{}
	// Unbuffered on purpose: a buffered reader could take bytes past the frame, and there must be none to take.
	if err := frame.NewReader(r).Read(l); err != nil {
		return nil, fmt.Errorf("shim: read launch: %w", err)
	}
	if err := CheckLaunch(l); err != nil {
		return nil, err
	}
	if err := checkIdentity(l); err != nil {
		return nil, err
	}
	if err := checkOutput(l.GetOutput()); err != nil {
		return nil, err
	}
	return l, nil
}

// CheckLaunch is step 3 of PROTOCOL §4.6 for what the shim execs, the fields that come from the manifest.
// ReadLaunch also checks the rest, the agent's own fields.
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

// nameSyntax is the syntax of an instance name (PROTOCOL §2), which a plane name also follows.
var nameSyntax = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// maxSocketPath is the longest path a unix socket can be bound to on Linux: sun_path holds 108 bytes, NUL included.
const maxSocketPath = 107

// checkIdentity is the rest of step 3 of PROTOCOL §4.6: who the worker is and where its socket goes. The plane is a
// path segment of the socket, and Attach is matched against the plane and the instance (§4.1, §4.2).
func checkIdentity(l *shimv1.Launch) error {
	wd := l.GetWorkDir()
	switch {
	case !nameSyntax.MatchString(l.GetPlane()):
		return fmt.Errorf("%w: plane %q is not a name", ErrBadLaunch, l.GetPlane())
	case !nameSyntax.MatchString(l.GetAgentInstance()):
		return fmt.Errorf("%w: agent instance %q is not a name", ErrBadLaunch, l.GetAgentInstance())
	case l.GetWorker().GetKey() == "":
		return fmt.Errorf("%w: no worker key", ErrBadLaunch)
	case !filepath.IsAbs(wd) || wd != filepath.Clean(wd):
		return fmt.Errorf("%w: work dir %q is not a clean absolute path", ErrBadLaunch, wd)
	}
	if p := SocketPath(wd, l.GetPlane(), l.GetWorker().GetKey()); len(p) > maxSocketPath {
		return fmt.Errorf("%w: socket path %q is %d bytes, more than %d", ErrBadLaunch, p, len(p), maxSocketPath)
	}
	return nil
}
