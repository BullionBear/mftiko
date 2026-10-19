package sdk

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"

	"github.com/BullionBear/mftiko/contract/wire/c4"
)

// emitGate prints the conformance line once subscriptions are up and before
// OnStart (PROTOCOL §9.8). Tests replace it.
var emitGate = func() {
	fmt.Printf("{\"hook\":\"gate\"}\n")
}

// Run hosts s as a session worker: it speaks C4 on fd 3, connects to NATS with the session's
// credentials, and dispatches hooks (PROTOCOL §5, §9). It does not return.
//
// SIGTERM belongs to Run, not to c4.Serve. Run takes it with signal.NotifyContext and passes that
// context to Serve, as mftiko-md-conn and mftiko-td-account do. Cancelling it is a Stop. Serve does
// not watch signals (DECISIONS D806).
func Run(s Strategy) {
	if s == nil {
		fmt.Fprintln(os.Stderr, "mftiko sdk: nil strategy")
		os.Exit(c4.ExitInit)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	fd, err := ctlFD()
	if err != nil {
		fmt.Fprintln(os.Stderr, "mftiko sdk: control fd")
		os.Exit(c4.ExitInit)
	}
	syscall.CloseOnExec(fd)
	f := os.NewFile(uintptr(fd), "ctl")
	if f == nil {
		fmt.Fprintln(os.Stderr, "mftiko sdk: control fd")
		os.Exit(c4.ExitInit)
	}
	os.Exit(serve(ctx, f, s, host{}))
}

// serve is Run without the process exit, so tests can drive a pipe instead of fd 3.
func serve(ctx context.Context, rw io.ReadWriteCloser, s Strategy, h host) int {
	if s == nil {
		return c4.ExitInit
	}
	p := newPlane(s, h)
	defer p.shutdown()
	logf := h.c4log
	if logf == nil {
		logf = func(msg string, err error) {
			fmt.Fprintf(os.Stderr, "mftiko sdk: %s: %v\n", msg, err)
		}
	}
	return c4.Serve(ctx, rw, c4.Config{
		Kind:            "session",
		Contract:        contractSTS,
		Contracts:       Contracts(),
		Release:         release(),
		InitCarriesLive: true,
		Log:             logf,
	}, p)
}

// release is the module version, or the VCS revision, from the build info.
// The session binary does not import the platform's buildinfo package.
func release() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && s.Value != "" {
			return s.Value
		}
	}
	return ""
}

// ctlFD is MFTIKO_CTL_FD, or 3, the socketpair end the shim leaves in place (PROTOCOL §5.1).
func ctlFD() (int, error) {
	v := os.Getenv("MFTIKO_CTL_FD")
	if v == "" {
		return 3, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("MFTIKO_CTL_FD %q", v)
	}
	return n, nil
}
