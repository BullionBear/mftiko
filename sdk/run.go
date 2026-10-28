package sdk

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
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
// not watch signals (DECISIONS E08-D006).
func Run(s Strategy) {
	if s == nil {
		log := Log()
		log.Error().Msg("nil strategy")
		os.Exit(c4.ExitInit)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	fd, err := ctlFD()
	if err != nil {
		log := Log()
		log.Error().Msg("control fd")
		os.Exit(c4.ExitInit)
	}
	syscall.CloseOnExec(fd)
	f := os.NewFile(uintptr(fd), "ctl")
	if f == nil {
		log := Log()
		log.Error().Msg("control fd")
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
			log := p.logger()
			log.Error().Err(err).Msg(msg)
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
