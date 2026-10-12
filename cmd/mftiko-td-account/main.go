// Command mftiko-td-account is the TD account worker for one api_id: its private connections, HTTP
// pool and recon (DECISIONS D603). It speaks C4 on fd 3 (PROTOCOL §5); Init carries the api_id, its
// account kind and its unsealed credentials.
//
// The hosts are the TD instance's choice, as flags (D403): -rest-endpoint and -stream-endpoint
// override the venue's defaults, and -simulated selects a venue's demo mode where that is a flag
// (OKX).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/td/account"
)

func main() {
	os.Exit(run())
}

func run() int {
	version := flag.Bool("version", false, "print the release and exit")
	restEndpoint := flag.String("rest-endpoint", "", "the venue's REST host, scheme://host; empty is the default")
	streamEndpoint := flag.String("stream-endpoint", "", "the venue's private websocket host; empty is the default")
	simulated := flag.Bool("simulated", false, "use the venue's demo trading mode where it is a flag (OKX)")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return 0
	}
	log := logger.New("mftiko-td-account")
	fd, err := ctlFD()
	if err != nil {
		log.Error().Err(err).Msg("control fd")
		return 1
	}
	f := os.NewFile(uintptr(fd), "ctl")
	if f == nil {
		log.Error().Int("fd", fd).Msg("control fd")
		return 1
	}
	syscall.CloseOnExec(fd)

	reg := all.Registry()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	return account.Serve(ctx, f, account.ServeConfig{
		Kinds:          func(name string) (exchange.AccountKind, bool) { return reg.AccountKind(name) },
		Doer:           fasthttp.New(nil, fasthttp.Options{}),
		RESTEndpoint:   *restEndpoint,
		StreamEndpoint: *streamEndpoint,
		Simulated:      *simulated,
		Log:            &log,
		Release:        buildinfo.Version,
	})
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
