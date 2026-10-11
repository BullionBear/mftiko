// Command mftiko-md-conn is one MD conn worker: a single public websocket for a (venue, route, n).
// It speaks C4 on fd 3 (PROTOCOL §5). Init carries the last reported conn_epoch; the dial uses
// the next one, and that number is written as a Report before the dial (D409).
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
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

func main() {
	os.Exit(run())
}

func run() int {
	version := flag.Bool("version", false, "print the release and exit")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return 0
	}
	log := logger.New("mftiko-md-conn")
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

	url := os.Getenv("MFTIKO_NATS_URL")
	if url == "" {
		log.Error().Msg("MFTIKO_NATS_URL is empty")
		return 1
	}
	nc, err := natsx.Connect(url, natsx.Options{
		Name:        "mftiko-md-conn",
		InboxPrefix: "_INBOX_md",
		CredsFile:   os.Getenv("MFTIKO_NATS_CREDS"),
	})
	if err != nil {
		log.Error().Err(err).Msg("nats")
		return 10
	}
	defer nc.Close()

	reg := all.Registry()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	return conn.Serve(ctx, f, conn.ServeConfig{
		Lookup: func(venue string) (exchange.Market, bool) {
			v, ok := reg.Venue(venue)
			if !ok || v.Market == nil {
				return nil, false
			}
			return v.Market, true
		},
		Publish: nc.Publish,
		Log:     &log,
		Release: buildinfo.Version,
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
