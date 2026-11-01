// Command mftiko-latency-exporter tails every session event log under an STS
// data directory and serves Prometheus latency metrics (DECISIONS E17-D001–E17-D006).
//
//	mftiko-latency-exporter -metrics-addr HOST:PORT -data DIR
//
// -metrics-addr is a loopback, link-local, or private address. -data is the
// agent's data directory, the parent of sessions/. A session that has not
// turned the event log on is skipped. -version prints the release and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/BullionBear/mftiko/internal/latencyexp"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/pkg/logger"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-latency-exporter:", err)
		os.Exit(1)
	}
}

const usage = "usage: mftiko-latency-exporter -metrics-addr HOST:PORT -data DIR"

func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("mftiko-latency-exporter", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.Bool("version", false, "print the release and exit")
	addr := fs.String("metrics-addr", "", "private host:port to serve /metrics on")
	data := fs.String("data", "", "STS data directory, the parent of sessions/")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	if *version {
		_, err := fmt.Fprintln(stdout, buildinfo.String())
		return err
	}
	if *addr == "" || *data == "" || fs.NArg() != 0 {
		return errors.New(usage)
	}
	if err := latencyexp.CheckAddr(*addr); err != nil {
		return err
	}
	log := logger.New("mftiko-latency-exporter", buildinfo.Version, buildinfo.CommitHash)
	exp, err := latencyexp.New(*data, log)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	log.Info().Str("metrics_addr", *addr).Str("data", *data).Msg("serving")
	return exp.Serve(ctx, ln)
}
