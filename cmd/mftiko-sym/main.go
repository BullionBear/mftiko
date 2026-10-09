// Command mftiko-sym is the symbol service. It answers the sym subject and refreshes
// the listings of the (venue, category) feeds it is configured with (DECISIONS D201, D006).
//
// Sources are registered in this process, not by init. exchange/all names each exchange.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/sym"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	database := flag.String("database", os.Getenv("DATABASE_URL"), "PostgreSQL URL (default $DATABASE_URL)")
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL (default $NATS_URL or the dev server)")
	feeds := flag.String("feeds", "", "comma-separated Venue/Category; empty serves every registered category that has a public REST reader")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-sym")
	if err := run(*database, *natsURL, *feeds, log); err != nil {
		log.Error().Err(err).Msg("exit")
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(database, natsURL, feeds string, log zerolog.Logger) error {
	if database == "" {
		return errors.New("database URL is empty")
	}
	if natsURL == "" {
		return errors.New("nats URL is empty")
	}
	// D006: the command names the exchanges. There is no init() registration.
	// No HTTP transport yet: exchange/transport/fasthttp is an E3 ticket, and every adapter's
	// Listing is still ErrNotImplemented. Until then a refresh fails with exchange.ErrNoTransport.
	selected, err := sym.SelectFeeds(all.Registry(), feeds, nil)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := sym.Open(ctx, database, nil)
	if err != nil {
		return err
	}
	defer st.Close()
	runner, err := sym.NewRunner(st, selected, sym.Options{Log: log})
	if err != nil {
		return err
	}
	nc, err := natsx.Connect(natsURL, natsx.Options{Name: "mftiko-sym", Logger: slog.Default()})
	if err != nil {
		return err
	}
	defer nc.Close()
	// After the revision commits. A subscriber that misses it compares revisions.
	st.SetPublisher(sym.NATSChanged(nc, log))
	if _, err := sym.NewService(st, runner, log).Serve(ctx, nc); err != nil {
		return err
	}
	log.Info().Int("feeds", len(selected)).Msg("serving sym")
	runner.Run(ctx)
	return nil
}
