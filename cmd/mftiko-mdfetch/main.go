// Command mftiko-mdfetch is the stateless md.fetch service (DECISIONS E04-D002, E04-D018–E04-D023).
//
// It is a platform process, released and restarted with the platform, not a
// worker artifact and not part of the MD agent. Each replica subscribes to the
// venues it can reach. NATS prefers a local queue-group member.
//
// Sources are registered in this process, not by init. exchange/all names each
// exchange. REST reads go through exchange/transport/fasthttp (E03-D002). The proxy
// and the TLS roots are this process's flags; adapters never see them.
// UIDs are resolved through SYM's NATS resolver, active table only (E04-D022, E08-D065).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/exchange/transport"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/internal/md/fetch"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/pkg/logger"
)

// defaultHTTPTimeout bounds one venue request when the caller sets no deadline.
const defaultHTTPTimeout = 30 * time.Second

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL (default $NATS_URL or the dev server)")
	creds := flag.String("nats-creds", os.Getenv("MFTIKO_NATS_CREDS"), "NATS user credentials file (default $MFTIKO_NATS_CREDS)")
	venues := flag.String("venues", "", "comma-separated venue names this replica can reach")
	proxy := flag.String("proxy", "", "HTTP proxy for venue connections (http://host:port); empty connects directly")
	roots := flag.String("tls-roots", "", "PEM file of TLS roots for venue connections; empty uses the system roots")
	timeout := flag.Duration("http-timeout", defaultHTTPTimeout, "bound on one venue HTTP request whose context has no deadline")
	rate := flag.Int("rate-limit", 0, "refuse a venue call when its reported usage reaches this count; 0 disables the pre-check")
	hold := flag.Duration("rate-hold", fetch.DefaultRateHold, "how long to hold a venue after a 429 or 418 that has no Retry-After and no reset time")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-mdfetch", buildinfo.Version, buildinfo.CommitHash)
	if err := run(*natsURL, *creds, *venues, *proxy, *roots, *timeout, *rate, *hold, log); err != nil {
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

func run(natsURL, creds, venues, proxy, roots string, timeout time.Duration, rate int, hold time.Duration, log zerolog.Logger) error {
	if natsURL == "" {
		return errors.New("nats URL is empty")
	}
	if creds == "" {
		return errors.New("nats creds are empty")
	}
	selected, err := fetch.SelectVenues(all.Registry(), venues)
	if err != nil {
		return err
	}
	doer, err := venueDoer(proxy, roots, timeout)
	if err != nil {
		return err
	}
	inbox, err := natsdev.InboxPrefix(natsdev.PlaneFetch)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := natsx.Connect(natsURL, natsx.Options{
		Name:        "mftiko-mdfetch",
		Logger:      slog.Default(),
		CredsFile:   creds,
		InboxPrefix: inbox,
	})
	if err != nil {
		return err
	}
	defer nc.Close()

	tables := make([]sym.Table, 0, len(selected))
	names := make([]string, 0, len(selected))
	for _, v := range selected {
		names = append(names, v.Name)
		for _, c := range v.Categories {
			tables = append(tables, sym.Table{Venue: v.Name, Category: c})
		}
	}
	remote, err := sym.OpenRemote(ctx, nc, tables, sym.RemoteOptions{Log: log, NotifySubject: sym.ChangedWildcard})
	if err != nil {
		return err
	}
	defer remote.Close()

	svc, err := fetch.New(selected, remote, doer, fetch.Options{Log: log, RateLimit: rate, RateHold: hold})
	if err != nil {
		return err
	}
	if _, err := svc.Serve(ctx, nc); err != nil {
		return err
	}
	log.Info().Strs("venues", names).Int("rate_limit", rate).Dur("rate_hold", hold).Msg("serving md.fetch")
	<-ctx.Done()
	return nil
}

// venueDoer is the REST transport this process runs public reads over (E03-D002).
// An empty roots path keeps the system certificate pool. A set path replaces it.
func venueDoer(proxy, roots string, timeout time.Duration) (exchange.Doer, error) {
	cfg := transport.Config{Proxy: proxy}
	if roots != "" {
		b, err := os.ReadFile(roots)
		if err != nil {
			return nil, fmt.Errorf("tls roots: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("tls roots %s: no certificates", roots)
		}
		cfg.TLS = &tls.Config{RootCAs: pool}
	}
	n, err := transport.New(cfg)
	if err != nil {
		return nil, err
	}
	return fasthttp.New(n, fasthttp.Options{Timeout: timeout}), nil
}
