// Command mftiko-sym is the symbol service. It answers the sym subject and refreshes
// the listings of the (venue, category) feeds it is configured with (DECISIONS E02-D001, E00-D006).
// With -grpc-addr it also serves contract S, SymService over gRPC, from the same store (E22-D012).
//
// Sources are registered in this process, not by init. exchange/all names each exchange.
// Listing reads go through exchange/transport/fasthttp (E03-D002). The proxy and the TLS roots
// are this process's flags; adapters never see them.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/exchange/transport"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/internal/sym/symgrpc"
	"github.com/BullionBear/mftiko/internal/sym/symnats"
	"github.com/BullionBear/mftiko/pkg/logger"
)

// defaultHTTPTimeout bounds one venue request when the caller sets no deadline.
// A listing page is one request; a paginated venue takes one bound per page.
const defaultHTTPTimeout = 30 * time.Second

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	database := flag.String("database", os.Getenv("DATABASE_URL"), "PostgreSQL URL (default $DATABASE_URL)")
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL (default $NATS_URL or the dev server)")
	creds := flag.String("nats-creds", os.Getenv("MFTIKO_NATS_CREDS"), "NATS user credentials file (default $MFTIKO_NATS_CREDS); empty connects without, which the dev server refuses")
	feeds := flag.String("feeds", "", "comma-separated Venue/Category; empty serves every registered category that has a public REST reader")
	proxy := flag.String("proxy", "", "HTTP proxy for venue connections (http://host:port); empty connects directly")
	roots := flag.String("tls-roots", "", "PEM file of TLS roots for venue connections; empty uses the system roots")
	timeout := flag.Duration("http-timeout", defaultHTTPTimeout, "bound on one venue HTTP request whose context has no deadline")
	var grpc grpcFlags
	flag.StringVar(&grpc.addr, "grpc-addr", "", "listen address of SymService over gRPC (contract S); empty does not serve it")
	flag.StringVar(&grpc.tokenFile, "grpc-token-file", "", `bearer tokens for -grpc-addr, one "name token" per line; required with -grpc-addr`)
	flag.StringVar(&grpc.certFile, "grpc-tls-cert", "", "PEM certificate chain for -grpc-addr; empty serves h2c")
	flag.StringVar(&grpc.keyFile, "grpc-tls-key", "", "PEM private key for -grpc-tls-cert")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-sym", buildinfo.Version, buildinfo.CommitHash)
	if err := run(*database, *natsURL, *creds, *feeds, *proxy, *roots, *timeout, grpc, log); err != nil {
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

func run(database, natsURL, creds, feeds, proxy, roots string, timeout time.Duration, grpc grpcFlags, log zerolog.Logger) error {
	if database == "" {
		return errors.New("database URL is empty")
	}
	if natsURL == "" {
		return errors.New("nats URL is empty")
	}
	grpcCfg, err := grpc.config(log)
	if err != nil {
		return err
	}
	// E00-D006: the command names the exchanges. There is no init() registration.
	doer, err := venueDoer(proxy, roots, timeout)
	if err != nil {
		return err
	}
	selected, err := sym.SelectFeeds(all.Registry(), feeds, doer)
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
	nc, err := natsx.Connect(natsURL, natsx.Options{Name: "mftiko-sym", Logger: slog.Default(), CredsFile: creds})
	if err != nil {
		return err
	}
	defer nc.Close()
	// After the revision commits. A subscriber that misses it compares revisions.
	st.SetPublisher(symnats.NATSChanged(nc, log))
	svc := sym.NewService(st, runner, log)
	if _, err := symnats.Serve(ctx, nc, svc, log); err != nil {
		return err
	}
	// Every part runs until ctx ends. One that stops or fails ends the process, and systemd
	// restarts it. The Store and NATS close after all of them return (E22-D012).
	g, gctx := errgroup.WithContext(ctx)
	if grpcCfg != nil {
		srv, err := symgrpc.New(svc, *grpcCfg)
		if err != nil {
			return err
		}
		ln, err := net.Listen("tcp", grpc.addr)
		if err != nil {
			return fmt.Errorf("grpc listen: %w", err)
		}
		stay(g, gctx, "grpc", func(ctx context.Context) error { return srv.Serve(ctx, ln) })
	}
	log.Info().Int("feeds", len(selected)).Str("grpc", grpc.addr).Msg("serving sym")
	stay(g, gctx, "refresh", func(ctx context.Context) error {
		runner.Run(ctx)
		return nil
	})
	return g.Wait()
}

// stay runs f in g until ctx ends. f returning earlier, even without an error, cancels the rest.
func stay(g *errgroup.Group, ctx context.Context, name string, f func(context.Context) error) {
	g.Go(func() error {
		err := f(ctx)
		if err == nil && ctx.Err() == nil {
			err = fmt.Errorf("%s stopped", name)
		}
		return err
	})
}

// grpcFlags are the -grpc-* flags (E22-D007).
type grpcFlags struct {
	addr, tokenFile, certFile, keyFile string
}

// config is the gRPC server these flags ask for, or nil when -grpc-addr is empty. It reads the
// token file and the certificate, so a bad one stops the process before anything connects.
func (f grpcFlags) config(log zerolog.Logger) (*symgrpc.Config, error) {
	if f.addr == "" {
		if f.tokenFile != "" || f.certFile != "" || f.keyFile != "" {
			return nil, errors.New("-grpc-token-file, -grpc-tls-cert and -grpc-tls-key need -grpc-addr")
		}
		return nil, nil
	}
	if f.tokenFile == "" {
		return nil, errors.New("-grpc-addr needs -grpc-token-file (E22-D007)")
	}
	if (f.certFile == "") != (f.keyFile == "") {
		return nil, errors.New("-grpc-tls-cert and -grpc-tls-key are set together")
	}
	tokens, err := symgrpc.LoadTokens(f.tokenFile)
	if err != nil {
		return nil, err
	}
	cfg := &symgrpc.Config{Tokens: tokens, Log: log}
	if f.certFile != "" {
		if cfg.TLS, err = symgrpc.LoadTLS(f.certFile, f.keyFile); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// venueDoer is the REST transport SYM runs Listing over (E03-D002). An empty roots
// path keeps the system certificate pool. A set path replaces it.
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
