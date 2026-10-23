// Command mftiko-api is the API server. It writes declarations (sessions, accounts) to etcd and serves the web
// console; it never calls an agent directly and never queries SYM (DECISIONS D1001, D004, D202). Several replicas
// may run side by side; none of them takes part in an election (D1006).
//
// Settings come in three kinds (D1023). -in is the deployment JSON that mftiko-etcd-roles reads (D1012, D1312);
// "-" reads stdin. -config is the API JSON, a file, or "-" when -in is not stdin:
//
//	users[].name, users[].token_sha256   one static token per user, stored as SHA-256 (D1009)
//	repositories                          strategy repositories, path without the registry host (D1013, D1060)
//	registry, registry_creds, plain_http  registry host[:port], Docker config.json, http for dev (D933, D1060)
//	td_keys                               TD instance name → public key, the mftiko-seal pubkey form (D1011)
//	retention                             W5 retention, Go duration, default 168h (D1017)
//	unavailable_after                     region unavailable threshold, Go duration, default 15s (D1014)
//	listen                                listen address, default :8080 (D1029)
//	shutdown_timeout                      drain limit after SIGTERM, Go duration, default 10s (D1027)
//
// A zero or omitted duration takes the default. Secrets are environment variables, and the matching flag
// overrides, as for the controller: the etcd user "api" password ($MFTIKO_ETCD_PASSWORD, -etcd-password), the
// cookie HMAC key ($MFTIKO_COOKIE_KEY, -cookie-key) and the Postgres DSN ($MFTIKO_POSTGRES_DSN, -postgres-dsn).
//
// The process validates all of that. Load does not dial Postgres or a registry (D1024, D1060); a strategy catalog
// request does. Authentication, audit, the other endpoints and W5 deletion are later tickets. Readiness watches
// etcd only (D1024). One Mirror per cluster watches the whole /mftiko/v1/ keyspace (D1025).
//
// /healthz is 200 while the process is up. /readyz is 200 when any region is available; the body lists every
// region and unavailable_regions. It is 503 only when every region is unavailable, and during shutdown
// (D1026, D1027). SIGTERM and SIGINT mark readiness 503, stop accepting, wait for in-flight requests up to
// shutdown_timeout, and exit 0.
//
// -web is the console build directory (web/build), read from disk on each request, not embedded. A path that is
// not under /api/ and does not name a file returns index.html. Without -web the process serves only the API
// (D1028). -tls-cert and -tls-key together serve HTTPS with HTTP/2; without them the listener is plaintext HTTP
// for development (D1029).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/api"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
)

func main() {
	opt, version, err := parseArgs(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-api:", err)
		os.Exit(2)
	}
	if version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-api")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opt, os.Stdin, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Msg("exit")
		os.Exit(1)
	}
}

type options struct {
	in, config, web, cert, key        string
	etcdPassword, cookieKey, postgres string
}

func parseArgs(args []string, getenv func(string) string) (options, bool, error) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	var opt options
	fs := flag.NewFlagSet("mftiko-api", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.Bool("version", false, "print the release and exit")
	fs.StringVar(&opt.in, "in", "", "deployment JSON, the same file as mftiko-etcd-roles; - for stdin")
	fs.StringVar(&opt.config, "config", "", "API JSON (users, repositories, registry, td keys, retention, thresholds); - for stdin")
	fs.StringVar(&opt.web, "web", "", "console build directory, served from disk; empty serves the API only")
	fs.StringVar(&opt.cert, "tls-cert", "", "TLS certificate; with -tls-key, serve HTTPS and HTTP/2")
	fs.StringVar(&opt.key, "tls-key", "", "TLS private key; with -tls-cert, serve HTTPS and HTTP/2")
	fs.StringVar(&opt.etcdPassword, "etcd-password", getenv(api.EnvEtcdPassword), "password of the etcd user \"api\" (default $"+api.EnvEtcdPassword+")")
	fs.StringVar(&opt.cookieKey, "cookie-key", getenv(api.EnvCookieKey), "HMAC key of the console cookie (default $"+api.EnvCookieKey+")")
	fs.StringVar(&opt.postgres, "postgres-dsn", getenv(api.EnvPostgresDSN), "Postgres DSN, checked and not connected (default $"+api.EnvPostgresDSN+")")
	if err := fs.Parse(args); err != nil {
		return options{}, false, err
	}
	return opt, *version, nil
}

func run(ctx context.Context, opt options, stdin io.Reader, log zerolog.Logger) error {
	if opt.in == "" || opt.config == "" {
		return errors.New("-in and -config are required")
	}
	if opt.in == "-" && opt.config == "-" {
		return errors.New("only one of -in and -config can read stdin")
	}
	dep, err := openArg(opt.in, stdin)
	if err != nil {
		return err
	}
	defer dep.Close()
	conf, err := openArg(opt.config, stdin)
	if err != nil {
		return err
	}
	defer conf.Close()
	cfg, err := api.Load(dep, conf, api.Input{
		EtcdPassword: opt.etcdPassword,
		CookieKey:    opt.cookieKey,
		PostgresDSN:  opt.postgres,
		WebDir:       opt.web,
		TLSCert:      opt.cert,
		TLSKey:       opt.key,
		Log:          log,
	})
	if err != nil {
		return err
	}
	return api.Run(ctx, cfg)
}

func openArg(path string, stdin io.Reader) (io.ReadCloser, error) {
	if path == "-" {
		return io.NopCloser(stdin), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return f, nil
}
