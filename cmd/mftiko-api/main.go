// Command mftiko-api is the API server. It writes declarations (sessions, accounts) to etcd and serves the web
// console; it never calls an agent directly and never queries SYM (DECISIONS E10-D001, E00-D004, E02-D002). Several replicas
// may run side by side; none of them takes part in an election (E10-D006).
//
// Settings come in three kinds (E10-D023). -in is the deployment JSON that mftiko-etcd-roles reads (E10-D012, E13-D012);
// "-" reads stdin. -config is the API JSON, a file, or "-" when -in is not stdin:
//
//	users[].name, users[].token_sha256   one static token per user, stored as SHA-256 (E10-D009, E10-D055)
//	allow                                 strategy repository allowlist; "*" or host plus path prefix (E10-D119)
//	registry, registry_creds, plain_http  registry host[:port], Docker config.json, http for dev (E09-D033, E10-D060)
//	td_keys                               TD instance name → public key, the mftiko-seal pubkey form (E10-D011)
//	retention                             W5 retention, Go duration, default 168h (E10-D017)
//	unavailable_after                     region unavailable threshold, Go duration, default 15s (E10-D014)
//	listen                                listen address, default :8080 (E10-D029)
//	shutdown_timeout                      drain limit after SIGTERM, Go duration, default 10s (E10-D027)
//
// Account kinds are not in the JSON. Run copies them from the exchange registry into the API
// (E10-D034). The API package does not import exchange.
//
// A zero or omitted duration takes the default. Secrets are environment variables, and the matching flag
// overrides, as for the controller: the etcd user "api" password ($MFTIKO_ETCD_PASSWORD, -etcd-password), the
// cookie HMAC key ($MFTIKO_COOKIE_KEY, -cookie-key) and the Postgres DSN ($MFTIKO_POSTGRES_DSN, -postgres-dsn).
//
// Load validates all of that and does not dial Postgres or a registry (E10-D024, E10-D060). Run opens a Postgres pool
// and does not wait for the connection; readiness watches etcd only, and Postgres being down only affects audit
// (E10-D048, modifies E10-D024). A strategy catalog request dials the registry. Every replica's reconcile deletes
// terminal sessions past retention (W5, E10-D017, E10-D083). One Mirror per cluster watches the whole /mftiko/v1/
// keyspace (E10-D025). Authentication is one static token per user,
// sent as a bearer token or exchanged for the console cookie (E10-D009). A token should be at least 32 random bytes;
// the config stores only its SHA-256, so the API cannot check the length (E10-D055):
//
//	token=$(openssl rand -base64 32 | tr -d '\n'); printf '%s' "$token" | sha256sum
//
// Put the hex digest in users[].token_sha256. Clients send the token string itself.
//
// /healthz is 200 while the process is up. /readyz is 200 when any region is available; the body lists every
// region and unavailable_regions. It is 503 only when every region is unavailable, and during shutdown
// (E10-D026, E10-D027). SIGTERM and SIGINT mark readiness 503, stop accepting, wait for in-flight requests up to
// shutdown_timeout, and exit 0.
//
// -web is the console build directory (web/build), read from disk on each request, not embedded. The directory
// itself is opened once at startup: replacing it (make web) needs a restart (E10-D114). A path that is
// not under /api/ and does not name a file returns index.html. Without -web the process serves only the API
// (E10-D028). -tls-cert and -tls-key together serve HTTPS with HTTP/2; without them the listener is plaintext HTTP
// for development (E10-D029).
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

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
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
	fs.StringVar(&opt.config, "config", "", "API JSON (users, allow, registry, td keys, retention, thresholds); - for stdin")
	fs.StringVar(&opt.web, "web", "", "console build directory, served from disk; empty serves the API only")
	fs.StringVar(&opt.cert, "tls-cert", "", "TLS certificate; with -tls-key, serve HTTPS and HTTP/2")
	fs.StringVar(&opt.key, "tls-key", "", "TLS private key; with -tls-cert, serve HTTPS and HTTP/2")
	fs.StringVar(&opt.etcdPassword, "etcd-password", getenv(api.EnvEtcdPassword), "password of the etcd user \"api\" (default $"+api.EnvEtcdPassword+")")
	fs.StringVar(&opt.cookieKey, "cookie-key", getenv(api.EnvCookieKey), "HMAC key of the console cookie (default $"+api.EnvCookieKey+")")
	fs.StringVar(&opt.postgres, "postgres-dsn", getenv(api.EnvPostgresDSN), "Postgres DSN; a pool is opened at startup and not waited on (default $"+api.EnvPostgresDSN+")")
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
	// Kinds are not configuration. The registry is the list GET /account-kinds serves, and the
	// check create and update apply (E10-D034). internal/api does not import exchange.
	cfg.Kinds = accountKinds(all.Registry())
	return api.Run(ctx, cfg)
}

// accountKinds copies the registry's account kinds into the API's view of them. Supported is derived
// from how many venues the kind trades (E06-D014).
func accountKinds(r *exchange.Registry) []api.Kind {
	names := r.AccountKinds()
	out := make([]api.Kind, 0, len(names))
	for _, name := range names {
		k, ok := r.AccountKind(name)
		if !ok {
			continue
		}
		types := make([]string, len(k.KeyTypes))
		for i, t := range k.KeyTypes {
			types[i] = string(t)
		}
		out = append(out, api.Kind{
			Name: k.Name, Label: k.Label,
			Trades: append([]string(nil), k.Trades...), KeyTypes: types, Passphrase: k.Passphrase,
		})
	}
	return out
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
