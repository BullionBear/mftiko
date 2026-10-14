// Command mftiko-td-account is the TD account worker for one api_id: its private connections, HTTP
// pool, recon, OMS and ledger, td.evt and td.view (DECISIONS D603, D606). It speaks C4 on fd 3
// (PROTOCOL §5); Init carries the api_id, its account kind, its unsealed credentials and its own NATS
// credentials (D611). NATS is at MFTIKO_NATS_URL.
//
// The venue's hosts are the TD instance's, per account kind, and come in Init (D616).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/internal/td/account"
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

	natsURL := os.Getenv("MFTIKO_NATS_URL")
	if natsURL == "" {
		log.Error().Msg("MFTIKO_NATS_URL is empty")
		return 1
	}
	var incarnation uint32
	if v := os.Getenv("MFTIKO_INCARNATION"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			log.Error().Str("MFTIKO_INCARNATION", v).Msg("incarnation")
			return 1
		}
		incarnation = uint32(n)
	}

	reg := all.Registry()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	return account.Serve(ctx, f, account.ServeConfig{
		Kinds:       func(name string) (exchange.AccountKind, bool) { return reg.AccountKind(name) },
		Doer:        fasthttp.New(nil, fasthttp.Options{}),
		WorkerKey:   os.Getenv("MFTIKO_WORKER_KEY"),
		Incarnation: incarnation,
		Connect: func(ctx context.Context, c *tdctlv1.NatsCredentials, venue string) (*account.Env, error) {
			nc, err := natsx.Connect(natsURL, natsx.Options{
				Name: "mftiko-td-account", InboxPrefix: c.GetInboxPrefix(), UserJWT: c.GetUserJwt(), Seed: c.GetSeed(),
			})
			if err != nil {
				return nil, err
			}
			v, ok := reg.Venue(venue)
			if !ok {
				nc.Close()
				return nil, fmt.Errorf("unknown venue %s", venue)
			}
			tables := make([]sym.Table, 0, len(v.Categories))
			for _, cat := range v.Categories {
				tables = append(tables, sym.Table{Venue: venue, Category: cat})
			}
			ins, err := sym.OpenRemote(ctx, nc, tables, sym.RemoteOptions{Log: log})
			if err != nil {
				nc.Close()
				return nil, fmt.Errorf("symbols: %w", err)
			}
			return &account.Env{Bus: bus{ctx: ctx, nc: nc}, Instruments: ins, Close: func() { ins.Close(); nc.Close() }}, nil
		},
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

// bus is the account's NATS connection as the worker uses it.
type bus struct {
	ctx context.Context
	nc  *natsx.Conn
}

func (b bus) Publish(subject string, data []byte) error { return b.nc.Publish(subject, data) }

func (b bus) Respond(subject string, h func(req []byte) []byte) (func(), error) {
	sub, err := b.nc.Subscribe(b.ctx, subject, "", func(m natsx.Msg) {
		if m.Reply == "" {
			return
		}
		_ = b.nc.Publish(m.Reply, h(append([]byte(nil), m.Data...)))
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}
