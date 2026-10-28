// Command mftiko-md-conn is one MD conn worker: a single public websocket for a (venue, route, n).
// It speaks C4 on fd 3 (PROTOCOL §5). Init carries the last reported conn_epoch; the dial uses
// the next one, and that number is written as a Report before the dial (E04-D009).
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
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/pkg/logger"
)

func main() {
	os.Exit(run())
}

func run() int {
	version := flag.Bool("version", false, "print the release and exit")
	hold := flag.Duration("rate-hold", conn.DefaultRateHold, "how long to hold depth after a 429 or 418 that has no Retry-After and no reset time")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return 0
	}
	log := logger.New("mftiko-md-conn", buildinfo.Version, buildinfo.CommitHash)
	if *hold < 0 {
		log.Error().Dur("rate_hold", *hold).Msg("rate-hold is negative")
		return 1
	}
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
	// Connect returned, so the bus starts up. Handlers only flip it. Reconnect
	// reports up immediately and does not change ReconnectBufSize (E12-D065).
	// The book can lag that by a few seconds.
	bus := conn.NewBus(true)
	conn.BindBus(bus, nc.OnDisconnect, nc.OnReconnect)

	restOverrides, err := conn.RESTOverridesFromEnv()
	if err != nil {
		log.Error().Err(err).Msg("rest endpoints")
		return 1
	}
	reg := all.Registry()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	doer := fasthttp.New(nil, fasthttp.Options{})
	return conn.Serve(ctx, f, conn.ServeConfig{
		Lookup: func(venue string) (exchange.Market, bool) {
			v, ok := reg.Venue(venue)
			if !ok || v.Market == nil {
				return nil, false
			}
			return v.Market, true
		},
		REST: func(venue string) (exchange.PublicREST, bool) {
			v, ok := reg.Venue(venue)
			if !ok || v.REST == nil {
				return nil, false
			}
			if base := restOverrides[venue]; base != "" {
				return conn.WithRESTBase(v.REST, base), true
			}
			return v.REST, true
		},
		Doer:     doer,
		RateHold: *hold,
		Publish:  nc.Publish,
		Subscribe: func(subject string, handler func(reply string, req []byte)) (func(), error) {
			sub, err := nc.Subscribe(ctx, subject, "", func(m natsx.Msg) {
				if m.Reply == "" {
					return
				}
				req := append([]byte(nil), m.Data...)
				handler(m.Reply, req)
			})
			if err != nil {
				return nil, err
			}
			return func() { _ = sub.Unsubscribe() }, nil
		},
		Symbols: func(ctx context.Context, venue string) (conn.Instruments, error) {
			v, ok := reg.Venue(venue)
			if !ok {
				return nil, fmt.Errorf("unknown venue %s", venue)
			}
			tables := make([]sym.Table, 0, len(v.Categories))
			for _, c := range v.Categories {
				tables = append(tables, sym.Table{Venue: venue, Category: c})
			}
			// One venue per conn worker. The instance JWT allows every venue (E04-D057).
			notify, err := sym.ChangedVenueWildcard(venue)
			if err != nil {
				return nil, err
			}
			return sym.OpenRemote(ctx, nc, tables, sym.RemoteOptions{Log: log, NotifySubject: notify})
		},
		Log:     &log,
		Release: buildinfo.Version,
		Bus:     bus,
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
