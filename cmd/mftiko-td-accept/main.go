// Command mftiko-td-accept drives the TD trade-path acceptance against a running stack (E6-12,
// E6-13, E6-14; epic #8): an account and its demand written by hand, orders sent on td.order as a
// session would, td.evt watched for seq per incarnation, td.view read, the private stream cut from
// outside the account worker, and the venue account switched to hedge mode. deploy/accept/README.md
// is the runbook. It is a dev tool: it reuses the observer's all-subjects NATS user and etcd root.
//
//	mftiko-td-accept setup   [-topology FILE]                etcd role and user for -instance (TD), auth on
//	mftiko-td-accept account -api-id N -kind K -seal-key F   seal credentials JSON from stdin, write the account
//	mftiko-td-accept demand  put|del SESSION API_ID          write or delete one TD demand entry
//	mftiko-td-accept acks                                    print the instance's acks
//	mftiko-td-accept submit  -api-id N -ticker T ...         one Submit on td.order; prints its client_order_id
//	mftiko-td-accept cancel  -api-id N -cid C                one Cancel on td.order
//	mftiko-td-accept batch   -api-id N -cid C1,C2,...        one BatchCancel on td.order
//	mftiko-td-accept view    -api-id N oms|ledger            ask td.view
//	mftiko-td-accept watch   -api-id N [-out FILE]           follow td.evt.N, check seq per incarnation
//	mftiko-td-accept conns                                   list account workers: pid, venue sockets
//	mftiko-td-accept cut     -pid PID -host H                shut down the worker's sockets to host H (Linux)
//	mftiko-td-accept mode    -venue V -creds F [-dual B]     read or set the position mode (BinanceUM, Bybit, Okx)
//
// Every subcommand exits 1 when its check fails, so a script can stop on it.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

var (
	etcdURL  = flag.String("etcd", envOr("MFTIKO_ETCD", "http://127.0.0.1:2379"), "etcd endpoint (default $MFTIKO_ETCD)")
	password = flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "password of root and of the user setup creates (default $MFTIKO_ETCD_PASSWORD)")
	natsURL  = flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL (default $NATS_URL)")
	creds    = flag.String("nats-creds", os.Getenv("MFTIKO_NATS_CREDS"), "NATS credentials of the observer (default $MFTIKO_NATS_CREDS)")
	instance = flag.String("instance", "td1", "TD instance")
)

var commands = map[string]func() error{
	"setup": setup, "account": account, "demand": demand, "acks": acks,
	"submit": submit, "cancel": cancel, "batch": batch, "view": view, "watch": watch,
	"conns": conns, "cut": cut, "mode": mode,
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	name := os.Args[1]
	if name == "-version" || name == "version" {
		fmt.Println(buildinfo.String())
		return
	}
	run, ok := commands[name]
	if !ok {
		usage()
	}
	os.Args = append(os.Args[:1], os.Args[2:]...)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-td-accept "+name+":", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mftiko-td-accept setup|account|demand|acks|submit|cancel|batch|view|watch|conns|cut|mode [flags]")
	os.Exit(2)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// stamp is the wall clock every printed line starts with.
func stamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// dial is the session's raw connection: td.order, td.view and td.evt carry bare protobuf, not
// natsx's reply framing.
func dial() (*nats.Conn, error) {
	if *creds == "" {
		return nil, errors.New("-nats-creds is empty")
	}
	return nats.Connect(*natsURL, nats.UserCredentials(*creds), nats.Name("mftiko-td-accept"),
		nats.CustomInboxPrefix("_INBOX_accept"))
}

// dialx is the same user through natsx, for SYM's framed replies.
func dialx() (*natsx.Conn, error) {
	if *creds == "" {
		return nil, errors.New("-nats-creds is empty")
	}
	return natsx.Connect(*natsURL, natsx.Options{Name: "mftiko-td-accept", InboxPrefix: "_INBOX_accept", CredsFile: *creds, Logger: slog.New(slog.DiscardHandler)})
}
