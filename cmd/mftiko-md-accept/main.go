// Command mftiko-md-accept drives the MD feed-path acceptance against a running stack (E4-09,
// E4-10, E4-11; epic #6): demand written by hand, md.f watched for seq and conn_epoch, a late
// join through md.view, a websocket cut from outside the conn worker, and an agent rotation.
// deploy/accept/README.md is the runbook. It is a dev tool: it signs an all-subjects NATS user
// and gives every etcd user it creates one password.
//
//	mftiko-md-accept setup   [-topology FILE]           etcd roles and users for -instance, root, auth on
//	mftiko-md-accept creds   -nats-dir DIR -out FILE    sign an all-subjects NATS user (observer, SYM)
//	mftiko-md-accept refresh -venue V [-ticker T]       ask SYM to refresh V; print T's uid and price_tick
//	mftiko-md-accept demand  put|del SESSION FEED       write or delete one feed demand entry
//	mftiko-md-accept acks                               print the instance's acks and placements
//	mftiko-md-accept watch   [-out FILE]                follow md.f.{inst}.>, check seq and conn_epoch
//	mftiko-md-accept late    -feed FEED                 join through md.view, then follow md.f
//	mftiko-md-accept conns                              list conn workers: key, pid, venue sockets
//	mftiko-md-accept cut     -conn KEY | -pid PID       shut down a conn worker's venue sockets (Linux)
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
	password = flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "password of root and of the users setup creates (default $MFTIKO_ETCD_PASSWORD)")
	natsURL  = flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL (default $NATS_URL)")
	creds    = flag.String("nats-creds", os.Getenv("MFTIKO_NATS_CREDS"), "NATS credentials of the observer (default $MFTIKO_NATS_CREDS)")
	instance = flag.String("instance", "dev", "MD instance; md.f and md.view subjects carry it")
)

var commands = map[string]func() error{
	"setup": setup, "creds": signCreds, "refresh": refresh, "demand": demand, "acks": acks,
	"watch": watch, "late": late, "conns": conns, "cut": cut,
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
		fmt.Fprintln(os.Stderr, "mftiko-md-accept "+name+":", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mftiko-md-accept setup|creds|refresh|demand|acks|watch|late|conns|cut [flags]")
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

// dial is the observer's raw connection: md.f carries the envelope and md.view answers bare
// protobuf, neither in natsx's reply framing.
func dial() (*nats.Conn, error) {
	if *creds == "" {
		return nil, errors.New("-nats-creds is empty")
	}
	return nats.Connect(*natsURL, nats.UserCredentials(*creds), nats.Name("mftiko-md-accept"),
		nats.CustomInboxPrefix("_INBOX_accept"))
}

// dialx is the same user through natsx, for SYM's framed replies.
func dialx() (*natsx.Conn, error) {
	if *creds == "" {
		return nil, errors.New("-nats-creds is empty")
	}
	return natsx.Connect(*natsURL, natsx.Options{Name: "mftiko-md-accept", InboxPrefix: "_INBOX_accept", CredsFile: *creds, Logger: slog.New(slog.DiscardHandler)})
}
