// Command mftiko-md-agent places feed demand on conn workers (D901, D403).
//
// It resolves each feed through SYM, writes the placement before the conn worker is pushed
// (D110), and spawns mftiko-md-conn. A running worker keeps the binary it was spawned with (P9).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/internal/md/agent"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/sym"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	instance := flag.String("instance", "", "MD instance name")
	topology := flag.String("topology", "", "deployment topology JSON")
	password := flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "etcd password (default $MFTIKO_ETCD_PASSWORD)")
	workDir := flag.String("work-dir", "", "sockets, exit files, and worker logs")
	shim := flag.String("shim", "", "mftiko-shim executable")
	conn := flag.String("conn", "", "mftiko-md-conn executable")
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL (default $NATS_URL or the dev server)")
	creds := flag.String("nats-creds", os.Getenv("MFTIKO_NATS_CREDS"), "NATS user credentials file (default $MFTIKO_NATS_CREDS)")
	endpoints := flag.String("endpoints", "", "JSON object of venue to websocket URL override")
	linger := flag.Duration("linger", agent.DefaultLinger, "how long an atom with no owners stays subscribed")
	bookDepth := flag.Uint("book-depth", 0, "orderbook top-N for this MD instance (D426); required")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-md-agent")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *instance, *topology, *password, *workDir, *shim, *conn, *natsURL, *creds, *endpoints, *linger, *bookDepth, log); err != nil && !errors.Is(err, context.Canceled) {
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

func run(ctx context.Context, instance, topology, password, workDir, shimPath, connPath, natsURL, creds, endpoints string, linger time.Duration, bookDepth uint, log zerolog.Logger) error {
	switch {
	case instance == "" || topology == "" || password == "" || workDir == "" || shimPath == "" || connPath == "":
		return errors.New("instance, topology, etcd password, work dir, shim, and conn are required")
	case natsURL == "" || creds == "":
		return errors.New("nats URL and creds are required")
	case bookDepth == 0 || bookDepth > math.MaxUint32:
		return errors.New("book-depth is required")
	}
	topo, err := readTopology(topology)
	if err != nil {
		return err
	}
	workDir, err = cleanAbs(workDir)
	if err != nil {
		return err
	}
	shimPath, err = cleanAbs(shimPath)
	if err != nil {
		return err
	}
	connPath, err = cleanAbs(connPath)
	if err != nil {
		return err
	}
	overrides, err := readOverrides(endpoints)
	if err != nil {
		return err
	}
	inbox, err := natsdev.InboxPrefix(natsdev.PlaneMD)
	if err != nil {
		return err
	}
	nc, err := natsx.Connect(natsURL, natsx.Options{
		Name:        "mftiko-md-agent",
		Logger:      slog.Default(),
		CredsFile:   creds,
		InboxPrefix: inbox,
	})
	if err != nil {
		return err
	}
	defer nc.Close()

	reg := all.Registry()
	var tables []sym.Table
	for _, name := range reg.Venues() {
		v, ok := reg.Venue(name)
		if !ok {
			continue
		}
		for _, c := range v.Categories {
			tables = append(tables, sym.Table{Venue: name, Category: c})
		}
	}
	remote, err := sym.OpenRemote(ctx, nc, tables, sym.RemoteOptions{Log: log})
	if err != nil {
		return err
	}
	defer remote.Close()

	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "md-agent"
	}
	return agent.Run(ctx, agent.Config{
		Topology: topo, Instance: instance, Dial: state.AuthDial(instance, password),
		Term:    state.TermOptions{ID: fmt.Sprintf("%s:%d", host, os.Getpid())},
		WorkDir: workDir, Shim: shimPath, Conn: connPath,
		WorkerEnv: []string{"MFTIKO_NATS_URL=" + natsURL, "MFTIKO_NATS_CREDS=" + creds},
		Resolver:  remote, Markets: agent.MarketsFrom(reg), Overrides: overrides,
		Linger: linger, BookDepth: uint32(bookDepth), Release: buildinfo.Version, Log: log,
	})
}

func readTopology(path string) (state.Topology, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return state.Topology{}, err
	}
	var topo state.Topology
	if err := json.Unmarshal(b, &topo); err != nil {
		return state.Topology{}, fmt.Errorf("topology: %w", err)
	}
	if err := topo.Validate(); err != nil {
		return state.Topology{}, err
	}
	return topo, nil
}

func readOverrides(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out map[string]string
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("endpoints: %w", err)
	}
	return out, nil
}

func cleanAbs(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}
