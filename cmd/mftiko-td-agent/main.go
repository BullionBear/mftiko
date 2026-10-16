// Command mftiko-td-agent is the execution-plane agent: one account worker per api_id a session
// needs, started by its owners and stopped linger after the last one leaves (DECISIONS D901, D905,
// D608). It does not connect to NATS and does not store fills (D638, D639).
//
// It unseals each account's credentials with this instance's key (-seal-key, D613), signs each
// worker's NATS user with the MFTIKO account's signing key (-nats-account, -nats-signing-key, D611),
// and gives the worker this instance's hosts for its account kind (-endpoints, -rest-endpoints,
// -simulated, D616). A running worker keeps the binary it was spawned with (P9).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/td/agent"
)

type options struct {
	instance, topology, password, workDir, shim, account string
	natsURL, natsAccount, natsSigningKey, sealKey        string
	endpoints, restEndpoints                             string
	simulated                                            bool
	linger                                               time.Duration
}

func main() {
	var o options
	version := flag.Bool("version", false, "print the release and exit")
	flag.StringVar(&o.instance, "instance", "", "TD instance name")
	flag.StringVar(&o.topology, "topology", "", "deployment topology JSON")
	flag.StringVar(&o.password, "etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "etcd password (default $MFTIKO_ETCD_PASSWORD)")
	flag.StringVar(&o.workDir, "work-dir", "", "sockets, exit files, and worker logs")
	flag.StringVar(&o.shim, "shim", "", "mftiko-shim executable")
	flag.StringVar(&o.account, "account", "", "mftiko-td-account executable")
	flag.StringVar(&o.natsURL, "nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL the account workers connect to (default $NATS_URL or the dev server)")
	flag.StringVar(&o.natsAccount, "nats-account", "", "the MFTIKO account's public key")
	flag.StringVar(&o.natsSigningKey, "nats-signing-key", "", "seed file of the MFTIKO account's signing key, which signs each account worker's user")
	flag.StringVar(&o.sealKey, "seal-key", "", "this instance's private key for sealed credentials (0600, mftiko-seal keygen)")
	flag.StringVar(&o.endpoints, "endpoints", "", "JSON object of account kind to private websocket URL; empty uses the venue default")
	flag.StringVar(&o.restEndpoints, "rest-endpoints", "", "JSON object of account kind to REST URL; empty uses the venue default")
	flag.BoolVar(&o.simulated, "simulated", false, "use the venue's demo trading mode where it is a flag (OKX)")
	flag.DurationVar(&o.linger, "linger", agent.DefaultLinger, "how long an account with no owners keeps its worker")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-td-agent")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o, log); err != nil && !errors.Is(err, context.Canceled) {
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

func run(ctx context.Context, o options, log zerolog.Logger) error {
	switch {
	case o.instance == "" || o.topology == "" || o.password == "" || o.workDir == "" || o.shim == "" || o.account == "":
		return errors.New("instance, topology, etcd password, work dir, shim, and account are required")
	case o.natsURL == "" || o.natsAccount == "" || o.natsSigningKey == "":
		return errors.New("nats URL, account, and signing key are required")
	case o.sealKey == "":
		return errors.New("seal key is required")
	}
	topo, err := readTopology(o.topology)
	if err != nil {
		return err
	}
	for _, p := range []*string{&o.workDir, &o.shim, &o.account} {
		if *p, err = cleanAbs(*p); err != nil {
			return err
		}
	}
	key, err := sealbox.LoadKeyFile(o.sealKey)
	if err != nil {
		return err
	}
	signer, err := natsdev.LoadSigner(o.natsAccount, o.natsSigningKey)
	if err != nil {
		return err
	}
	endpoints, err := readEndpoints(o.endpoints, o.restEndpoints, o.simulated)
	if err != nil {
		return err
	}
	reg := all.Registry()
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "td-agent"
	}
	return agent.Run(ctx, agent.Config{
		Topology: topo, Instance: o.instance, Dial: state.AuthDial(o.instance, o.password),
		Term:    state.TermOptions{ID: fmt.Sprintf("%s:%d", host, os.Getpid())},
		WorkDir: o.workDir, Shim: o.shim, Account: o.account,
		WorkerEnv: []string{"MFTIKO_NATS_URL=" + o.natsURL},
		Kinds:     reg, Endpoints: endpoints,
		Open: func(sealed []byte, id int64, kind string) error {
			_, err := sealbox.OpenCredentials(key, sealed, id, kind)
			return err
		},
		Unseal: func(sealed []byte, id int64, kind string) (sealbox.Credentials, error) {
			return sealbox.OpenCredentials(key, sealed, id, kind)
		},
		User:    signer.AccountWorker,
		Linger:  o.linger,
		Release: buildinfo.Version, Log: log,
	})
}

// readEndpoints merges the two files, each a JSON object of account kind to URL, and -simulated.
func readEndpoints(stream, rest string, simulated bool) (map[string]agent.Endpoint, error) {
	out := map[string]agent.Endpoint{}
	for _, f := range []struct {
		path string
		set  func(*agent.Endpoint, string)
	}{
		{stream, func(e *agent.Endpoint, u string) { e.Stream = u }},
		{rest, func(e *agent.Endpoint, u string) { e.REST = u }},
	} {
		if f.path == "" {
			continue
		}
		b, err := os.ReadFile(f.path)
		if err != nil {
			return nil, err
		}
		var m map[string]string
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("endpoints %s: %w", f.path, err)
		}
		for kind, u := range m {
			e := out[kind]
			f.set(&e, u)
			out[kind] = e
		}
	}
	if simulated {
		for _, kind := range all.Registry().AccountKinds() {
			e := out[kind]
			e.Simulated = true
			out[kind] = e
		}
	}
	return out, nil
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

func cleanAbs(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}
