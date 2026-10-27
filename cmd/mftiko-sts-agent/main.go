// Command mftiko-sts-agent is the strategy-plane agent (DECISIONS E09-D016, E09-D042–E09-D065).
// It runs the sessions the controller assigns to this instance and writes SessionStatus.
// Each session worker is the strategy artifact named by its spec (E09-D060).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/sts"
	"github.com/BullionBear/mftiko/internal/sts/agent"
	"github.com/BullionBear/mftiko/internal/sts/issuer"
)

type options struct {
	instance, topology, password         string
	workDir, shim, dataDir               string
	registryCreds, dataRepo              string
	plainHTTPHosts                       []string
	maxArtifactBytes                     int64
	recvWindowMS                         int
	hookTimeoutS                         int
	startTimeoutS                        int
	volumeMB                             int
	volumeInterval                       time.Duration
	grace                                time.Duration
	backoffMin, backoffMax               time.Duration
	maxRestarts                          int
	restartWindowS                       int
	artifactPoll                         time.Duration
	natsURL, natsAccount, natsSigningKey string
	logs                                 agent.LogConfig
}

func main() {
	o, version, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-sts-agent")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Msg("exit")
		os.Exit(1)
	}
}

func parseArgs(args []string) (options, bool, error) {
	var o options
	fs := flag.NewFlagSet("mftiko-sts-agent", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.Bool("version", false, "print the release and exit")
	fs.StringVar(&o.instance, "instance", "", "STS instance name")
	fs.StringVar(&o.topology, "topology", "", "deployment topology JSON")
	fs.StringVar(&o.password, "etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "etcd password (default $MFTIKO_ETCD_PASSWORD)")
	fs.StringVar(&o.workDir, "work-dir", "", "sockets, exit files, and worker logs")
	fs.StringVar(&o.shim, "shim", "", "mftiko-shim executable")
	fs.StringVar(&o.dataDir, "data", "", "artifact cache and session directories (${DATA})")
	fs.StringVar(&o.registryCreds, "registry-creds", "", "Docker config.json; one file, one auth entry per registry host")
	fs.StringVar(&o.dataRepo, "data-repository", "", "data artifact repository, host[:port]/path")
	var plainHosts string
	fs.StringVar(&plainHosts, "plain-http-hosts", "", "comma-separated registry hosts that use plain HTTP")
	fs.StringVar(&o.natsURL, "nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL session workers connect to (default $NATS_URL or the dev server)")
	fs.StringVar(&o.natsAccount, "nats-account", "", "the MFTIKO account's public key")
	fs.StringVar(&o.natsSigningKey, "nats-signing-key", "", "seed file of the MFTIKO account's signing key")
	fs.Int64Var(&o.maxArtifactBytes, "max-artifact-bytes", agent.DefaultMaxArtifactBytes, "artifact size limit")
	fs.IntVar(&o.recvWindowMS, "recv-window-ms", agent.DefaultRecvWindowMS, "default recv_window_ms")
	fs.IntVar(&o.hookTimeoutS, "hook-timeout-s", agent.DefaultHookTimeoutS, "default hook_timeout_s")
	fs.IntVar(&o.startTimeoutS, "start-timeout-s", agent.DefaultStartTimeoutS, "default start_timeout_s")
	fs.IntVar(&o.volumeMB, "volume-mb", agent.DefaultVolumeMB, "default volume_mb")
	fs.DurationVar(&o.volumeInterval, "volume-interval", agent.DefaultVolumeInterval, "how often to measure session volumes")
	fs.DurationVar(&o.grace, "grace", agent.DefaultGrace, "Stop grace passed to the fleet")
	fs.DurationVar(&o.backoffMin, "backoff-min", agent.DefaultBackoffMin, "restart backoff, first wait")
	fs.DurationVar(&o.backoffMax, "backoff-max", agent.DefaultBackoffMax, "restart backoff, upper bound")
	fs.IntVar(&o.maxRestarts, "max-restarts", agent.DefaultMaxRestarts, "default max_restarts")
	fs.IntVar(&o.restartWindowS, "restart-window", agent.DefaultRestartWindowS, "default restart window, in seconds")
	fs.DurationVar(&o.artifactPoll, "artifact-poll", time.Minute, "how often to resolve the latest tag")
	var logListen, logAdvertise, logToken string
	var logStreams, logRate, logKeep int
	var logBytes uint64
	fs.StringVar(&logListen, "log-listen", agent.DefaultLogListen, "log endpoint listen address; empty disables it")
	fs.StringVar(&logAdvertise, "log-advertise", "", "host:port published as AgentRecord.log_addr")
	fs.StringVar(&logToken, "log-token-sha256", "", "SHA-256 of the log bearer token, 64 lowercase hex")
	fs.IntVar(&logStreams, "log-max-streams", agent.DefaultLogStreams, "concurrent log reads and exports")
	fs.IntVar(&logRate, "log-max-rate", agent.DefaultLogRate, "log read rate, bytes per second")
	fs.Uint64Var(&logBytes, "log-max-bytes", 0, "shim log rotation size; 0 uses the shim default")
	fs.IntVar(&logKeep, "log-keep", 0, "shim log files to keep, including the live file; 0 uses the shim default")
	if err := fs.Parse(args); err != nil {
		return options{}, false, err
	}
	if *version {
		return o, true, nil
	}
	if o.instance == "" || o.topology == "" || o.password == "" || o.workDir == "" || o.shim == "" || o.dataDir == "" {
		return o, false, errors.New("instance, topology, etcd password, work dir, shim, and data dir are required")
	}
	if o.natsURL == "" || o.natsAccount == "" || o.natsSigningKey == "" {
		return o, false, errors.New("nats URL, account, and signing key are required")
	}
	hosts, err := splitHosts(plainHosts)
	if err != nil {
		return o, false, err
	}
	o.plainHTTPHosts = hosts
	if o.dataRepo != "" {
		if _, _, err := artifact.SplitRepository(o.dataRepo); err != nil {
			return o, false, fmt.Errorf("data-repository: %w", err)
		}
	}
	if err := agent.CheckLogKeep(logKeep); err != nil {
		return o, false, err
	}
	if logBytes != 0 && logBytes < shim.MinOutputMaxBytes {
		return o, false, errors.New("sts: log max bytes")
	}
	o.logs = agent.LogConfig{
		Listen: logListen, Advertise: logAdvertise, TokenSHA256: logToken,
		MaxStreams: logStreams, MaxRate: logRate, MaxBytes: logBytes, Keep: uint32(logKeep),
	}
	if err := o.logs.Validate(); err != nil {
		return o, false, err
	}
	return o, false, nil
}

func splitHosts(s string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, errors.New("plain-http host is empty")
		}
		if err := artifact.CheckHost(p); err != nil {
			return nil, fmt.Errorf("plain-http host: %w", err)
		}
		if _, ok := seen[p]; ok {
			return nil, fmt.Errorf("duplicate plain http host %q", p)
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(ctx context.Context, o options, log zerolog.Logger) error {
	topo, err := readTopology(o.topology)
	if err != nil {
		return err
	}
	for _, p := range []*string{&o.workDir, &o.shim, &o.dataDir} {
		if *p, err = cleanAbs(*p); err != nil {
			return err
		}
	}
	signer, err := natsdev.LoadSigner(o.natsAccount, o.natsSigningKey)
	if err != nil {
		return err
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "sts-agent"
	}
	return agent.Run(ctx, agent.Config{
		Topology: topo, Instance: o.instance, Dial: state.AuthDial(o.instance, o.password),
		Term:    state.TermOptions{ID: fmt.Sprintf("%s:%d", host, os.Getpid())},
		WorkDir: o.workDir, Shim: o.shim, DataDir: o.dataDir,
		WorkerEnv: []string{"MFTIKO_NATS_URL=" + o.natsURL},
		Sign: func(id string, perms sts.Permissions) (string, string, error) {
			return issuer.SignSession(id, perms, signer.AccountPublic(), signer.SigningKey())
		},
		RegistryCreds: o.registryCreds, DataRepository: o.dataRepo, PlainHTTPHosts: o.plainHTTPHosts,
		MaxArtifactBytes: o.maxArtifactBytes,
		RecvWindowMS:     o.recvWindowMS, HookTimeoutS: o.hookTimeoutS, StartTimeoutS: o.startTimeoutS,
		VolumeMB: o.volumeMB, VolumeInterval: o.volumeInterval, Grace: o.grace, MaxRestarts: o.maxRestarts, RestartWindowS: o.restartWindowS,
		BackoffMin: o.backoffMin, BackoffMax: o.backoffMax, ArtifactPoll: o.artifactPoll,
		Logs:    o.logs,
		Release: buildinfo.Version, Log: log,
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

func cleanAbs(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}
