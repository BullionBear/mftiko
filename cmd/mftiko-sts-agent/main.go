// Command mftiko-sts-agent is the strategy-plane agent (DECISIONS D916, D942–D949).
// It runs the sessions the controller assigns to this instance and writes SessionStatus.
//
// -worker is the executable every new spawn runs (D943). Registry flags and the
// deployment defaults are parsed and not applied yet: pulling the artifact and
// filling InitPayload is a later change.
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
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/sts/agent"
)

type options struct {
	instance, topology, password      string
	workDir, shim, worker             string
	registry, registryCreds, dataRepo string
	plainHTTP                         bool
	maxArtifactBytes                  int64
	recvWindowMS                      int
	hookTimeoutS                      int
	startTimeoutS                     int
	volumeMB                          int
	grace                             time.Duration
	maxRestarts                       int
	restartWindowS                    int
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
	fs.StringVar(&o.worker, "worker", "", "session worker executable")
	fs.StringVar(&o.registry, "registry", "", "OCI registry address (parsed, not used yet)")
	fs.StringVar(&o.registryCreds, "registry-creds", "", "registry credentials file (parsed, not read)")
	fs.StringVar(&o.dataRepo, "data-repository", "", "data artifact repository (parsed, not used yet)")
	fs.BoolVar(&o.plainHTTP, "plain-http", false, "talk to the registry over plain HTTP")
	fs.Int64Var(&o.maxArtifactBytes, "max-artifact-bytes", agent.DefaultMaxArtifactBytes, "artifact size limit")
	fs.IntVar(&o.recvWindowMS, "recv-window-ms", agent.DefaultRecvWindowMS, "default recv_window_ms")
	fs.IntVar(&o.hookTimeoutS, "hook-timeout-s", agent.DefaultHookTimeoutS, "default hook_timeout_s")
	fs.IntVar(&o.startTimeoutS, "start-timeout-s", agent.DefaultStartTimeoutS, "default start_timeout_s")
	fs.IntVar(&o.volumeMB, "volume-mb", agent.DefaultVolumeMB, "default volume_mb")
	fs.DurationVar(&o.grace, "grace", agent.DefaultGrace, "Stop grace passed to the fleet")
	fs.IntVar(&o.maxRestarts, "max-restarts", agent.DefaultMaxRestarts, "default max_restarts")
	fs.IntVar(&o.restartWindowS, "restart-window", agent.DefaultRestartWindowS, "default restart window, in seconds")
	if err := fs.Parse(args); err != nil {
		return options{}, false, err
	}
	if *version {
		return o, true, nil
	}
	if o.instance == "" || o.topology == "" || o.password == "" || o.workDir == "" || o.shim == "" || o.worker == "" {
		return o, false, errors.New("instance, topology, etcd password, work dir, shim, and worker are required")
	}
	return o, false, nil
}

func run(ctx context.Context, o options, log zerolog.Logger) error {
	topo, err := readTopology(o.topology)
	if err != nil {
		return err
	}
	for _, p := range []*string{&o.workDir, &o.shim, &o.worker} {
		if *p, err = cleanAbs(*p); err != nil {
			return err
		}
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "sts-agent"
	}
	return agent.Run(ctx, agent.Config{
		Topology: topo, Instance: o.instance, Dial: state.AuthDial(o.instance, o.password),
		Term:    state.TermOptions{ID: fmt.Sprintf("%s:%d", host, os.Getpid())},
		WorkDir: o.workDir, Shim: o.shim, Worker: o.worker,
		Registry: o.registry, RegistryCreds: o.registryCreds, DataRepository: o.dataRepo,
		PlainHTTP: o.plainHTTP, MaxArtifactBytes: o.maxArtifactBytes,
		RecvWindowMS: o.recvWindowMS, HookTimeoutS: o.hookTimeoutS, StartTimeoutS: o.startTimeoutS,
		VolumeMB: o.volumeMB, Grace: o.grace, MaxRestarts: o.maxRestarts, RestartWindowS: o.restartWindowS,
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
