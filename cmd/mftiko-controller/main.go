// Command mftiko-controller is the control-plane controller (DECISIONS D907, D1301).
// It reads the deployment JSON mftiko-etcd-roles reads, campaigns as controller in
// the home region, and writes its AgentRecord once. It does not derive demands yet.
//
// SIGTERM revokes the term and exits 0. ErrSuperseded, or a lost election session,
// exits non-zero so systemd restarts the process (D1315). A process that has not
// been elected waits in Campaign.
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
	"syscall"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/controller/daemon"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	in := flag.String("in", "", "deployment JSON, the same file as mftiko-etcd-roles; - for stdin")
	password := flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "password of the etcd user \"controller\" (default $MFTIKO_ETCD_PASSWORD)")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-controller")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *in, *password, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Msg("exit")
		os.Exit(1)
	}
}

// deployment is the mftiko-etcd-roles input. Planes is accepted so the same file
// decodes; the controller uses Topology only (D1312).
type deployment struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

func run(ctx context.Context, in, password string, log zerolog.Logger) error {
	if in == "" || password == "" {
		return errors.New("-in and the etcd password are required")
	}
	topo, err := readDeployment(in, os.Stdin)
	if err != nil {
		return err
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "controller"
	}
	return daemon.Run(ctx, daemon.Config{
		Topology: topo,
		Dial:     state.AuthDial(state.ControllerInstance, password),
		Term:     state.TermOptions{ID: fmt.Sprintf("%s:%d", host, os.Getpid())},
		Release:  buildinfo.Version,
		Log:      log,
	})
}

func readDeployment(in string, stdin io.Reader) (state.Topology, error) {
	r := stdin
	if in != "-" {
		f, err := os.Open(in)
		if err != nil {
			return state.Topology{}, err
		}
		defer f.Close()
		r = f
	}
	var d deployment
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return state.Topology{}, fmt.Errorf("read deployment: %w", err)
	}
	if err := d.Validate(); err != nil {
		return state.Topology{}, err
	}
	return d.Topology, nil
}
