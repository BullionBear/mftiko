// Command mftiko-controller is the control-plane controller (DECISIONS E09-D007, E13-D001).
// It reads the deployment JSON mftiko-etcd-roles reads, campaigns as controller in
// the home region, writes its AgentRecord once, keeps the global view (E13-D017),
// and derives session demands, gates start and ends sessions (N1–N3), then
// deletes ended sessions and orphan demands (N4, N5).
//
// SIGTERM revokes the term and exits 0. ErrSuperseded, or a lost election session,
// exits non-zero so systemd restarts the process (E13-D015). A process that has not
// been elected waits in Campaign.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/controller/daemon"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	in := flag.String("in", "", "deployment JSON, the same file as mftiko-etcd-roles; - for stdin")
	password := flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "fallback password of the etcd user \"controller\" when MFTIKO_ETCD_PASSWORD_<region> is unset (default $MFTIKO_ETCD_PASSWORD)")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-controller")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *in, *password, log, os.Getenv); err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Msg("exit")
		os.Exit(1)
	}
}

// deployment is the mftiko-etcd-roles input. Planes names the plane of every
// instance except the controller; intake watches those prefixes (E13-D012, E13-D017).
type deployment struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

// regionPasswordPrefix is MFTIKO_ETCD_PASSWORD_<region> (E12-D045). The region
// name is appended as written in the deployment file.
const regionPasswordPrefix = "MFTIKO_ETCD_PASSWORD_"

func run(ctx context.Context, in, password string, log zerolog.Logger, getenv func(string) string) error {
	if in == "" {
		return errors.New("-in is required")
	}
	topo, planes, err := readDeployment(in, os.Stdin)
	if err != nil {
		return err
	}
	passwords, err := regionPasswords(slices.Sorted(maps.Keys(topo.Regions)), password, getenv)
	if err != nil {
		return err
	}
	dial, err := controllerDial(topo, passwords)
	if err != nil {
		return err
	}
	stale, err := staleOverrides(topo)
	if err != nil {
		return err
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "controller"
	}
	return daemon.Run(ctx, daemon.Config{
		Topology:      topo,
		Planes:        planes,
		Dial:          dial,
		Term:          state.TermOptions{ID: fmt.Sprintf("%s:%d", host, os.Getpid())},
		Release:       buildinfo.Version,
		StaleByRegion: stale,
		Log:           log,
	})
}

// regionPasswords resolves the etcd user "controller" in each region (E12-D045).
// The region variable wins. An empty value is unset, and the fallback is used.
// The fallback is -etcd-password, whose default is MFTIKO_ETCD_PASSWORD. A
// region that still has no password is an error. The fallback may be empty
// when every region has its own password.
func regionPasswords(regions []string, fallback string, getenv func(string) string) (map[string]string, error) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	out := make(map[string]string, len(regions))
	var missing []string
	for _, region := range regions {
		pw := getenv(regionPasswordPrefix + region)
		if pw == "" {
			pw = fallback
		}
		if pw == "" {
			missing = append(missing, region)
			continue
		}
		out[region] = pw
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("etcd password for %s is required (MFTIKO_ETCD_PASSWORD_<region>, or the fallback)", strings.Join(missing, ", "))
	}
	return out, nil
}

// controllerDial authenticates as "controller" with that region's password and
// the region's dial and request timeouts (E12-D045, E12-D046). A zero duration
// from Timeouts is the default inside state.Dial.
func controllerDial(topo state.Topology, passwords map[string]string) (state.DialFunc, error) {
	dials := make(map[string]state.DialFunc, len(topo.Regions))
	for _, name := range slices.Sorted(maps.Keys(topo.Regions)) {
		pw := passwords[name]
		if pw == "" {
			return nil, fmt.Errorf("etcd password for %s is required", name)
		}
		dialTO, reqTO, _, err := topo.Regions[name].Timeouts()
		if err != nil {
			return nil, err
		}
		dials[name] = state.Dial(state.DialConfig{
			User:           state.ControllerInstance,
			Password:       pw,
			DialTimeout:    dialTO,
			RequestTimeout: reqTO,
		})
	}
	return func(region string, endpoints []string) (*clientv3.Client, error) {
		d, ok := dials[region]
		if !ok {
			return nil, fmt.Errorf("state: dial %s: no controller dial", region)
		}
		return d(region, endpoints)
	}, nil
}

// staleOverrides collects positive stale_after values (E12-D046). Omitted and
// zero durations stay out of the map, so the controller default applies.
func staleOverrides(topo state.Topology) (map[string]time.Duration, error) {
	out := map[string]time.Duration{}
	for _, name := range slices.Sorted(maps.Keys(topo.Regions)) {
		_, _, stale, err := topo.Regions[name].Timeouts()
		if err != nil {
			return nil, err
		}
		if stale > 0 {
			out[name] = stale
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func readDeployment(in string, stdin io.Reader) (state.Topology, map[string]state.Plane, error) {
	r := stdin
	if in != "-" {
		f, err := os.Open(in)
		if err != nil {
			return state.Topology{}, nil, err
		}
		defer f.Close()
		r = f
	}
	var d deployment
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return state.Topology{}, nil, fmt.Errorf("read deployment: %w", err)
	}
	if err := d.Validate(); err != nil {
		return state.Topology{}, nil, err
	}
	return d.Topology, d.Planes, nil
}
