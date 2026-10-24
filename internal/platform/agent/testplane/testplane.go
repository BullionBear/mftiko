// Package testplane is the test plane of E1-19: an agent whose workers are fake workers (shim/fakeworker), run
// through the whole skeleton (Intake, Planner, Policy, Fleet, Reconciler) against etcd and real shims, with no
// exchange and no strategy. Its demand is a list of fake workers a test writes under the instance's demand/
// prefix itself, in place of the controller.
//
// The plane is one of the real ones (Config.Plane): etcd's roles and keys know no other.
package testplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Worker is one demand entry: a fake worker, named by the entry's key after demand/.
type Worker struct {
	Script fakeworker.Script `json:"script"`
	Live   string            `json:"live,omitempty"` // the live config: pushed until the worker says it has applied it
	Hold   bool              `json:"hold,omitempty"` // do not Start it: as an STS session before the controller's start
}

// Entry is a decoded demand entry.
type Entry struct {
	Name string
	Worker
}

// Spec is a worker's spawn spec: its name and script. A new script is a new incarnation.
type Spec struct {
	Name   string
	Script fakeworker.Script
}

func (s Spec) Hash() string {
	b, _ := json.Marshal(s) // fields in a fixed order: a deterministic encoding
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// Live is a worker's live config.
type Live string

func (l Live) Hash() string { return string(l) }

// Decode takes one demand entry, its value the JSON of a Worker; unknown fields reject it.
func Decode(key string, value []byte) (Entry, error) {
	name := key[strings.LastIndexByte(key, '/')+1:]
	if name == "" {
		return Entry{}, errors.New("testplane: no name")
	}
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	var w Worker
	if err := dec.Decode(&w); err != nil {
		return Entry{}, fmt.Errorf("testplane: %s: %w", name, err)
	}
	return Entry{Name: name, Worker: w}, nil
}

// Key is the worker key of the entry named name: {plane}/{name}.
func Key(p state.Plane, name string) agent.WorkerKey { return agent.WorkerKey(string(p) + "/" + name) }

// Planner copies the demand into plans, one worker per entry.
type Planner struct {
	Plane state.Plane
}

func (p Planner) Plan(d agent.Demand[Entry]) map[agent.WorkerKey]agent.Plan[Spec, Live] {
	out := make(map[agent.WorkerKey]agent.Plan[Spec, Live], len(d.Items))
	for _, e := range d.Items {
		out[Key(p.Plane, e.Name)] = agent.Plan[Spec, Live]{
			Spawn: Spec{Name: e.Name, Script: e.Script}, Live: Live(e.Live), Start: !e.Hold,
		}
	}
	return out
}

// Program runs the fake worker: the test binary, which turns into one through fakeworker.MaybeMain.
type Program struct {
	Binary     string // the executable that runs fakeworker.MaybeMain first
	JournalDir string // each worker appends to {JournalDir}/{name}.journal, across incarnations
}

func (p Program) Launch(_ agent.WorkerKey, s Spec) (*shimv1.Launch, error) {
	env, err := fakeworker.Env(s.Script, p.Journal(s.Name))
	if err != nil {
		return nil, err
	}
	return &shimv1.Launch{Path: p.Binary, Dir: "/", Env: env}, nil
}

// Journal is where the worker named name writes its journal.
func (p Program) Journal(name string) string { return filepath.Join(p.JournalDir, name+".journal") }

func (Program) Init(_ agent.WorkerKey, _ Spec, live Live) (*workerv1.Init, error) {
	return &workerv1.Init{Contract: fakeworker.Contract, Payload: []byte(live)}, nil
}

func (Program) Start(agent.WorkerKey, Spec) (*workerv1.Start, error) {
	return &workerv1.Start{Contract: fakeworker.Contract}, nil
}

func (Program) Push(_ agent.WorkerKey, live Live) (*workerv1.Live, error) {
	return &workerv1.Live{Contract: fakeworker.Contract, Payload: []byte(live)}, nil
}

// Config is one test-plane agent.
type Config struct {
	Topology state.Topology
	Plane    state.Plane
	Instance string
	Dial     state.DialFunc
	Term     state.TermOptions

	WorkDir string
	Shim    string // the mftiko-shim executable
	ShimEnv []string
	Program Program
	Policy  agent.Policy // agent.Always with a 100ms backoff if nil

	Resync     time.Duration
	StaleAfter time.Duration
	LiveRetry  time.Duration
	Mirror     state.MirrorOptions
	Started    func(pid int) // each shim's pid
	Log        zerolog.Logger
}

// Run is the agent's main: connect to its own region and win the term, mirror its demand, and run the skeleton
// until the term ends (runner.ErrTermEnded) or ctx is done. The shims and their workers outlive it (E01-D002).
func Run(ctx context.Context, cfg Config) error {
	cl, term, err := runner.Connect(ctx, cfg.Topology, cfg.Plane, cfg.Instance, cfg.Dial, cfg.Term)
	if err != nil {
		return err
	}
	defer cl.Close()
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		term.Close(cctx) //nolint:errcheck // the record goes with the session either way
		cancel()
	}()
	home, err := runner.Home(cl, cfg.Instance)
	if err != nil {
		return err
	}
	reader, err := cl.Reader(home)
	if err != nil {
		return err
	}
	mopt := cfg.Mirror
	if mopt.Log == nil {
		mopt.Log = &cfg.Log
	}
	m, err := state.NewMirror(reader, state.DemandPrefix(cfg.Plane, cfg.Instance), mopt)
	if err != nil {
		return err
	}
	mctx, stop := context.WithCancel(ctx)
	defer stop()
	go m.Run(mctx) //nolint:errcheck // ends with mctx
	numbers, err := state.LoadIncarnations(ctx, term)
	if err != nil {
		return err
	}
	f := fleet.New(fleet.Config[Spec, Live]{
		Plane: cfg.Plane, Instance: cfg.Instance, Epoch: term.Epoch(), Release: "testplane",
		WorkDir: cfg.WorkDir, Shim: cfg.Shim, ShimEnv: cfg.ShimEnv, Numbers: numbers, Program: cfg.Program,
		Started: cfg.Started, Log: cfg.Log,
	})
	defer f.Close()
	policy := cfg.Policy
	if policy == nil {
		policy = agent.Always{Backoff: agent.Backoff{Min: 100 * time.Millisecond, Max: time.Second}}
	}
	return runner.Run(ctx, runner.Config[Entry, Spec, Live]{
		Term: term, Demand: m, Statuses: reader, Decode: Decode, Planner: Planner{Plane: cfg.Plane},
		Policy: policy, Fleet: f, Release: "testplane", WorkDir: cfg.WorkDir,
		Resync: cfg.Resync, StaleAfter: cfg.StaleAfter, LiveRetry: cfg.LiveRetry, Log: cfg.Log,
	})
}
