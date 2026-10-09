package fleet

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

type h string

func (x h) Hash() string { return string(x) }

func testFleet(t *testing.T) *Fleet[h, h] {
	wd, err := os.MkdirTemp("/tmp", "fl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	if err := os.MkdirAll(shim.RunDir(wd, "td"), 0o700); err != nil {
		t.Fatal(err)
	}
	return New(Config[h, h]{Plane: state.PlaneTD, Instance: "td-tw-1", Epoch: 2, WorkDir: wd, Log: zerolog.Nop()})
}

// A worker that ended while no agent was there, its shim gone too: Adopt observes it from the exit file, and
// Release removes the file. A socket left with no exit file is not adopted.
func TestAdoptExitFile(t *testing.T) {
	f := testFleet(t)
	key := agent.WorkerKey("td/7")
	exit := &commonv1.ExitInfo{Code: 20, Outcome: commonv1.Outcome_OUTCOME_CRASHED, Reason: "crash"}
	rec := &shimv1.ExitRecord{Worker: &commonv1.WorkerRef{Key: string(key), Incarnation: 4}, SpecHash: "s",
		Pid: 1 << 22, StartTime: 1, Exit: exit} // a pid no process has
	b, _ := proto.Marshal(rec)
	path := shim.ExitPath(f.cfg.WorkDir, "td", string(key))
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	stale := f.socket("td/8") // a shim killed before it recorded anything
	ln, err := net.Listen("unix", stale)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()

	if err := f.Adopt(context.Background()); err != nil {
		t.Fatal(err)
	}
	obs := f.Observe()
	o, ok := obs[key]
	if len(obs) != 1 || !ok || o.State != agent.StateExited || o.Incarnation != 4 || o.SpecHash != "s" ||
		!proto.Equal(o.Exit, exit) {
		t.Fatalf("observed %+v", obs)
	}
	if len(f.Unreachable()) != 0 {
		t.Fatalf("held %v", f.Unreachable())
	}
	if err := f.Release(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("exit file after Release: %v", err)
	}
	if len(f.Observe()) != 0 {
		t.Fatal("still observed after Release")
	}
}

// An exit file under another worker's hash is held, not adopted: its status stays, nothing is spawned over it.
func TestAdoptMisplacedExitFile(t *testing.T) {
	f := testFleet(t)
	rec := &shimv1.ExitRecord{Worker: &commonv1.WorkerRef{Key: "td/1", Incarnation: 1}, Exit: &commonv1.ExitInfo{}}
	b, _ := proto.Marshal(rec)
	h2 := state.WorkerHash("td/2")
	if err := os.WriteFile(filepath.Join(shim.RunDir(f.cfg.WorkDir, "td"), h2+".exit.pb"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.Adopt(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.Unreachable(); len(got) != 1 || got[0] != h2 || len(f.Observe()) != 0 {
		t.Fatalf("held %v, observed %v", got, f.Observe())
	}
	if err := f.Spawn(context.Background(), "td/2", "s", "l"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("Spawn over a held shim: %v", err)
	}
}

// What a shim's Attached says becomes the observation.
func TestObservedOf(t *testing.T) {
	for _, c := range []struct {
		state shimv1.WorkerState
		phase shimv1.Phase
		want  agent.State
	}{
		{shimv1.WorkerState_WORKER_STATE_STARTING, shimv1.Phase_PHASE_NEW, agent.StateInitializing},
		{shimv1.WorkerState_WORKER_STATE_RUNNING, shimv1.Phase_PHASE_INITIALIZING, agent.StateInitializing},
		{shimv1.WorkerState_WORKER_STATE_RUNNING, shimv1.Phase_PHASE_INITIALIZED, agent.StateInitialized},
		{shimv1.WorkerState_WORKER_STATE_RUNNING, shimv1.Phase_PHASE_STARTING, agent.StateStarting},
		{shimv1.WorkerState_WORKER_STATE_RUNNING, shimv1.Phase_PHASE_STARTED, agent.StateRunning},
		{shimv1.WorkerState_WORKER_STATE_RUNNING, shimv1.Phase_PHASE_STOPPING, agent.StateStopping},
		{shimv1.WorkerState_WORKER_STATE_EXITED, shimv1.Phase_PHASE_STARTED, agent.StateExited},
	} {
		at := &shimv1.Attached{State: c.state, Phase: c.phase, SpecHash: "s", Stage: "warm", ShimRelease: "r",
			LastBeatAtNs: 1_700_000_000_000_000_000,
			Worker:       &commonv1.WorkerRef{Key: "td/1", Incarnation: 3}, Hello: &workerv1.Hello{Release: "w"}}
		o := observedOf(at)
		if o.State != c.want || o.Key != "td/1" || o.Incarnation != 3 || o.SpecHash != "s" || o.Stage != "warm" ||
			o.Release != "w" || o.ShimRelease != "r" || o.LiveHash != "" || o.LastBeat.UnixNano() != at.GetLastBeatAtNs() {
			t.Errorf("%v %v: %+v, want %s", c.state, c.phase, o, c.want)
		}
	}
}
