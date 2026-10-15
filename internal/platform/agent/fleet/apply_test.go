package fleet

import (
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

func done(verb workerv1.Verb, ok bool) *shimv1.Event {
	return &shimv1.Event{Kind: &shimv1.Event_Done{Done: &workerv1.Done{Verb: verb, Ok: ok}}}
}

// The events of a worker's life, applied in order, walk its observation through the states of PROTOCOL §4.3.
func TestApply(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	exit := &commonv1.ExitInfo{Code: 0, Outcome: commonv1.Outcome_OUTCOME_DONE}
	o := agent.Observed{State: agent.StateInitializing}
	for _, step := range []struct {
		ev   *shimv1.Event
		want agent.State
	}{
		{&shimv1.Event{AtNs: at.UnixNano(), Kind: &shimv1.Event_Started{Started: &shimv1.Started{Pid: 1}}}, agent.StateInitializing},
		{&shimv1.Event{Kind: &shimv1.Event_Hello{Hello: &workerv1.Hello{Release: "r1"}}}, agent.StateInitializing},
		{&shimv1.Event{Kind: &shimv1.Event_Stage{Stage: &workerv1.Stage{Name: "warm"}}}, agent.StateInitializing},
		{&shimv1.Event{Kind: &shimv1.Event_Applied{Applied: &workerv1.Applied{Digest: "l1"}}}, agent.StateInitializing},
		{done(workerv1.Verb_VERB_INIT, true), agent.StateInitialized},
		{&shimv1.Event{AtNs: at.Add(time.Second).UnixNano(), Kind: &shimv1.Event_Beat{Beat: &workerv1.Beat{Seq: 1}}}, agent.StateInitialized},
		{done(workerv1.Verb_VERB_START, true), agent.StateRunning},
		{&shimv1.Event{Kind: &shimv1.Event_Exiting{Exiting: &workerv1.Exiting{}}}, agent.StateStopping},
		{done(workerv1.Verb_VERB_START, true), agent.StateStopping}, // late: stopping stays
		{&shimv1.Event{Kind: &shimv1.Event_Exited{Exited: &shimv1.Exited{Exit: exit}}}, agent.StateExited},
		{&shimv1.Event{Kind: &shimv1.Event_Stopped{Stopped: &workerv1.Stopped{}}}, agent.StateExited},
	} {
		apply(&o, step.ev)
		if o.State != step.want {
			t.Fatalf("after %v: %s, want %s", step.ev, o.State, step.want)
		}
	}
	if !o.StartedAt.Equal(at) || o.Release != "r1" || o.Stage != "warm" || o.Exit != exit ||
		!o.LastBeat.Equal(at.Add(time.Second)) || o.LiveHash != "l1" {
		t.Fatalf("observed %+v", o)
	}

	o = agent.Observed{State: agent.StateInitializing}
	apply(&o, done(workerv1.Verb_VERB_INIT, false))
	if o.State != agent.StateStopping {
		t.Fatalf("a failed Init: %s, want stopping", o.State)
	}
}

// A worker's Hello may come before the Fleet attaches to its new shim: the snapshot has it, and the events of it are
// skipped, so the observation of a spawned worker starts from the snapshot.
func TestSpawnedFromSnapshot(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	at := &shimv1.Attached{State: shimv1.WorkerState_WORKER_STATE_RUNNING, Phase: shimv1.Phase_PHASE_NEW,
		Worker: &commonv1.WorkerRef{Key: "td/1", Incarnation: 4}, SpecHash: "s", ShimRelease: "r",
		Hello: &workerv1.Hello{Release: "w"}, Stage: "boot", LastBeatAtNs: now.UnixNano()}
	o := spawned(at, "s", "l", now)
	if o.Key != "td/1" || o.Incarnation != 4 || o.State != agent.StateInitializing || o.Release != "w" ||
		o.Stage != "boot" || !o.LastBeat.Equal(now) || o.ShimRelease != "r" || o.SpecHash != "s" ||
		o.Pushed != "l" || !o.PushedAt.Equal(now) || !o.StartedAt.Equal(now) || o.LiveHash != "" {
		t.Fatalf("spawned %+v", o)
	}
}

// A worker that exits before the Fleet attaches to its new shim is exited in the snapshot, and its Exited event is
// skipped: the observation is exited, with the exit, not initializing.
func TestSpawnedExitedBeforeAttach(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	exit := &commonv1.ExitInfo{Code: 20, Outcome: commonv1.Outcome_OUTCOME_CRASHED}
	at := &shimv1.Attached{State: shimv1.WorkerState_WORKER_STATE_EXITED, Phase: shimv1.Phase_PHASE_NEW,
		Worker: &commonv1.WorkerRef{Key: "td/1", Incarnation: 4}, SpecHash: "s", Exit: exit}
	o := spawned(at, "s", "l", now)
	if o.State != agent.StateExited || o.Exit.GetCode() != 20 || o.Incarnation != 4 {
		t.Fatalf("spawned %+v", o)
	}
}
