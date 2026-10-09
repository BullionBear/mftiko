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
		!o.LastBeat.Equal(at.Add(time.Second)) {
		t.Fatalf("observed %+v", o)
	}

	o = agent.Observed{State: agent.StateInitializing}
	apply(&o, done(workerv1.Verb_VERB_INIT, false))
	if o.State != agent.StateStopping {
		t.Fatalf("a failed Init: %s, want stopping", o.State)
	}
}
