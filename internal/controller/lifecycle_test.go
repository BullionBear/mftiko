package controller_test

import (
	"slices"
	"testing"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func deleteKeys(t *testing.T, got []controller.Delete) []string {
	t.Helper()
	out := make([]string, len(got))
	prev := ""
	for i, d := range got {
		if d.Key <= prev {
			t.Fatalf("deletes not strictly sorted: %v", got)
		}
		prev = d.Key
		out[i] = d.Key
	}
	return out
}

func wantKeys(t *testing.T, got []controller.Delete, want ...string) {
	t.Helper()
	slices.Sort(want)
	if !slices.Equal(deleteKeys(t, got), want) {
		t.Fatalf("deletes %v, want %v", deleteKeys(t, got), want)
	}
}

func addDemand(v *controller.View, key, region, inst, plane, session string) {
	v.Demands[key] = controller.Demand{
		Key: key, Region: region, Instance: inst, Plane: plane, Session: session,
		Value: &statev1.DemandEntry{},
	}
}

// endedView is a terminal session with one demand in tw and one in jp, and
// its assignment. worker and agent say whether those keys are present.
func endedView(phase statev1.Phase, worker, agent bool) (controller.View, []string) {
	v := openView("tw", "jp")
	sess := controller.Session{
		ID: sessID, Instance: stsInst, Region: "tw",
		Spec: sampleSpec(), SpecRev: specRev,
		Status: &statev1.SessionStatus{Phase: phase},
		Demand: &statev1.SessionDemand{
			Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, IssuedAtNs: 1,
		},
	}
	if worker {
		sess.Worker = &controller.Worker{
			Key:      state.WorkerStatusKey(state.PlaneSTS, stsInst, state.WorkerHash("sts/"+sessID)),
			Instance: stsInst, Hash: state.WorkerHash("sts/" + sessID), Session: sessID,
		}
	}
	if agent {
		v.Agents[stsInst] = controller.Agent{
			Instance: stsInst, Region: "tw", Plane: "sts",
			Record: &statev1.AgentRecord{Epoch: 1},
		}
	}
	twKey := state.MDDemandKey("md-tw-1", sessID, "select/near")
	jpKey := state.MDDemandKey("md-jp-1", sessID, "feed/x")
	addDemand(&v, twKey, "tw", "md-tw-1", "md", sessID)
	addDemand(&v, jpKey, "jp", "md-jp-1", "md", sessID)
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	return v, []string{twKey, jpKey}
}

func TestLifecycleWaitsWhileWorkerRemains(t *testing.T) {
	for _, agent := range []bool{true, false} {
		v, _ := endedView(statev1.Phase_PHASE_DONE, true, agent)
		if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
			t.Fatalf("agent %v deletes %v", agent, got)
		}
	}
}

func TestLifecycleRemovesDemandsBeforeAssignment(t *testing.T) {
	for _, phase := range []statev1.Phase{statev1.Phase_PHASE_DONE, statev1.Phase_PHASE_FAILED} {
		v, demands := endedView(phase, false, false)
		wantKeys(t, controller.Lifecycle(v, places(), false), demands...)

		// The same view a round later, after those deletes have been listed.
		v.Demands = map[string]controller.Demand{}
		wantKeys(t, controller.Lifecycle(v, places(), false), state.SessionDemandKey(stsInst, sessID))
	}
}

func TestLifecycleStaleProviderHoldsAssignment(t *testing.T) {
	v, demands := endedView(statev1.Phase_PHASE_DONE, false, true)
	v.Regions["jp"] = controller.Region{Rev: 1, Synced: true, Stale: true}
	twKey := demands[0]
	wantKeys(t, controller.Lifecycle(v, places(), true), twKey)

	// Nothing left in the fresh region. The stale region may still be hiding
	// a demand, so the assignment waits (D1333).
	delete(v.Demands, twKey)
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("assignment deleted while jp is stale: %v", got)
	}

	v.Regions["jp"] = controller.Region{Rev: 2, Synced: true}
	delete(v.Demands, demands[1])
	wantKeys(t, controller.Lifecycle(v, places(), true), state.SessionDemandKey(stsInst, sessID))
}

func TestLifecycleStaleSTSDeletesNothing(t *testing.T) {
	v, _ := endedView(statev1.Phase_PHASE_DONE, false, true)
	v.Regions["tw"] = controller.Region{Rev: 1, Synced: true, Stale: true}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("stale STS region deleted %v", got)
	}
}

func TestLifecycleUndecodableBlocksRemoval(t *testing.T) {
	v, _ := endedView(statev1.Phase_PHASE_DONE, false, true)
	v.Demands = map[string]controller.Demand{}
	workerKey := state.WorkerStatusKey(state.PlaneSTS, stsInst, state.WorkerHash("sts/"+sessID))
	v.Undecodable = []string{workerKey}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("undecodable worker counted as gone: %v", got)
	}

	v.Undecodable = []string{state.MDDemandKey("md-jp-1", sessID, "feed/hidden")}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("assignment deleted over an undecodable demand: %v", got)
	}
}

func TestLifecycleOneLiveInstanceKeepsDemands(t *testing.T) {
	v, demands := endedView(statev1.Phase_PHASE_DONE, false, true)
	inst := places()
	inst["sts-jp-1"] = controller.Placement{Plane: "sts", Region: "jp"}
	v.Sessions[controller.SessionRef("sts-jp-1", sessID)] = controller.Session{
		ID: sessID, Instance: "sts-jp-1", Region: "jp",
		Spec:   sampleSpec(),
		Status: &statev1.SessionStatus{Phase: statev1.Phase_PHASE_RUNNING},
		Demand: &statev1.SessionDemand{IssuedAtNs: 1},
	}
	if got := controller.Lifecycle(v, inst, true); len(got) != 0 {
		t.Fatalf("a live instance lost its demands: %v (keys %v)", got, demands)
	}
}

func TestLifecycleRunningSessionIsKept(t *testing.T) {
	v, _ := endedView(statev1.Phase_PHASE_RUNNING, false, true)
	extra := state.MDDemandKey("md-jp-1", sessID, "feed/extra")
	addDemand(&v, extra, "jp", "md-jp-1", "md", sessID)
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("a running session was reduced: %v", got)
	}
}

func TestLifecycleOrphanOnFullResync(t *testing.T) {
	const id = "nope"
	v := openView("tw", "jp")
	jpKey := state.MDDemandKey("md-jp-1", id, "feed/x")
	twKey := state.MDDemandKey("md-tw-1", id, "feed/y")
	addDemand(&v, jpKey, "jp", "md-jp-1", "md", id)
	addDemand(&v, twKey, "tw", "md-tw-1", "md", id)

	// STS lives in tw. While that region is stale the session might be there
	// and not yet visible (D1334).
	v.Regions["tw"] = controller.Region{Rev: 1, Synced: true, Stale: true}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("orphan deleted while an STS region is stale: %v", got)
	}

	v.Regions["tw"] = controller.Region{Rev: 2, Synced: true}
	if got := controller.Lifecycle(v, places(), false); len(got) != 0 {
		t.Fatalf("orphan deleted on a notify round: %v", got)
	}
	// A stale provider region keeps its own key. The fresh region is reclaimed.
	v.Regions["jp"] = controller.Region{Rev: 1, Synced: true, Stale: true}
	wantKeys(t, controller.Lifecycle(v, places(), true), twKey)

	v.Regions["jp"] = controller.Region{Rev: 2, Synced: true}
	wantKeys(t, controller.Lifecycle(v, places(), true), jpKey, twKey)
}

func TestLifecycleWorkerListedOnTheInstanceBlocks(t *testing.T) {
	v, _ := endedView(statev1.Phase_PHASE_DONE, false, true)
	hash := state.WorkerHash("sts/" + sessID)
	v.Workers[state.WorkerStatusKey(state.PlaneSTS, stsInst, hash)] = controller.Worker{
		Instance: stsInst, Hash: hash, Session: sessID,
	}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("worker present only in the workers map: %v", got)
	}
}

func TestLifecycleOrphanRespectsExistingSession(t *testing.T) {
	const id = "nope"
	v := openView("tw", "jp")
	key := state.MDDemandKey("md-jp-1", id, "feed/x")
	addDemand(&v, key, "jp", "md-jp-1", "md", id)

	v.Sessions[controller.SessionRef(stsInst, id)] = controller.Session{
		ID: id, Instance: stsInst, Region: "tw",
		Spec: &sessionv1.SessionSpec{Name: "s"},
	}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("demand of a spec-only session deleted: %v", got)
	}

	delete(v.Sessions, controller.SessionRef(stsInst, id))
	v.Undecodable = []string{state.SessionSpecKey(stsInst, id)}
	if got := controller.Lifecycle(v, places(), true); len(got) != 0 {
		t.Fatalf("demand deleted over an undecodable spec: %v", got)
	}

	v.Undecodable = nil
	inst := places()
	inst["sts-jp-1"] = controller.Placement{Plane: "sts", Region: "jp"}
	v.Regions["jp"] = controller.Region{Rev: 1, Synced: true, Stale: true}
	if got := controller.Lifecycle(v, inst, true); len(got) != 0 {
		t.Fatalf("orphan deleted while another STS region is stale: %v", got)
	}
}

func TestLifecycleAssignmentWithoutDemands(t *testing.T) {
	v := openView("tw")
	v.Sessions[controller.SessionRef(stsInst, sessID)] = controller.Session{
		ID: sessID, Instance: stsInst, Region: "tw",
		Status: &statev1.SessionStatus{Phase: statev1.Phase_PHASE_FAILED},
		Demand: &statev1.SessionDemand{EndReason: "invalid_spec", IssuedAtNs: 1},
	}
	inst := map[string]controller.Placement{
		stsInst: {Plane: "sts", Region: "tw"},
	}
	wantKeys(t, controller.Lifecycle(v, inst, false), state.SessionDemandKey(stsInst, sessID))
}

func TestLifecycleDoesNotNameForeignKeys(t *testing.T) {
	v, demands := endedView(statev1.Phase_PHASE_DONE, false, true)
	ack := state.MDAckKey("md-tw-1", sessID, "select/near")
	v.Acks[ack] = controller.Ack{Key: ack, Session: sessID}
	for _, key := range deleteKeys(t, controller.Lifecycle(v, places(), true)) {
		for _, foreign := range []string{
			state.SessionSpecKey(stsInst, sessID),
			state.SessionStatusKey(stsInst, sessID),
			ack,
			state.WorkerStatusKey(state.PlaneSTS, stsInst, state.WorkerHash("sts/"+sessID)),
		} {
			if key == foreign {
				t.Fatalf("deleted %s", key)
			}
		}
		if !slices.Contains(demands, key) {
			t.Fatalf("unexpected key %s", key)
		}
	}
}
