package agent

import (
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// decide is the ack of one demand. LOST keeps the previous ack (P6). A worker that ended for a
// reason no restart fixes is REJECTED with it; one the Policy gave up on otherwise is DEAD. READY
// is a running worker whose report says READY or DEGRADED: a dropped private connection is not a
// dead account (E06-D003). PENDING while starting carries the worker's last Start failure.
func decide(o outcome, observed map[agent.WorkerKey]agent.Observed, gaveUp func(agent.WorkerKey) bool) (statev1.EntryState, string, bool) {
	if o.Reject != "" {
		return statev1.EntryState_ENTRY_STATE_REJECTED, o.Reject, true
	}
	obs, seen := observed[o.Worker]
	switch {
	case !seen:
		return statev1.EntryState_ENTRY_STATE_PENDING, "starting", true
	case obs.State == agent.StateLost:
		return 0, "", false
	case obs.State.Ended():
		if r := finalReason(obs.Exit); r != "" {
			return statev1.EntryState_ENTRY_STATE_REJECTED, r, true
		}
		if gaveUp != nil && gaveUp(o.Worker) {
			return statev1.EntryState_ENTRY_STATE_DEAD, "gave up", true
		}
		return statev1.EntryState_ENTRY_STATE_PENDING, "restarting", true
	}
	if obs.State == agent.StateRunning {
		switch reportState(obs.Report) {
		case tdv1.AccountState_ACCOUNT_STATE_READY, tdv1.AccountState_ACCOUNT_STATE_DEGRADED:
			return statev1.EntryState_ENTRY_STATE_READY, "", true
		}
	}
	return statev1.EntryState_ENTRY_STATE_PENDING, startingReason(obs.Report), true
}

// startingReason is "starting", or "starting: <why>" when the worker reported why its Start keeps
// failing: a report with no state and a reason (E06-D042, #542).
func startingReason(rep *workerv1.Report) string {
	r := reportOf(rep)
	if r == nil || r.GetState() != tdv1.AccountState_ACCOUNT_STATE_UNSPECIFIED || r.GetReason() == "" {
		return "starting"
	}
	return "starting: " + r.GetReason()
}

// reportState is the account state in the worker's latest tdctl.Report.
func reportState(rep *workerv1.Report) tdv1.AccountState {
	r := reportOf(rep)
	if r == nil {
		return tdv1.AccountState_ACCOUNT_STATE_UNSPECIFIED
	}
	return r.GetState()
}

func sameAck(prev string, next *statev1.EntryStatus) bool {
	old := &statev1.EntryStatus{}
	if err := proto.Unmarshal([]byte(prev), old); err != nil {
		return false
	}
	return old.GetState() == next.GetState() && old.GetReason() == next.GetReason() &&
		old.GetSubject() == next.GetSubject() &&
		old.GetWorker().GetKey() == next.GetWorker().GetKey() &&
		old.GetWorker().GetIncarnation() == next.GetWorker().GetIncarnation()
}
