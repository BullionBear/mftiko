package agent

import (
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/td/account"
)

// decide is the ack of one demand. LOST keeps the previous ack (P6). A worker that ended for a
// reason no restart fixes is REJECTED with it; one the Policy gave up on otherwise is DEAD. READY
// is a running worker whose report says READY or DEGRADED: a dropped private connection is not a
// dead account (D603).
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
	return statev1.EntryState_ENTRY_STATE_PENDING, "starting", true
}

// reportState is the account state in the worker's latest tdctl.Report.
func reportState(rep *workerv1.Report) tdv1.AccountState {
	if rep == nil || rep.GetContract() != account.Contract || len(rep.GetPayload()) == 0 {
		return tdv1.AccountState_ACCOUNT_STATE_UNSPECIFIED
	}
	r := &tdctlv1.Report{}
	if err := proto.Unmarshal(rep.GetPayload(), r); err != nil {
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
