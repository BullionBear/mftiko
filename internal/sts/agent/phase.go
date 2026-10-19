package agent

import (
	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// phaseOf is the SessionStatus phase for one session this round (D921, D946).
// write is false when the session has nothing to publish.
//
// An exited worker whose spec hash is not the plan's is the incarnation the
// skeleton is replacing (D947). That exit is not the session ending: DONE or
// FAILED here would keep the session out of the next plan (D945), and the new
// incarnation would never start. Stop has already been sent, so the phase stays
// STOPPING until the new incarnation is the one observed.
func phaseOf(in intent, obs *agent.Observed) (statev1.Phase, string, bool) {
	switch in.Kind {
	case kindConflict:
		return statev1.Phase_PHASE_FAILED, ReasonAlgoConflict, true
	case kindPlan:
		if obs == nil {
			return statev1.Phase_PHASE_PENDING, "", true
		}
		if obs.State == agent.StateExited && obs.SpecHash != in.SpecHash {
			return statev1.Phase_PHASE_STOPPING, "", true
		}
		ph, reason := phaseFromObs(obs, "")
		return ph, reason, true
	case kindEnded:
		if obs == nil {
			return statev1.Phase_PHASE_DONE, in.EndReason, true
		}
		ph, reason := phaseFromObs(obs, in.EndReason)
		return ph, reason, true
	default:
		return 0, "", false
	}
}

// phaseFromObs maps one worker. endReason, when set, replaces the exit reason
// on a normal DONE exit (the assignment's end_reason, or empty when the
// assignment key was removed and the worker has not exited yet).
func phaseFromObs(obs *agent.Observed, endReason string) (statev1.Phase, string) {
	switch {
	case obs.State == agent.StateLost:
		return statev1.Phase_PHASE_FAILED, ReasonShimLost
	case obs.State == agent.StateExited:
		if obs.Exit.GetOutcome() == commonv1.Outcome_OUTCOME_DONE {
			if endReason != "" {
				return statev1.Phase_PHASE_DONE, endReason
			}
			return statev1.Phase_PHASE_DONE, obs.Exit.GetReason()
		}
		return statev1.Phase_PHASE_FAILED, obs.Exit.GetReason()
	case obs.State == agent.StateStopping:
		return statev1.Phase_PHASE_STOPPING, ""
	case obs.Stage == "running" && !obs.State.Ended():
		return statev1.Phase_PHASE_RUNNING, ""
	case obs.State == agent.StateStarting || obs.State == agent.StateRunning:
		// Done(START, ok) moves the skeleton to Running before Stage(running).
		// RUNNING waits for the stage (D921, D946).
		return statev1.Phase_PHASE_STARTING, ""
	case obs.State == agent.StateInitializing || obs.State == agent.StateInitialized:
		return statev1.Phase_PHASE_WAITING, ""
	default:
		return statev1.Phase_PHASE_PENDING, ""
	}
}
