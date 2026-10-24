package agent

import (
	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"google.golang.org/protobuf/proto"
)

// phaseOf is the SessionStatus phase for one session this round (E09-D021, E09-D046, E09-D063, E09-D064, E09-D067–E09-D071).
// write is false when the session has nothing to publish. end is the policy's
// classification of the latest exit; the zero value leaves the exit itself.
//
// An exited worker whose spec hash is not the plan's is the incarnation the
// skeleton is replacing (E09-D047). That exit is not the session ending: DONE or
// FAILED here would keep the session out of the next plan (E09-D045), and the new
// incarnation would never start. Stop has already been sent, so the phase stays
// STOPPING until the new incarnation is the one observed.
//
// An ENDED assignment uses end_reason (E09-D063). ended_by_spec and a normal exit,
// or no worker yet, is DONE. deps_failed, deps_timeout, and invalid_spec are
// FAILED with that reason, including when the worker has not been spawned.
// A worker that is still running stays on its non-terminal phase until it exits.
//
// A wanted worker the policy is bringing back is RESTARTING, with the reason of
// the exit it is waiting out (E09-D071). restart_limit and a no_beat kill the policy
// will not restart are FAILED with those reasons (E09-D067, E09-D069).
//
// An exit the policy has not classified yet stays STOPPING (E09-D095). Publishing the
// exit itself would make a crash the policy is about to restart terminal, and a
// terminal session does not come back (E09-D045).
func phaseOf(in intent, obs *agent.Observed, end ...endView) (statev1.Phase, string, bool) {
	var ev endView
	if len(end) > 0 {
		ev = end[0]
	}
	switch in.Kind {
	case kindConflict:
		return statev1.Phase_PHASE_FAILED, ReasonAlgoConflict, true
	case kindFailed:
		return statev1.Phase_PHASE_FAILED, in.FailReason, true
	case kindPlan:
		if obs == nil {
			return statev1.Phase_PHASE_PENDING, "", true
		}
		if obs.State == agent.StateExited && obs.SpecHash != in.SpecHash {
			return statev1.Phase_PHASE_STOPPING, "", true
		}
		if obs.State == agent.StateExited && obs.SpecHash == in.SpecHash {
			if ev.undecided {
				return statev1.Phase_PHASE_STOPPING, "", true
			}
			if ev.terminalReason != "" {
				return statev1.Phase_PHASE_FAILED, ev.terminalReason, true
			}
			if ev.restarting {
				return statev1.Phase_PHASE_RESTARTING, ev.reason, true
			}
			if ev.overrideReason != "" {
				return statev1.Phase_PHASE_FAILED, ev.overrideReason, true
			}
		}
		ph, reason := phaseFromObs(obs, "")
		return ph, reason, true
	case kindEnded:
		return endedPhase(in, obs, ev)
	default:
		return 0, "", false
	}
}

// undecidedExit reports an exit the restart policy has not classified yet (E09-D095).
// The runner's Sync observes again after the step's actions, so it can see a
// worker that ended after the Step that would have asked the policy about it.
// The next Step asks. Without a policy (a test-supplied one) nothing is undecided.
func undecidedExit(obs *agent.Observed, ev endView, havePolicy bool) bool {
	return havePolicy && obs != nil && obs.State == agent.StateExited &&
		obs.Incarnation != 0 && ev.decided != obs.Incarnation
}

// endedPhase is the phase of an assignment that is ENDED or gone (E09-D063, E09-D064).
func endedPhase(in intent, obs *agent.Observed, end endView) (statev1.Phase, string, bool) {
	if obs != nil && obs.State == agent.StateLost {
		return statev1.Phase_PHASE_FAILED, ReasonShimLost, true
	}
	if failEnd(in.EndReason) {
		if obs == nil || obs.State == agent.StateExited {
			return statev1.Phase_PHASE_FAILED, in.EndReason, true
		}
		ph, _ := phaseFromObs(obs, "")
		return ph, "", true
	}
	if obs == nil {
		return statev1.Phase_PHASE_DONE, in.EndReason, true
	}
	if obs.State == agent.StateExited {
		if obs.Exit.GetOutcome() == commonv1.Outcome_OUTCOME_DONE {
			if in.EndReason != "" {
				return statev1.Phase_PHASE_DONE, in.EndReason, true
			}
			return statev1.Phase_PHASE_DONE, obs.Exit.GetReason(), true
		}
		return statev1.Phase_PHASE_FAILED, exitReason(obs.Exit, end), true
	}
	ph, _ := phaseFromObs(obs, "")
	return ph, "", true
}

// exitReason is the shim's reason, except a no_beat kill, which the status
// names no_beat even though the shim recorded signal 9 and no Exiting (E09-D067).
func exitReason(ex *commonv1.ExitInfo, end endView) string {
	if end.reason == ReasonNoBeat || end.overrideReason == ReasonNoBeat {
		return ReasonNoBeat
	}
	return ex.GetReason()
}

// exitForStatus is the ExitInfo written on SessionStatus. The shim's record
// stays as the process ended; a missing-beat SIGKILL is published as HUNG
// with reason no_beat (E09-D067).
func exitForStatus(ex *commonv1.ExitInfo, end endView) *commonv1.ExitInfo {
	out := proto.Clone(ex).(*commonv1.ExitInfo)
	if end.noBeat {
		out.Outcome = commonv1.Outcome_OUTCOME_HUNG
		out.Reason = ReasonNoBeat
	}
	return out
}

// failEnd reports the end reasons that terminate as FAILED (E09-D063).
func failEnd(reason string) bool {
	switch reason {
	case "deps_failed", "deps_timeout", "invalid_spec":
		return true
	default:
		return false
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
		// RUNNING waits for the stage (E09-D021, E09-D046).
		return statev1.Phase_PHASE_STARTING, ""
	case obs.State == agent.StateInitializing || obs.State == agent.StateInitialized:
		return statev1.Phase_PHASE_WAITING, ""
	default:
		return statev1.Phase_PHASE_PENDING, ""
	}
}
