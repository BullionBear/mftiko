package agent

import (
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/td/account"
)

// Policy is TD's (D608): an account worker that ended is started again with a backoff, and while
// the demand is stale too, except one that ended for its credentials, its position mode or its
// kind: it would end the same way. After GiveUpAfter restarts in a row the agent gives up, and the
// ack is DEAD (P6). A worker silent past Beat is stopped and restarted.
type Policy struct {
	Backoff     agent.Backoff
	Beat        time.Duration
	GiveUpAfter uint32
}

// DefaultPolicy is D608's numbers.
var DefaultPolicy = Policy{Backoff: agent.Backoff{Min: time.Second, Max: time.Minute}, Beat: 10 * time.Second, GiveUpAfter: 10}

func (p Policy) Restart(_ agent.WorkerKey, end agent.Ended) (bool, time.Duration) {
	if finalReason(end.Exit) != "" {
		return false, 0
	}
	if p.GiveUpAfter > 0 && end.Restarts >= p.GiveUpAfter {
		return false, 0
	}
	return true, p.Backoff.After(end.Restarts)
}

func (Policy) KeepWhileStale(agent.WorkerKey) bool { return true }

func (p Policy) BeatTimeout(agent.WorkerKey) time.Duration { return p.Beat }

func (Policy) Silent(agent.WorkerKey, time.Duration) bool { return true }

// finalReason is the reason of an ending that no restart can fix (D608): the venue refused the
// credentials, the account is in hedge mode, the kind is not run. "" for any other ending.
func finalReason(exit *commonv1.ExitInfo) string {
	switch exit.GetOutcome() {
	case commonv1.Outcome_OUTCOME_INIT_FAILED, commonv1.Outcome_OUTCOME_CRASHED:
	default:
		return ""
	}
	switch r := exit.GetReason(); r {
	case account.ReasonAuth, account.ReasonHedgeMode, account.ReasonKindUnsupported:
		return r
	}
	return ""
}
