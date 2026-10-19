package agent

import (
	"time"

	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// Policy is this ticket's. Restart is always false: a lost shim is recorded and
// not started again (D946); RestartPolicy, the backoff, and no_beat are #285.
// While the demand is stale the last plan is kept, and Start still runs (D109).
// There is no beat limit here.
type Policy struct{}

// DefaultPolicy is the policy Run uses when Config leaves one unset.
var DefaultPolicy Policy

func (Policy) Restart(agent.WorkerKey, agent.Ended) (bool, time.Duration) { return false, 0 }

func (Policy) KeepWhileStale(agent.WorkerKey) bool { return true }

func (Policy) BeatTimeout(agent.WorkerKey) time.Duration { return 0 }

func (Policy) Silent(agent.WorkerKey, time.Duration) bool { return false }
