package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// Deployment defaults (E09-D013, E09-D018, E09-D019). Spec values of 0 are replaced with these
// before they are handed to a worker.
const (
	DefaultRecvWindowMS     = 1000
	DefaultHookTimeoutS     = 10
	DefaultStartTimeoutS    = 60
	DefaultVolumeMB         = 1024
	DefaultMaxRestarts      = 5
	DefaultRestartWindowS   = 300
	DefaultMaxArtifactBytes = 1 << 30
)

// DefaultVolumeInterval is how often a session volume is measured (E09-D076).
const DefaultVolumeInterval = 10 * time.Second

// DefaultGrace is the fleet Stop grace (E09-D018). The skeleton's own default is the same.
const DefaultGrace = 10 * time.Second

// Restart backoff (E09-D019, E09-D071). Tests set a shorter pair on Config.
const (
	DefaultBackoffMin = time.Second
	DefaultBackoffMax = 60 * time.Second
)

// Reasons written on SessionStatus.
const (
	ReasonAlgoConflict    = "algo_conflict"
	ReasonShimLost        = "shim_lost"
	ReasonNoBeat          = "no_beat"
	ReasonRestartLimit    = "restart_limit"
	ReasonVolumeOverLimit = "volume_over_limit"
	ReasonInvalidInput    = "invalid_input"
)

// Spec is one session spec as etcd stored it. Hash is the whole encoding (E09-D047),
// not a re-marshal: two encodings of the same message are different incarnations.
// desired_state becoming ENDED is not one of those (E09-D097); the hash still
// covers those bytes so an upgraded agent can adopt a running worker.
type Spec struct {
	Raw []byte
	Rev int64 // mod_revision of the spec key; SessionStatus.observed_generation
}

// Hash is hex(sha256(Raw)), the full sum.
func (s Spec) Hash() string {
	sum := sha256.Sum256(s.Raw)
	return hex.EncodeToString(sum[:])
}

// Live is empty. Bindings are not a live config: Start and Command carry them (E09-D061).
type Live struct{}

// Hash is always "", which is also what a worker that applied nothing reports.
func (Live) Hash() string { return "" }

// WorkerKey is the session worker of id (PROTOCOL §2).
func WorkerKey(id string) agent.WorkerKey { return agent.WorkerKey("sts/" + id) }
