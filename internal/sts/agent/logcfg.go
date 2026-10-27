package agent

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/shim"
)

// Defaults of the session log endpoint (E14-D019, E14-D021, E14-D022).
const (
	// DefaultLogListen is the CLI default of -log-listen (E14-D002). The zero
	// LogConfig does not listen: tests and a disabled endpoint leave Listen empty.
	DefaultLogListen = ":7442"
	// DefaultLogStreams is the concurrent cap on stream reads and exports.
	DefaultLogStreams = 16
	// DefaultLogRate is the shared read rate, in bytes per second (16 MiB/s).
	DefaultLogRate = 16 << 20
	// LogRetry is how often a failed listen is tried again during the term.
	// It is not a flag. Tests set LogConfig.Retry shorter.
	LogRetry = 5 * time.Second
	// LogHeartbeat is the follow interval for an empty frame. It is not a flag.
	// Tests set LogConfig.Heartbeat shorter.
	LogHeartbeat = 15 * time.Second
)

// logRetryAfter is the Retry-After delay-seconds sent with 503 (E14-D022).
const logRetryAfter = "1"

// LogConfig is the session log endpoint (E14-D019–E14-D024). The zero value
// does not listen and does not warn.
type LogConfig struct {
	// Listen is the TCP address. Empty does not open the endpoint, and log_addr
	// stays empty (E14-D019).
	Listen string
	// Advertise is host:port with no scheme. Empty means AgentRecord.host plus
	// the port actually bound.
	Advertise string
	// TokenSHA256 is 64 lowercase hex digits, the SHA-256 of the bearer token.
	// It is not a secret. Empty with a non-empty Listen disables the endpoint
	// and the process logs one warning (E14-D020).
	TokenSHA256 string
	// MaxStreams is the cap on concurrent GET .../logs/{stream} and GET .../export.
	// Zero, once the endpoint is on, becomes DefaultLogStreams.
	MaxStreams int
	// MaxRate is the shared byte rate. Zero, once the endpoint is on, becomes
	// DefaultLogRate. A full rate slows the send; it does not reject it.
	MaxRate int
	// MaxBytes and Keep are Launch.output. Zero keeps the shim defaults (E14-D023).
	MaxBytes uint64
	Keep     uint32
	// Retry and Heartbeat override LogRetry and LogHeartbeat when positive.
	// They are not command-line flags.
	Retry     time.Duration
	Heartbeat time.Duration
}

func (c LogConfig) enabled() bool {
	return c.Listen != "" && c.TokenSHA256 != ""
}

// apply fills the endpoint defaults. It does nothing when the endpoint is off,
// so a zero config stays zero.
func (c *LogConfig) apply() {
	if !c.enabled() {
		return
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = DefaultLogStreams
	}
	if c.MaxRate <= 0 {
		c.MaxRate = DefaultLogRate
	}
	if c.Retry <= 0 {
		c.Retry = LogRetry
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = LogHeartbeat
	}
}

// Validate checks flags the process must reject before it campaigns.
// An omitted token is valid: the endpoint stays off (E14-D020).
func (c LogConfig) Validate() error {
	if c.TokenSHA256 != "" {
		if _, err := parseLogToken(c.TokenSHA256); err != nil {
			return err
		}
	}
	if c.MaxStreams < 0 || c.MaxRate < 0 {
		return errors.New("sts: log limits")
	}
	if c.MaxBytes != 0 && c.MaxBytes < shim.MinOutputMaxBytes {
		return errors.New("sts: log max bytes")
	}
	if c.Keep != 0 && c.Keep > shim.MaxOutputKeep {
		return errors.New("sts: log keep")
	}
	if c.Retry < 0 || c.Heartbeat < 0 {
		return errors.New("sts: log timing")
	}
	return nil
}

// CheckLogKeep reports whether n can be -log-keep. 0 uses the shim default.
func CheckLogKeep(n int) error {
	if n < 0 || n > int(shim.MaxOutputKeep) {
		return errors.New("sts: log keep")
	}
	return nil
}

func parseLogToken(s string) ([32]byte, error) {
	var out [32]byte
	if len(s) != 64 || s != strings.ToLower(s) {
		return out, errors.New("sts: -log-token-sha256 must be 64 lowercase hex characters")
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return out, errors.New("sts: -log-token-sha256 must be 64 lowercase hex characters")
	}
	copy(out[:], b)
	return out, nil
}
