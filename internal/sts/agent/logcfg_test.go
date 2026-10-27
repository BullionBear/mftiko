package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim"
)

func logTokenHex(plain string) string {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:])
}

func TestLogConfigDefaults(t *testing.T) {
	if DefaultLogListen != ":7442" || DefaultLogStreams != 16 || DefaultLogRate != 16<<20 {
		t.Fatalf("defaults %s %d %d", DefaultLogListen, DefaultLogStreams, DefaultLogRate)
	}
	if LogRetry != 5*time.Second || LogHeartbeat != 15*time.Second || logRetryAfter != "1" {
		t.Fatalf("retry %s heartbeat %s after %s", LogRetry, LogHeartbeat, logRetryAfter)
	}
	var off LogConfig
	off.apply()
	if off.MaxStreams != 0 || off.MaxRate != 0 || off.Retry != 0 || off.Heartbeat != 0 {
		t.Fatalf("zero config filled defaults: %+v", off)
	}
	on := LogConfig{Listen: DefaultLogListen, TokenSHA256: logTokenHex("log")}
	on.apply()
	if on.MaxStreams != DefaultLogStreams || on.MaxRate != DefaultLogRate || on.Retry != LogRetry || on.Heartbeat != LogHeartbeat {
		t.Fatalf("apply %+v", on)
	}
	if err := on.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLogConfigRejects(t *testing.T) {
	hash := logTokenHex("log")
	cases := []LogConfig{
		{TokenSHA256: strings.ToUpper(hash)},
		{TokenSHA256: hash[:63]},
		{TokenSHA256: strings.Repeat("z", 64)},
		{MaxStreams: -1},
		{MaxRate: -1},
		{MaxBytes: 100},
		{Keep: shim.MaxOutputKeep + 1},
		{Retry: -1},
		{Heartbeat: -1},
	}
	for _, c := range cases {
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	ok := LogConfig{TokenSHA256: hash, MaxBytes: shim.MinOutputMaxBytes, Keep: shim.MaxOutputKeep}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := CheckLogKeep(0); err != nil {
		t.Fatal(err)
	}
	if err := CheckLogKeep(-1); err == nil {
		t.Fatal("negative keep")
	}
	if err := CheckLogKeep(int(shim.MaxOutputKeep) + 1); err == nil {
		t.Fatal("keep above max")
	}
	if _, err := parseLogToken(hash); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalPhase(t *testing.T) {
	if !terminalPhase(statev1.Phase_PHASE_DONE) || !terminalPhase(statev1.Phase_PHASE_FAILED) {
		t.Fatal("done and failed are terminal")
	}
	for _, ph := range []statev1.Phase{
		statev1.Phase_PHASE_UNSPECIFIED,
		statev1.Phase_PHASE_PENDING,
		statev1.Phase_PHASE_WAITING,
		statev1.Phase_PHASE_STARTING,
		statev1.Phase_PHASE_RUNNING,
		statev1.Phase_PHASE_STOPPING,
		statev1.Phase_PHASE_RESTARTING,
	} {
		if terminalPhase(ph) {
			t.Fatalf("%s is terminal", ph)
		}
	}
}
