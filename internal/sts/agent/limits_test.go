package agent

import (
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/sts"
)

func TestUnpackLimitDefaults(t *testing.T) {
	cfg := Config{
		Instance: "s", WorkDir: "w", Shim: "shim", DataDir: "d",
		Sign: func(string, sts.Permissions) (string, string, error) { return "", "", nil },
	}
	cfg.applyDefaults()
	if cfg.UnpackLimits.MaxBytes != artifact.DefaultMaxUnpackedBytes || cfg.UnpackLimits.MaxFiles != artifact.DefaultMaxFiles {
		t.Fatalf("defaults %+v", cfg.UnpackLimits)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}

	cfg.UnpackLimits.MaxBytes = -1
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "unpack limits") {
		t.Fatalf("negative bytes: %v", err)
	}
	cfg.UnpackLimits.MaxBytes = 1
	cfg.UnpackLimits.MaxFiles = -1
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "unpack limits") {
		t.Fatalf("negative files: %v", err)
	}

	raw := Config{UnpackLimits: artifact.UnpackLimits{MaxBytes: -2, MaxFiles: -3}}
	raw.applyDefaults()
	if raw.UnpackLimits.MaxBytes != -2 || raw.UnpackLimits.MaxFiles != -3 {
		t.Fatalf("negative limits were rewritten: %+v", raw.UnpackLimits)
	}
}
