package shim

import (
	"os"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// envTestShim makes the re-executed test binary a shim; fakeworker's variables make it a worker.
const envTestShim = "MFTIKO_TEST_SHIM"

func TestMain(m *testing.M) {
	fakeworker.MaybeMain()
	if os.Getenv(envTestShim) == "1" {
		os.Exit(Run(logger.NewWithWriter("mftiko-shim", os.Stderr)))
	}
	os.Exit(m.Run())
}
