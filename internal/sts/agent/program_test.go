package agent

import (
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

func TestWorkerContractMatchesFake(t *testing.T) {
	if workerContract != fakeworker.Contract {
		t.Fatalf("contract %q, fake worker %q", workerContract, fakeworker.Contract)
	}
}
