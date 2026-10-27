// Command worker is the strategy binary the sts/agent acceptance tests pack.
// TestMain builds it without the race detector and packs that file once.
// The shim and the agent stay the race-instrumented test binary.
//
// Behavior is the scripted fake worker: MFTIKO_FAKE_WORKER, MFTIKO_FAKE_SCRIPT,
// and MFTIKO_FAKE_JOURNAL, the same variables the tests already set. The
// process closes stdout, stderr, and the control socket before it exits so
// the shim sees EOF instead of waiting out outputDrain and exitReadGrace.
package main

import (
	"os"
	"strconv"
	"syscall"

	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// contractsMarker is the byte sequence PackStrategy reads out of a strategy
// binary (E09-D035). It is the list sdk.Contracts returns. This program does
// not import sdk; the acceptance TestMain rejects a pack whose contracts differ.
var contractsMarker = "mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1\n"

func main() {
	if len(contractsMarker) < len("mftiko.contracts=\n") || contractsMarker[len(contractsMarker)-1] != '\n' {
		os.Exit(2)
	}
	code := fakeworker.Run()
	closeShimFDs()
	os.Exit(code)
}

// closeShimFDs closes the descriptors the shim is waiting on. outputDrain
// waits for stdout and stderr. exitReadGrace waits for the control socket.
// Closing them before exit drops those waits even if process shutdown would
// otherwise leave a copy open.
func closeShimFDs() {
	_ = os.Stdout.Close()
	_ = os.Stderr.Close()
	fd := 3
	if v := os.Getenv("MFTIKO_CTL_FD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			fd = n
		}
	}
	_ = syscall.Close(fd)
}
