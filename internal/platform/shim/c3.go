package shim

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

// Major is the C3 major this shim speaks, mftiko.shim.v1 (PROTOCOL §1). An agent that lists no major the shim
// speaks still attaches, but may only Signal (§4.2).
const Major = 1

// The codes of Response.error (PROTOCOL §4.2).
const (
	CodeWrongAgent    = "wrong_agent"     // Attach from another plane or instance than the one that spawned the shim
	CodeStaleEpoch    = "stale_epoch"     // Attach with an epoch below the largest accepted; details["epoch"] is that
	CodeNotController = "not_controller"  // a request on a connection that is not the controller's
	CodeNoCommonMajor = "no_common_major" // the controller shares no major with the shim: Signal only
	CodeBadRequest    = "bad_request"     // a field out of range: an epoch below 1, a signo outside 1-64
	CodeUnimplemented = "unimplemented"   // an operation the shim does not know, or does not do yet
	CodeExited        = "exited"          // the worker has been reaped
	CodeSignalFailed  = "signal_failed"   // kill failed; the message says why
	CodeRunning       = "running"         // Release before the worker has been reaped
	CodeBadStage      = "bad_stage"       // Init or Start in a phase that does not allow it; details["phase"] is the phase
	CodeWorkerBusy    = "worker_busy"     // the queue to the worker is full: it has stopped reading fd 3
)

// RunDir is where a plane's shims keep their sockets: ${WORK_DIR}/run/{plane} (PROTOCOL §4.1).
func RunDir(workDir, plane string) string { return filepath.Join(workDir, "run", plane) }

// SocketPath is the C3 socket of a worker's shim: ${WORK_DIR}/run/{plane}/{h}.sock, with h from the worker key
// (PROTOCOL §2, §4.1).
func SocketPath(workDir, plane, workerKey string) string {
	return filepath.Join(RunDir(workDir, plane), workerHash(workerKey)+".sock")
}

// workerHash is h of PROTOCOL §2, hex(sha256(worker key))[:16]: the same as state.WorkerHash, which the shim does
// not import, so as not to link the etcd client (S5).
func workerHash(workerKey string) string {
	sum := sha256.Sum256([]byte(workerKey))
	return hex.EncodeToString(sum[:8])
}
