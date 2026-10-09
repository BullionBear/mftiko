package shim

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// ExitPath is the exit file of a worker's shim, next to its socket: ${WORK_DIR}/run/{plane}/{h}.exit.pb (PROTOCOL §4.4,
// §4.5 S3).
func ExitPath(workDir, plane, workerKey string) string {
	return filepath.Join(RunDir(workDir, plane), workerHash(workerKey)+".exit.pb")
}

// ErrUncollectedExit is a shim started for a worker key whose last incarnation's exit file is still there: the agent
// has not read how it ended, and a new incarnation would overwrite it.
var ErrUncollectedExit = errors.New("shim: the exit file of an earlier incarnation has not been collected")

// composeExit is the worker's ExitInfo (PROTOCOL §5.3): how it ended by the wait status, what it meant by its Exiting,
// and whether its OnStop finished by its Stopped.
//
//   - code and signal are the wait status's, always: a worker killed by a signal has code -1 and that signal, whatever
//     it had said.
//   - outcome and reason are the Exiting's when there was one: Exiting comes before OnStop, so that a worker killed in
//     OnStop still says how it meant to end.
//   - With no Exiting, a worker that exited is classified by its code (exitCodeOutcome), and one killed by a signal
//     has no outcome: it was killed.
//   - on_stop_completed is the Stopped's; false without one.
func composeExit(exiting *workerv1.Exiting, stopped *workerv1.Stopped, ws syscall.WaitStatus, at time.Time) *commonv1.ExitInfo {
	info := &commonv1.ExitInfo{AtNs: at.UnixNano(), OnStopCompleted: stopped.GetOnStopCompleted()}
	if ws.Signaled() {
		info.Code, info.Signal = -1, int32(ws.Signal())
	} else {
		info.Code = int32(ws.ExitStatus())
	}
	switch {
	case exiting != nil:
		info.Outcome, info.Reason = exiting.GetOutcome(), exiting.GetReason()
	case ws.Signaled():
		info.Reason = fmt.Sprintf("signal %d (%s), no Exiting", int(ws.Signal()), ws.Signal())
	default:
		info.Outcome = exitCodeOutcome(info.Code)
		info.Reason = fmt.Sprintf("exit code %d, no Exiting", info.Code)
	}
	return info
}

// exitCodeOutcome is what an exit code says of a worker that sent no Exiting (PROTOCOL §5.3): 0 done, 10-19 init
// failed, 20 and 22-29 crashed, 21 hung, 30-39 failed. Anything else (1-9 among them: a Go runtime error is 2) is no
// outcome, like a worker killed.
func exitCodeOutcome(code int32) commonv1.Outcome {
	switch {
	case code == 0:
		return commonv1.Outcome_OUTCOME_DONE
	case code >= 10 && code <= 19:
		return commonv1.Outcome_OUTCOME_INIT_FAILED
	case code == 21:
		return commonv1.Outcome_OUTCOME_HUNG
	case code >= 20 && code <= 29:
		return commonv1.Outcome_OUTCOME_CRASHED
	case code >= 30 && code <= 39:
		return commonv1.Outcome_OUTCOME_FAILED
	default:
		return commonv1.Outcome_OUTCOME_UNSPECIFIED
	}
}

// writeExitFile writes the record to path atomically: a temporary file in the same directory, synced, then renamed
// over path, and the directory synced. A reader sees the whole record or none.
func writeExitFile(path string, rec *shimv1.ExitRecord) error {
	b, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync() //nolint:errcheck // best effort: the rename is done
		d.Close()
	}
	return nil
}

// ReadExitFile reads an exit file: what an agent does when it finds one with no shim behind it (E1-17).
func ReadExitFile(path string) (*shimv1.ExitRecord, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	rec := &shimv1.ExitRecord{}
	if err := proto.Unmarshal(b, rec); err != nil {
		return nil, fmt.Errorf("shim: exit file %s: %w", path, err)
	}
	return rec, nil
}

// checkNoExitFile refuses to start a shim over an exit file the agent has not collected (PROTOCOL §4.6 step 4).
func checkNoExitFile(l *shimv1.Launch) error {
	path := ExitPath(l.GetWorkDir(), l.GetPlane(), l.GetWorker().GetKey())
	switch _, err := os.Lstat(path); {
	case err == nil:
		return fmt.Errorf("%w: %s", ErrUncollectedExit, path)
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("shim: exit file: %w", err)
	}
}
