package shim

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// exitStatus is the wait status of a process that exited with code.
func exitStatus(code int) syscall.WaitStatus { return syscall.WaitStatus(code << 8) }

// signalStatus is the wait status of a process killed by sig.
func signalStatus(sig syscall.Signal) syscall.WaitStatus { return syscall.WaitStatus(sig) }

func exiting(o commonv1.Outcome, reason string) *workerv1.Exiting {
	return &workerv1.Exiting{Outcome: o, Reason: reason}
}

// What ExitInfo says for each way a worker ends (PROTOCOL §5.3): the wait status gives code and signal, always; the
// Exiting gives outcome and reason when there was one, the exit code when not; the Stopped says whether OnStop ran.
func TestComposeExit(t *testing.T) {
	at := time.Unix(100, 5)
	done := &workerv1.Stopped{OnStopCompleted: true}
	for _, c := range []struct {
		name    string
		exiting *workerv1.Exiting
		stopped *workerv1.Stopped
		ws      syscall.WaitStatus
		want    *commonv1.ExitInfo
	}{
		{"done", exiting(commonv1.Outcome_OUTCOME_DONE, "stop"), done, exitStatus(0),
			&commonv1.ExitInfo{Code: 0, Outcome: commonv1.Outcome_OUTCOME_DONE, Reason: "stop", OnStopCompleted: true}},
		{"init failed", exiting(commonv1.Outcome_OUTCOME_INIT_FAILED, "bad"), &workerv1.Stopped{}, exitStatus(10),
			&commonv1.ExitInfo{Code: 10, Outcome: commonv1.Outcome_OUTCOME_INIT_FAILED, Reason: "bad"}},
		{"crashed", exiting(commonv1.Outcome_OUTCOME_CRASHED, "panic"), done, exitStatus(20),
			&commonv1.ExitInfo{Code: 20, Outcome: commonv1.Outcome_OUTCOME_CRASHED, Reason: "panic", OnStopCompleted: true}},
		{"hung", exiting(commonv1.Outcome_OUTCOME_HUNG, "watchdog"), &workerv1.Stopped{}, exitStatus(21),
			&commonv1.ExitInfo{Code: 21, Outcome: commonv1.Outcome_OUTCOME_HUNG, Reason: "watchdog"}},
		{"failed", exiting(commonv1.Outcome_OUTCOME_FAILED, "fail"), done, exitStatus(30),
			&commonv1.ExitInfo{Code: 30, Outcome: commonv1.Outcome_OUTCOME_FAILED, Reason: "fail", OnStopCompleted: true}},
		{"killed", nil, nil, signalStatus(syscall.SIGKILL),
			&commonv1.ExitInfo{Code: -1, Signal: 9, Reason: "signal 9 (killed), no Exiting"}},
		// Killed in OnStop: the signal is how it ended, the Exiting what it meant.
		{"killed in OnStop", exiting(commonv1.Outcome_OUTCOME_DONE, "stop"), nil, signalStatus(syscall.SIGKILL),
			&commonv1.ExitInfo{Code: -1, Signal: 9, Outcome: commonv1.Outcome_OUTCOME_DONE, Reason: "stop"}},
		// No Exiting: the exit code is the fallback.
		{"panic", nil, nil, exitStatus(2),
			&commonv1.ExitInfo{Code: 2, Reason: "exit code 2, no Exiting"}},
		{"exit 30, no Exiting", nil, nil, exitStatus(30),
			&commonv1.ExitInfo{Code: 30, Outcome: commonv1.Outcome_OUTCOME_FAILED, Reason: "exit code 30, no Exiting"}},
	} {
		got := composeExit(c.exiting, c.stopped, c.ws, at)
		c.want.AtNs = at.UnixNano()
		if !proto.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestExitCodeOutcome(t *testing.T) {
	for code, want := range map[int32]commonv1.Outcome{
		0: commonv1.Outcome_OUTCOME_DONE, 1: commonv1.Outcome_OUTCOME_UNSPECIFIED, 2: commonv1.Outcome_OUTCOME_UNSPECIFIED,
		9: commonv1.Outcome_OUTCOME_UNSPECIFIED, 10: commonv1.Outcome_OUTCOME_INIT_FAILED,
		19: commonv1.Outcome_OUTCOME_INIT_FAILED, 20: commonv1.Outcome_OUTCOME_CRASHED, 21: commonv1.Outcome_OUTCOME_HUNG,
		29: commonv1.Outcome_OUTCOME_CRASHED, 30: commonv1.Outcome_OUTCOME_FAILED, 39: commonv1.Outcome_OUTCOME_FAILED,
		40: commonv1.Outcome_OUTCOME_UNSPECIFIED, 255: commonv1.Outcome_OUTCOME_UNSPECIFIED,
	} {
		if got := exitCodeOutcome(code); got != want {
			t.Errorf("exit %d: %v, want %v", code, got, want)
		}
	}
}

// The exit file is written whole, leaves no temporary file behind, and reads back as written; an existing one keeps
// a new shim of the key from starting.
func TestExitFile(t *testing.T) {
	wd := t.TempDir()
	l := testLaunch(wd)
	if err := os.MkdirAll(RunDir(wd, l.GetPlane()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := checkNoExitFile(l); err != nil {
		t.Fatalf("no file: %v", err)
	}
	path := ExitPath(wd, l.GetPlane(), l.GetWorker().GetKey())
	if filepath.Dir(path) != RunDir(wd, l.GetPlane()) || filepath.Base(path) != workerHash("td/42")+".exit.pb" {
		t.Errorf("path %s", path)
	}
	rec := &shimv1.ExitRecord{Worker: l.GetWorker(), SpecHash: "spec-1", Pid: 42, StartTime: 7,
		Exit: &commonv1.ExitInfo{Code: -1, Signal: 9, AtNs: 1}}
	if err := writeExitFile(path, rec); err != nil {
		t.Fatal(err)
	}
	got, err := ReadExitFile(path)
	if err != nil || !proto.Equal(got, rec) {
		t.Fatalf("read %v (%v), want %v", got, err, rec)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v (%v)", fi.Mode(), err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Error("temporary file left behind")
	}
	if err := checkNoExitFile(l); !errors.Is(err, ErrUncollectedExit) {
		t.Fatalf("with a file: %v", err)
	}
}
