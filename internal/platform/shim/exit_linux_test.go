package shim

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// These run a real shim, with the fake worker under it: E1-13's done criteria end to end.

func (p *shimProc) exitPath() string {
	return ExitPath(p.launch.GetWorkDir(), p.launch.GetPlane(), p.launch.GetWorker().GetKey())
}

// exited reads events until Exited, and returns its ExitInfo.
func (a *agent) exited() *commonv1.ExitInfo {
	a.t.Helper()
	return a.until("Exited", func(ev *shimv1.Event) bool { return ev.GetExited() != nil }).GetExited().GetExit()
}

// Done criterion 1: each way a worker ends gives the ExitInfo of PROTOCOL §5.3, in the event Exited and in the exit
// file alike; Release then removes the file and ends the shim.
func TestExitInfoEndings(t *testing.T) {
	for _, c := range []struct {
		name   string
		script fakeworker.Script
		stop   bool // send Stop{100, 100} once started
		want   *commonv1.ExitInfo
	}{
		{"done", fakeworker.Script{Exit: fakeworker.ExitDone}, false,
			&commonv1.ExitInfo{Code: 0, Outcome: commonv1.Outcome_OUTCOME_DONE, Reason: "exit", OnStopCompleted: true}},
		{"init failed", fakeworker.Script{FailInit: "no config"}, false,
			&commonv1.ExitInfo{Code: 10, Outcome: commonv1.Outcome_OUTCOME_INIT_FAILED, Reason: "no config"}},
		{"crashed", fakeworker.Script{Exit: fakeworker.ExitCrashed}, false,
			&commonv1.ExitInfo{Code: 20, Outcome: commonv1.Outcome_OUTCOME_CRASHED, Reason: "crash", OnStopCompleted: true}},
		{"hung", fakeworker.Script{Exit: fakeworker.ExitHung}, false,
			&commonv1.ExitInfo{Code: 21, Outcome: commonv1.Outcome_OUTCOME_HUNG, Reason: "hook timed out"}},
		{"failed", fakeworker.Script{Exit: fakeworker.ExitFailed}, false,
			&commonv1.ExitInfo{Code: 30, Outcome: commonv1.Outcome_OUTCOME_FAILED, Reason: "fail", OnStopCompleted: true}},
		{"killed", fakeworker.Script{Exit: fakeworker.ExitKilled}, false,
			&commonv1.ExitInfo{Code: -1, Signal: int32(syscall.SIGKILL), Reason: "signal 9 (killed), no Exiting"}},
		{"panic", fakeworker.Script{Exit: fakeworker.ExitPanic}, false,
			&commonv1.ExitInfo{Code: fakeworker.CodePanic, Reason: "exit code 2, no Exiting"}},
		{"killed in OnStop", fakeworker.Script{Hang: fakeworker.AtStop}, true,
			&commonv1.ExitInfo{Code: -1, Signal: int32(syscall.SIGKILL), Outcome: commonv1.Outcome_OUTCOME_DONE,
				Reason: "stop: test"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := startShim(t, c.script, "", "", nil)
			pid := p.workerPid()
			a := dial(t, p.socket())
			a.attachFrom(1, 0)
			wantOK(t, a.call(initReq()))
			if c.script.FailInit == "" {
				a.until("Done(INIT)", isDone(workerv1.Verb_VERB_INIT))
				wantOK(t, a.call(startReq()))
			}
			if c.stop {
				a.until("Done(START)", isDone(workerv1.Verb_VERB_START))
				wantOK(t, a.call(stopReq(100, 100)))
			}
			got := a.exited()
			if got.GetAtNs() == 0 {
				t.Error("no at_ns")
			}
			want := proto.CloneOf(c.want)
			want.AtNs = got.GetAtNs()
			if !proto.Equal(got, want) {
				t.Fatalf("Exited %v\nwant   %v", got, want)
			}

			rec, err := ReadExitFile(p.exitPath())
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(rec.GetExit(), got) || rec.GetWorker().GetKey() != "td/42" ||
				rec.GetWorker().GetIncarnation() != 3 || int(rec.GetPid()) != pid || rec.GetStartTime() == 0 {
				t.Errorf("exit file %v, want the same ExitInfo as Exited, for pid %d", rec, pid)
			}
			if at := a.waitExited(1); !proto.Equal(at.GetExit(), got) {
				t.Fatalf("Attached.exit %v, want what Exited and the exit file say: %v", at.GetExit(), got)
			}
			wantOK(t, a.call(releaseReq()))
			p.wantExit(0)
			if _, err := os.Lstat(p.exitPath()); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("exit file left after Release: %v", err)
			}
		})
	}
}

// Done criterion 2: the worker ends while no agent is there, and the shim goes too (the host shuts down). The exit
// file is still there for the next agent, and the next shim of the worker key will not start until it is collected.
func TestExitFileOutlivesShim(t *testing.T) {
	wd := workDir(t)
	p := outputShim(t, fakeworker.Script{}, wd, 3, nil)
	pid := p.workerPid()
	a := dial(t, p.socket())
	a.started(1)
	a.c.Close()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.wantExit(0)
	waitGone(t, pid)

	rec, err := ReadExitFile(p.exitPath())
	if err != nil {
		t.Fatalf("no exit file after the shim went: %v", err)
	}
	if e := rec.GetExit(); e.GetOutcome() != commonv1.Outcome_OUTCOME_DONE || e.GetCode() != 0 || !e.GetOnStopCompleted() ||
		e.GetReason() != "stop: shim: "+syscall.SIGTERM.String() || rec.GetWorker().GetIncarnation() != 3 {
		t.Errorf("exit file %v", rec)
	}
	if _, err := os.Lstat(p.socket()); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket left: %v", err)
	}

	next := outputShim(t, fakeworker.Script{}, wd, 4, nil)
	next.waitLog("exit file in the way", nil)
	next.wantExit(1)
	if _, ok := next.logged("worker started"); ok {
		t.Fatal("started over an uncollected exit file")
	}
	if _, err := ReadExitFile(p.exitPath()); err != nil {
		t.Fatalf("the refused shim touched the exit file: %v", err)
	}

	if err := os.Remove(p.exitPath()); err != nil { // the agent collects it
		t.Fatal(err)
	}
	after := outputShim(t, fakeworker.Script{}, wd, 5, nil)
	after.workerPid()
}
