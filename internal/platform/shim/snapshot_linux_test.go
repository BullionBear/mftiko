package shim

import (
	"strconv"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// E1-21's done criteria: an agent decides from Attached alone (DECISIONS D37).

// snapshot attaches with no common major, which streams nothing, until ok accepts what Attached says.
func (a *agent) snapshot(what string, ok func(*shimv1.Attached) bool) *shimv1.Attached {
	a.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := a.attach(2, Major+1)
		wantOK(a.t, r)
		if at := r.GetAttached(); ok(at) {
			return at
		}
		if time.Now().After(deadline) {
			a.t.Fatalf("Attached never showed %s: %v", what, r.GetAttached())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Done criterion 1: the worker said Exiting and hangs in OnStop while no agent is there. The next agent reads from
// Attached how it meant to end, though it is not over yet.
func TestAttachedExiting(t *testing.T) {
	p := startShim(t, fakeworker.Script{Hang: fakeworker.AtStop}, "", "", nil)
	a := dial(t, p.socket())
	a.started(1)
	wantOK(t, a.call(stopReq(30000, 30000))) // long enough to look while it hangs
	a.c.Close()
	p.waitJournal("hang")

	at := dial(t, p.socket()).snapshot("exiting", func(at *shimv1.Attached) bool { return at.GetExiting() != nil })
	if e := at.GetExiting(); e.GetOutcome() != commonv1.Outcome_OUTCOME_DONE || e.GetReason() != "stop: test" {
		t.Errorf("exiting %v", e)
	}
	if at.GetState() == shimv1.WorkerState_WORKER_STATE_EXITED || at.GetExit() != nil ||
		at.GetPhase() != shimv1.Phase_PHASE_STOPPING {
		t.Errorf("attached %v: the worker is still in OnStop", at)
	}
}

// Done criterion 3: everything the worker said while no agent was there overflowed the ring, and it ended. An agent
// that sends no Ack and reads no event still has all it decides by, from Attached alone.
func TestAttachedAloneSuffices(t *testing.T) {
	const stages = RingSize + 50
	p := startShim(t, fakeworker.Script{Stages: stages, FailInit: "no config"}, "", "", nil)
	p.workerPid()
	dial(t, p.socket()).pollNext(2 + stages + 1) // Started, Hello, the stages: all sent before the Init
	a := dial(t, p.socket())
	a.attach(1, Major)
	wantOK(t, a.call(initReq())) // the Init fails; the agent goes without reading or acking anything
	a.c.Close()

	at := dial(t, p.socket()).snapshot("the exit", func(at *shimv1.Attached) bool {
		return at.GetState() == shimv1.WorkerState_WORKER_STATE_EXITED
	})
	if at.GetStage() != "stage-"+strconv.Itoa(stages) {
		t.Errorf("stage %q, want the last one", at.GetStage())
	}
	if e := at.GetExiting(); e.GetOutcome() != commonv1.Outcome_OUTCOME_INIT_FAILED || e.GetReason() != "no config" {
		t.Errorf("exiting %v", e)
	}
	if x := at.GetExit(); x.GetCode() != fakeworker.CodeInitFailed || x.GetOutcome() != commonv1.Outcome_OUTCOME_INIT_FAILED ||
		x.GetOnStopCompleted() {
		t.Errorf("exit %v", x)
	}
	if at.GetHello().GetKind() != "fake" || at.GetPid() == 0 || at.GetStartTime() == 0 {
		t.Errorf("attached %v", at)
	}
	if rec, err := ReadExitFile(p.exitPath()); err != nil || rec.GetExit().GetCode() != at.GetExit().GetCode() {
		t.Errorf("exit file %v (%v)", rec, err)
	}
	if at.GetOldestSeq() == 1 {
		t.Error("the ring did not overflow: the test proves nothing")
	}
}

// Attached is one moment: its next_seq is exactly where its state stands. Every event before it is in the state,
// none after it. While the worker sends Stage after Stage, every snapshot's last stage is stage-k with next_seq k+3
// (Started is 1, Hello 2, stage-i 2+i).
func TestAttachedIsOneMoment(t *testing.T) {
	const stages = 5000
	p := startShim(t, fakeworker.Script{Stages: stages}, "", "", nil)
	p.workerPid()
	a := dial(t, p.socket())
	seen := 0
	for {
		at := a.attach(1, Major+1).GetAttached()
		stage, next := at.GetStage(), at.GetNextSeq()
		if stage == "" {
			if next > 3 {
				t.Fatalf("no stage yet, but next_seq %d: the snapshot is behind its own seq", next)
			}
			continue
		}
		k, err := strconv.Atoi(stage[len("stage-"):])
		if err != nil {
			t.Fatal(err)
		}
		if next != uint64(k)+3 {
			t.Fatalf("stage %s with next_seq %d, want %d: state and seq are from different moments", stage, next, k+3)
		}
		seen++
		if k == stages {
			break
		}
	}
	if seen < 10 {
		t.Fatalf("only %d snapshots during the stages: the test proves nothing", seen)
	}
}
