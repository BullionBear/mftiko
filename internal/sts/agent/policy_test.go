package agent

import (
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

func TestPolicyBeatTimeoutByPhase(t *testing.T) {
	p := newPolicy(Config{})
	spec := &sessionv1.SessionSpec{
		StartTimeoutS: 7,
		Limits:        &sessionv1.Limits{HookTimeoutS: 3},
	}
	key := WorkerKey("s")
	if got := p.BeatTimeout(key); got != 0 {
		t.Fatalf("unknown session %s", got)
	}
	p.note("s", spec, &agent.Observed{State: agent.StateInitializing})
	if got := p.BeatTimeout(key); got != 17*time.Second {
		t.Fatalf("init %s", got)
	}
	p.note("s", spec, &agent.Observed{State: agent.StateStarting})
	if got := p.BeatTimeout(key); got != 17*time.Second {
		t.Fatalf("start %s", got)
	}
	p.note("s", spec, &agent.Observed{State: agent.StateInitialized})
	if got := p.BeatTimeout(key); got != 13*time.Second {
		t.Fatalf("waiting %s", got)
	}
	p.note("s", spec, &agent.Observed{State: agent.StateRunning})
	if got := p.BeatTimeout(key); got != 13*time.Second {
		t.Fatalf("running %s", got)
	}
	if !p.Silent(key, time.Minute) {
		t.Fatal("a silent worker is not stopped")
	}
}

func TestPolicyRestartOutcomes(t *testing.T) {
	p := newPolicy(Config{BackoffMin: time.Millisecond, BackoffMax: 8 * time.Millisecond})
	key := WorkerKey("s")
	p.note("s", &sessionv1.SessionSpec{}, nil)

	again, wait := p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0))
	if !again || wait != time.Millisecond {
		t.Fatalf("crash again %v wait %s", again, wait)
	}
	if view := p.statusEnd("s"); !view.restarting || view.reason != "crash" {
		t.Fatalf("crash view %+v", view)
	}
	again, wait = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 3))
	if !again || wait != 8*time.Millisecond {
		t.Fatalf("grown backoff again %v wait %s", again, wait)
	}

	if again, _ = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_DONE, "exit", 0, 0)); again {
		t.Fatal("DONE restarted")
	}
	if again, _ = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_FAILED, "fail", 0, 0)); again {
		t.Fatal("FAILED restarted")
	}
	if again, _ = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_INIT_FAILED, "bad", 0, 0)); again {
		t.Fatal("INIT_FAILED restarted")
	}
	if again, _ = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_UNSPECIFIED, "signal 9 (killed), no Exiting", 9, 0)); !again {
		t.Fatal("an unrequested signal was not restarted")
	}
}

func TestPolicyRestartLimitSlides(t *testing.T) {
	var now time.Time
	p := newPolicy(Config{BackoffMin: time.Millisecond, BackoffMax: time.Millisecond})
	p.now = func() time.Time { return now }
	now = time.Unix(1_000, 0)
	key := WorkerKey("s")
	p.note("s", &sessionv1.SessionSpec{Restart: &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 2, WindowS: 10,
	}}, nil)

	for i := 0; i < 2; i++ {
		if again, _ := p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); !again {
			t.Fatalf("restart %d refused", i)
		}
	}
	if again, _ := p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); again {
		t.Fatal("third restart was allowed")
	}
	view := p.statusEnd("s")
	if view.restarting || view.terminalReason != ReasonRestartLimit {
		t.Fatalf("limit view %+v", view)
	}

	now = now.Add(11 * time.Second)
	if again, _ := p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); !again {
		t.Fatal("a restart outside the window was refused")
	}
}

func TestPolicyNeverAndShimLost(t *testing.T) {
	p := newPolicy(Config{})
	key := WorkerKey("s")
	p.note("s", &sessionv1.SessionSpec{Restart: &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_NEVER,
	}}, nil)
	if again, _ := p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); again {
		t.Fatal("NEVER restarted a crash")
	}
	if again, _ := p.Restart(key, agent.Ended{}); again {
		t.Fatal("a lost shim was restarted")
	}
	if view := p.statusEnd("s"); view.reason != ReasonShimLost || view.restarting {
		t.Fatalf("lost view %+v", view)
	}
}

func TestPolicyNoBeat(t *testing.T) {
	p := newPolicy(Config{BackoffMin: time.Millisecond, BackoffMax: time.Millisecond})
	key := WorkerKey("s")
	p.lookup = func(agent.WorkerKey) (agent.Observed, bool) {
		return agent.Observed{Incarnation: 4}, true
	}
	p.note("s", &sessionv1.SessionSpec{}, nil)
	p.noteKill(key, 4)
	again, _ := p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_UNSPECIFIED, "signal 9 (killed), no Exiting", 9, 0))
	if !again {
		t.Fatal("no_beat was not restarted")
	}
	view := p.statusEnd("s")
	if !view.noBeat || !view.restarting || view.reason != ReasonNoBeat {
		t.Fatalf("no_beat view %+v", view)
	}

	p.noteKill(key, 4)
	p.lookup = func(agent.WorkerKey) (agent.Observed, bool) {
		return agent.Observed{Incarnation: 5}, true
	}
	again, _ = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_UNSPECIFIED, "signal 9 (killed), no Exiting", 9, 0))
	if view = p.statusEnd("s"); view.noBeat {
		t.Fatal("a kill mark for another incarnation was applied")
	}
	if !again {
		t.Fatal("the signal itself was not restarted")
	}

	p.note("s", &sessionv1.SessionSpec{Restart: &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_NEVER,
	}}, nil)
	p.lookup = func(agent.WorkerKey) (agent.Observed, bool) {
		return agent.Observed{Incarnation: 4}, true
	}
	p.noteKill(key, 4)
	if again, _ = p.Restart(key, endedExit(commonv1.Outcome_OUTCOME_UNSPECIFIED, "signal 9 (killed), no Exiting", 9, 0)); again {
		t.Fatal("NEVER restarted no_beat")
	}
	view = p.statusEnd("s")
	if !view.noBeat || view.overrideReason != ReasonNoBeat || view.restarting {
		t.Fatalf("never no_beat view %+v", view)
	}
}

func TestPolicyRotationStartsTheWindowAtZero(t *testing.T) {
	old := newPolicy(Config{})
	key := WorkerKey("s")
	old.note("s", &sessionv1.SessionSpec{Restart: &sessionv1.RestartPolicy{MaxRestarts: 1, WindowS: 300}}, nil)
	if again, _ := old.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); !again {
		t.Fatal("first restart refused")
	}
	if again, _ := old.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); again {
		t.Fatal("limit not reached")
	}
	next := newPolicy(Config{})
	next.note("s", &sessionv1.SessionSpec{Restart: &sessionv1.RestartPolicy{MaxRestarts: 1, WindowS: 300}}, nil)
	if again, _ := next.Restart(key, endedExit(commonv1.Outcome_OUTCOME_CRASHED, "crash", 0, 0)); !again {
		t.Fatal("a new process kept the previous window")
	}
}

func endedExit(outcome commonv1.Outcome, reason string, signal int32, restarts uint32) agent.Ended {
	return agent.Ended{
		Exit:     &commonv1.ExitInfo{Outcome: outcome, Reason: reason, Signal: signal},
		Restarts: restarts,
	}
}
