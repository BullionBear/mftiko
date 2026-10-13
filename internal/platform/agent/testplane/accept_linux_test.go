package testplane

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/c3"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// E1-20: the acceptance scenarios of E1 (#33), end to end. Adoption after kill -9 and pid reuse are in
// TestAdoptAfterKill9 and TestOneIncarnationAtATime.

// recordPolicy restarts nothing, so that what ended stays observable, and records each end it is told of.
type recordPolicy struct {
	agent.Always
	mu    sync.Mutex
	ended map[agent.WorkerKey][]agent.Ended
}

func (p *recordPolicy) Restart(k agent.WorkerKey, e agent.Ended) (bool, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ended == nil {
		p.ended = map[agent.WorkerKey][]agent.Ended{}
	}
	p.ended[k] = append(p.ended[k], e)
	return false, 0
}

func (p *recordPolicy) ends(name string) []agent.Ended {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.ended[Key(plane, name)])
}

// processGone reports whether pid has ended: no such process, or a zombie no one reaped.
func processGone(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	i := strings.LastIndexByte(string(b), ')')
	return i >= 0 && strings.HasPrefix(string(b[i+1:]), " Z")
}

func (e *env) socket(name string) string {
	return shim.SocketPath(e.cfg.WorkDir, string(plane), string(Key(plane, name)))
}

func wantErr(t *testing.T, done <-chan struct{}, got *error, what string, ok func(error) bool) {
	t.Helper()
	select {
	case <-done:
		if !ok(*got) {
			t.Fatalf("the agent ended with %v, want %s", *got, what)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("the agent went on; want it ended by %s", what)
	}
}

// Rotation: the agent stops, a new one takes over. The workers do not die and are not spawned again; the new
// agent has their state from their shims' Attached, an ended one's exit included, and controls them.
func TestRotation(t *testing.T) {
	e := setup(t, nil)
	e.cfg.Policy = &recordPolicy{}
	done, ret, stop := e.runWith(e.cfg)
	e.want("a", Worker{})
	e.want("b", Worker{Script: fakeworker.Script{Exit: fakeworker.ExitDone, ExitAfterMS: 200}})
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	e.waitStatus("b", "EXITED", phase(runner.PhaseExited, 1))

	stop()
	wantErr(t, done, ret, "its context", func(err error) bool { return errors.Is(err, context.Canceled) })
	if rec := e.record(); rec != nil {
		t.Fatalf("the AgentRecord outlived a clean stop: %v", rec)
	}

	pol := &recordPolicy{}
	cfg := e.cfg
	cfg.Policy, cfg.Term.ID = pol, "next"
	e.runWith(cfg)
	deadline := time.Now().Add(20 * time.Second)
	for e.record().GetEpoch() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no AgentRecord of epoch 2: %v", e.record())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // a few reconciles
	if n := e.count("a", "start"); n != 1 {
		t.Fatalf("a started %d times, want once", n)
	}
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	if ends := pol.ends("b"); len(ends) != 1 || ends[0].Exit.GetOutcome() != commonv1.Outcome_OUTCOME_DONE {
		t.Fatalf("b's end, as the new agent read it from Attached: %+v", ends)
	}

	e.unwant("b")
	e.waitStatus("b", "none", gone)
	e.unwant("a")
	e.waitDown("a", "Stop", func(m *workerv1.WorkerDown) bool { return m.GetStop() != nil })
	e.waitStatus("a", "none", gone)
	for _, name := range []string{"a", "b"} {
		if _, err := os.Lstat(e.socket(name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s's shim still there: %v", name, err)
		}
	}
}

// An agent superseded at a shim: a newer term attaches, the old agent is preempted and ends at once without
// touching the worker, and its epoch is refused from then on.
func TestPreempted(t *testing.T) {
	e := setup(t, nil)
	done, ret := e.run()
	e.want("a", Worker{})
	e.waitStatus("a", "RUNNING at its stage running", func(st *statev1.WorkerStatus) bool { // nothing more to write
		return phase(runner.PhaseRunning, 1)(st) && st.GetStage() == "running"
	})
	pid := e.workerPid("a")
	before, rev := e.status("a")

	ctx := testCtx(t)
	newer, err := c3.Dial(ctx, e.socket("a"), c3.Config{Plane: string(plane), Instance: inst, Epoch: 2, Log: e.cfg.Log})
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	var pe *c3.PreemptedError
	wantErr(t, done, ret, "a preemption by epoch 2", func(err error) bool {
		return errors.Is(err, runner.ErrTermEnded) && errors.As(err, &pe) && pe.ByEpoch == 2
	})

	time.Sleep(200 * time.Millisecond)
	if processGone(pid) {
		t.Fatal("the worker died with the old agent")
	}
	if slices.ContainsFunc(e.downs("a"), func(m *workerv1.WorkerDown) bool { return m.GetStop() != nil }) {
		t.Fatal("the old agent stopped the worker")
	}
	if after, now := e.status("a"); now != rev {
		t.Fatalf("the old agent wrote the WorkerStatus after it was preempted: revision %d, then %d\n%v\n%v", rev, now, before, after)
	}
	_, err = c3.Dial(ctx, e.socket("a"), c3.Config{Plane: string(plane), Instance: inst, Epoch: 1, Log: e.cfg.Log})
	if c3.CodeOf(err) != shim.CodeStaleEpoch {
		t.Fatalf("the old epoch at the shim: %v, want %s", err, shim.CodeStaleEpoch)
	}
	if newer.Attached().GetState() == 0 || newer.Err() != nil {
		t.Fatalf("the newer term lost the shim: %v", newer.Err())
	}
}

// An agent superseded in etcd: a newer term has taken the writer key (PROTOCOL §3.2 W2 step 3). The old agent's
// next write fails with ErrSuperseded, and it ends without touching the worker.
func TestSuperseded(t *testing.T) {
	e := setup(t, nil)
	done, ret := e.run()
	e.want("a", Worker{})
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	pid := e.workerPid("a")

	if _, err := e.root.Put(testCtx(t), state.WriterKey(plane, inst), fmt.Sprintf("%020d", 2)); err != nil {
		t.Fatal(err)
	}
	e.want("b", Worker{}) // its incarnation is a fenced write
	wantErr(t, done, ret, "ErrSuperseded", func(err error) bool {
		return errors.Is(err, runner.ErrTermEnded) && errors.Is(err, state.ErrSuperseded)
	})
	if st, _ := e.status("b"); st != nil || e.count("b", "start") != 0 {
		t.Fatalf("a superseded agent spawned b: %v", st)
	}
	if processGone(pid) {
		t.Fatal("the worker died with the old agent")
	}
}

// The shim is killed: the worker reads EOF on fd 3 and ends by itself (D104), and the agent runs the next
// incarnation once the last is gone. SIGTERM is ignored because Pdeathsig is SIGTERM and arrives with the
// EOF; otherwise the worker can stop on the signal and never journal the EOF (TestShimKilledWorkerSeesEOF).
func TestShimKilled(t *testing.T) {
	e := setup(t, nil)
	e.run()
	e.want("a", Worker{Script: fakeworker.Script{IgnoreSIGTERM: true}})
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	pid := e.workerPid("a")
	if err := syscall.Kill(e.shimPids()[0], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !processGone(pid) {
		if time.Now().After(deadline) {
			t.Fatal("the worker outlived its shim")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := e.count("a", "eof"); n != 1 {
		t.Errorf("the worker journalled %d EOFs on fd 3, want 1", n)
	}
	e.waitStatus("a", "the next incarnation", phase(runner.PhaseRunning, 2))
}

// The host shuts down: SIGTERM to the shim, which sends the worker Stop and waits for its OnStop (PROTOCOL §4.3).
func TestHostSIGTERM(t *testing.T) {
	e := setup(t, nil)
	e.cfg.Policy = &recordPolicy{}
	e.run()
	e.want("a", Worker{})
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	if err := syscall.Kill(e.shimPids()[0], syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	reason := "shim: " + syscall.SIGTERM.String()
	e.waitDown("a", "Stop from its shim", func(m *workerv1.WorkerDown) bool { return m.GetStop().GetReason() == reason })
	st := e.waitStatus("a", "EXITED", phase(runner.PhaseExited, 1))
	if x := st.GetLastExit(); !x.GetOnStopCompleted() || x.GetReason() != "stop: "+reason ||
		x.GetOutcome() != commonv1.Outcome_OUTCOME_DONE || x.GetCode() != 0 {
		t.Fatalf("ExitInfo %v, want OnStop completed, stop: %s", x, reason)
	}
}
