package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/sts"
)

const (
	envTestShim  = "MFTIKO_TEST_SHIM"
	envTestAgent = "MFTIKO_STS_TEST_AGENT"
	testInst     = "sts-tw-1"
)

func TestMain(m *testing.M) {
	fakeworker.MaybeMain()
	if os.Getenv(envTestShim) == "1" {
		os.Exit(shim.Run(logger.NewWithWriter("mftiko-shim", os.Stderr)))
	}
	if v := os.Getenv(envTestAgent); v != "" {
		os.Exit(runAgent(v))
	}
	os.Exit(m.Run())
}

// agentArgs is what a second agent process needs. The password is passed in:
// etcdtest derives it from a secret of each process.
type agentArgs struct {
	Topology  state.Topology
	Password  string
	Instance  string
	WorkDir   string
	DataDir   string
	Shim      string
	Registry  string
	PlainHTTP bool
	ShimEnv   []string
	Resync    time.Duration
}

func runAgent(v string) int {
	var a agentArgs
	if err := json.Unmarshal([]byte(v), &a); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	err := Run(context.Background(), Config{
		Topology: a.Topology, Instance: a.Instance, Dial: state.AuthDial(a.Instance, a.Password),
		Term:    state.TermOptions{ID: "sts-subprocess"},
		WorkDir: a.WorkDir, DataDir: a.DataDir, Shim: a.Shim, ShimEnv: a.ShimEnv,
		Registry: a.Registry, PlainHTTP: a.PlainHTTP, Sign: testSigner,
		Resync: a.Resync, Grace: DefaultGrace, Release: "test", Log: log,
		Mirror: state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
	})
	fmt.Fprintln(os.Stderr, "agent:", err)
	if errors.Is(err, context.Canceled) {
		return 0
	}
	return 1
}

// An assignment moves the session through PENDING and WAITING, then RUNNING
// once start is set. A session removed from the plan passes through STOPPING
// and ends DONE. A deleted assignment ends the same way.
func TestAssignmentLifecycle(t *testing.T) {
	e := setup(t)
	const (
		waiting = "a1b2c3"
		hanging = "c3d4e5"
		ending  = "d4e5f6"
		deleted = "e5f6a7"
	)
	e.setScript(hanging, fakeworker.Script{Hang: fakeworker.AtStart})
	e.setScript(ending, fakeworker.Script{OnStopMS: 500})
	specRev := e.putSpec(waiting, sessionSpec(1, 1))
	e.putDemand(waiting, runningDemand(false, 1))
	e.putSpec(hanging, sessionSpec(2, 1))
	e.putDemand(hanging, runningDemand(true, 2))
	e.putSpec(ending, sessionSpec(3, 1))
	e.putDemand(ending, runningDemand(true, 3))
	e.putSpec(deleted, sessionSpec(4, 1))
	e.putDemand(deleted, runningDemand(true, 4))

	rev := e.rev()
	ph := e.collect(rev)
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()

	st := ph.wait(waiting, statev1.Phase_PHASE_PENDING, statev1.Phase_PHASE_WAITING)
	if st.GetObservedGeneration() != specRev {
		t.Fatalf("observed_generation %d, spec mod_revision %d", st.GetObservedGeneration(), specRev)
	}
	if st.GetIncarnation() == 0 {
		t.Fatal("WAITING without an incarnation")
	}
	e.waitDown(waiting, func(m *workerv1.WorkerDown) bool {
		init := m.GetInit()
		if init == nil || init.GetContract() != workerContract || init.GetDeadlineMs() != uint32(DefaultStartTimeoutS)*1000 {
			return false
		}
		msg := &stsctlv1.InitPayload{}
		if err := proto.Unmarshal(init.GetPayload(), msg); err != nil {
			return false
		}
		return msg.GetSessionId() == waiting && msg.GetIncarnation() != 0 &&
			msg.GetRecvWindowMs() == uint32(DefaultRecvWindowMS) && msg.GetHookTimeoutS() == uint32(DefaultHookTimeoutS) &&
			msg.GetNats().GetUserJwt() != "" && msg.GetNats().GetInboxPrefix() == "_INBOX_"+waiting
	})
	if e.hasDown(waiting, func(m *workerv1.WorkerDown) bool { return m.GetStart() != nil || m.GetRelay() != nil }) {
		t.Fatal("Start or a bindings command was sent while start was false")
	}
	ph.wait(hanging, statev1.Phase_PHASE_STARTING)

	e.putDemand(waiting, runningDemand(true, 1))
	ph.wait(waiting, statev1.Phase_PHASE_RUNNING)
	if got := ph.current(hanging); got.GetPhase() != statev1.Phase_PHASE_STARTING {
		t.Fatalf("hung session left STARTING: %s", got.GetPhase())
	}
	e.waitDown(waiting, func(m *workerv1.WorkerDown) bool { return m.GetStart() != nil })

	ph.wait(ending, statev1.Phase_PHASE_RUNNING)
	e.putDemand(ending, endedDemand("ended_by_spec", 3))
	st = ph.wait(ending, statev1.Phase_PHASE_STOPPING, statev1.Phase_PHASE_DONE)
	if st.GetReason() != "ended_by_spec" {
		t.Fatalf("DONE reason %q", st.GetReason())
	}

	ph.wait(deleted, statev1.Phase_PHASE_RUNNING)
	e.delDemand(deleted)
	st = ph.wait(deleted, statev1.Phase_PHASE_DONE)
	if st.GetPhase() != statev1.Phase_PHASE_DONE {
		t.Fatalf("deleted assignment phase %s", st.GetPhase())
	}
}

// The later of two overlapping sessions is FAILED with algo_conflict and is not spawned.
func TestAlgoConflict(t *testing.T) {
	e := setup(t)
	const early, late, other = "a1b2c3", "b2c3d4", "c3d4e5"
	e.putSpec(early, sessionSpec(7, 1, 2))
	e.putDemand(early, runningDemand(true, 1000))
	e.putSpec(late, sessionSpec(7, 2, 3))
	e.putDemand(late, runningDemand(true, 2000))
	e.putSpec(other, sessionSpec(8, 2))
	e.putDemand(other, runningDemand(true, 3000))

	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(early, statev1.Phase_PHASE_RUNNING)
	st := ph.wait(late, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonAlgoConflict {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(late); n != 0 {
		t.Fatalf("later session spawned %d times", n)
	}
	ph.wait(other, statev1.Phase_PHASE_RUNNING)
}

// A session that reached DONE, and one that failed algo_conflict, stay that way
// after the agent process is replaced, even when the assignment says RUNNING.
func TestTerminalNotRestarted(t *testing.T) {
	e := setup(t)
	const holder, late, doneID, probe, sibling = "a1b2c3", "b2c3d4", "c3d4e5", "d4e5f6", "e5f6a7"
	e.setScript(doneID, fakeworker.Script{OnStopMS: 500})
	e.putSpec(holder, sessionSpec(7, 1))
	e.putDemand(holder, runningDemand(true, 100))
	e.putSpec(late, sessionSpec(7, 1))
	e.putDemand(late, runningDemand(true, 200))
	e.putSpec(doneID, sessionSpec(8, 1))
	e.putDemand(doneID, runningDemand(true, 100))

	ph := e.collect(e.rev())
	stop, done := e.start()
	ph.wait(holder, statev1.Phase_PHASE_RUNNING)
	st := ph.wait(late, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonAlgoConflict {
		t.Fatalf("conflict reason %q", st.GetReason())
	}
	ph.wait(doneID, statev1.Phase_PHASE_RUNNING)
	e.putDemand(doneID, endedDemand("ended_by_spec", 100))
	ph.wait(doneID, statev1.Phase_PHASE_STOPPING, statev1.Phase_PHASE_DONE)
	starts := e.starts(doneID)
	e.putDemand(doneID, runningDemand(true, 100))
	e.putSpec(probe, sessionSpec(9, 1))
	e.putDemand(probe, runningDemand(false, 1))
	ph.wait(probe, statev1.Phase_PHASE_WAITING)
	if got := e.status(doneID); got.GetPhase() != statev1.Phase_PHASE_DONE || e.starts(doneID) != starts {
		t.Fatalf("revived before restart: phase %s starts %d", got.GetPhase(), e.starts(doneID))
	}
	if got := e.status(late); got.GetPhase() != statev1.Phase_PHASE_FAILED || got.GetReason() != ReasonAlgoConflict || e.starts(late) != 0 {
		t.Fatalf("conflict changed before restart: %+v starts %d", got, e.starts(late))
	}

	stop()
	waitDone(t, done)
	e.cfg.Term.ID = t.Name() + "-next"
	e.putSpec(sibling, sessionSpec(10, 1))
	e.putDemand(sibling, runningDemand(true, 1))
	stop, done = e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(sibling, statev1.Phase_PHASE_RUNNING)
	if got := e.status(doneID); got.GetPhase() != statev1.Phase_PHASE_DONE || got.GetReason() != "ended_by_spec" {
		t.Fatalf("done session after restart: %+v", got)
	}
	if e.starts(doneID) != starts {
		t.Fatalf("done session spawned again: %d starts, was %d", e.starts(doneID), starts)
	}
	if got := e.status(late); got.GetPhase() != statev1.Phase_PHASE_FAILED || got.GetReason() != ReasonAlgoConflict || e.starts(late) != 0 {
		t.Fatalf("conflict after restart: %+v starts %d", got, e.starts(late))
	}
}

// Two processes campaign. Only the leader writes a status or an AgentRecord.
func TestOneWriter(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(false, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_WAITING)
	if rec := e.record(); rec.GetEpoch() != 1 {
		t.Fatalf("epoch %d", rec.GetEpoch())
	}

	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args, err := json.Marshal(agentArgs{
		Topology: e.cfg.Topology, Password: etcdtest.Password(testInst), Instance: testInst,
		WorkDir: e.cfg.WorkDir, DataDir: e.cfg.DataDir, Shim: e.cfg.Shim, ShimEnv: e.cfg.ShimEnv,
		Registry: e.cfg.Registry, PlainHTTP: e.cfg.PlainHTTP, Resync: e.cfg.Resync,
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self)
	cmd.Env = append(os.Environ(), envTestAgent+"="+string(args))
	stderr := &syncBuf{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill() //nolint:errcheck // the process may already be gone
		cmd.Wait()         //nolint:errcheck
		if t.Failed() {
			t.Logf("subprocess:\n%s", stderr.String())
		}
	})
	e.waitElections(2)
	if strings.Contains(stderr.String(), "agent record written") {
		t.Fatalf("the second process wrote an agent record:\n%s", stderr.String())
	}
	if rec := e.record(); rec.GetEpoch() != 1 {
		t.Fatalf("epoch moved to %d", rec.GetEpoch())
	}
	if got := e.status(id); got.GetPhase() != statev1.Phase_PHASE_WAITING {
		t.Fatalf("status changed: %+v", got)
	}
}

// An assignment with no spec, or a spec that will not decode, is logged and
// gets no status. A usable session still runs.
func TestNoSpecNoStatus(t *testing.T) {
	e := setup(t)
	const missing, bad, good = "a1b2c3", "b2c3d4", "c3d4e5"
	e.putDemand(missing, runningDemand(true, 1))
	e.putRaw(state.SessionSpecKey(testInst, bad), []byte{0x0a, 0x05, 0x01})
	e.putDemand(bad, runningDemand(true, 2))
	e.putSpec(good, sessionSpec(1, 1))
	e.putDemand(good, runningDemand(false, 3))

	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(good, statev1.Phase_PHASE_WAITING)
	e.waitLog("assignment has no usable spec")
	for _, id := range []string{missing, bad} {
		if !strings.Contains(e.log.String(), id) {
			t.Fatalf("log does not name %s:\n%s", id, e.log.String())
		}
		if ph.seen(id) {
			t.Fatalf("%s got a status %v", id, ph.history(id))
		}
		if st := e.status(id); st != nil {
			t.Fatalf("%s status %+v", id, st)
		}
	}
}

// A changed spec encoding replaces the incarnation and moves observed_generation.
func TestSpecChangeReplacesIncarnation(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	first := sessionSpec(7, 1)
	e.putSpec(id, first)
	rev1 := e.modRev(state.SessionSpecKey(testInst, id))
	e.putDemand(id, runningDemand(true, 1))

	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_RUNNING)
	if st.GetObservedGeneration() != rev1 || e.starts(id) != 1 {
		t.Fatalf("before change: gen %d rev %d starts %d", st.GetObservedGeneration(), rev1, e.starts(id))
	}
	first.Name = "replaced"
	e.putSpec(id, first)
	rev2 := e.modRev(state.SessionSpecKey(testInst, id))
	if rev2 == rev1 {
		t.Fatal("spec revision did not move")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		st = ph.current(id)
		if st.GetPhase() == statev1.Phase_PHASE_RUNNING && st.GetObservedGeneration() == rev2 &&
			e.starts(id) >= 2 && strings.Contains(e.log.String(), "spec changed; replacing the incarnation") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after change: phase %s gen %d want %d starts %d\n%s",
				st.GetPhase(), st.GetObservedGeneration(), rev2, e.starts(id), e.log.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A crash restarts with backoff, the next Start keeps counting seq, and the
// window ends the session at FAILED restart_limit.
func TestRestartLimitContinuesSeq(t *testing.T) {
	e := setup(t)
	e.cfg.BackoffMin = 200 * time.Millisecond
	e.cfg.BackoffMax = 200 * time.Millisecond
	const id = "a1b2c3"
	e.setScript(id, fakeworker.Script{Exit: fakeworker.ExitCrashed, ExitAfterMS: 30})
	spec := sessionSpec(1, 1)
	spec.Restart = &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 2, WindowS: 300,
	}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_RESTARTING, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonRestartLimit {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(id); n != 3 {
		t.Fatalf("starts %d", n)
	}
	if seqs := e.startSeqs(id); len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("start seqs %v", seqs)
	}
}

// A worker that crashes while no agent runs is still restarted by the next one.
// The first Sync after adoption sees the exit before any Step has asked the
// restart policy about it; publishing the exit there made the crash terminal,
// and a terminal session never comes back (E09-D045, E09-D095, #327). The same gap
// exists inside a round, between the Step's observation and the Sync's.
func TestCrashWhileAgentDownStillRestarts(t *testing.T) {
	e := setup(t)
	e.cfg.BackoffMin = 100 * time.Millisecond
	e.cfg.BackoffMax = 100 * time.Millisecond
	const id = "a1b2c3"
	e.setScript(id, fakeworker.Script{Exit: fakeworker.ExitCrashed, ExitAfterMS: 2000})
	spec := sessionSpec(1, 1)
	spec.Restart = &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 5, WindowS: 300,
	}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	stop()
	waitDone(t, done)
	pollUntil(t, "the first incarnation to crash with no agent", func() bool {
		for _, en := range e.journalEntries(id) {
			if en.Event == "exit" {
				return true
			}
		}
		return false
	})
	if n := e.starts(id); n != 1 {
		t.Fatalf("starts %d before the next agent", n)
	}

	e.cfg.Term.ID = t.Name() + "-next"
	stop, done = e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	pollUntil(t, "a restart after adoption", func() bool { return e.starts(id) >= 2 })
	hist := ph.history(id)
	if !containsInOrder(hist, []statev1.Phase{statev1.Phase_PHASE_RUNNING, statev1.Phase_PHASE_RESTARTING}) {
		t.Fatalf("phases %v", hist)
	}
	for _, p := range hist {
		if p == statev1.Phase_PHASE_FAILED || p == statev1.Phase_PHASE_DONE {
			t.Fatalf("terminal before the restart: phases %v, status %s", hist, describeStatus(e.status(id)))
		}
	}
	// restart_count is this agent's restarts: the first agent made none (#385).
	pollUntil(t, "restart_count after the restart", func() bool { return e.status(id).GetRestartCount() >= 1 })
	if got, starts := e.status(id).GetRestartCount(), e.starts(id); int(got) > starts-1 {
		t.Fatalf("restart_count %d with %d starts", got, starts)
	}
}

// A worker that stops beating is SIGKILLed with no Stop. The shim records the
// signal; SessionStatus says HUNG / no_beat and does not restart.
func TestNoBeatKillsWithoutStop(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	e.setScript(id, fakeworker.Script{BeatMS: 50, Beats: 1})
	spec := sessionSpec(1, 1)
	spec.StartTimeoutS = 1
	spec.Limits = &sessionv1.Limits{HookTimeoutS: 1}
	spec.Restart = &sessionv1.RestartPolicy{Mode: sessionv1.RestartMode_RESTART_MODE_NEVER}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	st := ph.waitWithin(30*time.Second, id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonNoBeat {
		t.Fatalf("reason %q exit %+v", st.GetReason(), st.GetLastExit())
	}
	ex := st.GetLastExit()
	if ex.GetOutcome() != commonv1.Outcome_OUTCOME_HUNG || ex.GetReason() != ReasonNoBeat || ex.GetSignal() != int32(syscall.SIGKILL) {
		t.Fatalf("last exit %+v", ex)
	}
	if n := e.starts(id); n != 1 {
		t.Fatalf("starts %d", n)
	}
	if e.hasDown(id, func(m *workerv1.WorkerDown) bool { return m.GetStop() != nil }) {
		t.Fatal("Stop was sent for a missing beat")
	}
	for _, en := range e.journalEntries(id) {
		if en.Event == "sigterm" {
			t.Fatal("SIGTERM was delivered for a missing beat")
		}
	}
}

// DONE, FAILED, and INIT_FAILED are terminal. The worker is not spawned again.
func TestOutcomesThatDoNotRestart(t *testing.T) {
	e := setup(t)
	e.cfg.BackoffMin = time.Millisecond
	e.cfg.BackoffMax = time.Millisecond
	const doneID, failedID, initID = "a1b2c3", "b2c3d4", "c3d4e5"
	e.setScript(doneID, fakeworker.Script{Exit: fakeworker.ExitDone, ExitAfterMS: 20})
	e.setScript(failedID, fakeworker.Script{Exit: fakeworker.ExitFailed, ExitAfterMS: 20})
	e.setScript(initID, fakeworker.Script{FailInit: "bad config"})
	on := &sessionv1.RestartPolicy{Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 5, WindowS: 300}
	for i, id := range []string{doneID, failedID, initID} {
		spec := sessionSpec(int64(i+1), 1)
		spec.Restart = on
		e.putSpec(id, spec)
		e.putDemand(id, runningDemand(true, 1))
	}
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(doneID, statev1.Phase_PHASE_DONE)
	if st.GetReason() != "exit" {
		t.Fatalf("done reason %q", st.GetReason())
	}
	st = ph.wait(failedID, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != "fail" {
		t.Fatalf("failed reason %q", st.GetReason())
	}
	st = ph.wait(initID, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != "bad config" {
		t.Fatalf("init reason %q", st.GetReason())
	}
	if e.starts(doneID) != 1 || e.starts(failedID) != 1 || e.starts(initID) != 1 {
		t.Fatalf("starts done %d failed %d init %d", e.starts(doneID), e.starts(failedID), e.starts(initID))
	}
}

// Killing the shim writes FAILED shim_lost and does not spawn the session again.
func TestShimLost(t *testing.T) {
	e := setup(t)
	const id, sibling = "a1b2c3", "b2c3d4"
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	ppid := e.shimPID(id)
	worker := e.workerPID(id)
	t.Cleanup(func() { syscall.Kill(worker, syscall.SIGKILL) }) //nolint:errcheck // already exited
	if err := syscall.Kill(ppid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	st := ph.wait(id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonShimLost {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(id); n != 1 {
		t.Fatalf("starts %d", n)
	}
	e.putSpec(sibling, sessionSpec(2, 1))
	e.putDemand(sibling, runningDemand(false, 2))
	ph.wait(sibling, statev1.Phase_PHASE_WAITING)
	if n := e.starts(id); n != 1 {
		t.Fatalf("lost session spawned again: %d starts", n)
	}
}

// A missing digest is a permanent artifact error and ends the session without a worker.
func TestArtifactNotFound(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	spec := sessionSpec(1, 1)
	spec.Strategy = &sessionv1.StrategyRef{Name: "gone", Code: "sha256:" + strings.Repeat("ab", 32), Repository: "hello"}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != artifact.ReasonNotFound {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(id); n != 0 {
		t.Fatalf("spawned %d times", n)
	}
}

// A temporary registry error keeps the session PENDING and then starts it.
func TestPullRetries(t *testing.T) {
	e := setup(t)
	e.reg.failManifests.Store(1)
	const id = "a1b2c3"
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(false, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_PENDING, statev1.Phase_PHASE_WAITING)
}

// grace_ms is the Stop grace. 0 uses the deployment default of 10s (E09-D062).
func TestStopGraceFromSpec(t *testing.T) {
	e := setup(t)
	const custom, plain = "a1b2c3", "b2c3d4"
	spec := sessionSpec(1, 1)
	spec.GraceMs = 2500
	e.putSpec(custom, spec)
	e.putDemand(custom, runningDemand(true, 1))
	e.putSpec(plain, sessionSpec(2, 1))
	e.putDemand(plain, runningDemand(true, 2))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(custom, statev1.Phase_PHASE_RUNNING)
	ph.wait(plain, statev1.Phase_PHASE_RUNNING)
	e.putDemand(custom, endedDemand("ended_by_spec", 1))
	e.putDemand(plain, endedDemand("ended_by_spec", 2))
	ph.wait(custom, statev1.Phase_PHASE_DONE)
	ph.wait(plain, statev1.Phase_PHASE_DONE)
	if got := e.stopGrace(custom); got != 2500 {
		t.Fatalf("custom grace %d", got)
	}
	if got := e.stopGrace(plain); got != 10000 {
		t.Fatalf("default grace %d", got)
	}
}

func (e *env) stopGrace(id string) uint32 {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, en := range e.journalEntries(id) {
			if en.Event != "down" {
				continue
			}
			m, err := en.Down()
			if err != nil || m.GetStop() == nil {
				continue
			}
			return m.GetStop().GetGraceMs()
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("no Stop in %s\n%+v", id, e.journalEntries(id))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

var (
	packOnce sync.Once
	packImg  artifact.Image
	packErr  error
)

func packedStrategy() (artifact.Image, error) {
	packOnce.Do(func() {
		exe, err := os.Executable()
		if err != nil {
			packErr = err
			return
		}
		bin, err := os.ReadFile(exe)
		if err != nil {
			packErr = err
			return
		}
		packImg, packErr = artifact.PackStrategy(bin, "fake", nil)
	})
	return packImg, packErr
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type env struct {
	t       *testing.T
	root    *clientv3.Client
	cfg     Config
	log     *syncBuf
	journal string
	reg     *memRegistry
	digest  string
	repo    string

	mu      sync.Mutex
	pids    []int
	scripts map[string]fakeworker.Script
}

func testSigner(string, sts.Permissions) (string, string, error) {
	return "test-jwt", "SUAG" + "test-seed", nil
}

func setup(t *testing.T) *env {
	t.Helper()
	s := etcdtest.Start(t)
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{testInst: "tw", state.ControllerInstance: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{testInst: state.PlaneSTS})
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	img, err := packedStrategy()
	if err != nil {
		t.Fatal(err)
	}
	reg := newMemRegistry()
	t.Cleanup(reg.Close)
	pushCtx, pushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = artifact.Push(pushCtx, img, artifact.Destination{Repository: reg.repo(), PlainHTTP: true})
	pushCancel()
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "sts-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	data := t.TempDir()
	buf := &syncBuf{}
	log := zerolog.New(buf).Level(zerolog.InfoLevel)
	e := &env{t: t, root: root, log: buf, journal: t.TempDir(), scripts: map[string]fakeworker.Script{}, reg: reg, digest: img.Digest, repo: "hello"}
	e.cfg = Config{
		Topology: topo, Instance: testInst, Dial: state.AuthDial(testInst, etcdtest.Password(testInst)),
		Term:           state.TermOptions{ID: t.Name()},
		WorkDir:        wd,
		DataDir:        data,
		Shim:           self,
		ShimEnv:        []string{envTestShim + "=1"},
		Registry:       reg.addr(),
		DataRepository: "state",
		PlainHTTP:      true,
		Sign:           testSigner,
		Grace:          DefaultGrace,
		Resync:         50 * time.Millisecond,
		StaleAfter:     5 * time.Second,
		Mirror:         state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Release:        "test",
		Log:            log,
	}
	e.cfg.ExtraEnv = func(key agent.WorkerKey) []string {
		id := strings.TrimPrefix(string(key), "sts/")
		e.mu.Lock()
		script := e.scripts[id]
		e.mu.Unlock()
		env, err := fakeworker.Env(script, e.journalPath(id))
		if err != nil {
			return nil
		}
		return env
	}
	e.cfg.Started = func(pid int) {
		e.mu.Lock()
		e.pids = append(e.pids, pid)
		e.mu.Unlock()
	}
	t.Cleanup(func() {
		// Inputs are mode 0555 after unpack (E09-D075). TempDir cannot remove them until they are writable.
		_ = artifact.RemoveWritable(filepath.Join(e.cfg.DataDir, "sessions"))
		e.mu.Lock()
		pids := append([]int(nil), e.pids...)
		e.mu.Unlock()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already exited
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", buf.String())
		}
	})
	return e
}

func (e *env) setScript(id string, s fakeworker.Script) {
	e.mu.Lock()
	e.scripts[id] = s
	e.mu.Unlock()
}

func (e *env) journalPath(id string) string { return filepath.Join(e.journal, id) }

func (e *env) start() (stop func(), done <-chan struct{}) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		err := Run(ctx, e.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			e.log.Write([]byte("run: " + err.Error() + "\n")) //nolint:errcheck
		}
	}()
	var once sync.Once
	return func() { once.Do(cancel) }, ch
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("agent did not exit")
	}
}

func (e *env) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func (e *env) put(key string, msg proto.Message) {
	e.t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.putRaw(key, b)
}

func (e *env) putRaw(key string, b []byte) {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	if _, err := e.root.Put(ctx, key, string(b)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) putSpec(id string, spec *sessionv1.SessionSpec) int64 {
	e.t.Helper()
	if spec.GetStrategy() == nil {
		spec.Strategy = &sessionv1.StrategyRef{Name: "fake", Code: e.digest, Repository: e.repo}
	}
	e.put(state.SessionSpecKey(testInst, id), spec)
	return e.modRev(state.SessionSpecKey(testInst, id))
}

func (e *env) putDemand(id string, d *statev1.SessionDemand) {
	e.t.Helper()
	e.put(state.SessionDemandKey(testInst, id), d)
}

func (e *env) delDemand(id string) {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	if _, err := e.root.Delete(ctx, state.SessionDemandKey(testInst, id)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) delSpec(id string) {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	if _, err := e.root.Delete(ctx, state.SessionSpecKey(testInst, id)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) modRev(key string) int64 {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, key)
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		e.t.Fatalf("key %s: %d values", key, len(resp.Kvs))
	}
	return resp.Kvs[0].ModRevision
}

func (e *env) rev() int64 {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.SessionStatusPrefix(testInst), clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		e.t.Fatal(err)
	}
	return resp.Header.Revision
}

func (e *env) status(id string) *statev1.SessionStatus {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.SessionStatusKey(testInst, id))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	st := &statev1.SessionStatus{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *env) record() *statev1.AgentRecord {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.AgentKey(state.PlaneSTS, testInst))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		return nil
	}
	rec := &statev1.AgentRecord{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, rec); err != nil {
		e.t.Fatal(err)
	}
	return rec
}

func (e *env) waitElections(n int) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got int
	for {
		ctx, cancel := e.opCtx()
		resp, err := e.root.Get(ctx, state.ElectionPrefix(state.PlaneSTS, testInst)+"/", clientv3.WithPrefix())
		cancel()
		if err != nil {
			e.t.Fatal(err)
		}
		got = len(resp.Kvs)
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("election keys %d, want %d", got, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) waitLog(sub string) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if strings.Contains(e.log.String(), sub) {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("log missing %q:\n%s", sub, e.log.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func sessionSpec(api int64, algos ...uint32) *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: api, AlgoIds: algos}},
	}
}

func runningDemand(start bool, issued int64) *statev1.SessionDemand {
	return &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: start, IssuedAtNs: issued,
	}
}

func endedDemand(reason string, issued int64) *statev1.SessionDemand {
	return &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_ENDED, EndReason: reason, IssuedAtNs: issued,
	}
}

func (e *env) journalEntries(id string) []fakeworker.Entry {
	e.t.Helper()
	entries, err := fakeworker.ReadJournal(e.journalPath(id))
	if err != nil {
		e.t.Fatal(err)
	}
	return entries
}

func (e *env) starts(id string) int {
	n := 0
	for _, en := range e.journalEntries(id) {
		if en.Event == "start" {
			n++
		}
	}
	return n
}

func (e *env) startSeqs(id string) []uint64 {
	e.t.Helper()
	var out []uint64
	for _, en := range e.journalEntries(id) {
		if en.Event != "down" {
			continue
		}
		m, err := en.Down()
		if err != nil || m.GetStart() == nil {
			continue
		}
		var sp stsctlv1.StartPayload
		if err := proto.Unmarshal(m.GetStart().GetPayload(), &sp); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, sp.GetBindings().GetVersion().GetSeq())
	}
	return out
}

func (e *env) hasDown(id string, pred func(*workerv1.WorkerDown) bool) bool {
	for _, en := range e.journalEntries(id) {
		if en.Event != "down" {
			continue
		}
		m, err := en.Down()
		if err != nil {
			continue
		}
		if pred(m) {
			return true
		}
	}
	return false
}

func (e *env) waitDown(id string, pred func(*workerv1.WorkerDown) bool) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if e.hasDown(id, pred) {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("journal %s never matched\n%+v", id, e.journalEntries(id))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) shimPID(id string) int {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for i := len(e.journalEntries(id)) - 1; i >= 0; i-- {
			en := e.journalEntries(id)[i]
			if en.Event == "ids" && en.PPID > 0 {
				return en.PPID
			}
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("no shim pid in %s", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) workerPID(id string) int {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for i := len(e.journalEntries(id)) - 1; i >= 0; i-- {
			en := e.journalEntries(id)[i]
			if en.Event == "ids" && en.PID > 0 {
				return en.PID
			}
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("no worker pid in %s", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// feed is the SessionStatus watch, so a phase that is written and then replaced
// is still visible.
type feed struct {
	t    *testing.T
	mu   sync.Mutex
	hist map[string][]statev1.Phase
	last map[string]*statev1.SessionStatus
	ch   chan struct{}
	err  error
}

func (e *env) collect(rev int64) *feed {
	e.t.Helper()
	f := &feed{
		t: e.t, hist: map[string][]statev1.Phase{}, last: map[string]*statev1.SessionStatus{},
		ch: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	ch := e.root.Watch(ctx, state.SessionStatusPrefix(testInst), clientv3.WithPrefix(), clientv3.WithRev(rev+1))
	go func() {
		for resp := range ch {
			if err := resp.Err(); err != nil {
				f.mu.Lock()
				f.err = err
				f.mu.Unlock()
				return
			}
			for _, ev := range resp.Events {
				if ev.Type != clientv3.EventTypePut {
					continue
				}
				id := strings.TrimPrefix(string(ev.Kv.Key), state.SessionStatusPrefix(testInst))
				st := &statev1.SessionStatus{}
				if err := proto.Unmarshal(ev.Kv.Value, st); err != nil {
					continue
				}
				f.add(id, st)
			}
		}
	}()
	return f
}

func (f *feed) add(id string, st *statev1.SessionStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := statev1.Phase_PHASE_UNSPECIFIED
	if n := len(f.hist[id]); n > 0 {
		prev = f.hist[id][n-1]
	}
	if st.GetPhase() != prev {
		f.hist[id] = append(f.hist[id], st.GetPhase())
	}
	f.last[id] = st
	select {
	case f.ch <- struct{}{}:
	default:
	}
}

func (f *feed) current(id string) *statev1.SessionStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[id]
}

func (f *feed) history(id string) []statev1.Phase {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]statev1.Phase(nil), f.hist[id]...)
}

func (f *feed) seen(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.hist[id]) > 0
}

// wait blocks until id's phases contain seq in order and the latest phase is seq's last.
func (f *feed) wait(id string, seq ...statev1.Phase) *statev1.SessionStatus {
	f.t.Helper()
	return f.waitWithin(20*time.Second, id, seq...)
}

func (f *feed) waitWithin(limit time.Duration, id string, seq ...statev1.Phase) *statev1.SessionStatus {
	f.t.Helper()
	deadline := time.Now().Add(limit)
	var hist []statev1.Phase
	for {
		f.mu.Lock()
		hist = append([]statev1.Phase(nil), f.hist[id]...)
		last := f.last[id]
		err := f.err
		f.mu.Unlock()
		if err != nil {
			f.t.Fatalf("watch: %v", err)
		}
		if len(seq) > 0 && len(hist) > 0 && hist[len(hist)-1] == seq[len(seq)-1] && containsInOrder(hist, seq) {
			return last
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("%s phases %v, want %v; last status: %s", id, hist, seq, describeStatus(last))
		}
		select {
		case <-f.ch:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// describeStatus is the part of a SessionStatus a failed wait needs to tell why
// the session ended where it did.
func describeStatus(st *statev1.SessionStatus) string {
	if st == nil {
		return "none"
	}
	return fmt.Sprintf("phase=%s reason=%q incarnation=%d restart_count=%d last_exit={%v}",
		st.GetPhase(), st.GetReason(), st.GetIncarnation(), st.GetRestartCount(), st.GetLastExit())
}

func containsInOrder(got, want []statev1.Phase) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}
