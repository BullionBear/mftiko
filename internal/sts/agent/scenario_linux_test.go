package agent

import (
	"context"
	"errors"
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
	"github.com/BullionBear/mftiko/internal/controller/daemon"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// The scenarios are epic #11's done criteria, end to end: the test writes only
// SessionSpec. An in-process controller derives SessionDemand from it, and the
// agent pulls the artifact from the in-process registry and runs it (E09-D065).

// hello is deployed by digest. The controller assigns it and grants start, the
// agent runs it to RUNNING, and an ENDED spec takes it through STOPPING to DONE.
func TestScenarioHelloByDigest(t *testing.T) {
	e := setupHello(t)
	e.startController()
	const id = "e9a001"
	spec := &sessionv1.SessionSpec{
		Name: "hello", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Strategy: &sessionv1.StrategyRef{Name: "hello", Code: e.digest, Repository: e.repo},
	}
	ph := e.collect(e.rev())
	specRev := e.putSpec(id, spec)
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()

	st := ph.wait(id, statev1.Phase_PHASE_PENDING, statev1.Phase_PHASE_RUNNING)
	if st.GetObservedGeneration() != specRev || st.GetIncarnation() == 0 {
		t.Fatalf("RUNNING status %+v, spec mod_revision %d", st, specRev)
	}
	d := e.demand(id)
	if d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING || !d.GetStart() || d.GetIssuedAtNs() == 0 {
		t.Fatalf("controller assignment %+v", d)
	}

	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	endRev := e.putSpec(id, spec)
	st = ph.wait(id, statev1.Phase_PHASE_STOPPING, statev1.Phase_PHASE_DONE)
	if st.GetReason() != "ended_by_spec" {
		t.Fatalf("DONE reason %q", st.GetReason())
	}
	// Read from history: once the session is DONE and its worker gone, the
	// controller may already have removed the assignment (E13-D033, #327).
	ended := e.demandSince(id, endRev, func(d *statev1.SessionDemand) bool {
		return d.GetDesired() == sessionv1.DesiredState_DESIRED_STATE_ENDED
	})
	if ended.GetEndReason() != "ended_by_spec" {
		t.Fatalf("controller end %+v", ended)
	}
}

// A crashing worker is restarted after the backoff, longer each time, until the
// window ends it at FAILED restart_limit. A worker that stops beating is
// SIGKILLed and recorded as HUNG / no_beat, and is not restarted (E09-D019).
func TestScenarioCrashAndHung(t *testing.T) {
	e := setup(t)
	e.cfg.BackoffMin = 300 * time.Millisecond
	e.cfg.BackoffMax = 2 * time.Second
	spawns := e.spawnTimes()
	e.startController()
	const crash, hung = "e9b001", "e9b002"

	e.setScript(crash, fakeworker.Script{Exit: fakeworker.ExitCrashed, ExitAfterMS: 30})
	crashSpec := scenarioSpec(e)
	crashSpec.Restart = &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 2, WindowS: 300,
	}
	e.setScript(hung, fakeworker.Script{BeatMS: 50, Beats: 1})
	hungSpec := scenarioSpec(e)
	hungSpec.StartTimeoutS = 1
	hungSpec.Limits = &sessionv1.Limits{HookTimeoutS: 1}
	hungSpec.Restart = &sessionv1.RestartPolicy{Mode: sessionv1.RestartMode_RESTART_MODE_NEVER}

	ph := e.collect(e.rev())
	e.putSpec(crash, crashSpec)
	e.putSpec(hung, hungSpec)
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()

	st := ph.waitWithin(30*time.Second, crash,
		statev1.Phase_PHASE_RUNNING, statev1.Phase_PHASE_RESTARTING, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonRestartLimit {
		t.Fatalf("crash reason %q exit %+v", st.GetReason(), st.GetLastExit())
	}
	if n := e.starts(crash); n != 3 {
		t.Fatalf("crash starts %d, want 3", n)
	}
	at := spawns(crash)
	if len(at) != 3 {
		t.Fatalf("crash spawns %v", at)
	}
	b := agent.Backoff{Min: e.cfg.BackoffMin, Max: e.cfg.BackoffMax}
	for i := 1; i < len(at); i++ {
		if gap, want := at[i].Sub(at[i-1]), b.After(uint32(i-1)); gap < want {
			t.Fatalf("restart %d after %s, backoff %s", i, gap, want)
		}
	}

	st = ph.waitWithin(30*time.Second, hung, statev1.Phase_PHASE_RUNNING, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonNoBeat {
		t.Fatalf("hung reason %q exit %+v", st.GetReason(), st.GetLastExit())
	}
	ex := st.GetLastExit()
	if ex.GetOutcome() != commonv1.Outcome_OUTCOME_HUNG || ex.GetReason() != ReasonNoBeat || ex.GetSignal() != int32(syscall.SIGKILL) {
		t.Fatalf("hung last exit %+v", ex)
	}
	if n := e.starts(hung); n != 1 {
		t.Fatalf("hung starts %d, want 1", n)
	}
	// The controller removes a terminal session's assignment (E13-D033). Neither
	// session runs again, and both stay FAILED.
	for id, starts := range map[string]int{crash: 3, hung: 1} {
		pollUntil(t, "assignment of "+id+" removed", func() bool { return !e.hasDemand(id) })
		if st := e.status(id); st.GetPhase() != statev1.Phase_PHASE_FAILED {
			t.Fatalf("%s after its assignment went: %+v", id, st)
		}
		if n := e.starts(id); n != starts {
			t.Fatalf("%s starts %d, want %d", id, n, starts)
		}
	}
}

// The ended session's volume is pushed as state_out. A next session that names
// that digest as an input is assigned by the controller, and its worker reads
// the same contents the first one wrote (E09-D084).
func TestScenarioStateOutHandoff(t *testing.T) {
	e := setup(t)
	e.startController()
	const id, next = "e9c001", "e9c002"
	const body = "carried-over"
	e.setScript(id, fakeworker.Script{VolumeFile: "note", VolumeText: body})
	e.setScript(next, fakeworker.Script{InputFile: "prev/note"})

	ph := e.collect(e.rev())
	spec := scenarioSpec(e)
	e.putSpec(id, spec)
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	waitVolumeFile(t, e, id, "note", body)

	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	e.putSpec(id, spec)
	ph.wait(id, statev1.Phase_PHASE_STOPPING, statev1.Phase_PHASE_DONE)
	snap := e.waitStateOut(id)
	if snap.GetRepository() != "state" || !snap.GetClean() || snap.GetDigest() == "" {
		t.Fatalf("state_out %+v", snap)
	}
	if got := e.manifestDigest("state", id); got != snap.GetDigest() {
		t.Fatalf("tag digest %s, state_out %s", got, snap.GetDigest())
	}

	nextSpec := scenarioSpec(e)
	nextSpec.Inputs = []*sessionv1.InputDecl{{
		Name: "prev", Digest: snap.GetDigest(), Repository: snap.GetRepository(),
	}}
	e.putSpec(next, nextSpec)
	ph.wait(next, statev1.Phase_PHASE_PENDING, statev1.Phase_PHASE_RUNNING)
	pollUntil(t, "worker read the input", func() bool {
		for _, en := range e.journalEntries(next) {
			if en.Event == "input" {
				if en.Detail != body {
					t.Fatalf("worker read %q, want %q", en.Detail, body)
				}
				return true
			}
		}
		return false
	})
	pollUntil(t, "logs kept while the spec exists", func() bool { return e.logsKept(id) })
	e.delSpec(id)
	pollUntil(t, "no residue of "+id, func() bool { return e.sessionResidue(id) == "" })
	if e.status(next).GetPhase() != statev1.Phase_PHASE_RUNNING {
		t.Fatalf("next session phase %s", e.status(next).GetPhase())
	}
}

// scenarioSpec runs the fake with no feeds or accounts, so the controller
// grants start as soon as it assigns.
func scenarioSpec(e *env) *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Strategy: &sessionv1.StrategyRef{Name: "fake", Code: e.digest, Repository: e.repo},
	}
}

// startController runs a controller term for the env's topology until the test ends.
func (e *env) startController() {
	e.t.Helper()
	log := zerolog.New(e.log).Level(zerolog.InfoLevel).With().Str("test", "controller").Logger()
	// Start's ctx is the term's: canceling it ends the term.
	ctx, cancel := context.WithCancel(context.Background())
	c, err := daemon.Start(ctx, daemon.Config{
		Topology: e.cfg.Topology,
		Planes:   map[string]state.Plane{e.inst(): state.PlaneSTS},
		Dial:     state.AuthDial(state.ControllerInstance, etcdtest.Password(state.ControllerInstance)),
		Term:     state.TermOptions{ID: e.t.Name() + "-controller"},
		Release:  "test",
		Resync:   50 * time.Millisecond,
		Mirror:   state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Log:      log,
	})
	if err != nil {
		cancel()
		e.t.Fatalf("controller: %v", err)
	}
	e.t.Cleanup(func() {
		cancel()
		if err := c.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			e.t.Errorf("controller: %v", err)
		}
	})
	region := e.cfg.Topology.Instances[e.inst()]
	deadline := time.Now().Add(30 * time.Second)
	for {
		r, ok := c.View().Regions[region]
		if ok && r.Synced && !r.Stale {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("controller view of %s: %+v present %v", region, r, ok)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// demand is the SessionDemand the controller wrote for id.
func (e *env) demand(id string) *statev1.SessionDemand {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.SessionDemandKey(e.inst(), id))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		e.t.Fatalf("no assignment for %s", id)
	}
	d := &statev1.SessionDemand{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, d); err != nil {
		e.t.Fatal(err)
	}
	return d
}

// demandSince is the first value of id's assignment written at or after rev that
// ok accepts, read from etcd's history, so it is found even after the controller
// has removed the key.
func (e *env) demandSince(id string, rev int64, ok func(*statev1.SessionDemand) bool) *statev1.SessionDemand {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	for wr := range e.root.Watch(ctx, state.SessionDemandKey(e.inst(), id), clientv3.WithRev(rev)) {
		if err := wr.Err(); err != nil {
			e.t.Fatal(err)
		}
		for _, ev := range wr.Events {
			if ev.Type != clientv3.EventTypePut {
				continue
			}
			d := &statev1.SessionDemand{}
			if err := proto.Unmarshal(ev.Kv.Value, d); err != nil {
				e.t.Fatal(err)
			}
			if ok(d) {
				return d
			}
		}
	}
	e.t.Fatalf("no matching assignment for %s since revision %d", id, rev)
	return nil
}

func (e *env) hasDemand(id string) bool {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.SessionDemandKey(e.inst(), id), clientv3.WithCountOnly())
	if err != nil {
		e.t.Fatal(err)
	}
	return resp.Count != 0
}

// spawnTimes records when each worker is spawned. The returned func gives a
// session's spawn times, matched by the worker pid in its journal.
func (e *env) spawnTimes() func(id string) []time.Time {
	var mu sync.Mutex
	at := map[int]time.Time{}
	prev := e.cfg.Started
	e.cfg.Started = func(pid int) {
		mu.Lock()
		at[pid] = time.Now()
		mu.Unlock()
		prev(pid)
	}
	return func(id string) []time.Time {
		mu.Lock()
		defer mu.Unlock()
		var out []time.Time
		for _, en := range e.journalEntries(id) {
			if en.Event != "ids" {
				continue
			}
			for _, pid := range []int{en.PPID, en.PID} {
				if t, ok := at[pid]; ok {
					out = append(out, t)
					break
				}
			}
		}
		return out
	}
}
