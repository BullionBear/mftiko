package agent

import (
	"bytes"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestHashIsStoredEncoding(t *testing.T) {
	name := fieldBytes(1, []byte("n"))
	params := fieldBytes(5, []byte{1})
	a := append(append([]byte{}, name...), params...)
	b := append(append([]byte{}, params...), name...)
	if bytes.Equal(a, b) {
		t.Fatal("the two encodings are the same bytes")
	}
	if _, err := parseSpec(a); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSpec(b); err != nil {
		t.Fatal(err)
	}
	ma, _ := proto.Marshal(mustParse(t, a))
	mb, _ := proto.Marshal(mustParse(t, b))
	if !bytes.Equal(ma, mb) {
		t.Fatal("re-marshal should collapse the two encodings")
	}
	if (Spec{Raw: a}).Hash() == (Spec{Raw: b}).Hash() {
		t.Fatal("hash followed the re-marshal, not the stored bytes")
	}
}

func TestOverlap(t *testing.T) {
	// Later issued_at loses. The earlier session runs.
	early, late := "a1b2c3", "b2c3d4"
	d := decide(map[string]*sess{
		early: running(t, early, 10, true, acc(7, 1, 2)),
		late:  running(t, late, 20, true, acc(7, 2, 3)),
	}, nil)
	wantPlan(t, d, early, true)
	wantConflict(t, d, late)
	if _, ok := d.Plans[WorkerKey(late)]; ok {
		t.Fatal("the later session was planned")
	}

	// Equal issued_at: the larger session id is later.
	d = decide(map[string]*sess{
		early: running(t, early, 10, false, acc(7, 1)),
		late:  running(t, late, 10, true, acc(7, 1)),
	}, nil)
	wantPlan(t, d, early, false)
	wantConflict(t, d, late)

	// Disjoint algos, and the same algo on another api_id, both run.
	other := "c3d4e5"
	d = decide(map[string]*sess{
		early: running(t, early, 10, true, acc(7, 1)),
		late:  running(t, late, 20, true, acc(7, 2)),
		other: running(t, other, 30, true, acc(8, 1)),
	}, nil)
	wantPlan(t, d, early, true)
	wantPlan(t, d, late, true)
	wantPlan(t, d, other, true)

	// Empty algo lists do not overlap.
	d = decide(map[string]*sess{
		early: running(t, early, 10, true, acc(7)),
		late:  running(t, late, 20, true, acc(7)),
	}, nil)
	wantPlan(t, d, early, true)
	wantPlan(t, d, late, true)

	// A overlaps B, B overlaps C, A does not overlap C. B is non-terminal
	// this round, so it blocks C even though B itself conflicts with A.
	d = decide(map[string]*sess{
		"a1b2c3": running(t, "a1b2c3", 1, true, acc(7, 1)),
		"b2c3d4": running(t, "b2c3d4", 2, true, acc(7, 1, 2)),
		"c3d4e5": running(t, "c3d4e5", 3, true, acc(7, 2)),
	}, nil)
	wantPlan(t, d, "a1b2c3", true)
	wantConflict(t, d, "b2c3d4")
	wantConflict(t, d, "c3d4e5")

	// An earlier ENDED session occupies the algo. It is not itself a conflict.
	d = decide(map[string]*sess{
		early: ended(t, early, 10, "ended_by_spec", acc(7, 1)),
		late:  running(t, late, 20, true, acc(7, 1)),
	}, nil)
	wantEnded(t, d, early, "ended_by_spec")
	wantConflict(t, d, late)
	if _, ok := d.Plans[WorkerKey(early)]; ok {
		t.Fatal("ENDED was planned")
	}
}

func TestMissingAndTerminal(t *testing.T) {
	id := "a1b2c3"
	d := decide(map[string]*sess{
		id: {demandPresent: true, demand: dem(true, 1)},
	}, nil)
	if len(d.Plans) != 0 || len(d.Intents) != 0 || !slices.Contains(d.Missing, id) || len(d.Bad) != 0 {
		t.Fatalf("missing spec: %+v", d)
	}

	bad := &sess{demandPresent: true, demand: dem(true, 1), specPresent: true, specErr: errBadSpec}
	d = decide(map[string]*sess{id: bad}, nil)
	if len(d.Plans) != 0 || len(d.Intents) != 0 || len(d.Missing) != 0 || len(d.Bad) != 1 || d.Bad[0].ID != id {
		t.Fatalf("bad spec: %+v", d)
	}

	s := running(t, id, 1, true, acc(7, 1))
	s.local = &statusView{Phase: statev1.Phase_PHASE_FAILED, Reason: ReasonAlgoConflict}
	d = decide(map[string]*sess{id: s}, nil)
	if len(d.Plans) != 0 || len(d.Intents) != 0 || len(d.Missing) != 0 {
		t.Fatalf("terminal session re-entered: %+v", d)
	}

	// A spec with no assignment and no status is not touched.
	only := running(t, id, 1, true, acc(7, 1))
	only.demand, only.demandPresent = nil, false
	d = decide(map[string]*sess{id: only}, nil)
	if len(d.Plans) != 0 || len(d.Intents) != 0 || len(d.Missing) != 0 {
		t.Fatalf("spec only: %+v", d)
	}

	// UNSPECIFIED is not planned and not given a status, and it still occupies.
	unspec := running(t, id, 1, true, acc(7, 1))
	unspec.demand.Desired = sessionv1.DesiredState_DESIRED_STATE_UNSPECIFIED
	later := running(t, "b2c3d4", 2, true, acc(7, 1))
	d = decide(map[string]*sess{id: unspec, "b2c3d4": later}, nil)
	if _, ok := d.Intents[id]; ok {
		t.Fatal("UNSPECIFIED got a status")
	}
	wantConflict(t, d, "b2c3d4")
}

func TestEndedAndGone(t *testing.T) {
	id := "a1b2c3"
	s := ended(t, id, 5, "deps_failed", acc(7, 1))
	d := decide(map[string]*sess{id: s}, nil)
	wantEnded(t, d, id, "deps_failed")

	// The key disappeared. The remembered assignment is ENDED, including its reason.
	gone := running(t, id, 5, true, acc(7, 1))
	gone.demand, gone.demandPresent = nil, false
	gone.remembered = &demandView{Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, IssuedAt: 5, EndReason: ""}
	later := running(t, "b2c3d4", 9, true, acc(7, 1))
	d = decide(map[string]*sess{id: gone, "b2c3d4": later}, nil)
	wantEnded(t, d, id, "")
	wantConflict(t, d, "b2c3d4")

	// After a restart there is no remembered issued_at. A non-terminal status
	// and a spec, with no assignment, still ends, and does not block anyone.
	restarted := running(t, id, 1, true, acc(7, 1))
	restarted.demand, restarted.demandPresent = nil, false
	restarted.mirrored = &statusView{Phase: statev1.Phase_PHASE_RUNNING}
	fresh := running(t, "b2c3d4", 2, true, acc(7, 1))
	d = decide(map[string]*sess{id: restarted, "b2c3d4": fresh}, nil)
	wantEnded(t, d, id, "")
	wantPlan(t, d, "b2c3d4", true)
}

func TestSpecChangeAndGeneration(t *testing.T) {
	id := "a1b2c3"
	s := running(t, id, 1, true, acc(7, 1))
	d := decide(map[string]*sess{id: s}, nil)
	if len(d.Changed) != 0 || d.Hashes[id] != s.spec.Hash() || d.Intents[id].Rev != 42 {
		t.Fatalf("first plan: %+v rev %d", d.Changed, d.Intents[id].Rev)
	}
	d = decide(map[string]*sess{id: s}, map[string]string{id: s.spec.Hash()})
	if len(d.Changed) != 0 {
		t.Fatal("unchanged spec reported a change")
	}
	d = decide(map[string]*sess{id: s}, map[string]string{id: "old"})
	if !slices.Contains(d.Changed, id) || d.Hashes[id] != s.spec.Hash() {
		t.Fatalf("change: %+v", d)
	}
	// A session that left the plan keeps the hash it had.
	left := ended(t, id, 1, "ended_by_spec", acc(7, 1))
	d = decide(map[string]*sess{id: left}, map[string]string{id: "kept"})
	if d.Hashes[id] != "kept" || len(d.Changed) != 0 {
		t.Fatalf("hash of a session that left: %+v", d)
	}
}

func TestBuildViewsCopiesSpec(t *testing.T) {
	id := "a1b2c3"
	raw := mustRaw(t, specOf(acc(7, 1)))
	view := state.View{Entries: map[string]state.Entry{
		state.SessionSpecKey("sts-tw-1", id): {Value: raw, ModRevision: 42},
	}}
	demRaw, err := proto.Marshal(&statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: true, IssuedAtNs: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	demand := state.View{Entries: map[string]state.Entry{
		state.SessionDemandKey("sts-tw-1", id): {Value: demRaw},
	}}
	sessions := buildViews("sts-tw-1", view, demand, state.View{}, nil, nil, nil)
	if sessions[id].spec.Rev != 42 || !bytes.Equal(sessions[id].spec.Raw, raw) {
		t.Fatalf("spec %+v", sessions[id].spec)
	}
	raw[0] ^= 0xff
	if bytes.Equal(sessions[id].spec.Raw, raw) {
		t.Fatal("the plan shares the mirror's buffer")
	}
}

func TestPhase(t *testing.T) {
	plan := intent{Kind: kindPlan, SpecHash: "h", Rev: 9}
	endedI := intent{Kind: kindEnded, EndReason: "ended_by_spec", SpecHash: "h"}
	cases := []struct {
		name   string
		in     intent
		obs    *agent.Observed
		phase  statev1.Phase
		reason string
	}{
		{"pending", plan, nil, statev1.Phase_PHASE_PENDING, ""},
		{"waiting init", plan, obs(agent.StateInitializing, "", "h", nil), statev1.Phase_PHASE_WAITING, ""},
		{"waiting inited", plan, obs(agent.StateInitialized, "", "h", nil), statev1.Phase_PHASE_WAITING, ""},
		{"starting", plan, obs(agent.StateStarting, "", "h", nil), statev1.Phase_PHASE_STARTING, ""},
		{"running without stage stays starting", plan, obs(agent.StateRunning, "", "h", nil), statev1.Phase_PHASE_STARTING, ""},
		{"stage while starting", plan, obs(agent.StateStarting, "running", "h", nil), statev1.Phase_PHASE_RUNNING, ""},
		{"stage running", plan, obs(agent.StateRunning, "running", "h", nil), statev1.Phase_PHASE_RUNNING, ""},
		{"stopping", plan, obs(agent.StateStopping, "running", "h", nil), statev1.Phase_PHASE_STOPPING, ""},
		{"lost", plan, obs(agent.StateLost, "running", "h", nil), statev1.Phase_PHASE_FAILED, ReasonShimLost},
		{"done exit", plan, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_DONE, "exit")), statev1.Phase_PHASE_DONE, "exit"},
		{"crash", plan, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_CRASHED, "crash")), statev1.Phase_PHASE_FAILED, "crash"},
		{"replacement is not terminal", plan, obs(agent.StateExited, "", "old", exitInfo(commonv1.Outcome_OUTCOME_DONE, "stop: spec changed")), statev1.Phase_PHASE_STOPPING, ""},
		{"ended no worker", endedI, nil, statev1.Phase_PHASE_DONE, "ended_by_spec"},
		{"ended prefers end reason", endedI, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_DONE, "stop: not in demand")), statev1.Phase_PHASE_DONE, "ended_by_spec"},
		{"ended empty reason keeps the exit reason", intent{Kind: kindEnded, SpecHash: "h"}, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_DONE, "stop: not in demand")), statev1.Phase_PHASE_DONE, "stop: not in demand"},
		{"ended crash keeps the worker reason", endedI, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_CRASHED, "crash")), statev1.Phase_PHASE_FAILED, "crash"},
		{"deps_failed with no worker", intent{Kind: kindEnded, EndReason: "deps_failed"}, nil, statev1.Phase_PHASE_FAILED, "deps_failed"},
		{"deps_timeout with no worker", intent{Kind: kindEnded, EndReason: "deps_timeout"}, nil, statev1.Phase_PHASE_FAILED, "deps_timeout"},
		{"invalid_spec with no worker", intent{Kind: kindEnded, EndReason: "invalid_spec"}, nil, statev1.Phase_PHASE_FAILED, "invalid_spec"},
		{"deps_failed after a normal exit", intent{Kind: kindEnded, EndReason: "deps_failed", SpecHash: "h"}, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_DONE, "stop")), statev1.Phase_PHASE_FAILED, "deps_failed"},
		{"deps_failed while the worker runs", intent{Kind: kindEnded, EndReason: "deps_failed", SpecHash: "h"}, obs(agent.StateRunning, "running", "h", nil), statev1.Phase_PHASE_RUNNING, ""},
		{"invalid_spec while stopping", intent{Kind: kindEnded, EndReason: "invalid_spec", SpecHash: "h"}, obs(agent.StateStopping, "running", "h", nil), statev1.Phase_PHASE_STOPPING, ""},
		{"lost overrides end reason", endedI, obs(agent.StateLost, "", "h", nil), statev1.Phase_PHASE_FAILED, ReasonShimLost},
		{"artifact rejection", intent{Kind: kindFailed, FailReason: "artifact_not_found"}, nil, statev1.Phase_PHASE_FAILED, "artifact_not_found"},
		{"init failed exit", plan, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_INIT_FAILED, "bad config")), statev1.Phase_PHASE_FAILED, "bad config"},
		{"conflict ignores the worker", intent{Kind: kindConflict}, obs(agent.StateRunning, "running", "h", nil), statev1.Phase_PHASE_FAILED, ReasonAlgoConflict},
	}
	restarting := endView{restarting: true, reason: "crash"}
	ph, reason, write := phaseOf(plan, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_CRASHED, "crash")), restarting)
	if !write || ph != statev1.Phase_PHASE_RESTARTING || reason != "crash" {
		t.Fatalf("restarting %s %q write %v", ph, reason, write)
	}
	limited := endView{terminalReason: ReasonRestartLimit}
	ph, reason, write = phaseOf(plan, obs(agent.StateExited, "", "h", exitInfo(commonv1.Outcome_OUTCOME_CRASHED, "crash")), limited)
	if !write || ph != statev1.Phase_PHASE_FAILED || reason != ReasonRestartLimit {
		t.Fatalf("limit %s %q", ph, reason)
	}
	quiet := endView{noBeat: true, overrideReason: ReasonNoBeat, reason: ReasonNoBeat}
	ex := exitInfo(commonv1.Outcome_OUTCOME_UNSPECIFIED, "signal 9 (killed), no Exiting")
	ex.Signal = 9
	ph, reason, write = phaseOf(plan, obs(agent.StateExited, "", "h", ex), quiet)
	if !write || ph != statev1.Phase_PHASE_FAILED || reason != ReasonNoBeat {
		t.Fatalf("no_beat %s %q", ph, reason)
	}
	published := exitForStatus(ex, quiet)
	if published.GetOutcome() != commonv1.Outcome_OUTCOME_HUNG || published.GetReason() != ReasonNoBeat || published.GetSignal() != 9 {
		t.Fatalf("published exit %+v", published)
	}
	if ex.GetOutcome() != commonv1.Outcome_OUTCOME_UNSPECIFIED {
		t.Fatal("the shim's exit was rewritten")
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ph, reason, write := phaseOf(c.in, c.obs)
			if !write || ph != c.phase || reason != c.reason {
				t.Fatalf("got %s %q write %v", ph, reason, write)
			}
		})
	}
}

func fieldBytes(field int, data []byte) []byte {
	return append([]byte{byte(field<<3 | 2), byte(len(data))}, data...)
}

func mustParse(t *testing.T, raw []byte) *sessionv1.SessionSpec {
	t.Helper()
	msg, err := parseSpec(raw)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func acc(api int64, algos ...uint32) *sessionv1.AccountDecl {
	return &sessionv1.AccountDecl{Name: "a", ApiId: api, AlgoIds: algos}
}

func specOf(accounts ...*sessionv1.AccountDecl) *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{Name: "n", Accounts: accounts}
}

func mustRaw(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func dem(start bool, issued int64) *demandView {
	return &demandView{Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: start, IssuedAt: issued}
}

func running(t *testing.T, id string, issued int64, start bool, accounts ...*sessionv1.AccountDecl) *sess {
	t.Helper()
	raw := mustRaw(t, specOf(accounts...))
	msg, err := parseSpec(raw)
	if err != nil {
		t.Fatal(err)
	}
	d := dem(start, issued)
	return &sess{
		spec: &Spec{Raw: raw, Rev: 42}, parsed: msg, specPresent: true,
		demand: d, demandPresent: true,
	}
}

func ended(t *testing.T, id string, issued int64, reason string, accounts ...*sessionv1.AccountDecl) *sess {
	t.Helper()
	s := running(t, id, issued, false, accounts...)
	s.demand = &demandView{Desired: sessionv1.DesiredState_DESIRED_STATE_ENDED, EndReason: reason, IssuedAt: issued}
	return s
}

func wantPlan(t *testing.T, d decision, id string, start bool) {
	t.Helper()
	p, ok := d.Plans[WorkerKey(id)]
	in := d.Intents[id]
	if !ok || in.Kind != kindPlan || p.Start != start {
		t.Fatalf("%s: plan %+v intent %+v", id, p, in)
	}
}

func wantConflict(t *testing.T, d decision, id string) {
	t.Helper()
	if _, ok := d.Plans[WorkerKey(id)]; ok || d.Intents[id].Kind != kindConflict {
		t.Fatalf("%s: want conflict, plan %v intent %+v", id, d.Plans[WorkerKey(id)], d.Intents[id])
	}
}

func wantEnded(t *testing.T, d decision, id, reason string) {
	t.Helper()
	in := d.Intents[id]
	if _, ok := d.Plans[WorkerKey(id)]; ok || in.Kind != kindEnded || in.EndReason != reason {
		t.Fatalf("%s: want ended %q, intent %+v", id, reason, in)
	}
}

func obs(st agent.State, stage, hash string, exit *commonv1.ExitInfo) *agent.Observed {
	return &agent.Observed{State: st, Stage: stage, SpecHash: hash, Exit: exit, Incarnation: 1}
}

func exitInfo(outcome commonv1.Outcome, reason string) *commonv1.ExitInfo {
	return &commonv1.ExitInfo{Outcome: outcome, Reason: reason}
}
