package agent

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestPullStreakSignal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	p := newPuller(context.Background(), nil, t.TempDir(), 0, zerolog.Nop(), func() time.Time { return now })
	dig := sha256Repeat("ab")
	p.jobs[dig] = &pullJob{}
	transient := &artifact.Error{Reason: artifact.ReasonUnavailable, Err: errors.New("unavailable")}

	p.finish(dig, "", artifact.Manifest{}, transient)
	p.finish(dig, "", artifact.Manifest{}, transient)
	job := p.jobs[dig]
	if job.fails != 2 || job.streakFails != 2 || !job.streakSince.Equal(now) || job.streakReason != artifact.ReasonUnavailable {
		t.Fatalf("streak %+v", job)
	}
	if want := now.Add(pullBackoff.After(1)); !job.next.Equal(want) {
		t.Fatalf("backoff next %s, want %s", job.next, want)
	}
	fails, next := job.fails, job.next

	p.finish(dig, "/root", artifact.Manifest{}, nil)
	job = p.jobs[dig]
	if !job.ready || job.perm != "" || job.streakFails != 0 || !job.streakSince.IsZero() || job.streakReason != "" {
		t.Fatalf("success did not clear the streak: %+v", job)
	}
	if job.fails != fails || !job.next.Equal(next) {
		t.Fatalf("backoff changed on success: fails %d next %s", job.fails, job.next)
	}

	later := now.Add(time.Minute)
	p.now = func() time.Time { return later }
	job.ready = false
	p.finish(dig, "", artifact.Manifest{}, errors.New("unavailable"))
	job = p.jobs[dig]
	if job.fails != 3 || job.streakFails != 1 || !job.streakSince.Equal(later) || job.streakReason != artifact.ReasonUnavailable {
		t.Fatalf("new streak %+v", job)
	}

	p.finish(dig, "", artifact.Manifest{}, context.Canceled)
	job = p.jobs[dig]
	if job.streakFails != 1 || job.fails != 3 || !job.streakSince.Equal(later) {
		t.Fatalf("cancel changed the streak: %+v", job)
	}
}

func TestPullPermanentClearsSignal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	p := newPuller(context.Background(), nil, t.TempDir(), 0, zerolog.Nop(), func() time.Time { return now })
	dig := sha256Repeat("ab")
	p.jobs[dig] = &pullJob{}
	perm := &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("unavailable")}

	p.finish(dig, "", artifact.Manifest{}, perm)
	job := p.jobs[dig]
	if job.perm != artifact.ReasonNotFound || job.fails != 0 || job.streakFails != 0 || !job.streakSince.IsZero() {
		t.Fatalf("permanent %+v", job)
	}

	p.jobs[dig] = &pullJob{}
	p.finish(dig, "", artifact.Manifest{}, &artifact.Error{Reason: artifact.ReasonUnavailable, Err: errors.New("unavailable")})
	p.finish(dig, "", artifact.Manifest{}, perm)
	job = p.jobs[dig]
	if job.perm != artifact.ReasonNotFound || job.fails != 1 || job.streakFails != 0 || !job.streakSince.IsZero() {
		t.Fatalf("transient then permanent %+v", job)
	}
}

func TestInputPullStreakSignal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	p := newPuller(context.Background(), nil, t.TempDir(), 0, zerolog.Nop(), func() time.Time { return now })
	key := inputKey("a1b2c3", "cfg")
	p.inputs[key] = &inputJob{}
	transient := &artifact.Error{Reason: artifact.ReasonUnavailable, Err: errors.New("unavailable")}

	p.finishInput(key, transient)
	p.finishInput(key, transient)
	job := p.inputs[key]
	if job.fails != 2 || job.streakFails != 2 || !job.streakSince.Equal(now) {
		t.Fatalf("input streak %+v", job)
	}
	next := job.next
	p.finishInput(key, nil)
	job = p.inputs[key]
	if !job.ready || job.streakFails != 0 || !job.streakSince.IsZero() || job.fails != 2 || !job.next.Equal(next) {
		t.Fatalf("input success %+v", job)
	}

	p.inputs[key] = &inputJob{}
	p.finishInput(key, &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("unavailable")})
	job = p.inputs[key]
	if job.perm != artifact.ReasonNotFound || job.streakFails != 0 || job.fails != 0 {
		t.Fatalf("input permanent %+v", job)
	}
}

func TestArtifactPullConditions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	earlier := now.Add(-time.Minute)
	p := newPuller(context.Background(), nil, t.TempDir(), 0, zerolog.Nop(), func() time.Time { return now })
	digAB := sha256Repeat("ab")
	digCD := sha256Repeat("cd")
	id := "a1b2c3"
	spec := &sessionv1.SessionSpec{
		Strategy: &sessionv1.StrategyRef{Code: digAB, Repository: "strategies/demo"},
		Inputs:   []*sessionv1.InputDecl{{Name: "cfg", Digest: digCD, Repository: "data"}},
	}
	p.jobs[digAB] = &pullJob{streakFails: 2, streakSince: now, streakReason: artifact.ReasonUnavailable}
	p.inputs[inputKey(id, "cfg")] = &inputJob{streakFails: 4, streakSince: earlier, streakReason: artifact.ReasonUnavailable}

	got := p.artifactPullConditions(id, spec, []*commonv1.Condition{pullCond(digAB, 9, earlier.UnixNano())})
	if len(got) != 2 || condDigest(t, got[0]) != digAB || condDigest(t, got[1]) != digCD {
		t.Fatalf("order %+v", got)
	}
	if got[0].GetAtNs() != now.UnixNano() || !strings.HasPrefix(got[0].GetMessage(), "2 ") {
		t.Fatalf("streak did not override published: %+v", got[0])
	}
	if got[1].GetAtNs() != earlier.UnixNano() || !strings.HasPrefix(got[1].GetMessage(), "4 ") {
		t.Fatalf("input condition %+v", got[1])
	}

	p.jobs[digAB] = &pullJob{running: true}
	got = p.artifactPullConditions(id, spec, []*commonv1.Condition{pullCond(digAB, 4, earlier.UnixNano())})
	if len(got) != 2 || got[0].GetMessage() != "4 "+digAB {
		t.Fatalf("in flight should keep published: %+v", got)
	}

	p.jobs[digAB] = &pullJob{ready: true}
	got = p.artifactPullConditions(id, spec, []*commonv1.Condition{pullCond(digAB, 4, earlier.UnixNano())})
	if len(got) != 1 || condDigest(t, got[0]) != digCD {
		t.Fatalf("ready digest should clear: %+v", got)
	}

	p.jobs[digAB] = &pullJob{perm: artifact.ReasonNotFound}
	got = p.artifactPullConditions(id, spec, []*commonv1.Condition{pullCond(digAB, 4, earlier.UnixNano())})
	if len(got) != 1 || condDigest(t, got[0]) != digCD {
		t.Fatalf("permanent digest should not signal: %+v", got)
	}

	both := &sessionv1.SessionSpec{
		Strategy: &sessionv1.StrategyRef{Code: digAB},
		Inputs:   []*sessionv1.InputDecl{{Name: "cfg", Digest: digAB, Repository: "data"}},
	}
	p.jobs[digAB] = &pullJob{ready: true}
	p.inputs[inputKey(id, "cfg")] = &inputJob{streakFails: 3, streakSince: now, streakReason: artifact.ReasonUnavailable}
	if c := p.artifactPullConditions(id, both, []*commonv1.Condition{pullCond(digAB, 3, now.UnixNano())}); len(c) != 0 {
		t.Fatalf("a ready job should clear the digest: %+v", c)
	}

	p.jobs[digAB] = &pullJob{streakFails: 1, streakSince: now, streakReason: artifact.ReasonUnavailable}
	p.inputs[inputKey(id, "cfg")] = &inputJob{streakFails: 5, streakSince: earlier, streakReason: artifact.ReasonUnavailable}
	got = p.artifactPullConditions(id, both, nil)
	if len(got) != 1 || got[0].GetAtNs() != earlier.UnixNano() || !strings.HasPrefix(got[0].GetMessage(), "5 ") {
		t.Fatalf("earlier streak should win: %+v", got)
	}
}

func TestNotePullConditions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	dig := sha256Repeat("ab")
	pulls := newPuller(context.Background(), nil, t.TempDir(), 0, zerolog.Nop(), func() time.Time { return now })
	pulls.jobs[dig] = &pullJob{streakFails: 1, streakSince: now, streakReason: artifact.ReasonUnavailable}
	spec := &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: dig}}
	p := &plane{pulls: pulls}
	p.notePullConditions(map[string]*sess{
		"a1b2c3": {parsed: spec, local: &statusView{Phase: statev1.Phase_PHASE_FAILED}},
		"b2c3d4": {parsed: spec},
	})
	if _, ok := p.pullConds["a1b2c3"]; ok {
		t.Fatal("terminal session kept a condition")
	}
	if len(p.pullConds["b2c3d4"]) != 1 {
		t.Fatalf("live session: %+v", p.pullConds)
	}
	empty := &plane{}
	empty.notePullConditions(map[string]*sess{"b2c3d4": {parsed: spec}})
	if len(empty.pullConds) != 0 {
		t.Fatalf("nil puller: %+v", empty.pullConds)
	}
}

func TestSyncArtifactPullCondition(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	dig := sha256Repeat("ab")
	id := "a1b2c3"
	w := &putLog{}
	p := &plane{
		inst: "sts-a",
		w:    w,
		now:  func() time.Time { return now },
		intents: map[string]intent{
			id: {Kind: kindPlan, Rev: 3},
		},
		pullConds: map[string][]*commonv1.Condition{
			id: {pullCond(dig, 2, now.UnixNano())},
		},
		saved: map[string]*statev1.SessionStatus{},
		local: map[string]statusView{},
	}
	if err := p.sync(context.Background(), agent.Demand[Entry]{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	st := w.status(t, 0)
	if st.GetPhase() != statev1.Phase_PHASE_PENDING || st.GetReason() != "" || len(st.GetConditions()) != 1 {
		t.Fatalf("failure status %+v", st)
	}
	if st.GetConditions()[0].GetType() != artifactPullType || st.GetConditions()[0].GetMessage() != "2 "+dig {
		t.Fatalf("condition %+v", st.GetConditions()[0])
	}
	if err := p.sync(context.Background(), agent.Demand[Entry]{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(w.vals) != 1 {
		t.Fatalf("unchanged condition wrote again: %d", len(w.vals))
	}

	p.pullConds = map[string][]*commonv1.Condition{}
	if err := p.sync(context.Background(), agent.Demand[Entry]{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	st = w.status(t, 1)
	if st.GetPhase() != statev1.Phase_PHASE_PENDING || len(st.GetConditions()) != 0 {
		t.Fatalf("cleared status %+v", st)
	}
	if err := p.sync(context.Background(), agent.Demand[Entry]{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(w.vals) != 2 {
		t.Fatalf("third sync wrote: %d", len(w.vals))
	}

	p.intents[id] = intent{Kind: kindFailed, FailReason: artifact.ReasonNotFound, Rev: 3}
	p.pullConds[id] = []*commonv1.Condition{pullCond(dig, 2, now.UnixNano())}
	if err := p.sync(context.Background(), agent.Demand[Entry]{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	st = w.status(t, 2)
	if st.GetPhase() != statev1.Phase_PHASE_FAILED || st.GetReason() != artifact.ReasonNotFound {
		t.Fatalf("permanent status %+v", st)
	}
	for _, c := range st.GetConditions() {
		if c.GetType() == artifactPullType {
			t.Fatalf("permanent error emitted artifact_pull: %+v", st)
		}
	}
}

type putLog struct {
	vals []string
}

func (p *putLog) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) error {
	if key != state.SessionStatusKey("sts-a", "a1b2c3") {
		return errors.New("unexpected key")
	}
	p.vals = append(p.vals, val)
	return nil
}

func (p *putLog) status(t *testing.T, i int) *statev1.SessionStatus {
	t.Helper()
	if i >= len(p.vals) {
		t.Fatalf("put %d missing (%d)", i, len(p.vals))
	}
	st := &statev1.SessionStatus{}
	if err := proto.Unmarshal([]byte(p.vals[i]), st); err != nil {
		t.Fatal(err)
	}
	return st
}

func sha256Repeat(pair string) string { return "sha256:" + strings.Repeat(pair, 32) }

func pullCond(dig string, fails int, at int64) *commonv1.Condition {
	return &commonv1.Condition{
		Type: artifactPullType, Reason: artifact.ReasonUnavailable,
		Message: strconv.Itoa(fails) + " " + dig, AtNs: at,
	}
}

func condDigest(t *testing.T, c *commonv1.Condition) string {
	t.Helper()
	dig, _, _, ok := parseArtifactPull(c)
	if !ok {
		t.Fatalf("bad condition %+v", c)
	}
	return dig
}
