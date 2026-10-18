package controller_test

import (
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func gateAt(v controller.View, now time.Time) (controller.Plan, time.Time) {
	return controller.Gate(v, controller.Derive(v, places(), now), now)
}

func installPlan(v *controller.View, plan controller.Plan) {
	ref := controller.SessionRef(stsInst, sessID)
	sess := v.Sessions[ref]
	for _, w := range plan.Writes {
		switch m := w.Value.(type) {
		case *statev1.SessionDemand:
			sess.Demand = proto.Clone(m).(*statev1.SessionDemand)
		case *statev1.DemandEntry:
			d := v.Demands[w.Key]
			d.Key = w.Key
			d.Value = proto.Clone(m).(*statev1.DemandEntry)
			v.Demands[w.Key] = d
		}
	}
	v.Sessions[ref] = sess
}

func entryStatus(st statev1.EntryState, reason string) *statev1.EntryStatus {
	return &statev1.EntryStatus{State: st, Reason: reason, Subject: "sub", UpdatedAtNs: 5}
}

func putFeedAck(v *controller.View, inst, region, session, feed string, st *statev1.EntryStatus, lastKnown bool) {
	entry := "feed/" + feed
	key := state.MDAckKey(inst, session, entry)
	v.Acks[key] = controller.Ack{
		Key: key, Region: region, Instance: inst, Plane: "md",
		Session: session, Entry: entry, Value: st, LastKnown: lastKnown,
	}
}

func putSelectAck(v *controller.View, inst, region, session, name string, st *statev1.EntryStatus, lastKnown bool) {
	entry := "select/" + name
	key := state.MDAckKey(inst, session, entry)
	v.Acks[key] = controller.Ack{
		Key: key, Region: region, Instance: inst, Plane: "md",
		Session: session, Entry: entry, Value: st, LastKnown: lastKnown,
	}
}

func putAPIAck(v *controller.View, inst, region, session string, apiID int64, st *statev1.EntryStatus, lastKnown bool) {
	key := state.TDAckKey(inst, session, apiID)
	v.Acks[key] = controller.Ack{
		Key: key, Region: region, Instance: inst, Plane: "td",
		Session: session, Entry: strconv.FormatInt(apiID, 10), Value: st, LastKnown: lastKnown,
	}
}

func ackAll(v *controller.View, st *statev1.EntryStatus, lastKnown bool) {
	putFeedAck(v, "md-jp-1", "jp", sessID, "trade.BinanceUM_Perp_BTCUSDT", st, lastKnown)
	putSelectAck(v, "md-tw-1", "tw", sessID, "near", proto.Clone(st).(*statev1.EntryStatus), lastKnown)
	putAPIAck(v, "td-jp-1", "jp", sessID, 7, proto.Clone(st).(*statev1.EntryStatus), lastKnown)
}

func sessionWrite(t *testing.T, plan controller.Plan) *statev1.SessionDemand {
	t.Helper()
	got := byKey(t, plan)
	return asSession(t, got[state.SessionDemandKey(stsInst, sessID)])
}

func TestGateStartsWhenReady(t *testing.T) {
	v, _ := runningSession(sampleSpec())
	plan, next := gateAt(v, deriveNow)
	wrote := sessionWrite(t, plan)
	if wrote.GetStart() || wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
		wrote.GetBindings()[0].GetStatus() != nil {
		t.Fatalf("before acks %+v", wrote)
	}
	if !next.Equal(deriveNow.Add(60 * time.Second)) {
		t.Fatalf("next %s", next)
	}
	installPlan(&v, plan)

	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
	plan, next = gateAt(v, deriveNow.Add(time.Second))
	wrote = sessionWrite(t, plan)
	if !wrote.GetStart() || wrote.GetEndReason() != "" || wrote.GetIssuedAtNs() != deriveNow.UnixNano() {
		t.Fatalf("%+v", wrote)
	}
	for _, b := range wrote.GetBindings() {
		if b.GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_READY || b.GetStatus().GetReason() != "up" {
			t.Fatalf("binding %+v", b)
		}
	}
	if !next.IsZero() {
		t.Fatalf("started session still waits %s", next)
	}
	// Demand entries are not rewritten just because start flipped.
	if len(plan.Writes) != 1 {
		t.Fatalf("writes %v", keysOf(byKey(t, plan)))
	}

	installPlan(&v, plan)
	again, next := gateAt(v, deriveNow.Add(2*time.Second))
	if len(again.Writes) != 0 || !next.IsZero() {
		t.Fatalf("steady %+v next %s", again.Writes, next)
	}
}

func TestGateStartStaysAndSilenceClears(t *testing.T) {
	v, _ := runningSession(sampleSpec())
	first, _ := gateAt(v, deriveNow)
	installPlan(&v, first)
	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
	started, _ := gateAt(v, deriveNow)
	installPlan(&v, started)

	pending := entryStatus(statev1.EntryState_ENTRY_STATE_PENDING, "boot")
	putFeedAck(&v, "md-jp-1", "jp", sessID, "trade.BinanceUM_Perp_BTCUSDT", pending, false)
	plan, next := gateAt(v, deriveNow.Add(time.Second))
	wrote := sessionWrite(t, plan)
	if !wrote.GetStart() || wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
		wrote.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_PENDING {
		t.Fatalf("start went back %+v", wrote)
	}
	if !next.IsZero() {
		t.Fatalf("started session armed %s", next)
	}
	installPlan(&v, plan)

	delete(v.Acks, state.MDAckKey("md-jp-1", sessID, "feed/trade.BinanceUM_Perp_BTCUSDT"))
	plan, _ = gateAt(v, deriveNow.Add(2*time.Second))
	wrote = sessionWrite(t, plan)
	if !wrote.GetStart() || wrote.GetBindings()[0].GetStatus() != nil {
		t.Fatalf("silence %+v", wrote.GetBindings()[0])
	}
}

func TestGateNoDepsStarts(t *testing.T) {
	spec := &sessionv1.SessionSpec{
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		DepsTimeoutS: 1,
	}
	v, _ := runningSession(spec)
	plan, next := gateAt(v, deriveNow)
	wrote := sessionWrite(t, plan)
	if !wrote.GetStart() || wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
		wrote.GetEndReason() != "" || len(wrote.GetBindings()) != 0 {
		t.Fatalf("%+v", wrote)
	}
	if !next.IsZero() {
		t.Fatalf("next %s", next)
	}
}

func TestGateNackEndsOnce(t *testing.T) {
	for _, st := range []statev1.EntryState{
		statev1.EntryState_ENTRY_STATE_REJECTED,
		statev1.EntryState_ENTRY_STATE_DEAD,
	} {
		t.Run(st.String(), func(t *testing.T) {
			v, _ := runningSession(sampleSpec())
			first, _ := gateAt(v, deriveNow)
			installPlan(&v, first)
			ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
			putAPIAck(&v, "td-jp-1", "jp", sessID, 7, entryStatus(st, "nope"), false)
			plan, next := gateAt(v, deriveNow.Add(time.Second))
			wrote := sessionWrite(t, plan)
			if wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
				wrote.GetEndReason() != "deps_failed" || wrote.GetStart() ||
				wrote.GetIssuedAtNs() != deriveNow.UnixNano() ||
				wrote.GetBindings()[2].GetStatus().GetReason() != "nope" {
				t.Fatalf("%+v", wrote)
			}
			if !next.IsZero() {
				t.Fatalf("ended session armed %s", next)
			}
			installPlan(&v, plan)

			ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "back"), false)
			again, next := gateAt(v, deriveNow.Add(2*time.Second))
			if len(again.Writes) != 0 || !next.IsZero() {
				t.Fatalf("revived %+v", again.Writes)
			}
			if v.Sessions[controller.SessionRef(stsInst, sessID)].Demand.GetEndReason() != "deps_failed" ||
				v.Sessions[controller.SessionRef(stsInst, sessID)].Demand.GetBindings()[2].GetStatus().GetReason() != "nope" {
				t.Fatal("bindings moved after ENDED")
			}
		})
	}
}

func TestGateTimeoutAndHandoff(t *testing.T) {
	spec := sampleSpec()
	spec.DepsTimeoutS = 60
	v, sess := runningSession(spec)
	issued := deriveNow.Add(-30 * time.Second)
	sess.Demand = runningDemand(issued)
	boot := entryStatus(statev1.EntryState_ENTRY_STATE_PENDING, "boot")
	sess.Demand.Bindings[0].Status = boot
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	seedDemands(&v, spec)
	putFeedAck(&v, "md-jp-1", "jp", sessID, "trade.BinanceUM_Perp_BTCUSDT", boot, false)

	plan, next := gateAt(v, deriveNow)
	if len(plan.Writes) != 0 {
		t.Fatalf("halfway wrote %+v", plan.Writes)
	}
	if !next.Equal(issued.Add(60 * time.Second)) {
		t.Fatalf("next %s want %s", next, issued.Add(60*time.Second))
	}

	// A new leader reads the same issued_at_ns. The clock is not started over.
	plan, next = gateAt(v, issued.Add(60*time.Second))
	wrote := sessionWrite(t, plan)
	if wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
		wrote.GetEndReason() != "deps_timeout" || wrote.GetIssuedAtNs() != issued.UnixNano() ||
		wrote.GetStart() {
		t.Fatalf("%+v", wrote)
	}
	if wrote.GetBindings()[0].GetStatus().GetReason() != "boot" {
		t.Fatalf("status %+v", wrote.GetBindings()[0].GetStatus())
	}
	if !next.IsZero() {
		t.Fatalf("next %s", next)
	}
}

func TestGateTimeoutZeroIsSixty(t *testing.T) {
	spec := sampleSpec()
	v, sess := runningSession(spec)
	issued := deriveNow.Add(-60 * time.Second)
	sess.Demand = runningDemand(issued)
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	seedDemands(&v, spec)
	plan, _ := gateAt(v, deriveNow.Add(-time.Nanosecond))
	if _, ok := byKey(t, plan)[state.SessionDemandKey(stsInst, sessID)]; ok {
		t.Fatal("ended one nanosecond early")
	}
	wrote := sessionWrite(t, gateAtFirst(v, deriveNow))
	if wrote.GetEndReason() != "deps_timeout" || wrote.GetIssuedAtNs() != issued.UnixNano() {
		t.Fatalf("%+v", wrote)
	}
}

func TestGateReasonPriority(t *testing.T) {
	issued := deriveNow.Add(-time.Hour)
	cases := []struct {
		name   string
		ended  bool
		gone   bool
		nack   bool
		reason string
	}{
		{"spec over nack and timeout", true, false, true, "ended_by_spec"},
		{"deleted over nack and timeout", false, true, true, "ended_by_spec"},
		{"nack over timeout", false, false, true, "deps_failed"},
		{"timeout alone", false, false, false, "deps_timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := sampleSpec()
			spec.DepsTimeoutS = 1
			if tc.ended {
				spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
			}
			v, sess := runningSession(spec)
			if tc.gone {
				sess.Spec = nil
			}
			sess.Demand = runningDemand(issued)
			v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
			if tc.nack {
				putAPIAck(&v, "td-jp-1", "jp", sessID, 7, entryStatus(statev1.EntryState_ENTRY_STATE_DEAD, "down"), false)
			}
			wrote := sessionWrite(t, gateAtFirst(v, deriveNow))
			if wrote.GetEndReason() != tc.reason || wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
				wrote.GetIssuedAtNs() != issued.UnixNano() || wrote.GetSpecRevision() != specRev {
				t.Fatalf("%+v", wrote)
			}
		})
	}
}

func TestGateEndedBySpecWithoutAssignment(t *testing.T) {
	spec := sampleSpec()
	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	v, _ := runningSession(spec)
	plan, next := gateAt(v, deriveNow)
	if len(plan.Writes) != 0 || !next.IsZero() {
		t.Fatalf("%+v next %s", plan, next)
	}
}

func TestGateSpecDeletedSkipsTerminal(t *testing.T) {
	v, sess := runningSession(sampleSpec())
	sess.Spec = nil
	sess.Status = &statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE}
	sess.Demand = runningDemand(deriveNow)
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	plan, next := gateAt(v, deriveNow)
	if len(plan.Writes) != 0 || !next.IsZero() {
		t.Fatalf("%+v", plan)
	}
}

func TestGateStaleAck(t *testing.T) {
	v, _ := runningSession(sampleSpec())
	first, _ := gateAt(v, deriveNow)
	installPlan(&v, first)
	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
	jp := v.Regions["jp"]
	jp.Stale = true
	v.Regions["jp"] = jp

	plan, next := gateAt(v, deriveNow.Add(time.Second))
	wrote := sessionWrite(t, plan)
	if wrote.GetStart() || wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING {
		t.Fatalf("stale ready released start %+v", wrote)
	}
	if wrote.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_READY ||
		wrote.GetBindings()[1].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_READY {
		t.Fatalf("bindings not copied %+v", wrote.GetBindings())
	}
	if next.IsZero() {
		t.Fatal("waiting session has no deadline")
	}
	installPlan(&v, plan)

	putAPIAck(&v, "td-jp-1", "jp", sessID, 7, entryStatus(statev1.EntryState_ENTRY_STATE_DEAD, "down"), false)
	plan, _ = gateAt(v, deriveNow.Add(2*time.Second))
	wrote = sessionWrite(t, plan)
	if wrote.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING || wrote.GetEndReason() != "" ||
		wrote.GetBindings()[2].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_DEAD {
		t.Fatalf("stale nack ended the session %+v", wrote)
	}

	// A fresh nack still ends it, and a past deadline does not outrank that.
	tw := v.Regions["tw"]
	tw.Stale = false
	v.Regions["tw"] = tw
	putSelectAck(&v, "md-tw-1", "tw", sessID, "near", entryStatus(statev1.EntryState_ENTRY_STATE_REJECTED, "bad"), false)
	wrote = sessionWrite(t, gateAtFirst(v, deriveNow.Add(time.Hour)))
	if wrote.GetEndReason() != "deps_failed" {
		t.Fatalf("%+v", wrote)
	}
}

func TestGateStaleReadyTimesOut(t *testing.T) {
	spec := sampleSpec()
	spec.DepsTimeoutS = 5
	v, sess := runningSession(spec)
	issued := deriveNow.Add(-5 * time.Second)
	sess.Demand = runningDemand(issued)
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
	jp := v.Regions["jp"]
	jp.Stale = true
	v.Regions["jp"] = jp
	wrote := sessionWrite(t, gateAtFirst(v, deriveNow))
	if wrote.GetEndReason() != "deps_timeout" || wrote.GetStart() ||
		wrote.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_READY {
		t.Fatalf("%+v", wrote)
	}
}

func TestGateMissingAgentUsesAck(t *testing.T) {
	v, _ := runningSession(sampleSpec())
	first, _ := gateAt(v, deriveNow)
	installPlan(&v, first)
	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_PENDING, "boot"), true)
	plan, _ := gateAt(v, deriveNow.Add(time.Second))
	wrote := sessionWrite(t, plan)
	if wrote.GetStart() || wrote.GetEndReason() != "" ||
		wrote.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_PENDING {
		t.Fatalf("missing agent changed the session %+v", wrote)
	}
	installPlan(&v, plan)
	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), true)
	wrote = sessionWrite(t, gateAtFirst(v, deriveNow.Add(2*time.Second)))
	if !wrote.GetStart() || wrote.GetEndReason() != "" {
		t.Fatalf("last-known ready did not start %+v", wrote)
	}
}

func TestGateDropsRevival(t *testing.T) {
	v, sess := runningSession(sampleSpec())
	sess.Demand = &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_ENDED, EndReason: "deps_failed",
		SpecRevision: specRev, IssuedAtNs: 42,
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	ackAll(&v, entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
	key := state.SessionDemandKey(stsInst, sessID)
	plan := controller.Plan{Writes: []controller.Write{{
		Key: key,
		Value: &statev1.SessionDemand{
			Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: specRev,
			IssuedAtNs: 42, Start: true,
		},
	}}}
	out, next := controller.Gate(v, plan, deriveNow)
	if len(out.Writes) != 0 || !next.IsZero() {
		t.Fatalf("kept a revival %+v", out.Writes)
	}
}

func TestGateKeepsInvalidSpec(t *testing.T) {
	spec := sampleSpec()
	spec.Feeds[0].MdInstance = "md-nope"
	v, _ := runningSession(spec)
	plan, next := gateAt(v, deriveNow)
	wrote := sessionWrite(t, plan)
	if wrote.GetEndReason() != "invalid_spec" || len(wrote.GetBindings()) != 0 || !next.IsZero() {
		t.Fatalf("%+v next %s", wrote, next)
	}
	if len(plan.Writes) != 1 {
		t.Fatalf("writes %v", keysOf(byKey(t, plan)))
	}
}

func TestGateClosedRegionArmsDeadline(t *testing.T) {
	spec := sampleSpec()
	spec.DepsTimeoutS = 15
	v, sess := runningSession(spec)
	sess.Demand = runningDemand(deriveNow)
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	tw := v.Regions["tw"]
	tw.Stale = true
	v.Regions["tw"] = tw
	plan, next := gateAt(v, deriveNow.Add(time.Second))
	if len(plan.Writes) != 0 {
		t.Fatalf("wrote into a stale session region %+v", plan.Writes)
	}
	if !next.Equal(deriveNow.Add(15 * time.Second)) {
		t.Fatalf("next %s", next)
	}
}

func TestGateEarliestDeadline(t *testing.T) {
	spec := sampleSpec()
	spec.DepsTimeoutS = 30
	v, _ := runningSession(spec)
	other := proto.Clone(spec).(*sessionv1.SessionSpec)
	other.DepsTimeoutS = 10
	const otherID = "def456"
	v.Sessions[controller.SessionRef(stsInst, otherID)] = controller.Session{
		ID: otherID, Instance: stsInst, Region: "tw",
		Spec: other, SpecRev: specRev,
	}
	_, next := gateAt(v, deriveNow)
	if !next.Equal(deriveNow.Add(10 * time.Second)) {
		t.Fatalf("next %s", next)
	}
}

func TestGateDriftKeepsStoredBindings(t *testing.T) {
	spec := sampleSpec()
	spec.Feeds = append(spec.Feeds, &sessionv1.FeedDecl{Feed: "book.X", MdInstance: "md-tw-1"})
	v, sess := runningSession(spec)
	sess.Demand = &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: 4, IssuedAtNs: deriveNow.UnixNano(),
		Bindings: []*statev1.Binding{
			{Target: &statev1.Binding_Feed{Feed: "trade.BinanceUM_Perp_BTCUSDT"}, Instance: "md-jp-1"},
		},
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	putFeedAck(&v, "md-jp-1", "jp", sessID, "trade.BinanceUM_Perp_BTCUSDT",
		entryStatus(statev1.EntryState_ENTRY_STATE_READY, "up"), false)
	plan := controller.Derive(v, places(), deriveNow)
	if len(plan.Drifts) != 1 || len(plan.Writes) != 0 {
		t.Fatalf("derive %+v", plan)
	}
	out, next := controller.Gate(v, plan, deriveNow)
	if len(out.Drifts) != 1 {
		t.Fatalf("drifts %+v", out.Drifts)
	}
	wrote := sessionWrite(t, out)
	if !wrote.GetStart() || len(wrote.GetBindings()) != 1 ||
		wrote.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_READY ||
		wrote.GetSpecRevision() != 4 {
		t.Fatalf("%+v", wrote)
	}
	if !next.IsZero() {
		t.Fatalf("next %s", next)
	}
}

func TestGateReadsAssembledAck(t *testing.T) {
	feed := "trade.BinanceUM_Perp_BTCUSDT"
	entry := "feed/" + feed
	spec := &sessionv1.SessionSpec{
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		DepsTimeoutS: 30,
		Feeds:        []*sessionv1.FeedDecl{{Feed: feed, MdInstance: "md-tw-1"}},
	}
	demandKey := state.MDDemandKey("md-tw-1", sessID, entry)
	ackKey := state.MDAckKey("md-tw-1", sessID, entry)
	snaps := []controller.Snapshot{{
		Plane: "sts", Instance: stsInst, Region: "tw", Rev: 3, Synced: true, LastContact: deriveNow,
		Specs: []controller.SpecIn{{Session: sessID, Rev: specRev, Value: spec}},
		SessionDemands: []controller.SessionDemandIn{{Session: sessID, Value: &statev1.SessionDemand{
			Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: specRev,
			IssuedAtNs: deriveNow.UnixNano(),
			Bindings: []*statev1.Binding{{
				Target: &statev1.Binding_Feed{Feed: feed}, Instance: "md-tw-1",
			}},
		}}},
	}, {
		Plane: "md", Instance: "md-tw-1", Region: "tw", Rev: 4, Synced: true, LastContact: deriveNow,
		Demands: []controller.DemandIn{{
			Key: demandKey, Session: sessID, Entry: entry,
			Value: &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: feed}},
		}},
		Acks: []controller.AckIn{{
			Key: ackKey, DemandKey: demandKey, Session: sessID, Entry: entry,
			Value: &statev1.EntryStatus{
				State: statev1.EntryState_ENTRY_STATE_READY, Reason: "up", Subject: "md.v1.f.md-tw-1.trade.X",
			},
		}},
		Agent: &statev1.AgentRecord{Plane: "md", Instance: "md-tw-1"},
	}}
	v := controller.Assemble(deriveNow, time.Minute, []string{"tw"}, snaps)
	plan, next := controller.Gate(v, controller.Plan{}, deriveNow)
	wrote := sessionWrite(t, plan)
	if !wrote.GetStart() || wrote.GetBindings()[0].GetStatus().GetSubject() != "md.v1.f.md-tw-1.trade.X" ||
		!next.IsZero() {
		t.Fatalf("%+v next %s", wrote, next)
	}
}

func TestGateUndecodableSkipped(t *testing.T) {
	v, sess := runningSession(sampleSpec())
	sess.Demand = runningDemand(deriveNow.Add(-time.Hour))
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	v.Undecodable = []string{state.SessionDemandKey(stsInst, sessID)}
	plan, next := gateAt(v, deriveNow)
	if len(plan.Writes) != 0 || !next.IsZero() {
		t.Fatalf("%+v next %s", plan, next)
	}
}

func seedDemands(v *controller.View, spec *sessionv1.SessionSpec) {
	for _, f := range spec.GetFeeds() {
		key := state.MDDemandKey(f.GetMdInstance(), sessID, "feed/"+f.GetFeed())
		v.Demands[key] = controller.Demand{Key: key, Value: &statev1.DemandEntry{
			Target: &statev1.DemandEntry_Feed{Feed: f.GetFeed()},
		}}
	}
	for _, s := range spec.GetSelectors() {
		key := state.MDDemandKey(s.GetMdInstance(), sessID, "select/"+s.GetName())
		v.Demands[key] = controller.Demand{Key: key, Value: &statev1.DemandEntry{
			Target: &statev1.DemandEntry_Selector{Selector: proto.Clone(s).(*sessionv1.SelectorDecl)},
		}}
	}
	for _, a := range spec.GetAccounts() {
		key := state.TDDemandKey(a.GetTdInstance(), sessID, a.GetApiId())
		v.Demands[key] = controller.Demand{Key: key, Value: &statev1.DemandEntry{
			Target:  &statev1.DemandEntry_ApiId{ApiId: a.GetApiId()},
			AlgoIds: append([]uint32(nil), a.GetAlgoIds()...),
		}}
	}
}

func runningDemand(issued time.Time) *statev1.SessionDemand {
	return &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: specRev,
		IssuedAtNs: issued.UnixNano(),
		Bindings: []*statev1.Binding{
			{Target: &statev1.Binding_Feed{Feed: "trade.BinanceUM_Perp_BTCUSDT"}, Instance: "md-jp-1"},
			{Target: &statev1.Binding_Selector{Selector: "near"}, Instance: "md-tw-1"},
			{Target: &statev1.Binding_ApiId{ApiId: 7}, Instance: "td-jp-1"},
		},
	}
}

func gateAtFirst(v controller.View, now time.Time) controller.Plan {
	plan, _ := gateAt(v, now)
	return plan
}
