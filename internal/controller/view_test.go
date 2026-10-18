package controller_test

import (
	"testing"
	"time"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestAssembleJoinsRegions(t *testing.T) {
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	spec := &sessionv1.SessionSpec{Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING}
	demand := &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "book.BTCUSDT"}}
	td := &statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: 7}}
	v := controller.Assemble(now, 15*time.Second, []string{"tw", "jp", "empty"}, []controller.Snapshot{
		{
			Plane: "sts", Instance: "sts-tw-1", Region: "tw", Rev: 4, Synced: true, LastContact: now,
			Specs: []controller.SpecIn{{Session: "abc123", Rev: 11, Value: spec}},
		},
		{
			Plane: "md", Instance: "md-jp-1", Region: "jp", Rev: 9, Synced: true, LastContact: now,
			Demands: []controller.DemandIn{{
				Key:     "/mftiko/v1/md/md-jp-1/demand/abc123/feed/book.BTCUSDT",
				Session: "abc123", Entry: "feed/book.BTCUSDT", Value: demand,
			}},
		},
		{
			Plane: "td", Instance: "td-tw-1", Region: "tw", Rev: 6, Synced: true, LastContact: now,
			Demands: []controller.DemandIn{{
				Key:     "/mftiko/v1/td/td-tw-1/demand/abc123/7",
				Session: "abc123", Entry: "7", Value: td,
			}},
		},
	})
	sess := v.Sessions[controller.SessionRef("sts-tw-1", "abc123")]
	if sess.Region != "tw" || sess.Spec != spec || sess.SpecRev != 11 {
		t.Fatalf("session %+v", sess)
	}
	md := v.Demands["/mftiko/v1/md/md-jp-1/demand/abc123/feed/book.BTCUSDT"]
	if md.Region != "jp" || md.Instance != "md-jp-1" || md.Value != demand {
		t.Fatalf("md demand %+v", md)
	}
	got := v.Demands["/mftiko/v1/td/td-tw-1/demand/abc123/7"]
	if got.Region != "tw" || got.Plane != "td" || got.Entry != "7" {
		t.Fatalf("td demand %+v", got)
	}
	if v.Regions["tw"].Rev != 4 || v.Regions["jp"].Rev != 9 {
		t.Fatalf("revs tw %+v jp %+v", v.Regions["tw"], v.Regions["jp"])
	}
	if !v.Regions["tw"].Synced || v.Regions["tw"].Stale || !v.Regions["empty"].Synced || v.Regions["empty"].Stale || v.Regions["empty"].Rev != 0 {
		t.Fatalf("regions %+v", v.Regions)
	}
}

func TestAssembleDropsAckWithoutDemand(t *testing.T) {
	now := time.Unix(0, 0)
	demandKey := "/mftiko/v1/md/md-jp-1/demand/abc123/feed/x"
	ackKey := "/mftiko/v1/md/md-jp-1/ack/abc123/feed/x"
	orphan := "/mftiko/v1/md/md-jp-1/ack/abc123/feed/y"
	ack := &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY}
	v := controller.Assemble(now, time.Second, nil, []controller.Snapshot{{
		Plane: "md", Instance: "md-jp-1", Region: "jp", Rev: 2, Synced: true, LastContact: now,
		Agent: &statev1.AgentRecord{Epoch: 1},
		Demands: []controller.DemandIn{{
			Key: demandKey, Session: "abc123", Entry: "feed/x",
			Value: &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "x"}},
		}},
		Acks: []controller.AckIn{
			{Key: ackKey, DemandKey: demandKey, Session: "abc123", Entry: "feed/x", Value: ack},
			{Key: orphan, DemandKey: demandKey + "-missing", Session: "abc123", Entry: "feed/y", Value: ack},
		},
	}})
	if got := v.Acks[ackKey]; got.Value != ack || got.LastKnown || got.Region != "jp" {
		t.Fatalf("ack %+v", got)
	}
	if _, ok := v.Acks[orphan]; ok {
		t.Fatal("ack without a demand entered the view")
	}
}

func TestAssembleLastKnown(t *testing.T) {
	now := time.Unix(0, 0)
	const (
		inst    = "md-jp-1"
		sess    = "abc123"
		demandK = "/mftiko/v1/md/md-jp-1/demand/abc123/feed/x"
		ackK    = "/mftiko/v1/md/md-jp-1/ack/abc123/feed/x"
		sts     = "sts-tw-1"
		wkey    = "/mftiko/v1/sts/sts-tw-1/worker/h"
	)
	hash := state.WorkerHash("sts/" + sess)
	ack := &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY}
	worker := &statev1.WorkerStatus{Phase: "RUNNING"}
	base := controller.Snapshot{
		Plane: "md", Instance: inst, Region: "jp", Rev: 2, Synced: true, LastContact: now,
		Demands: []controller.DemandIn{{Key: demandK, Session: sess, Entry: "feed/x", Value: &statev1.DemandEntry{}}},
		Acks:    []controller.AckIn{{Key: ackK, DemandKey: demandK, Session: sess, Entry: "feed/x", Value: ack}},
	}
	stsSnap := controller.Snapshot{
		Plane: "sts", Instance: sts, Region: "tw", Rev: 2, Synced: true, LastContact: now,
		Specs:   []controller.SpecIn{{Session: sess, Rev: 1, Value: &sessionv1.SessionSpec{Name: "s"}}},
		Workers: []controller.WorkerIn{{Key: wkey, Hash: hash, Value: worker}},
	}
	with := controller.Assemble(now, time.Second, nil, []controller.Snapshot{
		baseWithAgent(base), stsWithAgent(stsSnap),
	})
	if with.Acks[ackK].LastKnown {
		t.Fatal("ack marked last-known while the agent record is present")
	}
	got := with.Sessions[controller.SessionRef(sts, sess)]
	if got.Worker == nil || got.Worker.LastKnown || got.Worker.Hash != hash || got.Worker.Status != worker {
		t.Fatalf("worker %+v", got.Worker)
	}
	without := controller.Assemble(now, time.Second, nil, []controller.Snapshot{base, stsSnap})
	if !without.Acks[ackK].LastKnown || without.Acks[ackK].Value != ack {
		t.Fatalf("ack without agent %+v", without.Acks[ackK])
	}
	w := without.Sessions[controller.SessionRef(sts, sess)].Worker
	if w == nil || !w.LastKnown || w.Status != worker {
		t.Fatalf("worker without agent %+v", w)
	}
	if _, ok := without.Agents[inst]; ok {
		t.Fatal("absent agent record entered the view")
	}
}

func TestAssembleOrphanWorker(t *testing.T) {
	now := time.Unix(0, 0)
	const key = "/mftiko/v1/sts/sts-tw-1/worker/deadbeef"
	v := controller.Assemble(now, time.Second, nil, []controller.Snapshot{{
		Plane: "sts", Instance: "sts-tw-1", Region: "tw", Rev: 1, Synced: true, LastContact: now,
		Workers: []controller.WorkerIn{{Key: key, Hash: "deadbeef", Value: &statev1.WorkerStatus{Phase: "EXITED"}}},
	}})
	w := v.Workers[key]
	if w.Session != "" || w.Status.GetPhase() != "EXITED" {
		t.Fatalf("%+v", w)
	}
	if len(v.Sessions) != 0 {
		t.Fatalf("orphan created a session: %+v", v.Sessions)
	}
}

func TestAssembleUndecodable(t *testing.T) {
	now := time.Unix(0, 0)
	spec := &sessionv1.SessionSpec{Name: "ok"}
	v := controller.Assemble(now, time.Second, nil, []controller.Snapshot{{
		Plane: "sts", Instance: "sts-tw-1", Region: "tw", Rev: 3, Synced: true, LastContact: now,
		Specs:       []controller.SpecIn{{Session: "abc123", Rev: 8, Value: spec}},
		Undecodable: []string{"/mftiko/v1/sts/sts-tw-1/spec/bad", "/mftiko/v1/sts/sts-tw-1/demand/zzz"},
	}})
	if v.Sessions[controller.SessionRef("sts-tw-1", "abc123")].Spec != spec {
		t.Fatal("a good spec was dropped with the bad keys")
	}
	if _, ok := v.Sessions[controller.SessionRef("sts-tw-1", "bad")]; ok {
		t.Fatal("undecodable spec entered the view")
	}
	if len(v.Undecodable) != 2 || v.Undecodable[0] != "/mftiko/v1/sts/sts-tw-1/demand/zzz" ||
		v.Undecodable[1] != "/mftiko/v1/sts/sts-tw-1/spec/bad" {
		t.Fatalf("undecodable %v", v.Undecodable)
	}
}

func TestAssembleStaleAndRevision(t *testing.T) {
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	const after = 15 * time.Second
	fresh := now.Add(-after)
	older := fresh.Add(-time.Nanosecond)
	demand := controller.DemandIn{Key: "/mftiko/v1/md/md-a/demand/abc123/feed/x", Session: "abc123", Entry: "feed/x", Value: &statev1.DemandEntry{}}

	v := controller.Assemble(now, after, []string{"tw"}, []controller.Snapshot{
		{Plane: "md", Instance: "md-a", Region: "tw", Rev: 4, Synced: true, LastContact: fresh, Demands: []controller.DemandIn{demand}},
		{Plane: "td", Instance: "td-a", Region: "tw", Rev: 9, Synced: true, LastContact: fresh},
	})
	if v.Regions["tw"] != (controller.Region{Rev: 4, Synced: true}) {
		t.Fatalf("fresh region %+v", v.Regions["tw"])
	}
	if _, ok := v.Demands[demand.Key]; !ok {
		t.Fatal("demand missing")
	}

	v = controller.Assemble(now, after, nil, []controller.Snapshot{
		{Plane: "md", Instance: "md-a", Region: "tw", Rev: 4, Synced: true, LastContact: older},
	})
	if !v.Regions["tw"].Stale || !v.Regions["tw"].Synced || v.Regions["tw"].Rev != 4 {
		t.Fatalf("just over the threshold %+v", v.Regions["tw"])
	}

	v = controller.Assemble(now, after, nil, []controller.Snapshot{
		{Plane: "md", Instance: "md-a", Region: "tw", Rev: 4, Synced: true, LastContact: fresh, Demands: []controller.DemandIn{demand}},
		{Plane: "td", Instance: "td-a", Region: "tw", Rev: 9, Synced: false, LastContact: fresh},
	})
	if v.Regions["tw"].Synced || !v.Regions["tw"].Stale || v.Regions["tw"].Rev != 0 {
		t.Fatalf("unsynced region %+v", v.Regions["tw"])
	}
	if _, ok := v.Demands[demand.Key]; !ok {
		t.Fatal("a synced instance's demand was dropped because its region is unsynced")
	}

	v = controller.Assemble(now, after, nil, []controller.Snapshot{
		{Plane: "md", Instance: "md-a", Region: "tw", Rev: 4, Synced: true},
	})
	if !v.Regions["tw"].Stale {
		t.Fatal("no contact yet is fresh")
	}
}

func baseWithAgent(s controller.Snapshot) controller.Snapshot {
	s.Agent = &statev1.AgentRecord{Plane: "md", Instance: s.Instance, Epoch: 1}
	return s
}

func stsWithAgent(s controller.Snapshot) controller.Snapshot {
	s.Agent = &statev1.AgentRecord{Plane: "sts", Instance: s.Instance, Epoch: 1}
	return s
}
