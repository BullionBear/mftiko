package daemon

import (
	"testing"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestClassify(t *testing.T) {
	const inst = "sts-tw-1"
	specKey := state.SessionSpecKey(inst, "abc123")
	spec := &sessionv1.SessionSpec{Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING}
	demandKey := state.SessionDemandKey(inst, "abc123")
	demand := &statev1.SessionDemand{IssuedAtNs: 42}
	badKey := state.SessionSpecKey(inst, "badkey")
	shapeKey := state.SessionSpecPrefix(inst) + "abc123/extra"
	entries := map[string]state.Entry{
		specKey:                              ent(specKey, mustMarshal(t, spec), 8),
		demandKey:                            ent(demandKey, mustMarshal(t, demand), 9),
		badKey:                               ent(badKey, []byte{0xff}, 4),
		shapeKey:                             ent(shapeKey, mustMarshal(t, spec), 5),
		state.EpochKey(state.PlaneSTS, inst): ent(state.EpochKey(state.PlaneSTS, inst), []byte("1"), 1),
		state.ElectionPrefix(state.PlaneSTS, inst) + "/xyz": ent(state.ElectionPrefix(state.PlaneSTS, inst)+"/xyz", []byte("e"), 2),
		state.AgentKey(state.PlaneSTS, inst):                ent(state.AgentKey(state.PlaneSTS, inst), mustMarshal(t, &statev1.AgentRecord{Epoch: 3}), 6),
	}
	snap, fails := classify(state.PlaneSTS, inst, entries)
	if len(snap.Specs) != 1 || snap.Specs[0].Session != "abc123" || snap.Specs[0].Rev != 8 || snap.Specs[0].Value.GetName() != "s" {
		t.Fatalf("specs %+v", snap.Specs)
	}
	if len(snap.SessionDemands) != 1 || snap.SessionDemands[0].Value.GetIssuedAtNs() != 42 {
		t.Fatalf("demands %+v", snap.SessionDemands)
	}
	if snap.Agent.GetEpoch() != 3 {
		t.Fatalf("agent %+v", snap.Agent)
	}
	if len(snap.Undecodable) != 2 || len(fails) != 2 {
		t.Fatalf("undecodable %v fails %d", snap.Undecodable, len(fails))
	}
	got := map[string]bool{}
	for _, k := range snap.Undecodable {
		got[k] = true
	}
	if !got[badKey] || !got[shapeKey] {
		t.Fatalf("undecodable %v", snap.Undecodable)
	}
}

func TestClassifyProvider(t *testing.T) {
	const inst = "md-jp-1"
	demandKey := state.MDDemandKey(inst, "abc123", "feed/book.BTCUSDT")
	ackKey := state.MDAckKey(inst, "abc123", "feed/book.BTCUSDT")
	workerKey := state.WorkerStatusKey(state.PlaneMD, inst, "abcd")
	entries := map[string]state.Entry{
		demandKey: ent(demandKey, mustMarshal(t, &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "book.BTCUSDT"}}), 2),
		ackKey:    ent(ackKey, mustMarshal(t, &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY}), 3),
		workerKey: ent(workerKey, mustMarshal(t, &statev1.WorkerStatus{Phase: "RUNNING"}), 4),
	}
	snap, fails := classify(state.PlaneMD, inst, entries)
	if len(fails) != 0 || len(snap.Undecodable) != 0 || len(snap.Workers) != 0 {
		t.Fatalf("md worker or failure leaked: %+v %v", snap, fails)
	}
	if len(snap.Demands) != 1 || snap.Demands[0].Entry != "feed/book.BTCUSDT" {
		t.Fatalf("demand %+v", snap.Demands)
	}
	if len(snap.Acks) != 1 || snap.Acks[0].DemandKey != demandKey || snap.Acks[0].Session != "abc123" {
		t.Fatalf("ack %+v", snap.Acks)
	}

	const td = "td-jp-1"
	tdDemand := state.TDDemandKey(td, "abc123", 7)
	tdAck := state.TDAckKey(td, "abc123", 7)
	badID := state.TDAckPrefix(td) + "abc123/01"
	tdEntries := map[string]state.Entry{
		tdDemand: ent(tdDemand, mustMarshal(t, &statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: 7}}), 1),
		tdAck:    ent(tdAck, mustMarshal(t, &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_PENDING}), 2),
		badID:    ent(badID, mustMarshal(t, &statev1.EntryStatus{}), 3),
	}
	snap, fails = classify(state.PlaneTD, td, tdEntries)
	if len(snap.Acks) != 1 || snap.Acks[0].DemandKey != tdDemand || snap.Acks[0].Entry != "7" {
		t.Fatalf("td ack %+v", snap.Acks)
	}
	if len(fails) != 1 || fails[0].key != badID {
		t.Fatalf("fails %+v", fails)
	}
}

func ent(key string, val []byte, mod int64) state.Entry {
	return state.Entry{Key: key, Value: val, ModRevision: mod}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
