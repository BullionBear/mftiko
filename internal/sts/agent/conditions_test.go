package agent

import (
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
)

func TestDependencyConditions(t *testing.T) {
	d := &statev1.SessionDemand{Bindings: []*statev1.Binding{
		{Target: &statev1.Binding_Feed{Feed: "book.X"}, Instance: "md-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_REJECTED, Reason: "no_feed"}},
		{Target: &statev1.Binding_Feed{Feed: "book.Y"}, Instance: "md-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY, Reason: "up"}},
		{Target: &statev1.Binding_ApiId{ApiId: 42}, Instance: "td-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_DEAD, Reason: "gone"}},
		{Target: &statev1.Binding_Selector{Selector: "main"}, Instance: "md-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_REJECTED, Reason: "empty"}},
		{Target: &statev1.Binding_Feed{Feed: "book.Z"}, Instance: "md-tw-1"},
	}}
	got := dependencyConditions(d, 99)
	want := []*commonv1.Condition{
		{Type: "dependency", Reason: "no_feed", Message: "feed book.X md-tw-1 rejected", AtNs: 99},
		{Type: "dependency", Reason: "gone", Message: "account 42 td-tw-1 dead", AtNs: 99},
		{Type: "dependency", Reason: "empty", Message: "selector main md-tw-1 rejected", AtNs: 99},
	}
	if len(got) != len(want) {
		t.Fatalf("len %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i].GetType() != want[i].GetType() || got[i].GetStatus() || got[i].GetReason() != want[i].GetReason() ||
			got[i].GetMessage() != want[i].GetMessage() || got[i].GetAtNs() != want[i].GetAtNs() {
			t.Fatalf("condition %d: %+v", i, got[i])
		}
	}
	if dependencyConditions(nil, 1) != nil {
		t.Fatal("nil demand wrote conditions")
	}
}

func TestLiveDemandIgnoresRemembered(t *testing.T) {
	p := &plane{
		inst: "sts-tw-1",
		remembered: map[string]demandView{
			"abc123": {
				Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
				Bindings: []*statev1.Binding{{
					Target:   &statev1.Binding_Feed{Feed: "book.X"},
					Instance: "md-tw-1",
					Status:   &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_REJECTED, Reason: "no_feed"},
				}},
			},
		},
	}
	if p.liveDemand("abc123") != nil {
		t.Fatal("remembered demand was used")
	}
	if dependencyConditions(p.liveDemand("abc123"), 1) != nil {
		t.Fatal("missing demand wrote conditions")
	}
}

// A deps_timeout end also names the bindings that never became ready: PENDING with the
// provider's reason, and no_ack where there was no ack at all (E06-D043, #542).
func TestDependencyConditionsDepsTimeout(t *testing.T) {
	d := &statev1.SessionDemand{EndReason: "deps_timeout", Bindings: []*statev1.Binding{
		{Target: &statev1.Binding_Feed{Feed: "book.X"}, Instance: "md-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY}},
		{Target: &statev1.Binding_ApiId{ApiId: 42}, Instance: "td-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_PENDING, Reason: "starting: private dial: refused"}},
		{Target: &statev1.Binding_ApiId{ApiId: 43}, Instance: "td-tw-1"},
		{Target: &statev1.Binding_Feed{Feed: "book.Y"}, Instance: "md-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_DEAD, Reason: "gone"}},
	}}
	got := dependencyConditions(d, 7)
	want := []struct{ reason, msg string }{
		{"starting: private dial: refused", "account 42 td-tw-1 pending"},
		{"no_ack", "account 43 td-tw-1 pending"},
		{"gone", "feed book.Y md-tw-1 dead"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i, w := range want {
		if got[i].GetType() != "dependency" || got[i].GetReason() != w.reason || got[i].GetMessage() != w.msg || got[i].GetAtNs() != 7 {
			t.Fatalf("condition %d: %+v", i, got[i])
		}
	}
	// Any other end reason keeps E10-D093: pending bindings are not named.
	d.EndReason = "deps_failed"
	if got := dependencyConditions(d, 7); len(got) != 1 || got[0].GetMessage() != "feed book.Y md-tw-1 dead" {
		t.Fatalf("deps_failed: %v", got)
	}
}
