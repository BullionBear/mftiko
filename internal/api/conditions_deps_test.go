package api

import (
	"testing"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
)

// A deps_timeout status names pending bindings; the terminal dependencies show them (E06-D043).
func TestConditionsToDepsPending(t *testing.T) {
	deps := conditionsToDeps(&statev1.SessionStatus{Conditions: []*commonv1.Condition{
		{Type: "dependency", Reason: "starting: private dial: refused", Message: "account 42 td-tw-1 pending"},
		{Type: "dependency", Reason: "gone", Message: "feed book.Y md-tw-1 dead"},
		{Type: "dependency", Message: "feed book.Z md-tw-1 weird"},
	}})
	if len(deps) != 2 {
		t.Fatalf("deps %+v", deps)
	}
	d := deps[0]
	if d.State != apiv1.EntryStatePending || d.Kind != apiv1.DependencyKindAccount || d.ApiId == nil || *d.ApiId != 42 ||
		d.ProviderInstance != "td-tw-1" || d.Reason == nil || *d.Reason != "starting: private dial: refused" {
		t.Fatalf("pending dep %+v", d)
	}
	if deps[1].State != apiv1.EntryStateDead {
		t.Fatalf("dead dep %+v", deps[1])
	}
}
