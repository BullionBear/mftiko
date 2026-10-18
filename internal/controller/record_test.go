package controller_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestRecord(t *testing.T) {
	rec := controller.Record(3, "rel", 42)
	if rec.GetPlane() != string(state.PlaneCtl) || rec.GetInstance() != state.ControllerInstance {
		t.Fatalf("identity %s/%s", rec.GetPlane(), rec.GetInstance())
	}
	if rec.GetEpoch() != 3 || rec.GetRelease() != "rel" || rec.GetStartedAtNs() != 42 {
		t.Fatalf("%+v", rec)
	}
	want, err := proto.MarshalOptions{Deterministic: true}.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	got := controller.Encode(rec)
	if got != string(want) || controller.Encode(rec) != got {
		t.Fatal("encode differs from deterministic protobuf")
	}
	var back statev1.AgentRecord
	if err := proto.Unmarshal([]byte(got), &back); err != nil {
		t.Fatal(err)
	}
	if back.GetEpoch() != rec.GetEpoch() || back.GetRelease() != rec.GetRelease() {
		t.Fatalf("round trip %+v", &back)
	}
}
