package controller

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
)

// Record is the controller's AgentRecord (E13-D014): plane ctl, instance
// controller, the term's epoch, the release, and when this process became
// leader. The strings match state.PlaneCtl and state.ControllerInstance; this
// package does not import state, because state links etcd (E13-D001).
func Record(epoch int64, release string, startedAtNs int64) *statev1.AgentRecord {
	return &statev1.AgentRecord{
		Plane: "ctl", Instance: "controller", Epoch: epoch,
		Release: release, StartedAtNs: startedAtNs,
	}
}

// Encode is the bytes PutRecord stores. It is the deterministic protobuf
// encoding internal/platform/agent/runner uses for an AgentRecord (E13-D014).
func Encode(m proto.Message) string {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("controller: encode %T: %v", m, err)) // built here, so it marshals
	}
	return string(b)
}
