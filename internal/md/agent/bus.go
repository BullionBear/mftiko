package agent

import (
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// BusDisconnected is the WorkerStatus condition for an md conn worker whose
// publish path to NATS is down (E12-D063). status true means down. status false
// means the connection is up. A worker with no such condition does not report.
const BusDisconnected = "bus_disconnected"

// BusConditions is the condition for one conn worker report. A report that does
// not carry bus_up, or is not mdctl, yields nothing, so TD, STS, and a worker
// that has not reported yet stay unset. at_ns is left zero: a timestamp would
// rewrite etcd on every reconcile (W7.2).
func BusConditions(rep *workerv1.Report) []*commonv1.Condition {
	if rep == nil || rep.GetContract() != conn.Contract || len(rep.GetPayload()) == 0 {
		return nil
	}
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(rep.GetPayload(), p); err != nil {
		return nil
	}
	if p.BusUp == nil {
		return nil
	}
	return []*commonv1.Condition{{
		Type:   BusDisconnected,
		Status: !p.GetBusUp(),
	}}
}

func workerConditions(o agent.Observed) []*commonv1.Condition {
	return BusConditions(o.Report)
}
