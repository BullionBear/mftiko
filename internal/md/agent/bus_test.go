package agent

import (
	"testing"

	"google.golang.org/protobuf/proto"

	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/md/conn"
)

func TestBusConditions(t *testing.T) {
	if BusConditions(nil) != nil {
		t.Fatal("nil report")
	}
	if BusConditions(&workerv1.Report{Contract: "other"}) != nil {
		t.Fatal("other contract")
	}
	if BusConditions(&workerv1.Report{Contract: conn.Contract, Payload: []byte{0xff}}) != nil {
		t.Fatal("bad payload")
	}
	plain, err := proto.Marshal(&mdctlv1.Report{ConnEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if BusConditions(&workerv1.Report{Contract: conn.Contract, Payload: plain}) != nil {
		t.Fatal("unset bus_up")
	}
	up := true
	down := false
	for _, tc := range []struct {
		up   *bool
		want bool
	}{
		{up: &up, want: false},
		{up: &down, want: true},
	} {
		body, err := proto.Marshal(&mdctlv1.Report{BusUp: tc.up})
		if err != nil {
			t.Fatal(err)
		}
		got := BusConditions(&workerv1.Report{Contract: conn.Contract, Payload: body})
		if len(got) != 1 || got[0].GetType() != BusDisconnected || got[0].GetStatus() != tc.want || got[0].GetAtNs() != 0 {
			t.Fatalf("bus %v -> %+v", tc.up, got)
		}
	}
}
