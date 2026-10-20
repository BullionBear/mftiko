package agent

import (
	"syscall"
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
)

func TestExitClean(t *testing.T) {
	cases := []struct {
		name string
		ex   *commonv1.ExitInfo
		want bool
	}{
		{name: "nil"},
		{name: "no stopped", ex: &commonv1.ExitInfo{}},
		{name: "completed", ex: &commonv1.ExitInfo{OnStopCompleted: true}, want: true},
		{name: "sigkill", ex: &commonv1.ExitInfo{OnStopCompleted: true, Signal: int32(syscall.SIGKILL)}},
	}
	for _, tc := range cases {
		if got := exitClean(tc.ex); got != tc.want {
			t.Fatalf("%s: got %v", tc.name, got)
		}
	}
}
