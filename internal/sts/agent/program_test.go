package agent

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/internal/sts"
	"github.com/BullionBear/mftiko/sdk"
)

func TestBindingsVersion(t *testing.T) {
	p := &Program{Epoch: 4, Sign: func(string, sts.Permissions) (string, string, error) {
		return "j", "s", nil
	}}
	spec := &sessionv1.SessionSpec{Name: "n"}
	raw, err := proto.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	p.note("s", "h", spec, &stsctlv1.Bindings{Accounts: []*stsctlv1.AccountBinding{{ApiId: 1, State: stsctlv1.BindingState_BINDING_STATE_PENDING}}})
	start, err := p.Start(WorkerKey("s"), Spec{Raw: raw})
	if err != nil {
		t.Fatal(err)
	}
	var sp stsctlv1.StartPayload
	if err := proto.Unmarshal(start.GetPayload(), &sp); err != nil {
		t.Fatal(err)
	}
	if sp.GetBindings().GetVersion().GetEpoch() != 4 || sp.GetBindings().GetVersion().GetSeq() != 1 {
		t.Fatalf("start version %+v", sp.GetBindings().GetVersion())
	}
	if _, _, _, ok := p.command("s"); ok {
		t.Fatal("unchanged bindings were pushed again")
	}
	p.note("s", "h", spec, &stsctlv1.Bindings{Accounts: []*stsctlv1.AccountBinding{{ApiId: 1, State: stsctlv1.BindingState_BINDING_STATE_READY}}})
	rawCmd, hash, seq, ok := p.command("s")
	if !ok || seq != 2 {
		t.Fatalf("command ok %v seq %d", ok, seq)
	}
	var cmd stsctlv1.Command
	if err := proto.Unmarshal(rawCmd, &cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.GetBindings().GetVersion().GetSeq() != 2 || cmd.GetBindings().GetAccounts()[0].GetState() != stsctlv1.BindingState_BINDING_STATE_READY {
		t.Fatalf("command %+v", cmd.GetBindings())
	}
	p.markSent("s", hash, seq)
	if _, _, _, ok := p.command("s"); ok {
		t.Fatal("published bindings were pushed again")
	}

	// A new spec encoding starts the version at 1 again.
	p.note("s", "h2", spec, &stsctlv1.Bindings{})
	start, err = p.Start(WorkerKey("s"), Spec{Raw: raw})
	if err != nil {
		t.Fatal(err)
	}
	sp.Reset()
	if err := proto.Unmarshal(start.GetPayload(), &sp); err != nil {
		t.Fatal(err)
	}
	if sp.GetBindings().GetVersion().GetSeq() != 1 {
		t.Fatalf("replacement start seq %d", sp.GetBindings().GetVersion().GetSeq())
	}

	// A new process, as after a rotation, publishes seq 1 at the new epoch
	// even when the worker is already running and Start is not sent again.
	next := &Program{Epoch: 5}
	next.note("s", "h", spec, &stsctlv1.Bindings{})
	_, _, seq, ok = next.command("s")
	if !ok || seq != 1 {
		t.Fatalf("rotated command ok %v seq %d", ok, seq)
	}
	b, _, got := next.versioned("s")
	if got != 1 || b.GetVersion().GetEpoch() != 5 {
		t.Fatalf("rotated version %+v seq %d", b.GetVersion(), got)
	}
}

func TestStopGraceFromSpecUnit(t *testing.T) {
	p := &Program{}
	p.note("c", "h", &sessionv1.SessionSpec{GraceMs: 2500}, nil)
	p.note("z", "h2", &sessionv1.SessionSpec{}, nil)
	if got := p.StopGrace(WorkerKey("c")); got != 2500*time.Millisecond {
		t.Fatalf("custom %s", got)
	}
	if got := p.StopGrace(WorkerKey("z")); got != 0 {
		t.Fatalf("zero %s", got)
	}
}

func TestWorkerContractIsSDK(t *testing.T) {
	if !containsStr(sdk.Contracts(), workerContract) {
		t.Fatalf("sdk contracts %q do not include %s", sdk.Contracts(), workerContract)
	}
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func TestVersionOrdersEpochFirst(t *testing.T) {
	old := &commonv1.Version{Epoch: 1, Seq: 9}
	next := &commonv1.Version{Epoch: 2, Seq: 1}
	if old.GetEpoch() > next.GetEpoch() || (old.GetEpoch() == next.GetEpoch() && old.GetSeq() >= next.GetSeq()) {
		t.Fatal("a new epoch must be a newer version")
	}
}
