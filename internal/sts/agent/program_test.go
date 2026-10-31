package agent

import (
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/sts"
	"github.com/BullionBear/mftiko/pkg/logger"
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

func TestBindingsSeqContinuesAcrossIncarnation(t *testing.T) {
	p := &Program{Epoch: 4}
	spec := &sessionv1.SessionSpec{Name: "n"}
	raw, err := proto.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	p.note("s", "h", spec, &stsctlv1.Bindings{})
	rawInit, err := proto.Marshal(&stsctlv1.InitPayload{SessionId: "s"})
	if err != nil {
		t.Fatal(err)
	}
	init := &workerv1.Init{Payload: rawInit}
	seqs := []uint64{}
	for inc := uint32(1); inc <= 3; inc++ {
		if err := p.StampIncarnation(init, inc); err != nil {
			t.Fatal(err)
		}
		start, err := p.Start(WorkerKey("s"), Spec{Raw: raw})
		if err != nil {
			t.Fatal(err)
		}
		var sp stsctlv1.StartPayload
		if err := proto.Unmarshal(start.GetPayload(), &sp); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, sp.GetBindings().GetVersion().GetSeq())
	}
	if seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("seqs %v", seqs)
	}
}

func TestInitCopiesEventLog(t *testing.T) {
	p := &Program{Sign: func(string, sts.Permissions) (string, string, error) {
		return "j", "s", nil
	}}
	cases := []struct {
		name string
		ev   *sessionv1.EventLog
	}{
		{"absent", nil},
		{"on", &sessionv1.EventLog{Enabled: true, KeepMb: 64}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := proto.Marshal(&sessionv1.SessionSpec{Name: "n", EventLog: tc.ev})
			if err != nil {
				t.Fatal(err)
			}
			init, err := p.Init(WorkerKey("s1"), Spec{Raw: raw}, Live{})
			if err != nil {
				t.Fatal(err)
			}
			var msg stsctlv1.InitPayload
			if err := proto.Unmarshal(init.GetPayload(), &msg); err != nil {
				t.Fatal(err)
			}
			if (msg.GetEventLog() == nil) != (tc.ev == nil) || !proto.Equal(msg.GetEventLog(), tc.ev) {
				t.Fatalf("event_log %v, spec %v", msg.GetEventLog(), tc.ev)
			}
		})
	}
}

func TestKillSilentRemembersTheIncarnation(t *testing.T) {
	pol := newPolicy(Config{})
	p := &Program{policy: pol}
	if !p.KillSilent(WorkerKey("s"), 7) {
		t.Fatal("KillSilent refused")
	}
	s := pol.sessions["s"]
	if s == nil {
		t.Fatal("no session")
	}
	if _, ok := s.killed[7]; !ok {
		t.Fatalf("killed %+v", s.killed)
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

func TestLaunchOutputLimits(t *testing.T) {
	root := t.TempDir()
	p := &Program{}
	p.note("a1b2c3", "h", &sessionv1.SessionSpec{Name: "n"}, nil)
	p.setImage("a1b2c3", root, artifact.Manifest{Entrypoint: []string{"bin"}})
	l, err := p.Launch(WorkerKey("a1b2c3"), Spec{})
	if err != nil {
		t.Fatal(err)
	}
	if l.GetOutput() != nil {
		t.Fatalf("output %+v", l.GetOutput())
	}
	p.LogMaxBytes = shim.MinOutputMaxBytes
	p.LogKeep = 2
	l, err = p.Launch(WorkerKey("a1b2c3"), Spec{})
	if err != nil {
		t.Fatal(err)
	}
	if l.GetOutput().GetMaxBytes() != shim.MinOutputMaxBytes || l.GetOutput().GetKeep() != 2 {
		t.Fatalf("output %+v", l.GetOutput())
	}
}

func TestLaunchPassesLogEnv(t *testing.T) {
	root := t.TempDir()
	launch := func(env []string) []string {
		t.Helper()
		p := &Program{Env: env}
		p.note("a1b2c3", "h", &sessionv1.SessionSpec{Name: "n"}, nil)
		p.setImage("a1b2c3", root, artifact.Manifest{Entrypoint: []string{"bin"}})
		l, err := p.Launch(WorkerKey("a1b2c3"), Spec{})
		if err != nil {
			t.Fatal(err)
		}
		return l.GetEnv()
	}
	countPrefix := func(env []string, prefix string) int {
		n := 0
		for _, e := range env {
			if strings.HasPrefix(e, prefix) {
				n++
			}
		}
		return n
	}

	t.Run("copied", func(t *testing.T) {
		t.Setenv(logger.EnvLevel, "debug")
		t.Setenv(logger.EnvFormat, "json")
		env := launch([]string{"MFTIKO_NATS_URL=nats://127.0.0.1:1"})
		if !containsStr(env, "MFTIKO_LOG_LEVEL=debug") || !containsStr(env, "MFTIKO_LOG_FORMAT=json") {
			t.Fatalf("env %v", env)
		}
		if !containsStr(env, "MFTIKO_NATS_URL=nats://127.0.0.1:1") {
			t.Fatalf("env %v", env)
		}
	})
	t.Run("empty is passed", func(t *testing.T) {
		t.Setenv(logger.EnvLevel, "")
		t.Setenv(logger.EnvFormat, "")
		env := launch(nil)
		if !containsStr(env, "MFTIKO_LOG_LEVEL=") || !containsStr(env, "MFTIKO_LOG_FORMAT=") {
			t.Fatalf("env %v", env)
		}
	})
	t.Run("unset is omitted", func(t *testing.T) {
		hideEnv(t, logger.EnvLevel)
		hideEnv(t, logger.EnvFormat)
		env := launch(nil)
		if countPrefix(env, "MFTIKO_LOG_LEVEL=") != 0 || countPrefix(env, "MFTIKO_LOG_FORMAT=") != 0 {
			t.Fatalf("env %v", env)
		}
	})
	t.Run("program env wins", func(t *testing.T) {
		t.Setenv(logger.EnvLevel, "debug")
		t.Setenv(logger.EnvFormat, "console")
		env := launch([]string{"MFTIKO_LOG_LEVEL=warn"})
		if !containsStr(env, "MFTIKO_LOG_LEVEL=warn") || containsStr(env, "MFTIKO_LOG_LEVEL=debug") {
			t.Fatalf("env %v", env)
		}
		if countPrefix(env, "MFTIKO_LOG_LEVEL=") != 1 {
			t.Fatalf("env %v", env)
		}
		if !containsStr(env, "MFTIKO_LOG_FORMAT=console") {
			t.Fatalf("env %v", env)
		}
	})
}

func hideEnv(t *testing.T, key string) {
	t.Helper()
	v, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if ok {
			os.Setenv(key, v)
		} else {
			os.Unsetenv(key)
		}
	})
}

func TestVersionOrdersEpochFirst(t *testing.T) {
	old := &commonv1.Version{Epoch: 1, Seq: 9}
	next := &commonv1.Version{Epoch: 2, Seq: 1}
	if old.GetEpoch() > next.GetEpoch() || (old.GetEpoch() == next.GetEpoch() && old.GetSeq() >= next.GetSeq()) {
		t.Fatal("a new epoch must be a newer version")
	}
}
