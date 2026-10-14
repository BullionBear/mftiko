package agent

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/td/account"
)

type fakeKinds map[string]exchange.AccountKind

func (k fakeKinds) AccountKind(n string) (exchange.AccountKind, bool) { x, ok := k[n]; return x, ok }

var kinds = fakeKinds{
	"Fake":  {Name: "Fake", Trades: []string{"Fake"}, Account: func(exchange.AccountConfig) (exchange.Account, error) { return nil, nil }},
	"Multi": {Name: "Multi", Trades: []string{"A", "B"}, Account: func(exchange.AccountConfig) (exchange.Account, error) { return nil, nil }},
}

// opener refuses sealed bytes "other-key" as sealed to another instance.
func opener(sealed []byte, _ int64, _ string) error {
	if string(sealed) == "other-key" {
		return sealbox.ErrOtherKey
	}
	return nil
}

const dkey = "/mftiko/v1/td/td-1/demand/"

func demandIn(items map[string]Entry) agent.Demand[Entry] {
	return agent.Demand[Entry]{Items: items, Rejected: map[string]error{}}
}

func acct(id int64, kind string, enabled bool, sealed string) Entry {
	return Entry{Account: &Account{APIID: id, Kind: kind, Enabled: enabled, Sealed: []byte(sealed)}}
}

func dem(session string, id int64) Entry { return Entry{Demand: &Demand{Session: session, APIID: id}} }

func runPlan(now time.Time, d agent.Demand[Entry], prev planOut) planOut {
	return plan(planIn{
		now: now, linger: 5 * time.Second, kinds: kinds, open: opener,
		endpoints: map[string]Endpoint{"Fake": {REST: "https://rest", Stream: "wss://stream"}},
		demand:    d, lingering: prev.lingering, prev: prev.specs,
	})
}

// Two sessions on one account share one worker.
func TestPlanSharesAWorker(t *testing.T) {
	out := runPlan(time.Unix(100, 0), demandIn(map[string]Entry{
		"/mftiko/v1/td/td-1/account/42": acct(42, "Fake", true, "s"),
		dkey + "s1/42":                  dem("s1", 42),
		dkey + "s2/42":                  dem("s2", 42),
	}), planOut{})
	if len(out.plans) != 1 {
		t.Fatalf("plans %v", out.plans)
	}
	p := out.plans[WorkerKey(42)]
	if !p.Start || p.Spawn.Kind != "Fake" || p.Spawn.Endpoint.Stream != "wss://stream" {
		t.Fatalf("plan %+v", p)
	}
	for _, k := range []string{dkey + "s1/42", dkey + "s2/42"} {
		if o := out.outcomes[k]; o.Worker != WorkerKey(42) || o.Reject != "" {
			t.Fatalf("%s: %+v", k, o)
		}
	}
}

// What the plan refuses gets no worker, and every demand on it is REJECTED with the reason.
func TestPlanRejects(t *testing.T) {
	out := runPlan(time.Unix(100, 0), demandIn(map[string]Entry{
		"/mftiko/v1/td/td-1/account/2": acct(2, "Fake", false, "s"),
		"/mftiko/v1/td/td-1/account/3": acct(3, "Nope", true, "s"),
		"/mftiko/v1/td/td-1/account/4": acct(4, "Multi", true, "s"),
		"/mftiko/v1/td/td-1/account/5": acct(5, "Fake", true, "other-key"),
		dkey + "s/1":                   dem("s", 1),
		dkey + "s/2":                   dem("s", 2),
		dkey + "s/3":                   dem("s", 3),
		dkey + "s/4":                   dem("s", 4),
		dkey + "s/5":                   dem("s", 5),
	}), planOut{})
	if len(out.plans) != 0 {
		t.Fatalf("plans %v", out.plans)
	}
	want := map[int64]string{1: ReasonNoAccount, 2: ReasonDisabled, 3: ReasonUnknownKind, 4: ReasonKindUnsupported, 5: "credentials_other_key"}
	for id, reason := range want {
		o := out.outcomes[dkey+"s/"+string(rune('0'+id))]
		if o.Reject != reason || o.Worker != "" {
			t.Errorf("%d: %+v, want %s", id, o, reason)
		}
	}
}

// A demand key Decode refused is REJECTED with Decode's reason.
func TestPlanDecodeRejects(t *testing.T) {
	d := demandIn(map[string]Entry{})
	d.Rejected[dkey+"s/7"] = errMismatch
	out := runPlan(time.Unix(100, 0), d, planOut{})
	if o := out.outcomes[dkey+"s/7"]; o.Reject != errMismatch.Error() || o.APIID != 7 || o.Session != "s" {
		t.Fatalf("%+v", o)
	}
}

// The last owner leaving keeps the worker for linger, with the spec it ran; then it is dropped. An
// account disabled meanwhile stops at once.
func TestPlanLinger(t *testing.T) {
	t0 := time.Unix(100, 0)
	acc := map[string]Entry{"/mftiko/v1/td/td-1/account/42": acct(42, "Fake", true, "s")}
	withDemand := map[string]Entry{"/mftiko/v1/td/td-1/account/42": acct(42, "Fake", true, "s"), dkey + "s1/42": dem("s1", 42)}
	out := runPlan(t0, demandIn(withDemand), planOut{})
	ran := out.plans[WorkerKey(42)].Spawn

	out = runPlan(t0.Add(time.Second), demandIn(acc), out)
	if p, ok := out.plans[WorkerKey(42)]; !ok || p.Spawn.Hash() != ran.Hash() || len(out.outcomes) != 0 {
		t.Fatalf("lingering: %v %v", out.plans, out.outcomes)
	}
	if !out.wake.Equal(t0.Add(6 * time.Second)) {
		t.Fatalf("wake %v", out.wake)
	}
	held := runPlan(t0.Add(5*time.Second), demandIn(acc), out)
	if _, ok := held.plans[WorkerKey(42)]; !ok {
		t.Fatal("dropped before linger")
	}
	gone := runPlan(t0.Add(6*time.Second), demandIn(acc), held)
	if len(gone.plans) != 0 {
		t.Fatalf("still planned after linger: %v", gone.plans)
	}

	// A new owner during linger takes the worker back, and linger starts over next time.
	back := runPlan(t0.Add(5*time.Second), demandIn(withDemand), out)
	if _, ok := back.plans[WorkerKey(42)]; !ok || len(back.lingering) != 0 {
		t.Fatalf("back: %v %v", back.plans, back.lingering)
	}

	disabled := runPlan(t0.Add(2*time.Second), demandIn(map[string]Entry{"/mftiko/v1/td/td-1/account/42": acct(42, "Fake", false, "s")}), out)
	if len(disabled.plans) != 0 {
		t.Fatalf("disabled account lingered: %v", disabled.plans)
	}
}

// A spec changes with what Init carries, and only then.
func TestSpecHash(t *testing.T) {
	base := Spec{APIID: 1, Kind: "Fake", Sealed: []byte("s"), Endpoint: Endpoint{REST: "r"}}
	same := base
	same.Sealed = []byte("s")
	if base.Hash() != same.Hash() {
		t.Fatal("unstable hash")
	}
	for _, mod := range []func(*Spec){
		func(s *Spec) { s.Sealed = []byte("resealed") },
		func(s *Spec) { s.Kind = "Other" },
		func(s *Spec) { s.CancelOnDisconnectS = 5 },
		func(s *Spec) { s.Endpoint.Stream = "x" },
		func(s *Spec) { s.Endpoint.Simulated = true },
	} {
		s := base
		mod(&s)
		if s.Hash() == base.Hash() {
			t.Errorf("%+v hashes like the base", s)
		}
	}
	if (Live{}).Hash() != "" {
		t.Fatal("Live hash")
	}
}

func exit(o commonv1.Outcome, reason string) *commonv1.ExitInfo {
	return &commonv1.ExitInfo{Outcome: o, Reason: reason}
}

func TestPolicy(t *testing.T) {
	p := DefaultPolicy
	for _, e := range []*commonv1.ExitInfo{
		exit(commonv1.Outcome_OUTCOME_INIT_FAILED, account.ReasonAuth),
		exit(commonv1.Outcome_OUTCOME_INIT_FAILED, account.ReasonHedgeMode),
		exit(commonv1.Outcome_OUTCOME_INIT_FAILED, account.ReasonKindUnsupported),
		exit(commonv1.Outcome_OUTCOME_CRASHED, account.ReasonAuth),
	} {
		if again, _ := p.Restart(WorkerKey(1), agent.Ended{Exit: e}); again {
			t.Errorf("restarts after %v", e)
		}
	}
	if again, after := p.Restart(WorkerKey(1), agent.Ended{Exit: exit(commonv1.Outcome_OUTCOME_CRASHED, "boom"), Restarts: 3}); !again || after != 8*time.Second {
		t.Fatalf("crash: %v %v", again, after)
	}
	if again, _ := p.Restart(WorkerKey(1), agent.Ended{Restarts: 10}); again {
		t.Fatal("did not give up after 10")
	}
	if !p.KeepWhileStale(WorkerKey(1)) || p.BeatTimeout(WorkerKey(1)) != 10*time.Second || !p.Silent(WorkerKey(1), time.Minute) {
		t.Fatal("policy defaults")
	}
}

func report(st tdv1.AccountState) *workerv1.Report {
	b, _ := proto.Marshal(&tdctlv1.Report{State: st})
	return &workerv1.Report{Contract: account.Contract, Payload: b}
}

func TestDecide(t *testing.T) {
	o := outcome{Session: "s", APIID: 1, Worker: WorkerKey(1)}
	never := func(agent.WorkerKey) bool { return false }
	always := func(agent.WorkerKey) bool { return true }
	for name, c := range map[string]struct {
		o      outcome
		obs    *agent.Observed
		gaveUp func(agent.WorkerKey) bool
		state  statev1.EntryState
		reason string
		write  bool
	}{
		"rejected":     {outcome{Reject: "account_disabled"}, nil, never, statev1.EntryState_ENTRY_STATE_REJECTED, "account_disabled", true},
		"not spawned":  {o, nil, never, statev1.EntryState_ENTRY_STATE_PENDING, "starting", true},
		"lost":         {o, &agent.Observed{State: agent.StateLost}, never, 0, "", false},
		"initializing": {o, &agent.Observed{State: agent.StateInitialized}, never, statev1.EntryState_ENTRY_STATE_PENDING, "starting", true},
		"ready":        {o, &agent.Observed{State: agent.StateRunning, Report: report(tdv1.AccountState_ACCOUNT_STATE_READY)}, never, statev1.EntryState_ENTRY_STATE_READY, "", true},
		"degraded":     {o, &agent.Observed{State: agent.StateRunning, Report: report(tdv1.AccountState_ACCOUNT_STATE_DEGRADED)}, never, statev1.EntryState_ENTRY_STATE_READY, "", true},
		"auth":         {o, &agent.Observed{State: agent.StateExited, Exit: exit(commonv1.Outcome_OUTCOME_INIT_FAILED, account.ReasonAuth)}, always, statev1.EntryState_ENTRY_STATE_REJECTED, account.ReasonAuth, true},
		"hedge":        {o, &agent.Observed{State: agent.StateExited, Exit: exit(commonv1.Outcome_OUTCOME_INIT_FAILED, account.ReasonHedgeMode)}, always, statev1.EntryState_ENTRY_STATE_REJECTED, account.ReasonHedgeMode, true},
		"restarting":   {o, &agent.Observed{State: agent.StateExited, Exit: exit(commonv1.Outcome_OUTCOME_CRASHED, "boom")}, never, statev1.EntryState_ENTRY_STATE_PENDING, "restarting", true},
		"dead":         {o, &agent.Observed{State: agent.StateExited, Exit: exit(commonv1.Outcome_OUTCOME_CRASHED, "boom")}, always, statev1.EntryState_ENTRY_STATE_DEAD, "gave up", true},
	} {
		obs := map[agent.WorkerKey]agent.Observed{}
		if c.obs != nil {
			obs[WorkerKey(1)] = *c.obs
		}
		st, reason, write := decide(c.o, obs, c.gaveUp)
		if st != c.state || reason != c.reason || write != c.write {
			t.Errorf("%s: %v %q %v", name, st, reason, write)
		}
	}
}

// Init carries the unsealed credentials, a NATS user for the api_id and the kind's hosts; an
// unseal failure says why and never quotes the credentials.
func TestProgramInit(t *testing.T) {
	k, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	secret := "v-" + hex.EncodeToString(b)
	sealed, err := sealbox.SealCredentials(k.Public(), 42, "Fake", sealbox.Credentials{KeyType: "hmac", Key: "key", Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	p := &Program{
		Account: "/bin/td-account", Instance: "td-1", Env: []string{"MFTIKO_NATS_URL=nats://x"},
		Unseal: func(s []byte, id int64, kind string) (sealbox.Credentials, error) {
			return sealbox.OpenCredentials(k, s, id, kind)
		},
		User: func(id int64) (string, string, string, error) { return "jwt", "seed", "_INBOX_td_42", nil },
	}
	spec := Spec{APIID: 42, Kind: "Fake", Sealed: sealed, CancelOnDisconnectS: 30, Endpoint: Endpoint{REST: "https://r", Stream: "wss://s", Simulated: true}}
	init, err := p.Init(WorkerKey(42), spec, Live{})
	if err != nil {
		t.Fatal(err)
	}
	in := &tdctlv1.InitPayload{}
	if init.GetContract() != account.Contract || proto.Unmarshal(init.GetPayload(), in) != nil {
		t.Fatalf("init %v", init)
	}
	if in.GetApiId() != 42 || in.GetKind() != "Fake" || in.GetTdInstance() != "td-1" || in.GetCancelOnDisconnectS() != 30 ||
		in.GetCredentials().GetSecret() != secret || in.GetNats().GetUserJwt() != "jwt" || in.GetNats().GetInboxPrefix() != "_INBOX_td_42" ||
		in.GetRestUrl() != "https://r" || in.GetStreamUrl() != "wss://s" || !in.GetSimulated() {
		t.Fatalf("payload %v", in.GetApiId())
	}
	spec.APIID = 43 // sealed for 42
	_, err = p.Init(WorkerKey(43), spec, Live{})
	if err == nil || !strings.Contains(err.Error(), "credentials_other_account") || strings.Contains(err.Error(), secret) {
		t.Fatalf("other account: %v", err)
	}
	if l, _ := p.Launch(WorkerKey(42), spec); l.GetPath() != "/bin/td-account" || len(l.GetEnv()) != 1 {
		t.Fatalf("launch %v", l)
	}
}
