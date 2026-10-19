package agent

import (
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
)

func TestToBindings(t *testing.T) {
	spec := &sessionv1.SessionSpec{Accounts: []*sessionv1.AccountDecl{{Name: "main", ApiId: 7}}}
	got := toBindings(spec, []*statev1.Binding{
		{Target: &statev1.Binding_Feed{Feed: "trades.Binance_Spot_BTCUSDT"}, Instance: "md-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY, Subject: "md.v1.f.md-1.trades.Binance_Spot_BTCUSDT"}},
		{Target: &statev1.Binding_Selector{Selector: "near"}, Instance: "md-1",
			Status: &statev1.EntryStatus{Subject: "md.v1.f.md-1", Universe: &statev1.Universe{Epoch: 3, Members: []string{"a"}}}},
		{Target: &statev1.Binding_ApiId{ApiId: 7}, Instance: "td-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_PENDING, Subject: "td.order.7",
				Worker: &commonv1.WorkerRef{Key: "td/7", Incarnation: 2}}},
		{Target: &statev1.Binding_ApiId{ApiId: 8}, Instance: "td-1"},
	})
	if len(got.GetFeeds()) != 1 || got.Feeds[0].GetState() != stsctlv1.BindingState_BINDING_STATE_READY || got.Feeds[0].GetSubject() == "" {
		t.Fatalf("feed %+v", got.GetFeeds())
	}
	if len(got.GetSelectors()) != 1 || got.Selectors[0].GetUniverse().GetEpoch() != 3 || got.Selectors[0].GetSubjectPrefix() != "md.v1.f.md-1" {
		t.Fatalf("selector %+v", got.GetSelectors())
	}
	if len(got.GetAccounts()) != 2 || got.Accounts[0].GetName() != "main" || got.Accounts[0].GetWorker().GetIncarnation() != 2 {
		t.Fatalf("account %+v", got.GetAccounts())
	}
	if got.Accounts[1].GetState() != stsctlv1.BindingState_BINDING_STATE_UNSPECIFIED || got.Accounts[1].GetName() != "" {
		t.Fatalf("silent account %+v", got.Accounts[1])
	}
	if got.GetVersion() != nil {
		t.Fatal("conversion stamped a version")
	}
	a := contentHash(got)
	got.Version = &commonv1.Version{Epoch: 1, Seq: 2}
	if contentHash(got) != a {
		t.Fatal("version changed the content hash")
	}
}

func TestKeepDigests(t *testing.T) {
	live := &sess{parsed: &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: "sha256:" + repeatHex('a')}}}
	done := &sess{parsed: &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: "sha256:" + repeatHex('b')}}, local: &statusView{Phase: statev1.Phase_PHASE_DONE}}
	bad := &sess{parsed: &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: "not-a-digest"}}}
	got := keepDigests(map[string]*sess{"live": live, "done": done, "bad": bad}, []string{"sha256:" + repeatHex('c'), "nope"})
	want := []string{"sha256:" + repeatHex('a'), "sha256:" + repeatHex('c')}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("keep %v", got)
	}
}

func repeatHex(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
