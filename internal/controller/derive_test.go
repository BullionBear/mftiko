package controller_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const (
	stsInst = "sts-tw-1"
	sessID  = "abc123"
	specRev = int64(11)
)

var deriveNow = time.Unix(1_700_000_000, 0)

func places() map[string]controller.Placement {
	return map[string]controller.Placement{
		"sts-tw-1": {Plane: "sts", Region: "tw"},
		"md-tw-1":  {Plane: "md", Region: "tw"},
		"md-jp-1":  {Plane: "md", Region: "jp"},
		"td-tw-1":  {Plane: "td", Region: "tw"},
		"td-jp-1":  {Plane: "td", Region: "jp"},
	}
}

func openView(regions ...string) controller.View {
	v := controller.View{
		Regions:  map[string]controller.Region{},
		Sessions: map[string]controller.Session{},
		Demands:  map[string]controller.Demand{},
		Acks:     map[string]controller.Ack{},
		Workers:  map[string]controller.Worker{},
		Agents:   map[string]controller.Agent{},
	}
	for _, r := range regions {
		v.Regions[r] = controller.Region{Rev: 1, Synced: true}
	}
	return v
}

func sampleSpec() *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Name:         "s",
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Feeds: []*sessionv1.FeedDecl{
			{Feed: "trade.BinanceUM_Perp_BTCUSDT", MdInstance: "md-jp-1"},
		},
		Selectors: []*sessionv1.SelectorDecl{
			{
				Name: "near", MdInstance: "md-tw-1", Venue: "BinanceUM",
				Topics: []string{"trade"},
				Kind:   &sessionv1.SelectorDecl_OptionChain{OptionChain: &sessionv1.OptionChain{NearestExpiries: 2}},
			},
		},
		Accounts: []*sessionv1.AccountDecl{
			{Name: "main", ApiId: 7, TdInstance: "td-jp-1", AlgoIds: []uint32{1, 2}},
		},
	}
}

func runningSession(spec *sessionv1.SessionSpec) (controller.View, controller.Session) {
	v := openView("tw", "jp")
	sess := controller.Session{
		ID: sessID, Instance: stsInst, Region: "tw",
		Spec: spec, SpecRev: specRev,
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	return v, sess
}

func byKey(t *testing.T, p controller.Plan) map[string]proto.Message {
	t.Helper()
	out := map[string]proto.Message{}
	for _, w := range p.Writes {
		if _, ok := out[w.Key]; ok {
			t.Fatalf("duplicate write %s", w.Key)
		}
		out[w.Key] = w.Value
	}
	return out
}

func asSession(t *testing.T, m proto.Message) *statev1.SessionDemand {
	t.Helper()
	d, ok := m.(*statev1.SessionDemand)
	if !ok {
		t.Fatalf("%T", m)
	}
	return d
}

func asEntry(t *testing.T, m proto.Message) *statev1.DemandEntry {
	t.Helper()
	d, ok := m.(*statev1.DemandEntry)
	if !ok {
		t.Fatalf("%T", m)
	}
	return d
}

func TestDeriveWritesDemands(t *testing.T) {
	spec := sampleSpec()
	v, _ := runningSession(spec)
	plan := controller.Derive(v, places(), deriveNow)
	if len(plan.Drifts) != 0 {
		t.Fatalf("drifts %+v", plan.Drifts)
	}
	got := byKey(t, plan)
	sessKey := state.SessionDemandKey(stsInst, sessID)
	feedKey := state.MDDemandKey("md-jp-1", sessID, "feed/trade.BinanceUM_Perp_BTCUSDT")
	selKey := state.MDDemandKey("md-tw-1", sessID, "select/near")
	tdKey := state.TDDemandKey("td-jp-1", sessID, 7)
	if len(got) != 4 {
		t.Fatalf("keys %v", keysOf(got))
	}
	sess := asSession(t, got[sessKey])
	if sess.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING || sess.GetStart() ||
		sess.GetEndReason() != "" || sess.GetSpecRevision() != specRev ||
		sess.GetIssuedAtNs() != deriveNow.UnixNano() || sess.GetUpdatedAtNs() != deriveNow.UnixNano() {
		t.Fatalf("%+v", sess)
	}
	if len(sess.GetBindings()) != 3 {
		t.Fatalf("bindings %+v", sess.GetBindings())
	}
	b0, b1, b2 := sess.GetBindings()[0], sess.GetBindings()[1], sess.GetBindings()[2]
	if b0.GetFeed() != "trade.BinanceUM_Perp_BTCUSDT" || b0.GetInstance() != "md-jp-1" || b0.GetStatus() != nil ||
		b1.GetSelector() != "near" || b1.GetInstance() != "md-tw-1" || b1.GetStatus() != nil ||
		b2.GetApiId() != 7 || b2.GetInstance() != "td-jp-1" || b2.GetStatus() != nil {
		t.Fatalf("bindings %+v", sess.GetBindings())
	}
	feed := asEntry(t, got[feedKey])
	if feed.GetFeed() != "trade.BinanceUM_Perp_BTCUSDT" || feed.GetSpecRevision() != specRev {
		t.Fatalf("feed %+v", feed)
	}
	sel := asEntry(t, got[selKey])
	if !proto.Equal(sel.GetSelector(), spec.GetSelectors()[0]) || sel.GetSpecRevision() != specRev {
		t.Fatalf("selector %+v", sel)
	}
	td := asEntry(t, got[tdKey])
	if td.GetApiId() != 7 || len(td.GetAlgoIds()) != 2 || td.GetAlgoIds()[0] != 1 || td.GetAlgoIds()[1] != 2 {
		t.Fatalf("td %+v", td)
	}
}

func TestDeriveSkipsEqualValues(t *testing.T) {
	v, sess := runningSession(sampleSpec())
	first := controller.Derive(v, places(), deriveNow)
	for _, w := range first.Writes {
		switch m := w.Value.(type) {
		case *statev1.SessionDemand:
			sess.Demand = m
		case *statev1.DemandEntry:
			v.Demands[w.Key] = controller.Demand{Key: w.Key, Value: m}
		}
	}
	// A later clock, a touched updated_at_ns, and a touched entry spec_revision
	// are not differences (E13-D008, E13-D023). issued_at_ns stays the first one.
	sess.Demand.UpdatedAtNs = 1
	for k, d := range v.Demands {
		d.Value.SpecRevision = 99
		v.Demands[k] = d
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	second := controller.Derive(v, places(), deriveNow.Add(time.Hour))
	if len(second.Writes) != 0 || len(second.Drifts) != 0 {
		t.Fatalf("rewrote %+v drifts %+v", second.Writes, second.Drifts)
	}
}

func TestDeriveRepairsOneEntry(t *testing.T) {
	v, sess := runningSession(sampleSpec())
	first := controller.Derive(v, places(), deriveNow)
	feedKey := state.MDDemandKey("md-jp-1", sessID, "feed/trade.BinanceUM_Perp_BTCUSDT")
	for _, w := range first.Writes {
		switch m := w.Value.(type) {
		case *statev1.SessionDemand:
			m.UpdatedAtNs = 5
			m.IssuedAtNs = 42
			sess.Demand = m
		case *statev1.DemandEntry:
			v.Demands[w.Key] = controller.Demand{Key: w.Key, Value: m}
		}
	}
	broken := proto.Clone(v.Demands[feedKey].Value).(*statev1.DemandEntry)
	broken.Target = &statev1.DemandEntry_Feed{Feed: "other.X"}
	v.Demands[feedKey] = controller.Demand{Key: feedKey, Value: broken}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess

	plan := controller.Derive(v, places(), deriveNow.Add(time.Minute))
	got := byKey(t, plan)
	if len(got) != 1 {
		t.Fatalf("keys %v", keysOf(got))
	}
	if asEntry(t, got[feedKey]).GetFeed() != "trade.BinanceUM_Perp_BTCUSDT" {
		t.Fatalf("%+v", got[feedKey])
	}
}

func TestDeriveInvalid(t *testing.T) {
	base := sampleSpec()
	cases := []struct {
		name string
		edit func(*sessionv1.SessionSpec)
	}{
		{"missing md", func(s *sessionv1.SessionSpec) { s.Feeds[0].MdInstance = "md-nope" }},
		{"td labeled as md", func(s *sessionv1.SessionSpec) { s.Feeds[0].MdInstance = "td-jp-1" }},
		{"md labeled as td", func(s *sessionv1.SessionSpec) { s.Accounts[0].TdInstance = "md-jp-1" }},
		{"duplicate feed", func(s *sessionv1.SessionSpec) {
			s.Feeds = append(s.Feeds, &sessionv1.FeedDecl{Feed: s.Feeds[0].GetFeed(), MdInstance: "md-tw-1"})
		}},
		{"duplicate selector", func(s *sessionv1.SessionSpec) {
			s.Selectors = append(s.Selectors, &sessionv1.SelectorDecl{Name: "near", MdInstance: "md-jp-1"})
		}},
		{"duplicate account", func(s *sessionv1.SessionSpec) {
			s.Accounts = append(s.Accounts, &sessionv1.AccountDecl{Name: "other", ApiId: 7, TdInstance: "td-tw-1"})
		}},
		{"nil feed", func(s *sessionv1.SessionSpec) { s.Feeds = append(s.Feeds, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := proto.Clone(base).(*sessionv1.SessionSpec)
			tc.edit(spec)
			v, _ := runningSession(spec)
			plan := controller.Derive(v, places(), deriveNow)
			got := byKey(t, plan)
			key := state.SessionDemandKey(stsInst, sessID)
			if len(got) != 1 {
				t.Fatalf("keys %v", keysOf(got))
			}
			sess := asSession(t, got[key])
			if sess.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED || sess.GetEndReason() != "invalid_spec" ||
				sess.GetStart() || len(sess.GetBindings()) != 0 || sess.GetSpecRevision() != specRev ||
				sess.GetIssuedAtNs() != deriveNow.UnixNano() {
				t.Fatalf("%+v", sess)
			}
		})
	}
}

func TestDeriveSameNameDifferentAccount(t *testing.T) {
	spec := &sessionv1.SessionSpec{
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Accounts: []*sessionv1.AccountDecl{
			{Name: "main", ApiId: 7, TdInstance: "td-jp-1"},
			{Name: "main", ApiId: 8, TdInstance: "td-tw-1"},
		},
	}
	v, _ := runningSession(spec)
	plan := controller.Derive(v, places(), deriveNow)
	got := byKey(t, plan)
	if len(got) != 3 {
		t.Fatalf("keys %v", keysOf(got))
	}
	if asEntry(t, got[state.TDDemandKey("td-jp-1", sessID, 7)]).GetApiId() != 7 {
		t.Fatal("jp account")
	}
	if asEntry(t, got[state.TDDemandKey("td-tw-1", sessID, 8)]).GetApiId() != 8 {
		t.Fatal("tw account")
	}
}

func TestDeriveSpecRevisionDrift(t *testing.T) {
	spec := sampleSpec()
	spec.Feeds = append(spec.Feeds, &sessionv1.FeedDecl{Feed: "book.BinanceUM_Perp_ETHUSDT", MdInstance: "md-nope"})
	v, sess := runningSession(spec)
	sess.Demand = &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: 4, IssuedAtNs: 42,
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	// A demand that the new spec would not produce, and one it would. Neither moves.
	oldKey := state.MDDemandKey("md-jp-1", sessID, "feed/trade.BinanceUM_Perp_BTCUSDT")
	v.Demands[oldKey] = controller.Demand{Key: oldKey, Value: &statev1.DemandEntry{
		Target: &statev1.DemandEntry_Feed{Feed: "trade.BinanceUM_Perp_BTCUSDT"},
	}}
	plan := controller.Derive(v, places(), deriveNow)
	if len(plan.Writes) != 0 {
		t.Fatalf("writes %+v", plan.Writes)
	}
	if len(plan.Drifts) != 1 || plan.Drifts[0].Instance != stsInst || plan.Drifts[0].Session != sessID ||
		plan.Drifts[0].SpecRev != specRev || plan.Drifts[0].Have != 4 {
		t.Fatalf("drifts %+v", plan.Drifts)
	}
}

func TestDeriveEndedWritesNothing(t *testing.T) {
	spec := sampleSpec()
	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	v, sess := runningSession(spec)
	if plan := controller.Derive(v, places(), deriveNow); len(plan.Writes) != 0 || len(plan.Drifts) != 0 {
		t.Fatalf("fresh ended %+v", plan)
	}
	sess.Demand = &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: 1, IssuedAtNs: 42,
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	if plan := controller.Derive(v, places(), deriveNow); len(plan.Writes) != 0 || len(plan.Drifts) != 0 {
		t.Fatalf("existing assignment %+v", plan)
	}
}

func TestDeriveSkipsTerminalStaleAndUnspecified(t *testing.T) {
	spec := sampleSpec()
	v, sess := runningSession(spec)
	sess.Status = &statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	if plan := controller.Derive(v, places(), deriveNow); len(plan.Writes) != 0 {
		t.Fatalf("terminal %+v", plan.Writes)
	}

	v, sess = runningSession(spec)
	tw := v.Regions["tw"]
	tw.Stale = true
	v.Regions["tw"] = tw
	if plan := controller.Derive(v, places(), deriveNow); len(plan.Writes) != 0 {
		t.Fatalf("stale spec region %+v", plan.Writes)
	}

	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_UNSPECIFIED
	v, _ = runningSession(spec)
	if plan := controller.Derive(v, places(), deriveNow); len(plan.Writes) != 0 || len(plan.Drifts) != 0 {
		t.Fatalf("unspecified %+v", plan)
	}
}

func TestDeriveSkipsClosedProviderRegion(t *testing.T) {
	spec := sampleSpec()
	spec.Accounts[0].TdInstance = "td-tw-1"
	v, sess := runningSession(spec)
	jp := v.Regions["jp"]
	jp.Synced = false
	jp.Stale = true
	v.Regions["jp"] = jp
	plan := controller.Derive(v, places(), deriveNow)
	got := byKey(t, plan)
	feedKey := state.MDDemandKey("md-jp-1", sessID, "feed/trade.BinanceUM_Perp_BTCUSDT")
	if _, ok := got[feedKey]; ok {
		t.Fatal("wrote into a region that is not synced")
	}
	sessKey := state.SessionDemandKey(stsInst, sessID)
	if asSession(t, got[sessKey]).GetBindings()[0].GetFeed() != "trade.BinanceUM_Perp_BTCUSDT" {
		t.Fatal("binding dropped with the closed region")
	}
	if _, ok := got[state.MDDemandKey("md-tw-1", sessID, "select/near")]; !ok {
		t.Fatal("open region was skipped")
	}
	if _, ok := got[state.TDDemandKey("td-tw-1", sessID, 7)]; !ok {
		t.Fatal("td in the open region was skipped")
	}

	for _, w := range plan.Writes {
		if m, ok := w.Value.(*statev1.SessionDemand); ok {
			sess.Demand = m
		} else if m, ok := w.Value.(*statev1.DemandEntry); ok {
			v.Demands[w.Key] = controller.Demand{Key: w.Key, Value: m}
		}
	}
	v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
	v.Regions["jp"] = controller.Region{Rev: 2, Synced: true}
	again := controller.Derive(v, places(), deriveNow.Add(time.Second))
	got = byKey(t, again)
	if len(got) != 1 || asEntry(t, got[feedKey]).GetFeed() != "trade.BinanceUM_Perp_BTCUSDT" {
		t.Fatalf("after open: %v", keysOf(got))
	}
}

func TestDeriveUndecodable(t *testing.T) {
	v, _ := runningSession(sampleSpec())
	v.Undecodable = []string{state.SessionDemandKey(stsInst, sessID)}
	if plan := controller.Derive(v, places(), deriveNow); len(plan.Writes) != 0 {
		t.Fatalf("rewrote an undecodable assignment %+v", plan.Writes)
	}

	v, _ = runningSession(sampleSpec())
	feedKey := state.MDDemandKey("md-jp-1", sessID, "feed/trade.BinanceUM_Perp_BTCUSDT")
	v.Undecodable = []string{feedKey}
	plan := controller.Derive(v, places(), deriveNow)
	got := byKey(t, plan)
	if _, ok := got[feedKey]; ok {
		t.Fatal("rewrote an undecodable demand")
	}
	if len(got) != 3 {
		t.Fatalf("keys %v", keysOf(got))
	}
}

func TestDeriveEndedStaysEnded(t *testing.T) {
	for _, reason := range []string{"invalid_spec", "deps_failed", "deps_timeout", "ended_by_spec"} {
		t.Run(reason, func(t *testing.T) {
			spec := sampleSpec()
			// The spec runs again and its revision still matches, which is
			// what a recovered topology or a recovered ack looks like. Ending
			// does not go back to RUNNING (E13-D028).
			v, sess := runningSession(spec)
			sess.Demand = &statev1.SessionDemand{
				Desired: sessionv1.DesiredState_DESIRED_STATE_ENDED, EndReason: reason,
				SpecRevision: specRev, IssuedAtNs: 42, UpdatedAtNs: 7,
			}
			v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
			plan := controller.Derive(v, places(), deriveNow)
			if len(plan.Writes) != 0 || len(plan.Drifts) != 0 {
				t.Fatalf("revived %+v", plan)
			}

			sess.Demand.SpecRevision = 1
			v.Sessions[controller.SessionRef(stsInst, sessID)] = sess
			plan = controller.Derive(v, places(), deriveNow)
			if len(plan.Writes) != 0 || len(plan.Drifts) != 0 {
				t.Fatalf("ended session drifted %+v", plan)
			}
		})
	}
}

func TestDeriveEmpty(t *testing.T) {
	if plan := controller.Derive(controller.View{}, nil, deriveNow); len(plan.Writes) != 0 || len(plan.Drifts) != 0 {
		t.Fatalf("%+v", plan)
	}
}

func keysOf(m map[string]proto.Message) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
