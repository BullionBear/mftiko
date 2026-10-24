package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

const gateFeed = "trade.BinanceUM_Perp_BTCUSDT"

func gateSpec(timeout uint32) *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Name:         "s",
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		DepsTimeoutS: timeout,
		Feeds:        []*sessionv1.FeedDecl{{Feed: gateFeed, MdInstance: "md-tw-1"}},
	}
}

func gateStack(t *testing.T) (context.Context, map[string]*etcdtest.Server, state.Topology, map[string]state.Plane) {
	t.Helper()
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw")
	topo := state.Topology{
		Regions: map[string]state.Region{"tw": {Endpoints: servers["tw"].Endpoints()}},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"sts-tw-1":               "tw",
			"md-tw-1":                "tw",
		},
	}
	planes := map[string]state.Plane{"sts-tw-1": state.PlaneSTS, "md-tw-1": state.PlaneMD}
	applyPlanes(t, ctx, topo, planes, servers)
	return ctx, servers, topo, planes
}

func startGate(t *testing.T, resync time.Duration) (context.Context, *etcdtest.Server, *Controller) {
	t.Helper()
	ctx, servers, topo, planes := gateStack(t)
	cfg := config(topo, strings.ReplaceAll(t.Name(), "/", "_"), testLog(t))
	cfg.Planes = planes
	cfg.Resync = resync
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	return ctx, servers["tw"], c
}

func gateSessionKey(id string) string { return state.SessionDemandKey("sts-tw-1", id) }

func gateFeedKey(id string) (demand, ack string) {
	entry := "feed/" + gateFeed
	return state.MDDemandKey("md-tw-1", id, entry), state.MDAckKey("md-tw-1", id, entry)
}

func putGateAck(t *testing.T, ctx context.Context, srv *etcdtest.Server, id string, st statev1.EntryState, reason string) {
	t.Helper()
	_, ack := gateFeedKey(id)
	putProto(t, ctx, srv, ack, &statev1.EntryStatus{State: st, Reason: reason, Subject: "md.v1.f.md-tw-1.trade.X"})
}

func readGate(t *testing.T, ctx context.Context, srv *etcdtest.Server, id string) (*statev1.SessionDemand, stored, bool) {
	t.Helper()
	st, ok := kvOf(t, ctx, srv, gateSessionKey(id))
	if !ok {
		return nil, stored{}, false
	}
	d := &statev1.SessionDemand{}
	mustProto(t, st.val, d)
	return d, st, true
}

// A later round has run since mark, so a rewrite that round would show up.
func settled(t *testing.T, c *Controller) {
	t.Helper()
	afterRound(t, c, func() error { return nil })
}

func TestGateReadyThenPending(t *testing.T) {
	ctx, srv, c := startGate(t, 20*time.Millisecond)
	const id = deriveSess
	putProto(t, ctx, srv, state.SessionSpecKey("sts-tw-1", id), gateSpec(3600))
	var issued int64
	until(t, 20*time.Second, "assignment is waiting", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetStart() || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
			d.GetIssuedAtNs() == 0 || d.GetBindings()[0].GetStatus() != nil {
			return fmt.Errorf("%+v present %v", d, ok)
		}
		issued = d.GetIssuedAtNs()
		return nil
	})

	putGateAck(t, ctx, srv, id, statev1.EntryState_ENTRY_STATE_READY, "up")
	until(t, 20*time.Second, "ready releases start", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		st := d.GetBindings()[0].GetStatus()
		if !ok || !d.GetStart() || st.GetState() != statev1.EntryState_ENTRY_STATE_READY ||
			st.GetReason() != "up" || st.GetSubject() != "md.v1.f.md-tw-1.trade.X" ||
			d.GetIssuedAtNs() != issued {
			return fmt.Errorf("%+v", d)
		}
		return nil
	})

	putGateAck(t, ctx, srv, id, statev1.EntryState_ENTRY_STATE_PENDING, "boot")
	var mod int64
	until(t, 20*time.Second, "pending updates the binding and leaves start", func() error {
		d, st, ok := readGate(t, ctx, srv, id)
		if !ok || !d.GetStart() || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
			d.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_PENDING ||
			d.GetEndReason() != "" {
			return fmt.Errorf("%+v", d)
		}
		mod = st.mod
		return nil
	})
	settled(t, c)
	if _, st, ok := readGate(t, ctx, srv, id); !ok || st.mod != mod {
		t.Fatalf("rewritten after the binding settled: %+v", st)
	}
}

func TestGateNackEnds(t *testing.T) {
	ctx, srv, c := startGate(t, 20*time.Millisecond)
	cases := []struct {
		id string
		st statev1.EntryState
	}{
		{"rej", statev1.EntryState_ENTRY_STATE_REJECTED},
		{"dead", statev1.EntryState_ENTRY_STATE_DEAD},
	}
	for _, tc := range cases {
		putProto(t, ctx, srv, state.SessionSpecKey("sts-tw-1", tc.id), gateSpec(3600))
	}
	until(t, 20*time.Second, "both assignments exist", func() error {
		for _, tc := range cases {
			d, _, ok := readGate(t, ctx, srv, tc.id)
			if !ok || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING {
				return fmt.Errorf("%s %+v", tc.id, d)
			}
		}
		return nil
	})
	for _, tc := range cases {
		putGateAck(t, ctx, srv, tc.id, tc.st, "nope")
	}
	mods := map[string]int64{}
	until(t, 20*time.Second, "nack ends the session", func() error {
		for _, tc := range cases {
			d, st, ok := readGate(t, ctx, srv, tc.id)
			if !ok || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
				d.GetEndReason() != "deps_failed" || d.GetStart() ||
				d.GetBindings()[0].GetStatus().GetReason() != "nope" {
				return fmt.Errorf("%s %+v", tc.id, d)
			}
			mods[tc.id] = st.mod
		}
		return nil
	})
	for _, tc := range cases {
		putGateAck(t, ctx, srv, tc.id, statev1.EntryState_ENTRY_STATE_READY, "back")
	}
	until(t, 20*time.Second, "the view has the recovered acks", func() error {
		v := c.View()
		for _, tc := range cases {
			_, ack := gateFeedKey(tc.id)
			if v.Acks[ack].Value.GetReason() != "back" {
				return fmt.Errorf("%s ack %+v", tc.id, v.Acks[ack])
			}
		}
		return nil
	})
	settled(t, c)
	for _, tc := range cases {
		d, st, ok := readGate(t, ctx, srv, tc.id)
		if !ok || st.mod != mods[tc.id] || d.GetEndReason() != "deps_failed" ||
			d.GetBindings()[0].GetStatus().GetReason() != "nope" {
			t.Fatalf("%s revived %+v mod %d want %d", tc.id, d, st.mod, mods[tc.id])
		}
	}
}

func TestGateSilence(t *testing.T) {
	ctx, srv, c := startGate(t, 20*time.Millisecond)
	const id = deriveSess
	putProto(t, ctx, srv, state.SessionSpecKey("sts-tw-1", id), gateSpec(86400))
	var mod int64
	until(t, 20*time.Second, "silence leaves the session running", func() error {
		d, st, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetStart() || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
			d.GetEndReason() != "" || d.GetBindings()[0].GetStatus() != nil {
			return fmt.Errorf("%+v", d)
		}
		mod = st.mod
		return nil
	})
	settled(t, c)
	d, st, ok := readGate(t, ctx, srv, id)
	if !ok || st.mod != mod || d.GetStart() || d.GetEndReason() != "" {
		t.Fatalf("silence changed the assignment %+v mod %d want %d", d, st.mod, mod)
	}
}

func TestGateMissingAgent(t *testing.T) {
	ctx, srv, c := startGate(t, 20*time.Millisecond)
	const id = deriveSess
	putProto(t, ctx, srv, state.SessionSpecKey("sts-tw-1", id), gateSpec(86400))
	until(t, 20*time.Second, "assignment exists", func() error {
		_, _, ok := readGate(t, ctx, srv, id)
		if !ok {
			return errors.New("missing")
		}
		return nil
	})
	putGateAck(t, ctx, srv, id, statev1.EntryState_ENTRY_STATE_PENDING, "boot")
	_, ackKey := gateFeedKey(id)
	until(t, 20*time.Second, "a missing agent with pending does not end the session", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		ack := c.View().Acks[ackKey]
		if !ok || d.GetStart() || d.GetEndReason() != "" || !ack.LastKnown ||
			d.GetBindings()[0].GetStatus().GetState() != statev1.EntryState_ENTRY_STATE_PENDING {
			return fmt.Errorf("demand %+v ack %+v", d, ack)
		}
		return nil
	})
	putGateAck(t, ctx, srv, id, statev1.EntryState_ENTRY_STATE_READY, "up")
	until(t, 20*time.Second, "a missing agent with ready still releases start", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		ack := c.View().Acks[ackKey]
		if !ok || !d.GetStart() || d.GetEndReason() != "" || !ack.LastKnown ||
			ack.Value.GetReason() != "up" {
			return fmt.Errorf("demand %+v ack %+v", d, ack)
		}
		return nil
	})
}

func TestGateDepsTimeoutWakesBeforeResync(t *testing.T) {
	ctx, srv, c := startGate(t, time.Hour)
	const id = deriveSess
	putProto(t, ctx, srv, state.SessionSpecKey("sts-tw-1", id), gateSpec(3))
	putGateAck(t, ctx, srv, id, statev1.EntryState_ENTRY_STATE_PENDING, "boot")
	var issued int64
	until(t, 20*time.Second, "the session is waiting, not yet ended", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING || d.GetStart() ||
			d.GetEndReason() != "" || d.GetIssuedAtNs() == 0 {
			return fmt.Errorf("%+v present %v", d, ok)
		}
		issued = d.GetIssuedAtNs()
		return nil
	})
	until(t, 20*time.Second, "the deps deadline ends the session", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
			d.GetEndReason() != "deps_timeout" || d.GetIssuedAtNs() != issued || d.GetStart() {
			return fmt.Errorf("%+v", d)
		}
		return nil
	})
	if full := c.Stats().Full; full != 1 {
		t.Fatalf("full resyncs %d, the deadline should not have waited for one", full)
	}
}

func TestGateTimeoutHandoff(t *testing.T) {
	ctx, servers, topo, planes := gateStack(t)
	srv := servers["tw"]
	const id = deriveSess
	spec := gateSpec(60)
	specKey := state.SessionSpecKey("sts-tw-1", id)
	putProto(t, ctx, srv, specKey, spec)
	specKV, ok := kvOf(t, ctx, srv, specKey)
	if !ok {
		t.Fatal("spec missing")
	}
	issued := time.Now().Add(-2 * time.Minute).UnixNano()
	demandKey, _ := gateFeedKey(id)
	putProto(t, ctx, srv, demandKey, &statev1.DemandEntry{
		Target: &statev1.DemandEntry_Feed{Feed: gateFeed}, SpecRevision: specKV.mod,
	})
	putGateAck(t, ctx, srv, id, statev1.EntryState_ENTRY_STATE_PENDING, "boot")
	putProto(t, ctx, srv, gateSessionKey(id), &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, SpecRevision: specKV.mod,
		IssuedAtNs: issued,
		Bindings: []*statev1.Binding{{
			Target:   &statev1.Binding_Feed{Feed: gateFeed},
			Instance: "md-tw-1",
			Status:   &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_PENDING, Reason: "old"},
		}},
	})
	before, ok := kvOf(t, ctx, srv, gateSessionKey(id))
	if !ok {
		t.Fatal("assignment missing")
	}

	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = time.Hour
	startAsync(t, ctx, cfg).ready(30 * time.Second)
	until(t, 20*time.Second, "the new leader ends on the stored issued_at_ns", func() error {
		d, st, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
			d.GetEndReason() != "deps_timeout" || d.GetIssuedAtNs() != issued || d.GetStart() ||
			d.GetBindings()[0].GetStatus().GetReason() != "boot" || st.create != before.create {
			return fmt.Errorf("%+v create %d want %d present %v", d, st.create, before.create, ok)
		}
		return nil
	})
}

func TestGateEndedBySpec(t *testing.T) {
	ctx, srv, c := startGate(t, 20*time.Millisecond)
	const id = deriveSess
	spec := gateSpec(86400)
	specKey := state.SessionSpecKey("sts-tw-1", id)
	putProto(t, ctx, srv, specKey, spec)
	var issued int64
	until(t, 20*time.Second, "assignment exists", func() error {
		d, _, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetIssuedAtNs() == 0 {
			return errors.New("missing")
		}
		issued = d.GetIssuedAtNs()
		return nil
	})
	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	putProto(t, ctx, srv, specKey, spec)
	var mod int64
	until(t, 20*time.Second, "the spec ends the session", func() error {
		d, st, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetEndReason() != "ended_by_spec" || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED ||
			d.GetIssuedAtNs() != issued {
			return fmt.Errorf("%+v", d)
		}
		mod = st.mod
		return nil
	})
	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_RUNNING
	putProto(t, ctx, srv, specKey, spec)
	until(t, 20*time.Second, "the view sees the spec running again", func() error {
		got := c.View().Sessions[controller.SessionRef("sts-tw-1", id)]
		if got.Spec.GetDesiredState() != sessionv1.DesiredState_DESIRED_STATE_RUNNING {
			return fmt.Errorf("spec %v", got.Spec.GetDesiredState())
		}
		return nil
	})
	settled(t, c)
	d, st, ok := readGate(t, ctx, srv, id)
	if !ok || st.mod != mod || d.GetEndReason() != "ended_by_spec" || d.GetIssuedAtNs() != issued {
		t.Fatalf("spec running again revived %+v mod %d want %d", d, st.mod, mod)
	}
}

func TestGateSpecDeleted(t *testing.T) {
	ctx, srv, c := startGate(t, 20*time.Millisecond)
	const id = deriveSess
	specKey := state.SessionSpecKey("sts-tw-1", id)
	putProto(t, ctx, srv, specKey, gateSpec(86400))
	demandKey, _ := gateFeedKey(id)
	// Wait for the assignment too. The entry commits first; a spec deleted
	// before SessionDemand lands leaves the entry an orphan (E13-D034).
	until(t, 20*time.Second, "the feed demand and assignment exist", func() error {
		if _, ok := kvOf(t, ctx, srv, demandKey); !ok {
			return errors.New("missing")
		}
		if _, _, ok := readGate(t, ctx, srv, id); !ok {
			return errors.New("no assignment")
		}
		return nil
	})
	if _, err := client(t, srv).Delete(ctx, specKey); err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "a deleted spec ends the session", func() error {
		got := c.View().Sessions[controller.SessionRef("sts-tw-1", id)]
		if got.Spec != nil {
			return errors.New("spec still in the view")
		}
		d, _, ok := readGate(t, ctx, srv, id)
		if !ok || d.GetEndReason() != "ended_by_spec" || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED {
			return fmt.Errorf("%+v", d)
		}
		return nil
	})
	if _, ok := kvOf(t, ctx, srv, demandKey); !ok {
		t.Fatal("ending deleted the demand entry")
	}
}

func TestGateStaleReadyDoesNotStart(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	topo, planes := mixedTopo(servers)
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.StaleAfter = time.Second
	cfg.Resync = 50 * time.Millisecond
	cfg.Mirror = state.MirrorOptions{
		ProbeInterval: 100 * time.Millisecond,
		MinBackoff:    50 * time.Millisecond,
		MaxBackoff:    200 * time.Millisecond,
	}
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	const id = deriveSess
	twFeed := "trade.BinanceUM_Perp_BTCUSDT"
	jpFeed := "book.BinanceUM_Perp_ETHUSDT"
	spec := &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING, DepsTimeoutS: 3600,
		Feeds: []*sessionv1.FeedDecl{
			{Feed: twFeed, MdInstance: "md-tw-1"},
			{Feed: jpFeed, MdInstance: "md-jp-1"},
		},
	}
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", id), spec)
	twAck := state.MDAckKey("md-tw-1", id, "feed/"+twFeed)
	jpAck := state.MDAckKey("md-jp-1", id, "feed/"+jpFeed)
	sessKey := state.SessionDemandKey("sts-tw-1", id)
	until(t, 20*time.Second, "assignment is waiting on both feeds", func() error {
		d, _, ok := readGate(t, ctx, servers["tw"], id)
		if !ok || d.GetStart() || len(d.GetBindings()) != 2 {
			return fmt.Errorf("%+v", d)
		}
		return nil
	})
	putProto(t, ctx, servers["jp"], jpAck, &statev1.EntryStatus{
		State: statev1.EntryState_ENTRY_STATE_READY, Reason: "jp",
	})
	until(t, 20*time.Second, "the fresh jp ack is copied and does not start alone", func() error {
		d, _, ok := readGate(t, ctx, servers["tw"], id)
		if !ok || d.GetStart() || d.GetBindings()[1].GetStatus().GetReason() != "jp" {
			return fmt.Errorf("%+v", d)
		}
		return nil
	})

	proxy.Cut()
	waitStable(t, c, "jp", func(r controller.Region) bool {
		return r.Synced && r.Stale
	}, "jp is stale")
	putProto(t, ctx, servers["tw"], twAck, &statev1.EntryStatus{
		State: statev1.EntryState_ENTRY_STATE_READY, Reason: "tw",
	})
	until(t, 20*time.Second, "a stale ready does not release start", func() error {
		if !c.View().Regions["jp"].Stale {
			return errors.New("jp went fresh")
		}
		d, _, ok := readGate(t, ctx, servers["tw"], id)
		if !ok || d.GetStart() || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING ||
			d.GetEndReason() != "" || d.GetBindings()[0].GetStatus().GetReason() != "tw" ||
			d.GetBindings()[1].GetStatus().GetReason() != "jp" {
			return fmt.Errorf("%+v", d)
		}
		return nil
	})
	settled(t, c)
	d, _, ok := readGate(t, ctx, servers["tw"], id)
	if !ok || d.GetStart() || d.GetEndReason() != "" {
		t.Fatalf("stale ready started the session %+v", d)
	}

	proxy.Restore()
	until(t, 20*time.Second, "both fresh readies release start", func() error {
		r := c.View().Regions["jp"]
		if !r.Synced || r.Stale {
			return fmt.Errorf("jp %+v", r)
		}
		got, _, ok := readGate(t, ctx, servers["tw"], id)
		if !ok || !got.GetStart() || got.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING {
			return fmt.Errorf("%+v", got)
		}
		return nil
	})
	if _, ok := kvOf(t, ctx, servers["tw"], sessKey); !ok {
		t.Fatal("assignment disappeared")
	}
}
