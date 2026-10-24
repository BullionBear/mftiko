package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func putProto(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string, m proto.Message) {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client(t, srv).Put(ctx, key, string(b)); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func mixedTopo(servers map[string]*etcdtest.Server) (state.Topology, map[string]state.Plane) {
	topo := state.Topology{
		Regions: map[string]state.Region{},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"sts-tw-1":               "tw",
			"md-tw-1":                "tw",
			"md-jp-1":                "jp",
			"td-jp-1":                "jp",
		},
	}
	for region, srv := range servers {
		topo.Regions[region] = state.Region{Endpoints: srv.Endpoints()}
	}
	planes := map[string]state.Plane{
		"sts-tw-1": state.PlaneSTS,
		"md-tw-1":  state.PlaneMD,
		"md-jp-1":  state.PlaneMD,
		"td-jp-1":  state.PlaneTD,
	}
	return topo, planes
}

// A write on either region shows up in the one view, on the region that holds the key.
func TestViewTwoRegions(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)

	const sess = "abc123"
	specKey := state.SessionSpecKey("sts-tw-1", sess)
	// ENDED so derivation leaves this fixture assignment alone (E13-D026).
	putProto(t, ctx, servers["tw"], specKey, &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_ENDED,
	})
	putProto(t, ctx, servers["tw"], state.SessionStatusKey("sts-tw-1", sess), &statev1.SessionStatus{
		Phase: statev1.Phase_PHASE_RUNNING,
	})
	putProto(t, ctx, servers["tw"], state.SessionDemandKey("sts-tw-1", sess), &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, IssuedAtNs: 42,
	})
	hash := state.WorkerHash("sts/" + sess)
	putProto(t, ctx, servers["tw"], state.WorkerStatusKey(state.PlaneSTS, "sts-tw-1", hash), &statev1.WorkerStatus{Phase: "RUNNING"})
	putProto(t, ctx, servers["tw"], state.AgentKey(state.PlaneSTS, "sts-tw-1"), &statev1.AgentRecord{
		Plane: "sts", Instance: "sts-tw-1", Epoch: 1,
	})
	if _, err := client(t, servers["tw"]).Put(ctx, state.EpochKey(state.PlaneSTS, "sts-tw-1"), "00000000000000000001"); err != nil {
		t.Fatal(err)
	}

	mdKey := state.MDDemandKey("md-jp-1", sess, "feed/book.BTCUSDT")
	putProto(t, ctx, servers["jp"], mdKey, &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "book.BTCUSDT"}})
	ackKey := state.MDAckKey("md-jp-1", sess, "feed/book.BTCUSDT")
	putProto(t, ctx, servers["jp"], ackKey, &statev1.EntryStatus{
		State: statev1.EntryState_ENTRY_STATE_READY, Reason: "up",
	})
	orphan := state.MDAckKey("md-jp-1", sess, "feed/orphan")
	putProto(t, ctx, servers["jp"], orphan, &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_PENDING})
	putProto(t, ctx, servers["jp"], state.AgentKey(state.PlaneMD, "md-jp-1"), &statev1.AgentRecord{
		Plane: "md", Instance: "md-jp-1", Epoch: 2,
	})
	tdKey := state.TDDemandKey("td-jp-1", sess, 7)
	putProto(t, ctx, servers["jp"], tdKey, &statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: 7}})

	until(t, 20*time.Second, "both regions show the writes", func() error {
		v := c.View()
		got := v.Sessions[controller.SessionRef("sts-tw-1", sess)]
		if got.Region != "tw" || got.Spec.GetName() != "s" || got.SpecRev == 0 ||
			got.Demand.GetIssuedAtNs() != 42 || got.Status.GetPhase() != statev1.Phase_PHASE_RUNNING ||
			got.Worker == nil || got.Worker.LastKnown || got.Worker.Hash != hash {
			return fmt.Errorf("session %+v worker %+v", got, got.Worker)
		}
		md := v.Demands[mdKey]
		if md.Region != "jp" || md.Instance != "md-jp-1" || md.Entry != "feed/book.BTCUSDT" || md.Value.GetFeed() != "book.BTCUSDT" {
			return fmt.Errorf("md %+v", md)
		}
		ack := v.Acks[ackKey]
		if ack.Region != "jp" || ack.LastKnown || ack.Value.GetReason() != "up" {
			return fmt.Errorf("ack %+v", ack)
		}
		if _, ok := v.Acks[orphan]; ok {
			return errors.New("ack without a demand entered the view")
		}
		td := v.Demands[tdKey]
		if td.Region != "jp" || td.Plane != string(state.PlaneTD) || td.Value.GetApiId() != 7 {
			return fmt.Errorf("td %+v", td)
		}
		tw, jp := v.Regions["tw"], v.Regions["jp"]
		if tw.Stale || !tw.Synced || tw.Rev == 0 || jp.Stale || !jp.Synced || jp.Rev == 0 {
			return fmt.Errorf("tw %+v jp %+v", tw, jp)
		}
		if v.Agents["sts-tw-1"].Record.GetEpoch() != 1 || v.Agents["md-jp-1"].Region != "jp" {
			return fmt.Errorf("agents %+v", v.Agents)
		}
		for _, k := range v.Undecodable {
			if k == state.EpochKey(state.PlaneSTS, "sts-tw-1") {
				return errors.New("epoch listed as undecodable")
			}
		}
		return nil
	})
}

// Cutting a region past the threshold marks it stale; restoring clears the mark.
// The other region keeps reconciling.
func TestRegionStaleClears(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	topo := topology(servers, "tw")
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	// The demand below belongs to a session. A fast resync would otherwise
	// reclaim it as an orphan (N5).
	topo.Instances["sts-tw-1"] = "tw"
	planes := mdPlanes(topo)
	planes["sts-tw-1"] = state.PlaneSTS
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

	const sess = "abc123"
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", sess), &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_ENDED,
	})
	until(t, 20*time.Second, "the session is in the view", func() error {
		if c.View().Sessions[controller.SessionRef("sts-tw-1", sess)].Spec.GetName() != "s" {
			return errors.New("spec not in the view")
		}
		return nil
	})
	key := state.MDDemandKey("md-jp-1", sess, "feed/x")
	putProto(t, ctx, servers["jp"], key, &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "x"}})
	waitStable(t, c, "jp", func(r controller.Region) bool {
		return r.Synced && !r.Stale && r.Rev > 0
	}, "jp is fresh")
	until(t, 10*time.Second, "the jp demand is in the view", func() error {
		if c.View().Demands[key].Region != "jp" {
			return errors.New("demand not in jp")
		}
		return nil
	})

	proxy.Cut()
	waitStable(t, c, "jp", func(r controller.Region) bool {
		return r.Synced && r.Stale
	}, "jp is stale after the cut")
	v, _ := c.reading()
	if v.Regions["tw"].Stale || !v.Regions["tw"].Synced {
		t.Fatalf("tw stopped while jp was cut: %+v", v.Regions["tw"])
	}
	if v.Demands[key].Value.GetFeed() != "x" {
		t.Fatalf("stale view dropped the demand: %+v", v.Demands[key])
	}

	proxy.Restore()
	waitStable(t, c, "jp", func(r controller.Region) bool {
		return r.Synced && !r.Stale
	}, "jp is fresh after the restore")
}

// The provider's ack stays in the view, marked last-known, after its AgentRecord disappears.
func TestAckLastKnownWhenAgentGone(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw")
	topo := state.Topology{
		Regions: map[string]state.Region{"tw": {Endpoints: servers["tw"].Endpoints()}},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"md-tw-1":                "tw",
			"sts-tw-1":               "tw",
		},
	}
	planes := map[string]state.Plane{"md-tw-1": state.PlaneMD, "sts-tw-1": state.PlaneSTS}
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)

	const sess = "abc123"
	demandKey := state.MDDemandKey("md-tw-1", sess, "feed/x")
	ackKey := state.MDAckKey("md-tw-1", sess, "feed/x")
	agentKey := state.AgentKey(state.PlaneMD, "md-tw-1")
	putProto(t, ctx, servers["tw"], demandKey, &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "x"}})
	putProto(t, ctx, servers["tw"], ackKey, &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY, Reason: "up"})
	putProto(t, ctx, servers["tw"], agentKey, &statev1.AgentRecord{Plane: "md", Instance: "md-tw-1", Epoch: 4})

	hash := state.WorkerHash("sts/" + sess)
	workerKey := state.WorkerStatusKey(state.PlaneSTS, "sts-tw-1", hash)
	stsAgent := state.AgentKey(state.PlaneSTS, "sts-tw-1")
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", sess), &sessionv1.SessionSpec{Name: "s"})
	putProto(t, ctx, servers["tw"], workerKey, &statev1.WorkerStatus{Phase: "RUNNING"})
	putProto(t, ctx, servers["tw"], stsAgent, &statev1.AgentRecord{Plane: "sts", Instance: "sts-tw-1", Epoch: 5})

	until(t, 20*time.Second, "ack and worker are current", func() error {
		v := c.View()
		ack := v.Acks[ackKey]
		if ack.Value.GetReason() != "up" || ack.LastKnown {
			return fmt.Errorf("ack %+v", ack)
		}
		w := v.Sessions[controller.SessionRef("sts-tw-1", sess)].Worker
		if w == nil || w.LastKnown || w.Hash != hash {
			return fmt.Errorf("worker %+v", w)
		}
		return nil
	})
	if _, err := client(t, servers["tw"]).Delete(ctx, agentKey); err != nil {
		t.Fatal(err)
	}
	if _, err := client(t, servers["tw"]).Delete(ctx, stsAgent); err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "ack and worker are last-known", func() error {
		v := c.View()
		ack := v.Acks[ackKey]
		if ack.Value.GetReason() != "up" || !ack.LastKnown {
			return fmt.Errorf("ack %+v", ack)
		}
		if _, ok := v.Agents["md-tw-1"]; ok {
			return errors.New("md agent still in the view")
		}
		w := v.Sessions[controller.SessionRef("sts-tw-1", sess)].Worker
		if w == nil || !w.LastKnown || w.Status.GetPhase() != "RUNNING" {
			return fmt.Errorf("worker %+v", w)
		}
		return nil
	})
}

// A value that does not decode stays out of the view, is counted, and is not deleted.
func TestUndecodableKeyIsKept(t *testing.T) {
	ctx := t.Context()
	buf := &safeBuf{}
	log := zerolog.New(buf)
	servers := etcdtest.StartRegions(t, "tw")
	topo := topology(servers, "tw")
	// topology() only has an MD instance. Watch an STS instance so a spec is in scope.
	topo.Instances["sts-tw-1"] = "tw"
	planes := map[string]state.Plane{"md-tw-1": state.PlaneMD, "sts-tw-1": state.PlaneSTS}
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), log)
	cfg.Planes = planes
	cfg.Resync = 20 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)

	const sess = "abc123"
	good := state.SessionSpecKey("sts-tw-1", sess)
	bad := state.SessionSpecKey("sts-tw-1", "badkey")
	putProto(t, ctx, servers["tw"], good, &sessionv1.SessionSpec{Name: "ok"})
	if _, err := client(t, servers["tw"]).Put(ctx, bad, string([]byte{0xff, 0x00})); err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "the bad key is listed and the good spec is in the view", func() error {
		v := c.View()
		if v.Sessions[controller.SessionRef("sts-tw-1", sess)].Spec.GetName() != "ok" {
			return errors.New("good spec missing")
		}
		if _, ok := v.Sessions[controller.SessionRef("sts-tw-1", "badkey")]; ok {
			return errors.New("bad spec entered the view")
		}
		if !listed(v.Undecodable, bad) {
			return fmt.Errorf("undecodable %v", v.Undecodable)
		}
		if c.Stats().DecodeFailures < 1 {
			return errors.New("decode failure not counted")
		}
		return nil
	})
	marked := c.Stats().Rounds
	until(t, 10*time.Second, "later rounds still list the bad key", func() error {
		v, st := c.reading()
		if st.Rounds < marked+2 {
			return fmt.Errorf("rounds %d", st.Rounds)
		}
		if !listed(v.Undecodable, bad) {
			return errors.New("bad key left the list")
		}
		return nil
	})
	if v, ok := valueOf(t, ctx, servers["tw"], bad); !ok || v == "" {
		t.Fatal("controller deleted the undecodable key")
	}
	if !bytes.Contains([]byte(buf.String()), []byte(bad)) || !bytes.Contains([]byte(buf.String()), []byte("undecodable value")) {
		t.Fatalf("log did not warn:\n%s", buf.String())
	}

	putProto(t, ctx, servers["tw"], bad, &sessionv1.SessionSpec{Name: "recovered"})
	until(t, 20*time.Second, "a rewritten value enters the view", func() error {
		v := c.View()
		if listed(v.Undecodable, bad) {
			return errors.New("still undecodable")
		}
		if v.Sessions[controller.SessionRef("sts-tw-1", "badkey")].Spec.GetName() != "recovered" {
			return errors.New("recovered spec missing")
		}
		return nil
	})
}

// The resync timer runs the same loop when no mirror is notifying.
func TestResyncRuns(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Resync = 30 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	until(t, 5*time.Second, "a second full round", func() error {
		st := c.Stats()
		if st.Full < 2 || st.Rounds < st.Full {
			return fmt.Errorf("%+v", st)
		}
		return nil
	})
}

func waitStable(t *testing.T, c *Controller, region string, want func(controller.Region) bool, what string) {
	t.Helper()
	var seen, last int
	until(t, 20*time.Second, what, func() error {
		v, st := c.reading()
		r, ok := v.Regions[region]
		if !ok || !want(r) {
			seen, last = 0, 0
			return fmt.Errorf("%+v present %v", r, ok)
		}
		if st.Rounds != last {
			seen++
			last = st.Rounds
		}
		if seen < 2 {
			return fmt.Errorf("%d stable round", seen)
		}
		return nil
	})
}

func listed(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
