package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func probeMirror() state.MirrorOptions {
	return state.MirrorOptions{
		ProbeInterval: 100 * time.Millisecond,
		MinBackoff:    50 * time.Millisecond,
		MaxBackoff:    200 * time.Millisecond,
	}
}

func present(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string) bool {
	t.Helper()
	_, ok := kvOf(t, ctx, srv, key)
	return ok
}

// A terminal session keeps every demand until its worker key is gone. Demands
// in a fresh region go first; a stale provider region holds its demands and
// the assignment. Restoring it removes the rest, and the assignment last.
// Spec, status and ack stay (E13-D033, E13-D035, E13-D009).
func TestLifecycleOrder(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	topo, planes := mixedTopo(servers)
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 40 * time.Millisecond
	cfg.StaleAfter = time.Second
	cfg.Mirror = probeMirror()
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess), sampleSpec())
	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	twKey := state.MDDemandKey("md-tw-1", deriveSess, "select/near")
	feedKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	tdKey := state.TDDemandKey("td-jp-1", deriveSess, 7)
	specKey := state.SessionSpecKey("sts-tw-1", deriveSess)
	statusKey := state.SessionStatusKey("sts-tw-1", deriveSess)
	ackKey := state.MDAckKey("md-tw-1", deriveSess, "select/near")
	hash := state.WorkerHash("sts/" + deriveSess)
	workerKey := state.WorkerStatusKey(state.PlaneSTS, "sts-tw-1", hash)
	agentKey := state.AgentKey(state.PlaneSTS, "sts-tw-1")

	until(t, 20*time.Second, "the derived demands are stored", func() error {
		for _, item := range []struct {
			srv *etcdtest.Server
			key string
		}{
			{servers["tw"], sessKey}, {servers["tw"], twKey},
			{servers["jp"], feedKey}, {servers["jp"], tdKey},
		} {
			if !present(t, ctx, item.srv, item.key) {
				return fmt.Errorf("missing %s", item.key)
			}
		}
		return nil
	})

	putProto(t, ctx, servers["tw"], workerKey, &statev1.WorkerStatus{Phase: "RUNNING"})
	putProto(t, ctx, servers["tw"], agentKey, &statev1.AgentRecord{Plane: "sts", Instance: "sts-tw-1", Epoch: 1})
	putProto(t, ctx, servers["tw"], ackKey, &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY})
	putProto(t, ctx, servers["tw"], statusKey, &statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE})

	afterRound(t, c, func() error {
		sess := c.View().Sessions[controller.SessionRef("sts-tw-1", deriveSess)]
		if sess.Status.GetPhase() != statev1.Phase_PHASE_DONE || sess.Worker == nil {
			return fmt.Errorf("status %v worker %v", sess.Status.GetPhase(), sess.Worker != nil)
		}
		return nil
	})
	for _, item := range []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], sessKey}, {servers["tw"], twKey},
		{servers["jp"], feedKey}, {servers["jp"], tdKey},
	} {
		if !present(t, ctx, item.srv, item.key) {
			t.Fatalf("worker still present, but %s was deleted", item.key)
		}
	}

	proxy.Cut()
	waitStable(t, c, "jp", func(r controller.Region) bool {
		return r.Synced && r.Stale
	}, "jp is stale after the cut")
	if !present(t, ctx, servers["tw"], twKey) || !present(t, ctx, servers["tw"], sessKey) {
		t.Fatal("demands deleted while the worker key was still there")
	}

	if _, err := client(t, servers["tw"]).Delete(ctx, workerKey); err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "the fresh region's demand is removed", func() error {
		if present(t, ctx, servers["tw"], twKey) {
			return errors.New("tw demand still present")
		}
		return nil
	})
	afterRound(t, c, func() error {
		if !c.View().Regions["jp"].Stale {
			return errors.New("jp recovered on its own")
		}
		return nil
	})
	for _, item := range []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["jp"], feedKey}, {servers["jp"], tdKey}, {servers["tw"], sessKey},
		{servers["tw"], specKey}, {servers["tw"], statusKey}, {servers["tw"], ackKey},
		{servers["tw"], agentKey},
	} {
		if !present(t, ctx, item.srv, item.key) {
			t.Fatalf("deleted while jp was stale: %s", item.key)
		}
	}
	if present(t, ctx, servers["tw"], workerKey) {
		t.Fatal("worker key was rewritten")
	}

	proxy.Restore()
	waitFresh(t, c, "jp")
	until(t, 20*time.Second, "the remaining demands go, then the assignment", func() error {
		feed := present(t, ctx, servers["jp"], feedKey)
		td := present(t, ctx, servers["jp"], tdKey)
		assign := present(t, ctx, servers["tw"], sessKey)
		if !assign && (feed || td) {
			t.Fatal("assignment removed while a demand remains")
		}
		if feed || td || assign {
			return errors.New("still present")
		}
		return nil
	})
	for _, key := range []string{specKey, statusKey, ackKey, agentKey} {
		if !present(t, ctx, servers["tw"], key) {
			t.Fatalf("controller deleted %s", key)
		}
	}
}

// An orphan put while the controller is up survives notify rounds and is
// removed by the next full resync. A demand whose session exists is not.
func TestOrphanRemovedOnResync(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 40 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	const live = "live01"
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", live), &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_ENDED,
	})
	until(t, 20*time.Second, "the live session is in the view", func() error {
		if c.View().Sessions[controller.SessionRef("sts-tw-1", live)].Spec.GetName() != "s" {
			return errors.New("spec missing")
		}
		return nil
	})
	liveKey := state.MDDemandKey("md-jp-1", live, "feed/x")
	orphanKey := state.MDDemandKey("md-jp-1", "nope", "feed/x")
	entry := &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "x"}}
	putProto(t, ctx, servers["jp"], liveKey, entry)
	putProto(t, ctx, servers["jp"], orphanKey, entry)

	mark := c.Stats().Full
	until(t, 20*time.Second, "the next full resync removes the orphan", func() error {
		if present(t, ctx, servers["jp"], orphanKey) {
			return errors.New("orphan still present")
		}
		if c.Stats().Full <= mark {
			t.Fatal("orphan removed without a full resync")
		}
		return nil
	})
	if !present(t, ctx, servers["jp"], liveKey) {
		t.Fatal("a demand whose session exists was removed")
	}
	if c.View().Sessions[controller.SessionRef("sts-tw-1", live)].Spec.GetName() != "s" {
		t.Fatal("the live spec was removed")
	}
}

// A mirror notify reconciles, but it is not a full resync, so an orphan stays.
func TestOrphanNotRemovedOnNotify(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = time.Hour
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	key := state.MDDemandKey("md-jp-1", "nope", "feed/x")
	putProto(t, ctx, servers["jp"], key, &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "x"}})
	until(t, 20*time.Second, "the orphan is in the view", func() error {
		if c.View().Demands[key].Region != "jp" {
			return errors.New("not listed")
		}
		return nil
	})
	full, rounds := c.Stats().Full, c.Stats().Rounds
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", "other"), &sessionv1.SessionSpec{
		Name: "o", DesiredState: sessionv1.DesiredState_DESIRED_STATE_ENDED,
	})
	until(t, 20*time.Second, "a later notify round has run", func() error {
		st := c.Stats()
		if st.Rounds <= rounds {
			return fmt.Errorf("rounds %d", st.Rounds)
		}
		if c.View().Sessions[controller.SessionRef("sts-tw-1", "other")].Spec.GetName() != "o" {
			return errors.New("spec not in the view")
		}
		return nil
	})
	if !present(t, ctx, servers["jp"], key) {
		t.Fatal("a notify round removed the orphan")
	}
	if got := c.Stats().Full; got != full {
		t.Fatalf("full rounds %d, was %d", got, full)
	}
}

func twoSTS(servers map[string]*etcdtest.Server) (state.Topology, map[string]state.Plane) {
	topo := state.Topology{
		Regions: map[string]state.Region{},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"sts-tw-1":               "tw",
			"md-tw-1":                "tw",
			"sts-jp-1":               "jp",
			"md-jp-1":                "jp",
		},
	}
	for region, srv := range servers {
		topo.Regions[region] = state.Region{Endpoints: srv.Endpoints()}
	}
	planes := map[string]state.Plane{
		"sts-tw-1": state.PlaneSTS,
		"md-tw-1":  state.PlaneMD,
		"sts-jp-1": state.PlaneSTS,
		"md-jp-1":  state.PlaneMD,
	}
	return topo, planes
}

// While an STS region has not synced, demands of a session that lives there
// are kept, including demands in a region that is fresh. So is an orphan: it
// might belong to a session on the region we cannot see (E13-D034, E13-D006).
func TestStaleSTSKeepsDemands(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	proxy.Cut()
	topo, planes := twoSTS(servers)
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	applyPlanes(t, ctx, topo, planes, servers)

	const kept = "kept01"
	specKey := state.SessionSpecKey("sts-jp-1", kept)
	statusKey := state.SessionStatusKey("sts-jp-1", kept)
	sessKey := state.SessionDemandKey("sts-jp-1", kept)
	agentKey := state.AgentKey(state.PlaneSTS, "sts-jp-1")
	jpKey := state.MDDemandKey("md-jp-1", kept, "feed/x")
	twKey := state.MDDemandKey("md-tw-1", kept, "feed/x")
	orphanKey := state.MDDemandKey("md-tw-1", "orphan1", "feed/x")
	entry := &statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: "x"}}
	putProto(t, ctx, servers["jp"], specKey, &sessionv1.SessionSpec{Name: "kept"})
	putProto(t, ctx, servers["jp"], statusKey, &statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE})
	putProto(t, ctx, servers["jp"], sessKey, &statev1.SessionDemand{IssuedAtNs: 1})
	putProto(t, ctx, servers["jp"], agentKey, &statev1.AgentRecord{Plane: "sts", Instance: "sts-jp-1", Epoch: 1})
	putProto(t, ctx, servers["jp"], jpKey, entry)
	putProto(t, ctx, servers["tw"], twKey, entry)
	putProto(t, ctx, servers["tw"], orphanKey, entry)

	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 30 * time.Millisecond
	cfg.StaleAfter = time.Second
	cfg.Mirror = probeMirror()
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	until(t, 20*time.Second, "two full rounds while jp is unsynced", func() error {
		r := c.View().Regions["jp"]
		if r.Synced || !r.Stale {
			return fmt.Errorf("jp %+v", r)
		}
		if c.Stats().Full < 2 {
			return fmt.Errorf("full %d", c.Stats().Full)
		}
		return nil
	})
	afterRound(t, c, func() error { return nil })
	for _, item := range []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], twKey}, {servers["tw"], orphanKey},
		{servers["jp"], jpKey}, {servers["jp"], sessKey},
		{servers["jp"], specKey}, {servers["jp"], statusKey}, {servers["jp"], agentKey},
	} {
		if !present(t, ctx, item.srv, item.key) {
			t.Fatalf("removed while the STS region was unsynced: %s", item.key)
		}
	}

	proxy.Restore()
	waitFresh(t, c, "jp")
	until(t, 20*time.Second, "jp fresh: the orphan and the ended session are removed in order", func() error {
		tw := present(t, ctx, servers["tw"], twKey)
		jp := present(t, ctx, servers["jp"], jpKey)
		assign := present(t, ctx, servers["jp"], sessKey)
		orphan := present(t, ctx, servers["tw"], orphanKey)
		if !assign && (tw || jp) {
			t.Fatal("assignment removed while a demand remains")
		}
		if tw || jp || assign || orphan {
			return errors.New("still present")
		}
		return nil
	})
	for _, key := range []string{specKey, statusKey, agentKey} {
		if !present(t, ctx, servers["jp"], key) {
			t.Fatalf("controller deleted %s", key)
		}
	}
}

// A retired STS instance ends its session and releases demands even while the
// status is non-terminal and the worker key is still there (E12-D034).
func TestRetireSTSReleasesDemands(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 40 * time.Millisecond
	cfg.StaleAfter = time.Second
	cfg.Mirror = probeMirror()
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess), sampleSpec())
	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	twKey := state.MDDemandKey("md-tw-1", deriveSess, "select/near")
	feedKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	tdKey := state.TDDemandKey("td-jp-1", deriveSess, 7)
	specKey := state.SessionSpecKey("sts-tw-1", deriveSess)
	statusKey := state.SessionStatusKey("sts-tw-1", deriveSess)
	hash := state.WorkerHash("sts/" + deriveSess)
	workerKey := state.WorkerStatusKey(state.PlaneSTS, "sts-tw-1", hash)

	until(t, 20*time.Second, "the derived demands are stored", func() error {
		for _, item := range []struct {
			srv *etcdtest.Server
			key string
		}{
			{servers["tw"], sessKey}, {servers["tw"], twKey},
			{servers["jp"], feedKey}, {servers["jp"], tdKey},
		} {
			if !present(t, ctx, item.srv, item.key) {
				return fmt.Errorf("missing %s", item.key)
			}
		}
		return nil
	})
	putProto(t, ctx, servers["tw"], workerKey, &statev1.WorkerStatus{Phase: "RUNNING"})
	putProto(t, ctx, servers["tw"], statusKey, &statev1.SessionStatus{Phase: statev1.Phase_PHASE_RUNNING})
	mark := c.Stats().Rounds
	until(t, 20*time.Second, "more rounds with the worker still present", func() error {
		if c.Stats().Rounds < mark+2 {
			return errors.New("waiting")
		}
		return nil
	})
	for _, item := range []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], sessKey}, {servers["tw"], twKey},
		{servers["jp"], feedKey}, {servers["jp"], tdKey}, {servers["tw"], workerKey},
	} {
		if !present(t, ctx, item.srv, item.key) {
			t.Fatalf("deleted before retirement: %s", item.key)
		}
	}

	if _, err := client(t, servers["tw"]).Put(ctx, state.RetiredKey("sts-tw-1"), "sts-tw-1"); err != nil {
		t.Fatal(err)
	}
	ref := controller.SessionRef("sts-tw-1", deriveSess)
	var saw bool
	until(t, 20*time.Second, "the assignment ends because the instance retired", func() error {
		if c.View().Sessions[ref].Demand.GetEndReason() == "instance_retired" {
			saw = true
			return nil
		}
		st, ok := kvOf(t, ctx, servers["tw"], sessKey)
		if ok {
			var d statev1.SessionDemand
			mustProto(t, st.val, &d)
			if d.GetEndReason() == "instance_retired" {
				saw = true
				return nil
			}
			return fmt.Errorf("reason %q", d.GetEndReason())
		}
		if saw {
			return nil
		}
		return errors.New("session demand is gone and the reason was not instance_retired")
	})
	if !saw {
		t.Fatal("missed instance_retired")
	}
	until(t, 20*time.Second, "demands and the assignment are gone", func() error {
		for _, item := range []struct {
			srv *etcdtest.Server
			key string
		}{
			{servers["tw"], twKey}, {servers["jp"], feedKey},
			{servers["jp"], tdKey}, {servers["tw"], sessKey},
		} {
			if present(t, ctx, item.srv, item.key) {
				return fmt.Errorf("still present %s", item.key)
			}
		}
		return nil
	})
	for _, key := range []string{specKey, statusKey, workerKey, state.RetiredKey("sts-tw-1")} {
		if !present(t, ctx, servers["tw"], key) {
			t.Fatalf("controller deleted %s", key)
		}
	}
}

func TestRetireMDKeepsExistingDemands(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 40 * time.Millisecond
	cfg.StaleAfter = time.Second
	cfg.Mirror = probeMirror()
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess), sampleSpec())
	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	feedKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	until(t, 20*time.Second, "the feed demand is stored", func() error {
		if !present(t, ctx, servers["jp"], feedKey) || !present(t, ctx, servers["tw"], sessKey) {
			return errors.New("missing")
		}
		return nil
	})
	if _, err := client(t, servers["jp"]).Put(ctx, state.RetiredKey("md-jp-1"), "md-jp-1"); err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "the view lists the retired MD", func() error {
		if _, ok := c.View().Retired["md-jp-1"]; !ok {
			return errors.New("not yet")
		}
		return nil
	})
	mark := c.Stats().Rounds
	until(t, 20*time.Second, "rounds after the MD retired", func() error {
		if c.Stats().Rounds < mark+2 {
			return errors.New("waiting")
		}
		return nil
	})
	st, ok := kvOf(t, ctx, servers["tw"], sessKey)
	if !ok {
		t.Fatal("assignment deleted")
	}
	var d statev1.SessionDemand
	mustProto(t, st.val, &d)
	if d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING || d.GetEndReason() == "invalid_spec" {
		t.Fatalf("existing assignment %+v", d)
	}
	if !present(t, ctx, servers["jp"], feedKey) {
		t.Fatal("existing feed demand was deleted")
	}
}

func TestRetireProviderBlocksNewAssignment(t *testing.T) {
	for _, tc := range []struct {
		name, inst string
	}{
		{"md", "md-jp-1"},
		{"td", "td-jp-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			servers := etcdtest.StartRegions(t, "tw", "jp")
			topo, planes := mixedTopo(servers)
			applyPlanes(t, ctx, topo, planes, servers)
			cfg := config(topo, t.Name(), testLog(t))
			cfg.Planes = planes
			cfg.Resync = 40 * time.Millisecond
			cfg.StaleAfter = time.Second
			cfg.Mirror = probeMirror()
			c := startAsync(t, ctx, cfg).ready(30 * time.Second)
			waitFresh(t, c, "tw")
			waitFresh(t, c, "jp")
			if _, err := client(t, servers["jp"]).Put(ctx, state.RetiredKey(tc.inst), tc.inst); err != nil {
				t.Fatal(err)
			}
			until(t, 20*time.Second, "the view lists "+tc.inst, func() error {
				if _, ok := c.View().Retired[tc.inst]; !ok {
					return errors.New("not yet")
				}
				return nil
			})
			const id = "def456"
			putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", id), sampleSpec())
			sessKey := state.SessionDemandKey("sts-tw-1", id)
			feedKey := state.MDDemandKey("md-jp-1", id, "feed/trade.BinanceUM_Perp_BTCUSDT")
			tdKey := state.TDDemandKey("td-jp-1", id, 7)
			until(t, 20*time.Second, "the new session is invalid_spec", func() error {
				st, ok := kvOf(t, ctx, servers["tw"], sessKey)
				if !ok {
					return errors.New("assignment missing")
				}
				var d statev1.SessionDemand
				mustProto(t, st.val, &d)
				if d.GetEndReason() != "invalid_spec" {
					return fmt.Errorf("reason %q", d.GetEndReason())
				}
				return nil
			})
			if present(t, ctx, servers["jp"], feedKey) || present(t, ctx, servers["jp"], tdKey) {
				t.Fatal("a retired provider was assigned")
			}
		})
	}
}
