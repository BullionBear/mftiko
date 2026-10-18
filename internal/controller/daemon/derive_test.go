package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

const deriveSess = "abc123"

func sampleSpec() *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Name:         "s",
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Feeds: []*sessionv1.FeedDecl{
			{Feed: "trade.BinanceUM_Perp_BTCUSDT", MdInstance: "md-jp-1"},
		},
		Selectors: []*sessionv1.SelectorDecl{
			{
				Name: "near", MdInstance: "md-tw-1", Venue: "BinanceUM", Topics: []string{"trade"},
				Kind: &sessionv1.SelectorDecl_OptionChain{OptionChain: &sessionv1.OptionChain{NearestExpiries: 2}},
			},
		},
		Accounts: []*sessionv1.AccountDecl{
			{Name: "main", ApiId: 7, TdInstance: "td-jp-1", AlgoIds: []uint32{1, 2}},
		},
	}
}

type stored struct {
	create, mod int64
	val         string
}

func kvOf(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string) (stored, bool) {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return stored{}, false
	}
	kv := resp.Kvs[0]
	return stored{create: kv.CreateRevision, mod: kv.ModRevision, val: string(kv.Value)}, true
}

func mustProto(t *testing.T, raw string, m proto.Message) {
	t.Helper()
	if err := proto.Unmarshal([]byte(raw), m); err != nil {
		t.Fatal(err)
	}
}

func listKeys(t *testing.T, ctx context.Context, srv *etcdtest.Server, prefix string) []string {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
	}
	slices.Sort(keys)
	return keys
}

// afterRound waits until a reconcile has finished after cond first held, so a
// write that round decided has been submitted. Rounds is incremented when the
// view is published, which is before that round's puts return.
func afterRound(t *testing.T, c *Controller, cond func() error) {
	t.Helper()
	var mark int
	until(t, 20*time.Second, "a round after the condition", func() error {
		if err := cond(); err != nil {
			mark = 0
			return err
		}
		rounds := c.Stats().Rounds
		if mark == 0 {
			mark = rounds
			return errors.New("want the next round")
		}
		if rounds <= mark {
			return fmt.Errorf("rounds %d", rounds)
		}
		return nil
	})
}

func TestDeriveAcrossRegions(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 20 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	spec := sampleSpec()
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess), spec)

	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	feedKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	selKey := state.MDDemandKey("md-tw-1", deriveSess, "select/near")
	tdKey := state.TDDemandKey("td-jp-1", deriveSess, 7)
	var issued int64
	until(t, 20*time.Second, "demands are written in each provider region", func() error {
		tw, ok := kvOf(t, ctx, servers["tw"], sessKey)
		if !ok {
			return errors.New("no assignment")
		}
		d := &statev1.SessionDemand{}
		mustProto(t, tw.val, d)
		if d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_RUNNING || d.GetStart() || d.GetIssuedAtNs() == 0 ||
			len(d.GetBindings()) != 3 || d.GetBindings()[1].GetSelector() != "near" {
			return fmt.Errorf("assignment %+v", d)
		}
		issued = d.GetIssuedAtNs()
		for _, key := range []string{selKey} {
			if _, ok := kvOf(t, ctx, servers["tw"], key); !ok {
				return fmt.Errorf("missing %s", key)
			}
		}
		feed, ok := kvOf(t, ctx, servers["jp"], feedKey)
		if !ok {
			return errors.New("jp feed missing")
		}
		entry := &statev1.DemandEntry{}
		mustProto(t, feed.val, entry)
		if entry.GetFeed() != spec.GetFeeds()[0].GetFeed() {
			return fmt.Errorf("feed %+v", entry)
		}
		td, ok := kvOf(t, ctx, servers["jp"], tdKey)
		if !ok {
			return errors.New("jp account missing")
		}
		mustProto(t, td.val, entry)
		if entry.GetApiId() != 7 || len(entry.GetAlgoIds()) != 2 {
			return fmt.Errorf("account %+v", entry)
		}
		return nil
	})
	specKV, ok := kvOf(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess))
	if !ok {
		t.Fatal("spec missing")
	}
	assign, _ := kvOf(t, ctx, servers["tw"], sessKey)
	got := &statev1.SessionDemand{}
	mustProto(t, assign.val, got)
	if got.GetSpecRevision() != specKV.mod {
		t.Fatalf("spec_revision %d, spec mod %d", got.GetSpecRevision(), specKV.mod)
	}
	before := map[string]stored{}
	for _, item := range []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], sessKey}, {servers["tw"], selKey},
		{servers["jp"], feedKey}, {servers["jp"], tdKey},
	} {
		st, ok := kvOf(t, ctx, item.srv, item.key)
		if !ok {
			t.Fatalf("missing %s", item.key)
		}
		before[item.key] = st
	}
	afterRound(t, c, func() error {
		if c.View().Sessions[controller.SessionRef("sts-tw-1", deriveSess)].Demand.GetIssuedAtNs() != issued {
			return errors.New("view has not caught the assignment")
		}
		return nil
	})
	for _, item := range []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], sessKey}, {servers["tw"], selKey},
		{servers["jp"], feedKey}, {servers["jp"], tdKey},
	} {
		st, ok := kvOf(t, ctx, item.srv, item.key)
		if !ok || st != before[item.key] {
			t.Fatalf("%s changed from %+v to %+v present %v", item.key, before[item.key], st, ok)
		}
	}
	if _, ok := kvOf(t, ctx, servers["jp"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); !ok {
		t.Fatal("jp was not fenced on its first demand write")
	}
}

// Demands have no lease. After the leader is gone they are still there, and
// the next leader's relist does not delete and recreate them (N6).
func TestDeriveSurvivesRestart(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, "first", testLog(t))
	cfg.Planes = planes
	cfg.Resync = 20 * time.Millisecond
	first := startAsync(t, ctx, cfg)
	c := first.ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess), sampleSpec())

	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	feedKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	selKey := state.MDDemandKey("md-tw-1", deriveSess, "select/near")
	tdKey := state.TDDemandKey("td-jp-1", deriveSess, 7)
	keys := []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], sessKey}, {servers["tw"], selKey},
		{servers["jp"], feedKey}, {servers["jp"], tdKey},
	}
	until(t, 20*time.Second, "the first leader has written every demand", func() error {
		for _, item := range keys {
			if _, ok := kvOf(t, ctx, item.srv, item.key); !ok {
				return fmt.Errorf("missing %s", item.key)
			}
		}
		return nil
	})
	kept := map[string]stored{}
	for _, item := range keys {
		st, ok := kvOf(t, ctx, item.srv, item.key)
		if !ok {
			t.Fatalf("missing %s", item.key)
		}
		kept[item.key] = st
	}
	var issued int64
	assign := &statev1.SessionDemand{}
	mustProto(t, kept[sessKey].val, assign)
	issued = assign.GetIssuedAtNs()

	first.cancel()
	if err := c.Wait(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	until(t, 10*time.Second, "the record is revoked and the demands remain", func() error {
		if _, _, ok := recordOf(t, ctx, servers["tw"]); ok {
			return errors.New("record still there")
		}
		for _, item := range keys {
			st, ok := kvOf(t, ctx, item.srv, item.key)
			if !ok || st != kept[item.key] {
				return fmt.Errorf("%s %+v present %v", item.key, st, ok)
			}
		}
		return nil
	})

	cfg.Term.ID = "second"
	next := startAsync(t, ctx, cfg)
	c2 := next.ready(30 * time.Second)
	afterRound(t, c2, func() error {
		d := c2.View().Sessions[controller.SessionRef("sts-tw-1", deriveSess)].Demand
		if d.GetIssuedAtNs() != issued {
			return fmt.Errorf("issued %d", d.GetIssuedAtNs())
		}
		return nil
	})
	for _, item := range keys {
		st, ok := kvOf(t, ctx, item.srv, item.key)
		if !ok || st.create != kept[item.key].create || st.mod != kept[item.key].mod {
			t.Fatalf("new leader rewrote %s: was %+v now %+v present %v", item.key, kept[item.key], st, ok)
		}
	}

	putProto(t, ctx, servers["jp"], feedKey, &statev1.DemandEntry{
		Target: &statev1.DemandEntry_Feed{Feed: "other.X"}, SpecRevision: 1,
	})
	until(t, 20*time.Second, "the differing feed is restored in place", func() error {
		st, ok := kvOf(t, ctx, servers["jp"], feedKey)
		if !ok {
			return errors.New("feed deleted")
		}
		entry := &statev1.DemandEntry{}
		mustProto(t, st.val, entry)
		if entry.GetFeed() != "trade.BinanceUM_Perp_BTCUSDT" {
			return fmt.Errorf("feed %q", entry.GetFeed())
		}
		if st.create != kept[feedKey].create {
			return fmt.Errorf("create %d, was %d", st.create, kept[feedKey].create)
		}
		if st.mod == kept[feedKey].mod {
			return errors.New("mod revision has not moved")
		}
		return nil
	})
	for _, item := range keys {
		if item.key == feedKey {
			continue
		}
		st, ok := kvOf(t, ctx, item.srv, item.key)
		if !ok || st != kept[item.key] {
			t.Fatalf("untouched key %s changed: %+v present %v", item.key, st, ok)
		}
	}
}

func TestDeriveInvalidSpec(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 20 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)

	missing := proto.Clone(sampleSpec()).(*sessionv1.SessionSpec)
	missing.Feeds[0].MdInstance = "md-nope"
	wrong := proto.Clone(sampleSpec()).(*sessionv1.SessionSpec)
	wrong.Feeds[0].MdInstance = "td-jp-1"
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", "abc123"), missing)
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", "def456"), wrong)

	for _, id := range []string{"abc123", "def456"} {
		key := state.SessionDemandKey("sts-tw-1", id)
		until(t, 20*time.Second, id+" ends invalid_spec", func() error {
			st, ok := kvOf(t, ctx, servers["tw"], key)
			if !ok {
				return errors.New("no assignment")
			}
			d := &statev1.SessionDemand{}
			mustProto(t, st.val, d)
			if d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED || d.GetEndReason() != "invalid_spec" ||
				d.GetStart() || len(d.GetBindings()) != 0 || d.GetIssuedAtNs() == 0 {
				return fmt.Errorf("%+v", d)
			}
			return nil
		})
	}
	afterRound(t, c, func() error {
		if c.View().Sessions[controller.SessionRef("sts-tw-1", "abc123")].Demand.GetEndReason() != "invalid_spec" {
			return errors.New("view has not caught invalid_spec")
		}
		return nil
	})
	for _, item := range []struct {
		srv    *etcdtest.Server
		prefix string
	}{
		{servers["tw"], state.DemandPrefix(state.PlaneMD, "md-tw-1")},
		{servers["jp"], state.DemandPrefix(state.PlaneMD, "md-jp-1")},
		{servers["jp"], state.DemandPrefix(state.PlaneTD, "td-jp-1")},
	} {
		if keys := listKeys(t, ctx, item.srv, item.prefix); len(keys) != 0 {
			t.Fatalf("demand entries written: %v", keys)
		}
	}
}

func TestDeriveRegionDown(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	proxy.Cut()
	topo, planes := mixedTopo(servers)
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 20 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)

	spec := &sessionv1.SessionSpec{
		Name:         "s",
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Feeds: []*sessionv1.FeedDecl{
			{Feed: "trade.BinanceUM_Perp_BTCUSDT", MdInstance: "md-tw-1"},
			{Feed: "book.BinanceUM_Perp_ETHUSDT", MdInstance: "md-jp-1"},
		},
	}
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", deriveSess), spec)
	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	twKey := state.MDDemandKey("md-tw-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	jpKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/book.BinanceUM_Perp_ETHUSDT")
	afterRound(t, c, func() error {
		if _, ok := kvOf(t, ctx, servers["tw"], sessKey); !ok {
			return errors.New("no assignment")
		}
		if _, ok := kvOf(t, ctx, servers["tw"], twKey); !ok {
			return errors.New("tw feed missing")
		}
		return nil
	})
	if _, ok := kvOf(t, ctx, servers["jp"], jpKey); ok {
		t.Fatal("unreachable region got a demand")
	}
	if _, ok := kvOf(t, ctx, servers["jp"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); ok {
		t.Fatal("unreachable region was fenced")
	}
	if _, _, ok := recordOf(t, ctx, servers["tw"]); !ok {
		t.Fatal("term ended")
	}
}

func TestDeriveSpecChangeLogsOnce(t *testing.T) {
	ctx := t.Context()
	buf := &safeBuf{}
	log := zerolog.New(buf)
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), log)
	cfg.Planes = planes
	cfg.Resync = 20 * time.Millisecond
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)

	spec := sampleSpec()
	specKey := state.SessionSpecKey("sts-tw-1", deriveSess)
	putProto(t, ctx, servers["tw"], specKey, spec)
	feedKey := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_BTCUSDT")
	sessKey := state.SessionDemandKey("sts-tw-1", deriveSess)
	var derived int64
	until(t, 20*time.Second, "the assignment is derived", func() error {
		st, ok := kvOf(t, ctx, servers["tw"], sessKey)
		if !ok {
			return errors.New("no assignment")
		}
		d := &statev1.SessionDemand{}
		mustProto(t, st.val, d)
		if d.GetSpecRevision() == 0 {
			return errors.New("spec revision not set")
		}
		derived = d.GetSpecRevision()
		return nil
	})
	before, ok := kvOf(t, ctx, servers["jp"], feedKey)
	if !ok {
		t.Fatal("feed missing")
	}

	spec.Feeds = append(spec.Feeds, &sessionv1.FeedDecl{Feed: "book.BinanceUM_Perp_ETHUSDT", MdInstance: "md-tw-1"})
	putProto(t, ctx, servers["tw"], specKey, spec)
	const msg = "spec revision changed; demands left as stored"
	until(t, 20*time.Second, "the revision change is logged", func() error {
		if bytes.Count([]byte(buf.String()), []byte(msg)) < 1 {
			return errors.New("not logged")
		}
		return nil
	})
	added := state.MDDemandKey("md-tw-1", deriveSess, "feed/book.BinanceUM_Perp_ETHUSDT")
	afterRound(t, c, func() error { return nil })
	if n := bytes.Count([]byte(buf.String()), []byte(msg)); n != 1 {
		t.Fatalf("log count %d\n%s", n, buf.String())
	}
	if _, ok := kvOf(t, ctx, servers["tw"], added); ok {
		t.Fatal("a changed spec added a demand")
	}
	if st, ok := kvOf(t, ctx, servers["jp"], feedKey); !ok || st != before {
		t.Fatalf("existing feed changed %+v present %v", st, ok)
	}
	assign, _ := kvOf(t, ctx, servers["tw"], sessKey)
	d := &statev1.SessionDemand{}
	mustProto(t, assign.val, d)
	if d.GetSpecRevision() != derived {
		t.Fatalf("spec_revision moved from %d to %d", derived, d.GetSpecRevision())
	}

	spec.Feeds = append(spec.Feeds, &sessionv1.FeedDecl{Feed: "trade.BinanceUM_Perp_ETHUSDT", MdInstance: "md-jp-1"})
	putProto(t, ctx, servers["tw"], specKey, spec)
	until(t, 20*time.Second, "the next revision is logged once", func() error {
		if bytes.Count([]byte(buf.String()), []byte(msg)) < 2 {
			return errors.New("second change not logged")
		}
		return nil
	})
	afterRound(t, c, func() error { return nil })
	if n := bytes.Count([]byte(buf.String()), []byte(msg)); n != 2 {
		t.Fatalf("log count %d\n%s", n, buf.String())
	}
	second := state.MDDemandKey("md-jp-1", deriveSess, "feed/trade.BinanceUM_Perp_ETHUSDT")
	if _, ok := kvOf(t, ctx, servers["jp"], second); ok {
		t.Fatal("the second spec change added a demand")
	}
	if st, ok := kvOf(t, ctx, servers["jp"], feedKey); !ok || st != before {
		t.Fatalf("existing feed changed after the second edit %+v present %v", st, ok)
	}
}
