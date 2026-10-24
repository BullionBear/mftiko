package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// scenarioSpec runs one feed on md, with deps_timeout_s timeout.
func scenarioSpec(md, feed string, timeout uint32) *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Name:         "s",
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		DepsTimeoutS: timeout,
		Feeds:        []*sessionv1.FeedDecl{{Feed: feed, MdInstance: md}},
	}
}

func assignment(t *testing.T, ctx context.Context, srv *etcdtest.Server, id string) (*statev1.SessionDemand, stored, bool) {
	t.Helper()
	st, ok := kvOf(t, ctx, srv, state.SessionDemandKey("sts-tw-1", id))
	if !ok {
		return nil, stored{}, false
	}
	d := &statev1.SessionDemand{}
	mustProto(t, st.val, d)
	return d, st, true
}

// history is every event on srv after rev, up to the current revision. Every
// revision in these tests is a key under /, so the watch reaches it.
func history(t *testing.T, ctx context.Context, srv *etcdtest.Server, rev int64) []*clientv3.Event {
	t.Helper()
	cli := client(t, srv)
	cur, err := cli.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatal(err)
	}
	end := cur.Header.Revision
	if end <= rev {
		return nil
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var out []*clientv3.Event
	for resp := range cli.Watch(wctx, "/", clientv3.WithPrefix(), clientv3.WithRev(rev+1)) {
		if err := resp.Err(); err != nil {
			t.Fatalf("watch: %v", err)
		}
		for _, ev := range resp.Events {
			if ev.Kv.ModRevision > end {
				return out
			}
			out = append(out, ev)
			if ev.Kv.ModRevision == end {
				return out
			}
		}
	}
	t.Fatalf("history ended before revision %d", end)
	return nil
}

// A region cut after it synced, and before the view marks it stale, still
// gets a write. That write cannot finish while the cut lasts. Sessions that
// need only the other region are derived meanwhile (E13-D006, W9), and on
// restore the cut region gets its demand (N6).
func TestScenarioCutBeforeStale(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	topo, planes := mixedTopo(servers)
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	applyPlanes(t, ctx, topo, planes, servers)
	cfg := config(topo, t.Name(), testLog(t))
	cfg.Planes = planes
	cfg.Resync = 50 * time.Millisecond
	cfg.StaleAfter = 3 * time.Second
	cfg.Mirror = probeMirror()
	c := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, c, "tw")
	waitFresh(t, c, "jp")

	feed := func(md, f string) *sessionv1.SessionSpec { return scenarioSpec(md, f, 0) }
	const cross, local = "cross1", "local1"
	jpKey := state.MDDemandKey("md-jp-1", cross, "feed/trade.X")
	twKey := state.MDDemandKey("md-tw-1", local, "feed/trade.Y")

	proxy.Cut()
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", cross), feed("md-jp-1", "trade.X"))
	until(t, 20*time.Second, "jp is stale", func() error {
		if r := c.View().Regions["jp"]; !r.Stale {
			return fmt.Errorf("jp %+v", r)
		}
		return nil
	})

	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", local), feed("md-tw-1", "trade.Y"))
	until(t, 20*time.Second, "the tw-only session is derived while jp is cut", func() error {
		if !present(t, ctx, servers["tw"], twKey) {
			return errors.New("tw demand missing")
		}
		if !present(t, ctx, servers["tw"], state.SessionDemandKey("sts-tw-1", local)) {
			return errors.New("tw assignment missing")
		}
		return nil
	})

	proxy.Restore()
	waitFresh(t, c, "jp")
	until(t, 20*time.Second, "jp gets its demand after the restore", func() error {
		if !present(t, ctx, servers["jp"], jpKey) {
			return errors.New("jp demand missing")
		}
		return nil
	})
}

// The leader stops without revoking, as a crash or a cut from every cluster
// would. While it is down, longer than the session TTL, the stale threshold,
// the resync interval and a deps timeout, the world moves on: a spec is added,
// one is ended, a provider's AgentRecord goes and its ack stays READY, and a
// pending session runs past its deps timeout. The next leader relists and
// changes only what differs (N6, W7, E13-D002). No demand or assignment is
// deleted, and a started session's keys are not written at all.
func TestScenarioLeaderDown(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	twProxy, jpProxy := etcdtest.NewProxy(t, servers["tw"]), etcdtest.NewProxy(t, servers["jp"])
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cut := topo
	cut.Regions = map[string]state.Region{
		"tw": {Endpoints: twProxy.Endpoints()},
		"jp": {Endpoints: jpProxy.Endpoints()},
	}
	const ttl = 2 * time.Second
	cfg := config(cut, "first", testLog(t))
	cfg.Planes = planes
	cfg.Term.SessionTTL = ttl
	cfg.Resync = 50 * time.Millisecond
	cfg.StaleAfter = time.Second
	cfg.Mirror = probeMirror()
	first := startAsync(t, ctx, cfg).ready(30 * time.Second)
	waitFresh(t, first, "tw")
	waitFresh(t, first, "jp")

	const kept, ended, late, fresh = "kept01", "ended1", "late01", "fresh1"
	keptFeed := state.MDDemandKey("md-jp-1", kept, "feed/trade.K")
	keptAck := state.MDAckKey("md-jp-1", kept, "feed/trade.K")
	endedAck := state.MDAckKey("md-tw-1", ended, "feed/trade.E")
	mdAgent := state.AgentKey(state.PlaneMD, "md-jp-1")
	putProto(t, ctx, servers["jp"], mdAgent, &statev1.AgentRecord{Plane: "md", Instance: "md-jp-1", Epoch: 1})
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", kept), scenarioSpec("md-jp-1", "trade.K", 3600))
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", ended), scenarioSpec("md-tw-1", "trade.E", 3600))
	until(t, 20*time.Second, "both demands are stored", func() error {
		if !present(t, ctx, servers["jp"], keptFeed) ||
			!present(t, ctx, servers["tw"], state.MDDemandKey("md-tw-1", ended, "feed/trade.E")) {
			return errors.New("demand missing")
		}
		return nil
	})
	ready := &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY, Subject: "md.v1.f.x"}
	putProto(t, ctx, servers["jp"], keptAck, ready)
	putProto(t, ctx, servers["tw"], endedAck, ready)
	until(t, 20*time.Second, "both sessions are started", func() error {
		for _, id := range []string{kept, ended} {
			if d, _, ok := assignment(t, ctx, servers["tw"], id); !ok || !d.GetStart() {
				return fmt.Errorf("%s %+v", id, d)
			}
		}
		return nil
	})
	const timeout = 3
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", late), scenarioSpec("md-tw-1", "trade.L", timeout))
	var lateIssued int64
	until(t, 20*time.Second, "the late session is assigned", func() error {
		d, _, ok := assignment(t, ctx, servers["tw"], late)
		if !ok || d.GetIssuedAtNs() == 0 {
			return errors.New("no assignment")
		}
		lateIssued = d.GetIssuedAtNs()
		return nil
	})
	settled(t, first)

	keptKeys := []struct {
		srv *etcdtest.Server
		key string
	}{
		{servers["tw"], state.SessionDemandKey("sts-tw-1", kept)}, {servers["jp"], keptFeed},
	}
	before := map[string]stored{}
	for _, item := range keptKeys {
		st, ok := kvOf(t, ctx, item.srv, item.key)
		if !ok {
			t.Fatalf("missing %s", item.key)
		}
		before[item.key] = st
	}
	revs := map[string]int64{}
	for region, srv := range servers {
		resp, err := client(t, srv).Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			t.Fatal(err)
		}
		revs[region] = resp.Header.Revision
	}

	down := time.Now()
	twProxy.Cut()
	jpProxy.Cut()
	if err := first.Wait(); !errors.Is(err, state.ErrSessionLost) {
		t.Fatalf("first exit: %v", err)
	}

	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", fresh), scenarioSpec("md-jp-1", "trade.F", 3600))
	endedSpec := scenarioSpec("md-tw-1", "trade.E", 3600)
	endedSpec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", ended), endedSpec)
	if _, err := client(t, servers["jp"]).Delete(ctx, mdAgent); err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "the first leader's record expires", func() error {
		if _, _, ok := recordOf(t, ctx, servers["tw"]); ok {
			return errors.New("record still there")
		}
		return nil
	})
	// Down past every threshold: TTL, StaleAfter, Resync and the late
	// session's deps timeout.
	if wait := time.Until(time.Unix(0, lateIssued).Add(timeout*time.Second + time.Second)); wait > 0 {
		time.Sleep(wait)
	}
	for _, item := range keptKeys {
		if st, ok := kvOf(t, ctx, item.srv, item.key); !ok || st != before[item.key] {
			t.Fatalf("%s changed while no leader ran: %+v present %v", item.key, st, ok)
		}
	}

	cfg = config(topo, "second", testLog(t))
	cfg.Planes = planes
	cfg.Term.SessionTTL = ttl
	cfg.Resync = 50 * time.Millisecond
	cfg.StaleAfter = time.Second
	cfg.Mirror = probeMirror()
	second := startAsync(t, ctx, cfg).ready(30 * time.Second)
	t.Logf("down %s", time.Since(down).Round(time.Millisecond))
	if rec, _, ok := recordOf(t, ctx, servers["tw"]); !ok || rec.GetEpoch() != 2 {
		t.Fatalf("second record %+v present %v", rec, ok)
	}
	until(t, 20*time.Second, "the new leader aligns what changed", func() error {
		d, _, ok := assignment(t, ctx, servers["tw"], ended)
		if !ok || d.GetDesired() != sessionv1.DesiredState_DESIRED_STATE_ENDED || d.GetEndReason() != "ended_by_spec" {
			return fmt.Errorf("ended %+v", d)
		}
		d, _, ok = assignment(t, ctx, servers["tw"], late)
		if !ok || d.GetEndReason() != "deps_timeout" || d.GetIssuedAtNs() != lateIssued {
			return fmt.Errorf("late %+v", d)
		}
		if _, _, ok := assignment(t, ctx, servers["tw"], fresh); !ok {
			return errors.New("fresh not assigned")
		}
		if !present(t, ctx, servers["jp"], state.MDDemandKey("md-jp-1", fresh, "feed/trade.F")) {
			return errors.New("fresh demand missing")
		}
		return nil
	})
	settled(t, second)

	d, _, _ := assignment(t, ctx, servers["tw"], kept)
	if !d.GetStart() || d.GetEndReason() != "" {
		t.Fatalf("kept session changed: %+v", d)
	}
	if !second.View().Acks[keptAck].LastKnown {
		t.Fatal("the ack of a provider without an AgentRecord is not last known")
	}
	for _, item := range keptKeys {
		if st, ok := kvOf(t, ctx, item.srv, item.key); !ok || st != before[item.key] {
			t.Fatalf("new leader rewrote %s: was %+v now %+v present %v", item.key, before[item.key], st, ok)
		}
	}
	for region, srv := range servers {
		for _, ev := range history(t, ctx, srv, revs[region]) {
			key := string(ev.Kv.Key)
			if ev.Type == mvccpb.DELETE && strings.Contains(key, "/demand/") {
				t.Errorf("%s: %s deleted across the restart", region, key)
			}
		}
	}
}

// supervise runs cfg as systemd would with Restart=on-failure (E13-D015): a term
// that ends with an error is started again after a short pause. starts counts
// elections. It returns when ctx is canceled.
func supervise(t *testing.T, ctx context.Context, cfg Config, starts *atomic.Int32) {
	t.Helper()
	for ctx.Err() == nil {
		c, err := Start(ctx, cfg)
		if err == nil {
			starts.Add(1)
			err = c.Wait()
		}
		if err == nil || ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Two controllers disagree on the home region, as a move of
// instances.controller would leave them while both run. Each is elected in its
// own home, so both lead. A cluster takes writes only from the one whose home
// it is: the other is superseded as soon as it writes there, exits, and is
// restarted. Nothing livelocks: with no change to make, neither writes, and a
// change spanning both regions costs each at most a restart before the
// demands are complete (E13-D038, W2).
func TestScenarioInconsistentHomes(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", "cross1"), scenarioSpec("md-jp-1", "trade.X", 3600))

	homed := func(home string) Config {
		tp := topo
		tp.Instances = map[string]string{}
		for inst, region := range topo.Instances {
			tp.Instances[inst] = region
		}
		tp.Instances[state.ControllerInstance] = home
		cfg := config(tp, home, testLog(t))
		cfg.Planes = planes
		cfg.Release = home
		cfg.Term.SessionTTL = time.Second
		cfg.Resync = 50 * time.Millisecond
		return cfg
	}
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	starts := map[string]*atomic.Int32{"tw": {}, "jp": {}}
	for _, home := range []string{"tw", "jp"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(t, runCtx, homed(home), starts[home])
		}()
	}
	t.Cleanup(func() {
		stop()
		wg.Wait()
	})

	complete := func(id, feed string) error {
		if !present(t, ctx, servers["tw"], state.SessionDemandKey("sts-tw-1", id)) {
			return fmt.Errorf("%s: no assignment in tw", id)
		}
		if !present(t, ctx, servers["jp"], state.MDDemandKey("md-jp-1", id, "feed/"+feed)) {
			return fmt.Errorf("%s: no demand in jp", id)
		}
		return nil
	}
	until(t, 30*time.Second, "the first session is complete", func() error { return complete("cross1", "trade.X") })
	settle := func() (map[string]int32, error) {
		// Quiet for a few TTLs: no one is superseded once nothing differs.
		before := map[string]int32{"tw": starts["tw"].Load(), "jp": starts["jp"].Load()}
		time.Sleep(3 * time.Second)
		for home, n := range before {
			if got := starts[home].Load(); got != n {
				return nil, fmt.Errorf("%s restarted with nothing to change: %d -> %d", home, n, got)
			}
		}
		return before, nil
	}
	var quiet map[string]int32
	until(t, 30*time.Second, "both controllers are quiet", func() error {
		var err error
		quiet, err = settle()
		return err
	})

	putProto(t, ctx, servers["tw"], state.SessionSpecKey("sts-tw-1", "cross2"), scenarioSpec("md-jp-1", "trade.Z", 3600))
	until(t, 30*time.Second, "the new session is complete", func() error { return complete("cross2", "trade.Z") })
	until(t, 30*time.Second, "both controllers are quiet again", func() error {
		_, err := settle()
		return err
	})
	for home, n := range quiet {
		t.Logf("%s elected %d more times for one change", home, starts[home].Load()-n)
		if got := starts[home].Load() - n; got > 2 {
			t.Errorf("%s was elected %d more times for one change", home, got)
		}
	}
	for _, region := range []string{"tw", "jp"} {
		rec, _, ok := recordOf(t, ctx, servers[region])
		if !ok || rec.GetRelease() != region {
			t.Errorf("%s is led by %+v present %v, want the controller whose home it is", region, rec, ok)
		}
		writer, _ := valueOf(t, ctx, servers[region], state.WriterKey(state.PlaneCtl, state.ControllerInstance))
		if want := epochString(rec.GetEpoch()); writer != want {
			t.Errorf("%s writer %s, want the home leader's %s", region, writer, want)
		}
	}
	t.Logf("elections tw=%d jp=%d", starts["tw"].Load(), starts["jp"].Load())
}

// The leader loses its home region but still reaches jp, and a follower takes
// over. The term ends on the keepalive deadline, before the server deletes the
// election key (E01-D013, E01-D014, E01-D016). Until #277 the old term outlived
// that delete and could still write jp (E13-D005, E13-D039).
func TestScenarioZombieWindow(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["tw"])
	topo, planes := mixedTopo(servers)
	applyPlanes(t, ctx, topo, planes, servers)
	cut := topo
	cut.Regions = map[string]state.Region{"tw": {Endpoints: proxy.Endpoints()}, "jp": topo.Regions["jp"]}
	const ttl = 2 * time.Second
	cfg := config(cut, "old", testLog(t))
	cfg.Planes = planes
	cfg.Term.SessionTTL = ttl
	old := startAsync(t, ctx, cfg).ready(30 * time.Second)

	tw := client(t, servers["tw"])
	prefix := state.ElectionPrefix(state.PlaneCtl, state.ControllerInstance) + "/"
	got, err := tw.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 1 {
		t.Fatalf("%d election keys, want the leader's", len(got.Kvs))
	}
	leaderKey := string(got.Kvs[0].Key)
	// Watch returns only after the server has created the watcher. The Created
	// response stays inside the client unless WithCreatedNotify is set, so it
	// never arrives on wch; a closed channel is how a failed watch shows up.
	wch := tw.Watch(ctx, leaderKey)
	select {
	case resp, ok := <-wch:
		if !ok {
			t.Fatal("watch on the election key closed before it was established")
		}
		if err := resp.Err(); err != nil {
			t.Fatalf("watch election key: %v", err)
		}
		t.Fatalf("watch event before the cut: %v", resp.Events)
	default:
	}

	cfg = config(topo, "new", testLog(t))
	cfg.Planes = planes
	cfg.Term.SessionTTL = ttl
	next := startAsync(t, ctx, cfg)
	if !next.waiting() {
		t.Fatal("follower elected while the leader runs")
	}
	proxy.Cut()
	var ended time.Time
	select {
	case <-old.term.Done():
		ended = time.Now()
	case <-time.After(30 * time.Second):
		t.Fatal("old term did not end")
	}
	if err := old.term.Err(); !errors.Is(err, state.ErrSessionLost) || !strings.Contains(err.Error(), "keepalive deadline") {
		t.Fatalf("old term: %v, want ErrSessionLost from the keepalive deadline", err)
	}
	still, err := tw.Get(ctx, leaderKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(still.Kvs) == 0 {
		t.Fatal("election key was already deleted when the term ended")
	}
	deleted := waitElectionDelete(t, wch, leaderKey)
	t.Logf("election key deleted %s after the term ended", deleted.Sub(ended).Round(time.Millisecond))
	if !deleted.After(ended) {
		t.Fatalf("election key deleted at %s, term ended at %s", deleted, ended)
	}
	next.ready(30 * time.Second)
}

// waitElectionDelete blocks until the server deletes leaderKey, or fails the test.
func waitElectionDelete(t *testing.T, wch clientv3.WatchChan, leaderKey string) time.Time {
	t.Helper()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case resp, ok := <-wch:
			if !ok {
				t.Fatal("watch closed before the election key was deleted")
			}
			if err := resp.Err(); err != nil {
				t.Fatalf("watch: %v", err)
			}
			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE && string(ev.Kv.Key) == leaderKey {
					return time.Now()
				}
			}
		case <-timer.C:
			t.Fatal("server did not delete the election key after the term ended")
		}
	}
}
