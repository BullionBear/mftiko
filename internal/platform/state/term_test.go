package state

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// campaign wins (p, inst) or fails the test, and closes the term when the test ends.
func campaign(t *testing.T, ctx context.Context, cl *Clusters, p Plane, inst string, opt TermOptions) *Term {
	t.Helper()
	if opt.ID == "" {
		opt.ID = t.Name()
	}
	term, err := Campaign(ctx, cl, p, inst, opt)
	if err != nil {
		t.Fatalf("Campaign(%s/%s): %v", p, inst, err)
	}
	t.Cleanup(func() { term.Close(context.Background()) })
	return term
}

// waitDone waits for the term to end, or fails the test.
func waitDone(t *testing.T, term *Term, within time.Duration) {
	t.Helper()
	timer := time.NewTimer(within)
	defer timer.Stop()
	select {
	case <-term.Done():
	case <-timer.C:
		t.Fatalf("term still running after %s", within)
	}
}

func running(term *Term) bool {
	select {
	case <-term.Done():
		return false
	default:
		return term.Err() == nil
	}
}

func singleRegion(t *testing.T, instances ...string) (*etcdtest.Server, Topology) {
	t.Helper()
	s := etcdtest.Start(t)
	m := map[string]string{}
	for _, inst := range instances {
		m[inst] = "tw"
	}
	return s, topologyOf(map[string]*etcdtest.Server{"tw": s}, m)
}

func TestCampaignArgs(t *testing.T) {
	ctx := testCtx(t)
	cl := newClusters(t, twoRegions(), []string{"tw"}, nil) // never dialled: every case fails before I/O
	for name, c := range map[string]struct {
		p    Plane
		inst string
		opt  TermOptions
		want error
	}{
		"unknown plane":          {"xx", "md-tw-1", TermOptions{ID: "a"}, ErrUnknownPlane},
		"controller, other name": {PlaneCtl, "ctl-tw-1", TermOptions{ID: "a"}, ErrNotController},
		"no ID":                  {PlaneMD, "md-tw-1", TermOptions{}, ErrBadOptions},
		"negative TTL":           {PlaneMD, "md-tw-1", TermOptions{ID: "a", SessionTTL: -time.Second}, ErrBadOptions},
		"unknown instance":       {PlaneMD, "md-kr-1", TermOptions{ID: "a"}, ErrUnknownInstance},
		"home not reachable":     {PlaneMD, "md-jp-1", TermOptions{ID: "a"}, ErrRegionNotAllowed},
	} {
		if _, err := Campaign(ctx, cl, c.p, c.inst, c.opt); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
}

func TestCampaignOneLeader(t *testing.T) {
	ctx := testCtx(t)
	_, topo := singleRegion(t, "md-tw-1")
	type result struct {
		term *Term
		err  error
	}
	results := make(chan result, 2)
	for _, id := range []string{"a", "b"} {
		cl := newClusters(t, topo, nil, nil) // one process each
		go func() {
			term, err := Campaign(ctx, cl, PlaneMD, "md-tw-1", TermOptions{ID: id})
			results <- result{term, err}
		}()
	}
	next := func() result {
		t.Helper()
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("Campaign: %v", r.err)
			}
			t.Cleanup(func() { r.term.Close(context.Background()) })
			return r
		case <-ctx.Done():
			t.Fatal("no candidate elected")
			return result{}
		}
	}

	first := next()
	select {
	case r := <-results:
		t.Fatalf("both candidates elected (epochs %d and %v)", first.term.Epoch(), r)
	case <-time.After(time.Second):
	}
	if !running(first.term) {
		t.Fatalf("leader's term ended: %v", first.term.Err())
	}
	if err := first.term.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second := next()
	if second.term.Epoch() != first.term.Epoch()+1 {
		t.Fatalf("epochs %d then %d", first.term.Epoch(), second.term.Epoch())
	}
}

func TestEpochPersists(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	cl := newClusters(t, topo, nil, nil)
	for want := int64(1); want <= 3; want++ {
		term := campaign(t, ctx, cl, PlaneTD, "td-tw-1", TermOptions{})
		if term.Epoch() != want || term.Plane() != PlaneTD || term.Instance() != "td-tw-1" {
			t.Fatalf("term %s/%s epoch %d, want td/td-tw-1 epoch %d", term.Plane(), term.Instance(), term.Epoch(), want)
		}
		for _, key := range []string{EpochKey(PlaneTD, "td-tw-1"), WriterKey(PlaneTD, "td-tw-1")} {
			kvs := mustGet(t, ctx, raw, key).Kvs
			if len(kvs) != 1 || string(kvs[0].Value) != fmt.Sprintf("%020d", want) || len(kvs[0].Value) != 20 {
				t.Fatalf("%s = %v, want 20-digit %d", key, kvs, want)
			}
			if kvs[0].Lease != 0 {
				t.Fatalf("%s is bound to lease %x", key, kvs[0].Lease)
			}
		}
		if err := term.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err := term.Close(ctx); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if !errors.Is(term.Err(), ErrClosed) {
			t.Fatalf("Err after Close = %v", term.Err())
		}
	}
}

func TestCampaignCancelled(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "leader"})

	cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := Campaign(cctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "late"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Campaign past its deadline: %v", err)
	}
	// The losing candidate withdrew: only the leader's key is left.
	resp, err := raw.Get(ctx, ElectionPrefix(PlaneMD, "md-tw-1")+"/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "leader" {
		t.Fatalf("election keys after a cancelled campaign: %v", resp.Kvs)
	}
}

// A lease that is gone before the session notices (default TTL: the next keepalive is seconds away) makes
// Revoke fail with "lease not found"; Close does not report it.
func TestCloseLeaseAlreadyGone(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	if _, err := raw.Revoke(ctx, term.session.Lease()); err != nil {
		t.Fatal(err)
	}
	if err := term.Close(ctx); err != nil {
		t.Fatalf("Close with the lease already revoked: %v", err)
	}
	if !errors.Is(term.Err(), ErrClosed) && !errors.Is(term.Err(), ErrSessionLost) {
		t.Fatalf("Err = %v", term.Err())
	}
}

// The election key goes between winning and bumping the epoch: the bump's leader compare fails, Campaign
// reports the session lost and takes no epoch.
func TestBumpEpochLeaderLost(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	hooks := &termHooks{beforeEpochTxn: func(term *Term, attempt int) {
		if _, err := raw.Delete(ctx, term.election.Key()); err != nil {
			t.Error(err)
		}
	}}
	if _, err := Campaign(ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "a", hooks: hooks}); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("Campaign after losing the election key: %v, want ErrSessionLost", err)
	}
	for _, key := range []string{EpochKey(PlaneMD, "md-tw-1"), WriterKey(PlaneMD, "md-tw-1")} {
		if kvs := mustGet(t, ctx, raw, key).Kvs; len(kvs) != 0 {
			t.Fatalf("%s written: %v", key, kvs)
		}
	}
}

// The epoch moves between the read and the bump: the bump retries from the new value.
func TestBumpEpochRetry(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	var attempts []int
	hooks := &termHooks{beforeEpochTxn: func(term *Term, attempt int) {
		attempts = append(attempts, attempt)
		if attempt == 0 {
			if _, err := raw.Put(ctx, EpochKey(PlaneMD, "md-tw-1"), formatEpoch(41)); err != nil {
				t.Error(err)
			}
		}
	}}
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{hooks: hooks})
	if term.Epoch() != 42 || !slices.Equal(attempts, []int{0, 1}) {
		t.Fatalf("epoch %d after attempts %v, want 42 after [0 1]", term.Epoch(), attempts)
	}
	if kvs := mustGet(t, ctx, raw, EpochKey(PlaneMD, "md-tw-1")).Kvs; string(kvs[0].Value) != formatEpoch(42) {
		t.Fatalf("stored epoch %s", kvs[0].Value)
	}
}

// The epoch keeps moving: the bump gives up after epochAttempts and the candidate does not take office.
func TestBumpEpochGivesUp(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	calls := 0
	hooks := &termHooks{beforeEpochTxn: func(term *Term, attempt int) {
		calls++
		if _, err := raw.Put(ctx, EpochKey(PlaneMD, "md-tw-1"), formatEpoch(int64(100+attempt))); err != nil {
			t.Error(err)
		}
	}}
	if _, err := Campaign(ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{ID: "a", hooks: hooks}); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Campaign with a moving epoch: %v, want ErrSuperseded", err)
	}
	if calls != epochAttempts {
		t.Fatalf("%d attempts, want %d", calls, epochAttempts)
	}
	if kvs := mustGet(t, ctx, raw, WriterKey(PlaneMD, "md-tw-1")).Kvs; len(kvs) != 0 {
		t.Fatal("writer key taken without an epoch")
	}
}

func TestSessionLost(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{SessionTTL: 2 * time.Second})
	if !running(term) {
		t.Fatalf("fresh term not running: %v", term.Err())
	}

	if _, err := raw.Revoke(ctx, term.session.Lease()); err != nil {
		t.Fatal(err)
	}
	waitDone(t, term, 15*time.Second)
	if !errors.Is(term.Err(), ErrSessionLost) {
		t.Fatalf("Err = %v, want ErrSessionLost", term.Err())
	}
	if _, err := term.Fence(ctx, "tw"); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("Fence after session lost: %v", err)
	}
	if err := term.Close(ctx); err != nil {
		t.Fatalf("Close after session lost: %v", err)
	}
	if !errors.Is(term.Err(), ErrSessionLost) {
		t.Fatalf("Close replaced the first cause: %v", term.Err())
	}
}

func twoServers(t *testing.T) (tw, jp *etcdtest.Server, topo Topology) {
	t.Helper()
	tw, jp = etcdtest.Start(t), etcdtest.Start(t)
	return tw, jp, topologyOf(map[string]*etcdtest.Server{"tw": tw, "jp": jp}, twoRegions().Instances)
}

func TestFenceTwoRegions(t *testing.T) {
	ctx := testCtx(t)
	tw, jp, topo := twoServers(t)
	raws := map[string]*clientv3.Client{"tw": tw.Client(t), "jp": jp.Client(t)}
	key := WriterKey(PlaneCtl, ControllerInstance)

	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})
	if kvs := mustGet(t, ctx, raws["jp"], key).Kvs; len(kvs) != 0 {
		t.Fatal("jp's writer key acquired before first use")
	}
	revs := map[string]int64{}
	for _, region := range []string{"tw", "jp"} {
		r, err := term.Fence(ctx, region)
		if err != nil {
			t.Fatalf("Fence(%s): %v", region, err)
		}
		kvs := mustGet(t, ctx, raws[region], key).Kvs
		if len(kvs) != 1 || kvs[0].ModRevision != r || string(kvs[0].Value) != formatEpoch(term.Epoch()) {
			t.Fatalf("%s writer %v, Fence returned R=%d for epoch %d", region, kvs, r, term.Epoch())
		}
		revs[region] = r
	}
	for _, region := range []string{"tw", "jp"} {
		if r, err := term.Fence(ctx, region); err != nil || r != revs[region] {
			t.Fatalf("second Fence(%s) = %d, %v; want cached %d", region, r, err, revs[region])
		}
		if kvs := mustGet(t, ctx, raws[region], key).Kvs; kvs[0].ModRevision != revs[region] {
			t.Fatalf("second Fence(%s) wrote again", region)
		}
	}

	// An MD agent never reaches beyond its own region (D17 rule 2).
	allowed, _ := AgentRegions(topo, PlaneMD, "md-tw-1")
	md := campaign(t, ctx, newClusters(t, topo, allowed, nil), PlaneMD, "md-tw-1", TermOptions{})
	if _, err := md.Fence(ctx, "jp"); !errors.Is(err, ErrRegionNotAllowed) {
		t.Fatalf("MD agent Fence(jp): %v", err)
	}
	if !running(md) {
		t.Fatalf("a refused region ended the term: %v", md.Err())
	}
}

func TestFenceSupersededAtAcquisition(t *testing.T) {
	ctx := testCtx(t)
	_, jp, topo := twoServers(t)
	rawJP := jp.Client(t)
	key := WriterKey(PlaneCtl, ControllerInstance)

	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})
	newer := formatEpoch(term.Epoch() + 1)
	if _, err := rawJP.Put(ctx, key, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := term.Fence(ctx, "jp"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Fence(jp) against a newer writer: %v", err)
	}
	waitDone(t, term, time.Second)
	if !errors.Is(term.Err(), ErrSuperseded) {
		t.Fatalf("Err = %v", term.Err())
	}
	if kvs := mustGet(t, ctx, rawJP, key).Kvs; string(kvs[0].Value) != newer {
		t.Fatalf("jp writer overwritten: %s", kvs[0].Value)
	}
	// The home region was fenced already; the ended term answers without asking etcd.
	if _, err := term.Fence(ctx, "tw"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Fence(tw) after the term ended: %v", err)
	}
}

// A writer key at our own epoch that this term never wrote belongs to another term with the same epoch (epochs
// were not unique): never shared, superseded.
func TestFenceEqualEpochNotOurs(t *testing.T) {
	ctx := testCtx(t)
	_, jp, topo := twoServers(t)
	rawJP := jp.Client(t)
	key := WriterKey(PlaneCtl, ControllerInstance)

	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})
	put, err := rawJP.Put(ctx, key, formatEpoch(term.Epoch()))
	if err != nil {
		t.Fatal(err)
	}
	if r, err := term.Fence(ctx, "jp"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Fence(jp) over another term's equal epoch = %d, %v; want ErrSuperseded", r, err)
	}
	waitDone(t, term, time.Second)
	if kvs := mustGet(t, ctx, rawJP, key).Kvs; kvs[0].ModRevision != put.Header.Revision {
		t.Fatal("writer key rewritten")
	}
}

// The reviewer's reproduction: two deployments that disagree on an instance's home region each elect an agent
// at epoch 1 in their own cluster. Neither may share the other's writer key.
func TestFenceInconsistentTopologies(t *testing.T) {
	ctx := testCtx(t)
	tw, jp, _ := twoServers(t)
	servers := map[string]*etcdtest.Server{"tw": tw, "jp": jp}
	inTW := topologyOf(servers, map[string]string{ControllerInstance: "tw"})
	inJP := topologyOf(servers, map[string]string{ControllerInstance: "jp"})

	a := campaign(t, ctx, newClusters(t, inTW, nil, nil), PlaneCtl, ControllerInstance, TermOptions{ID: "a"})
	b := campaign(t, ctx, newClusters(t, inJP, nil, nil), PlaneCtl, ControllerInstance, TermOptions{ID: "b"})
	if a.Epoch() != 1 || b.Epoch() != 1 {
		t.Fatalf("epochs %d and %d, want 1 and 1", a.Epoch(), b.Epoch())
	}
	if _, err := a.Fence(ctx, "jp"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("a taking b's equal-epoch writer in jp: %v", err)
	}
	if _, err := b.Fence(ctx, "tw"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("b taking a's equal-epoch writer in tw: %v", err)
	}
}

// A put that applied but whose reply was lost leaves our own epoch in the key; the next Fence adopts it.
func TestFenceLostReply(t *testing.T) {
	ctx := testCtx(t)
	_, jp, topo := twoServers(t)
	rawJP := jp.Client(t)
	key := WriterKey(PlaneCtl, ControllerInstance)

	errLost := errors.New("reply lost")
	dropped := 0
	hooks := &termHooks{writerTxn: func(region string, resp *clientv3.TxnResponse, err error) (*clientv3.TxnResponse, error) {
		if region == "jp" && dropped == 0 && err == nil && resp.Succeeded {
			dropped++
			return nil, errLost
		}
		return resp, err
	}}
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{hooks: hooks})

	if _, err := term.Fence(ctx, "jp"); !errors.Is(err, errLost) {
		t.Fatalf("first Fence(jp): %v, want the lost reply", err)
	}
	if !running(term) {
		t.Fatalf("a lost reply ended the term: %v", term.Err())
	}
	kvs := mustGet(t, ctx, rawJP, key).Kvs
	if len(kvs) != 1 || string(kvs[0].Value) != formatEpoch(term.Epoch()) {
		t.Fatalf("jp writer after the lost reply: %v", kvs)
	}
	r, err := term.Fence(ctx, "jp")
	if err != nil || r != kvs[0].ModRevision {
		t.Fatalf("second Fence(jp) = %d, %v; want our own put at %d", r, err, kvs[0].ModRevision)
	}
	if kvs := mustGet(t, ctx, rawJP, key).Kvs; kvs[0].Version != 1 {
		t.Fatalf("writer key put %d times, want once", kvs[0].Version)
	}
	if err := term.Put(ctx, MDDemandKey("md-jp-1", "a1b2c3", "feed/x"), "demand"); err != nil {
		t.Fatalf("write behind the adopted fence: %v", err)
	}
}

func TestFenceOlderWriter(t *testing.T) {
	ctx := testCtx(t)
	_, jp, topo := twoServers(t)
	rawJP := jp.Client(t)
	key := WriterKey(PlaneCtl, ControllerInstance)

	// A writer key left behind by an older epoch is taken over (W2 step 3: smaller or absent).
	if _, err := rawJP.Put(ctx, key, formatEpoch(0)); err != nil {
		t.Fatal(err)
	}
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})
	r, err := term.Fence(ctx, "jp")
	if err != nil {
		t.Fatal(err)
	}
	if kvs := mustGet(t, ctx, rawJP, key).Kvs; kvs[0].ModRevision != r || string(kvs[0].Value) != formatEpoch(term.Epoch()) {
		t.Fatalf("jp writer %v, R=%d", kvs, r)
	}
}

func TestParseEpoch(t *testing.T) {
	for _, v := range []string{"", "1", "-0000000000000000001", "0000000000000000000x", "99999999999999999999"} {
		if _, err := parseEpoch([]byte(v)); err == nil {
			t.Errorf("parseEpoch(%q) accepted", v)
		}
	}
	if e, err := parseEpoch([]byte(formatEpoch(42))); err != nil || e != 42 {
		t.Errorf("round trip: %d, %v", e, err)
	}
}

// Many writers hitting a cluster for the first time at once: one acquisition, no false ErrSuperseded.
func TestFenceConcurrentFirstUse(t *testing.T) {
	ctx := testCtx(t)
	_, jp, topo := twoServers(t)
	rawJP := jp.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})

	const n = 16
	revs := make(chan int64, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			r, err := term.Fence(ctx, "jp")
			if err != nil {
				errs <- err
				return
			}
			revs <- r
		})
	}
	wg.Wait()
	close(revs)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	kvs := mustGet(t, ctx, rawJP, WriterKey(PlaneCtl, ControllerInstance)).Kvs
	if len(kvs) != 1 || kvs[0].Version != 1 {
		t.Fatalf("jp writer %v: want exactly one put", kvs)
	}
	for r := range revs {
		if r != kvs[0].ModRevision {
			t.Fatalf("Fence returned %d, writer is at %d", r, kvs[0].ModRevision)
		}
	}
	if !running(term) {
		t.Fatal(term.Err())
	}
}

// end keeps the first cause; bind on an ended term hands out a context that is already cancelled.
func TestEndAndBind(t *testing.T) {
	term := &Term{done: make(chan struct{})}
	term.ctx, term.cancel = context.WithCancel(context.Background())

	ctx, release := term.bind(context.Background())
	if ctx.Err() != nil {
		t.Fatal("bound context cancelled while the term runs")
	}
	term.end(ErrSessionLost)
	term.end(ErrSuperseded)
	if !errors.Is(term.Err(), ErrSessionLost) || errors.Is(term.Err(), ErrSuperseded) {
		t.Fatalf("Err = %v, want the first cause", term.Err())
	}
	select {
	case <-term.Done():
	default:
		t.Fatal("Done open after end")
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("bound context not cancelled when the term ended")
	}
	release()

	for range 100 {
		ctx, release := term.bind(context.Background())
		if ctx.Err() == nil {
			t.Fatal("bind on an ended term returned a live context")
		}
		release()
	}
}

// unusedEndpoint is a local address nothing listens on: a client dialled to it never becomes ready.
func unusedEndpoint(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return "http://" + addr
}

// holding reports whether some call holds region's fence semaphore.
func holding(term *Term, region string) bool {
	term.mu.Lock()
	f := term.fences[region]
	term.mu.Unlock()
	return f != nil && len(f.sem) == 1
}

// A Fence stuck on an unreachable cluster holds that region's fence. Other callers wait behind it no longer than
// their own ctx allows, and the stuck call still honours its ctx.
func TestFenceWaitHonoursCtx(t *testing.T) {
	ctx := testCtx(t)
	tw := etcdtest.Start(t)
	topo := Topology{
		Regions:   map[string]Region{"tw": {Endpoints: tw.Endpoints()}, "jp": {Endpoints: []string{unusedEndpoint(t)}}},
		Instances: twoRegions().Instances,
	}
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneCtl, ControllerInstance, TermOptions{})

	stuckCtx, cancelStuck := context.WithCancel(ctx)
	defer cancelStuck()
	stuck := make(chan error, 1)
	go func() {
		_, err := term.Fence(stuckCtx, "jp")
		stuck <- err
	}()
	for deadline := time.Now().Add(10 * time.Second); !holding(term, "jp"); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the first Fence never took jp's fence")
		}
	}

	key := MDDemandKey("md-jp-1", "abc123", "feed/trade.Binance_Spot_BTCUSDT")
	calls := []struct {
		name string
		call func(context.Context) error
	}{
		{"Fence", func(c context.Context) error { _, err := term.Fence(c, "jp"); return err }},
		{"Put", func(c context.Context) error { return term.Put(c, key, "x") }},
	}
	for _, c := range calls {
		short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		start := time.Now()
		err := c.call(short)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s behind a stuck Fence: %v", c.name, err)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("%s took %s on a 200ms ctx", c.name, d)
		}
	}

	select {
	case err := <-stuck:
		t.Fatalf("the stuck Fence returned before its ctx ended: %v", err)
	default:
	}
	cancelStuck()
	select {
	case err := <-stuck:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stuck Fence after cancel: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stuck Fence ignored its ctx")
	}
	if !running(term) {
		t.Fatalf("a caller's ctx ended the term: %v", term.Err())
	}
}
