package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// sessionSeq is an id source that draws ids, in order, on every allocation: every replica and every request
// draws the same ones, so they collide on purpose.
func sessionSeq(t *testing.T, ids ...string) func() io.Reader {
	t.Helper()
	var b []byte
	for _, id := range ids {
		h, err := hex.DecodeString(id)
		if err != nil || len(h) != 3 {
			t.Fatalf("session id %q", id)
		}
		b = append(b, h...)
	}
	return func() io.Reader { return bytes.NewReader(b) }
}

func apiSeq(ids ...int64) func() io.Reader {
	var b []byte
	for _, id := range ids {
		b = binary.BigEndian.AppendUint64(b, uint64(id))
	}
	return func() io.Reader { return bytes.NewReader(b) }
}

func TestNewIDs(t *testing.T) {
	if id, err := newSessionID(bytes.NewReader([]byte{0xa1, 0xb2, 0xc3})); err != nil || id != "a1b2c3" {
		t.Fatalf("newSessionID = %q, %v", id, err)
	}
	if _, err := newSessionID(bytes.NewReader([]byte{1, 2})); err == nil {
		t.Fatal("short read: no error")
	}
	format := regexp.MustCompile(`^[0-9a-f]{6}$`)
	src := defaultIDs()
	for range 1000 {
		id, err := newSessionID(src)
		if err != nil || !format.MatchString(id) {
			t.Fatalf("newSessionID = %q, %v", id, err)
		}
		api, err := newAPIID(src)
		if err != nil || api < 1 || api > maxAPIID {
			t.Fatalf("newAPIID = %d, %v", api, err)
		}
	}
	all := bytes.Repeat([]byte{0xff}, 8)
	if id, err := newAPIID(bytes.NewReader(all)); err != nil || id != 1<<53-1 {
		t.Fatalf("all ones: %d, %v", id, err)
	}
	// The top 11 bits are dropped; a draw that leaves 0 is drawn again.
	zero := []byte{0xff, 0xe0, 0, 0, 0, 0, 0, 0}
	if id, err := newAPIID(bytes.NewReader(append(zero, 0, 0, 0, 0, 0, 0, 0, 7))); err != nil || id != 7 {
		t.Fatalf("0 then 7: %d, %v", id, err)
	}
	if _, err := newAPIID(bytes.NewReader(zero)); err == nil {
		t.Fatal("only zero, then EOF: no error")
	}
}

func TestAllocate(t *testing.T) {
	draws := func(ids ...string) func() (string, error) {
		return func() (string, error) {
			if len(ids) == 0 {
				return "", errors.New("drawn out")
			}
			id := ids[0]
			ids = ids[1:]
			return id, nil
		}
	}
	never := func(string) bool { return false }

	var tried []string
	id, err := allocate(5, draws("a", "b", "c"), func(id string) bool { return id == "a" }, func(id string) error {
		tried = append(tried, id)
		if id == "b" {
			return fmt.Errorf("%w: b exists", errConflict)
		}
		return nil
	})
	if err != nil || id != "c" || !slices.Equal(tried, []string{"b", "c"}) {
		t.Fatalf("skip a, lose b, take c: %q %v, tried %v", id, err, tried)
	}

	n := 0
	_, err = allocate(5, draws("a", "b", "c", "d", "e", "f"), never, func(string) error {
		n++
		return errConflict
	})
	if !errors.Is(err, errIDsExhausted) || n != 5 {
		t.Fatalf("five conflicts: %v after %d tries", err, n)
	}
	if code, name := problemOf(err); code != http.StatusInternalServerError || name != "internal" {
		t.Fatalf("exhausted is %d %s", code, name)
	}
	_, err = allocate(5, draws("a", "b", "c", "d", "e", "f"), func(string) bool { return true }, func(string) error {
		t.Fatal("tried a taken id")
		return nil
	})
	if !errors.Is(err, errIDsExhausted) {
		t.Fatalf("all taken: %v", err)
	}

	boom := errors.New("boom")
	n = 0
	if _, err := allocate(5, draws("a", "b"), never, func(string) error { n++; return boom }); !errors.Is(err, boom) || n != 1 {
		t.Fatalf("other error: %v after %d tries", err, n)
	}
	if _, err := allocate(5, draws(), never, func(string) error { return nil }); err == nil || errors.Is(err, errIDsExhausted) {
		t.Fatalf("draw error: %v", err)
	}
}

func TestClassify(t *testing.T) {
	unavail := []error{
		rpctypes.ErrNoLeader,
		rpctypes.ErrTimeout,
		rpctypes.ErrTooManyRequests,
		status.Error(codes.Unavailable, "connection refused"),
		status.Error(codes.DeadlineExceeded, "deadline"),
		context.DeadlineExceeded,
		fmt.Errorf("state: txn in tw: %w", rpctypes.ErrTimeoutDueToLeaderFail),
		fmt.Errorf("state: waiting for the dial of jp: %w", context.DeadlineExceeded),
		state.ErrClustersClosed,
	}
	for _, err := range unavail {
		got := classify(err)
		if !errors.Is(got, errRegionUnavailable) || !errors.Is(got, err) {
			t.Errorf("classify(%v) = %v", err, got)
		}
		if code, name := problemOf(got); code != http.StatusServiceUnavailable || name != "region_unavailable" {
			t.Errorf("%v: %d %s", err, code, name)
		}
	}
	other := []error{
		rpctypes.ErrPermissionDenied,
		status.Error(codes.InvalidArgument, "bad"),
		fmt.Errorf("%w: api may not write", state.ErrNotWriter),
		state.ErrCrossCluster,
		state.ErrBadKey,
		context.Canceled,
		errors.New("anything"),
	}
	for _, err := range other {
		got := classify(err)
		if errors.Is(got, errRegionUnavailable) {
			t.Errorf("classify(%v) = %v, want as is", err, got)
		}
		if code, name := problemOf(got); code != http.StatusInternalServerError || name != "internal" {
			t.Errorf("%v: %d %s", err, code, name)
		}
	}
	if code, name := problemOf(fmt.Errorf("%w: x", errConflict)); code != http.StatusConflict || name != "conflict" {
		t.Errorf("conflict: %d %s", code, name)
	}
	if code, name := problemOf(fmt.Errorf("%w: x", errNotFound)); code != http.StatusNotFound || name != "not_found" {
		t.Errorf("not found: %d %s", code, name)
	}
}

// writeEnv is two regions under RBAC: tw dialled directly, jp through a proxy that a test can cut. Replicas of the
// API run as the api user.
type writeEnv struct {
	servers map[string]*etcdtest.Server
	proxy   *etcdtest.Proxy
	cfg     Config
	log     *lockedBuf
}

func newWriteEnv(t *testing.T) *writeEnv {
	t.Helper()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	topo := state.Topology{
		Regions: map[string]state.Region{
			"tw": {Endpoints: servers["tw"].Endpoints()},
			"jp": {Endpoints: proxy.Endpoints()},
		},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"sts-tw-1":               "tw",
			"md-tw-1":                "tw",
			"td-tw-1":                "tw",
			"sts-jp-1":               "jp",
			"md-jp-1":                "jp",
			"td-jp-1":                "jp",
		},
	}
	planes := map[string]state.Plane{
		"sts-tw-1": state.PlaneSTS, "md-tw-1": state.PlaneMD, "td-tw-1": state.PlaneTD,
		"sts-jp-1": state.PlaneSTS, "md-jp-1": state.PlaneMD, "td-jp-1": state.PlaneTD,
	}
	applyAuth(t, t.Context(), topo, planes, servers)
	dep, err := json.Marshal(deployment{Topology: topo, Planes: planes})
	if err != nil {
		t.Fatal(err)
	}
	conf, err := json.Marshal(map[string]any{
		"listen":            "127.0.0.1:0",
		"unavailable_after": "2s",
		"repositories":      []string{"mftiko/strategies"},
		"td_keys":           map[string]string{"td-tw-1": mustPub(t), "td-jp-1": mustPub(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	buf := &lockedBuf{}
	cfg, err := Load(bytes.NewReader(dep), bytes.NewReader(conf), Input{
		EtcdPassword: etcdtest.Password(state.UserAPI),
		CookieKey:    "cookie-key",
		PostgresDSN:  "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable",
		Mirror: state.MirrorOptions{
			ProbeInterval: 100 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: zerolog.New(buf).Level(zerolog.InfoLevel),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &writeEnv{servers: servers, proxy: proxy, cfg: cfg, log: buf}
}

// replica starts one API process and waits until both regions are available to it.
func (e *writeEnv) replica(t *testing.T) *Server {
	t.Helper()
	r := startRunning(t, e.cfg, e.log)
	until(t, 30*time.Second, "both regions available", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		if body, _ := r.s.readiness(time.Now()); len(body.UnavailableRegions) != 0 {
			return fmt.Errorf("unavailable: %v", body.UnavailableRegions)
		}
		return nil
	})
	return r.s
}

// seen waits until s's mirror of region has key.
func seen(t *testing.T, s *Server, region, key string) {
	t.Helper()
	until(t, 10*time.Second, region+" mirror has "+key, func() error {
		if v, _ := s.snapshot(region); !hasKey(v, key) {
			return errors.New("not yet")
		}
		return nil
	})
}

func hasKey(v state.View, key string) bool {
	_, ok := v.Entries[key]
	return ok
}

func (e *writeEnv) region(t *testing.T, key string) string {
	t.Helper()
	region, err := e.cfg.Topology.RegionOfKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return region
}

// get reads key as root, nil if absent.
func (e *writeEnv) get(t *testing.T, region, key string) *mvccpb.KeyValue {
	t.Helper()
	resp, err := e.servers[region].Client(t).Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	return resp.Kvs[0]
}

// Two replicas, five requests at once, every one drawing the same five ids in the same order (P8 acceptance): each
// request gets an id of its own, and every spec holds what its creator wrote, never overwritten.
func TestCreateSessionConcurrentReplicas(t *testing.T) {
	e := newWriteEnv(t)
	a, b := e.replica(t), e.replica(t)
	ids := []string{"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"}
	a.ids = sessionSeq(t, ids...)
	b.ids = sessionSeq(t, ids...)

	type result struct {
		id, val string
		rev     int64
		err     error
	}
	results := make([]result, 5)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		s := a
		if i%2 == 1 {
			s = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			val := fmt.Sprintf("spec of request %d", i)
			id, rev, err := s.createSession(t.Context(), "sts-tw-1", []byte(val))
			results[i] = result{id, val, rev, err}
		}()
	}
	close(start)
	wg.Wait()

	var got []string
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("request %d: %v", i, r.err)
		}
		got = append(got, r.id)
		kv := e.get(t, "tw", state.SessionSpecKey("sts-tw-1", r.id))
		if kv == nil || string(kv.Value) != r.val || kv.Version != 1 || kv.CreateRevision != r.rev {
			t.Errorf("request %d, %s: %+v, want %q at version 1, revision %d", i, r.id, kv, r.val, r.rev)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, ids) {
		t.Fatalf("ids %v, want each of %v once", got, ids)
	}

	// A sixth request has nothing left to draw from the same five.
	if _, _, err := a.createSession(t.Context(), "sts-tw-1", []byte("sixth")); !errors.Is(err, errIDsExhausted) {
		t.Fatalf("sixth: %v", err)
	}
}

// An id is taken while any key in an available region names it: a live spec in another STS instance, a status left
// without a spec, an MD ack in another region. The mirror check skips them; a spec the mirror has not seen yet is
// caught by the create-only txn and given up. Nothing that was there is touched.
func TestCreateSkipsTakenIDs(t *testing.T) {
	e := newWriteEnv(t)
	s := e.replica(t)
	ctx := t.Context()
	before := map[string]string{
		state.SessionSpecKey("sts-tw-1", "aaaaaa"):                             "live spec",
		state.SessionStatusKey("sts-tw-1", "bbbbbb"):                           "status put back after W5",
		state.MDAckKey("md-jp-1", "cccccc", "feed/trade.Binance_Spot_BTCUSDT"): "ack",
		state.TDDemandKey("td-tw-1", "dddddd", 42):                             "demand entry",
		state.AccountKey("td-jp-1", 7):                                         "account 7",
		state.AccountKey("td-tw-1", 8):                                         "account 8",
	}
	for key, val := range before {
		region := e.region(t, key)
		putKey(t, ctx, e.servers[region], key, val)
		seen(t, s, region, key)
	}

	s.ids = sessionSeq(t, "aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee")
	id, rev, err := s.createSession(ctx, "sts-jp-1", []byte("new"))
	if err != nil || id != "eeeeee" {
		t.Fatalf("createSession = %q, %v; want eeeeee", id, err)
	}
	if kv := e.get(t, "jp", state.SessionSpecKey("sts-jp-1", id)); kv == nil || string(kv.Value) != "new" || kv.ModRevision != rev {
		t.Fatalf("new spec: %+v", kv)
	}
	for _, old := range []string{"aaaaaa", "bbbbbb", "cccccc", "dddddd"} {
		if kv := e.get(t, "jp", state.SessionSpecKey("sts-jp-1", old)); kv != nil {
			t.Errorf("spec %s written", old)
		}
	}

	s.ids = apiSeq(7, 8, 9)
	api, _, err := s.createAccount(ctx, "td-tw-1", func(int64) ([]byte, error) { return []byte("acct"), nil })
	if err != nil || api != 9 {
		t.Fatalf("createAccount = %d, %v; want 9", api, err)
	}

	// Past the mirror: a spec it has not seen is refused by create-only, and the allocation moves on.
	w := s.cl.APIWriter()
	putKey(t, ctx, e.servers["tw"], state.SessionSpecKey("sts-tw-1", "ffffff"), "raced")
	var tried []string
	id, err = allocate(idAttempts,
		func() (string, error) { return []string{"ffffff", "f0f0f0"}[len(tried)], nil },
		func(string) bool { return false },
		func(id string) error {
			tried = append(tried, id)
			_, err := create(ctx, w, state.SessionSpecKey("sts-tw-1", id), []byte("mine"))
			return err
		})
	if err != nil || id != "f0f0f0" || len(tried) != 2 {
		t.Fatalf("past the mirror: %q, %v, tried %v", id, err, tried)
	}
	for key, val := range before {
		if kv := e.get(t, e.region(t, key), key); kv == nil || string(kv.Value) != val || kv.Version != 1 {
			t.Errorf("%s: %+v, want %q untouched", key, kv, val)
		}
	}
	if kv := e.get(t, "tw", state.SessionSpecKey("sts-tw-1", "ffffff")); string(kv.Value) != "raced" || kv.Version != 1 {
		t.Errorf("raced spec overwritten: %+v", kv)
	}
	if _, _, err := s.createSession(ctx, "td-tw-1", nil); err == nil {
		t.Error("createSession on a TD instance")
	}
}

// casPut, update and deleteIf against etcd, and the scope: the API cannot put a status.
func TestConditionalWrites(t *testing.T) {
	e := newWriteEnv(t)
	s := e.replica(t)
	ctx := t.Context()
	w := s.cl.APIWriter()
	key := state.SessionSpecKey("sts-tw-1", "a1b2c3")

	rev, err := create(ctx, w, key, []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create(ctx, w, key, []byte("again")); !errors.Is(err, errConflict) {
		t.Fatalf("second create: %v", err)
	}
	rev2, err := casPut(ctx, w, key, []byte("v2"), rev)
	if err != nil {
		t.Fatal(err)
	}
	_, err = casPut(ctx, w, key, []byte("stale"), rev)
	if !errors.Is(err, errConflict) {
		t.Fatalf("stale CAS: %v", err)
	}
	if code, name := problemOf(err); code != http.StatusConflict || name != "conflict" {
		t.Fatalf("stale CAS is %d %s", code, name)
	}
	if kv := e.get(t, "tw", key); string(kv.Value) != "v2" || kv.ModRevision != rev2 {
		t.Fatalf("after the stale CAS: %+v", kv)
	}

	// update: another write lands between the read and the CAS, so the txn fails and update runs f again on what that
	// failed txn read back.
	var saw []string
	rev3, err := update(ctx, w, key, 3, func(cur *mvccpb.KeyValue) ([]byte, bool, error) {
		saw = append(saw, string(cur.Value))
		if len(saw) == 1 {
			putKey(t, ctx, e.servers["tw"], key, "v2 by someone else")
		}
		return append(slices.Clone(cur.Value), " + ended"...), true, nil
	})
	if err != nil || !slices.Equal(saw, []string{"v2", "v2 by someone else"}) {
		t.Fatalf("update: %v, saw %q", err, saw)
	}
	if kv := e.get(t, "tw", key); string(kv.Value) != "v2 by someone else + ended" || kv.ModRevision != rev3 {
		t.Fatalf("after update: %+v", kv)
	}
	// No change: nothing written, the revision read is returned.
	if r, err := update(ctx, w, key, 3, func(*mvccpb.KeyValue) ([]byte, bool, error) { return nil, false, nil }); err != nil || r != rev3 {
		t.Fatalf("no change: %d, %v; want %d", r, err, rev3)
	}
	// Changed on every attempt: errConflict after them.
	n := 0
	_, err = update(ctx, w, key, 3, func(cur *mvccpb.KeyValue) ([]byte, bool, error) {
		n++
		putKey(t, ctx, e.servers["tw"], key, fmt.Sprintf("bump %d", n))
		return []byte("lost"), true, nil
	})
	if !errors.Is(err, errConflict) || n != 3 {
		t.Fatalf("always raced: %v after %d", err, n)
	}
	_, err = update(ctx, w, state.SessionSpecKey("sts-tw-1", "000000"), 3, func(cur *mvccpb.KeyValue) ([]byte, bool, error) {
		if cur == nil {
			return nil, false, errNotFound
		}
		return nil, false, nil
	})
	if code, name := problemOf(err); code != http.StatusNotFound || name != "not_found" {
		t.Fatalf("update of nothing: %v, %d %s", err, code, name)
	}

	// W5: spec and status in one txn, on the revisions read.
	status := state.SessionStatusKey("sts-tw-1", "a1b2c3")
	sRev := putKey(t, ctx, e.servers["tw"], status, "terminal")
	spec := e.get(t, "tw", key)
	if _, err := deleteIf(ctx, w, cond{key, spec.ModRevision}, cond{status, sRev - 1}); !errors.Is(err, errConflict) {
		t.Fatalf("stale status: %v", err)
	}
	if e.get(t, "tw", key) == nil || e.get(t, "tw", status) == nil {
		t.Fatal("a failed delete removed a key")
	}
	if _, err := deleteIf(ctx, w, cond{key, spec.ModRevision}, cond{status, sRev}); err != nil {
		t.Fatal(err)
	}
	if e.get(t, "tw", key) != nil || e.get(t, "tw", status) != nil {
		t.Fatal("spec or status left")
	}
	if _, err := deleteIf(ctx, w, cond{key, spec.ModRevision}); !errors.Is(err, errConflict) {
		t.Fatalf("delete of a deleted key: %v", err)
	}
	if _, err := casPut(ctx, w, key, []byte("v"), spec.ModRevision); !errors.Is(err, errConflict) {
		t.Fatalf("CAS on a deleted key: %v", err)
	}

	// etcd lets the api user put a status (D1040); the scope does not.
	_, err = create(ctx, w, status, []byte("forged"))
	if !errors.Is(err, state.ErrNotWriter) {
		t.Fatalf("status put: %v", err)
	}
	if code, name := problemOf(err); code != http.StatusInternalServerError || name != "internal" {
		t.Fatalf("status put is %d %s", code, name)
	}
	if e.get(t, "tw", status) != nil {
		t.Fatal("status written")
	}
}

// A region cut off: creating in it is 503 before any write, creating elsewhere still works and skips its ids, and a
// write sent to it anyway fails as region_unavailable.
func TestRegionUnavailable(t *testing.T) {
	e := newWriteEnv(t)
	s := e.replica(t)
	ctx := t.Context()
	e.proxy.Cut()
	until(t, 15*time.Second, "jp unavailable", func() error {
		if body, _ := s.readiness(time.Now()); !slices.Equal(body.UnavailableRegions, []string{"jp"}) {
			return fmt.Errorf("unavailable: %v", body.UnavailableRegions)
		}
		return nil
	})

	s.ids = sessionSeq(t, "aaaaaa")
	_, _, err := s.createSession(ctx, "sts-jp-1", []byte("x"))
	if !errors.Is(err, errRegionUnavailable) {
		t.Fatalf("create in jp: %v", err)
	}
	if code, name := problemOf(err); code != http.StatusServiceUnavailable || name != "region_unavailable" {
		t.Fatalf("create in jp is %d %s", code, name)
	}
	built := false
	if _, _, err := s.createAccount(ctx, "td-jp-1", func(int64) ([]byte, error) {
		built = true
		return []byte("x"), nil
	}); !errors.Is(err, errRegionUnavailable) || built {
		t.Fatalf("account in jp: %v, built %v", err, built)
	}
	if id, _, err := s.createSession(ctx, "sts-tw-1", []byte("x")); err != nil || id != "aaaaaa" {
		t.Fatalf("create in tw while jp is cut: %q, %v", id, err)
	}

	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, err = create(short, s.cl.APIWriter(), state.SessionSpecKey("sts-jp-1", "bbbbbb"), []byte("x"))
	if !errors.Is(err, errRegionUnavailable) {
		t.Fatalf("txn to a cut region: %v", err)
	}

	e.proxy.Restore()
	until(t, 15*time.Second, "jp back", func() error {
		if body, _ := s.readiness(time.Now()); len(body.UnavailableRegions) != 0 {
			return fmt.Errorf("unavailable: %v", body.UnavailableRegions)
		}
		return nil
	})
	if kv := e.get(t, "jp", state.SessionSpecKey("sts-jp-1", "aaaaaa")); kv != nil {
		t.Fatalf("a spec was written in jp while it was unavailable: %+v", kv)
	}
}
