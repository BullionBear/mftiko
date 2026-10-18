package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// pollInterval is how often a condition is sampled. It is not the bound: a
// true condition returns at once.
const pollInterval = 20 * time.Millisecond

func until(t *testing.T, within time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for {
		last = cond()
		if last == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("waited %s for %s; last: %v", within, what, last)
		}
		time.Sleep(pollInterval)
	}
}

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func testLog(t *testing.T) zerolog.Logger {
	t.Helper()
	buf := &safeBuf{}
	t.Cleanup(func() {
		if t.Failed() && buf.String() != "" {
			t.Logf("log:\n%s", buf.String())
		}
	})
	return zerolog.New(buf)
}

func topology(servers map[string]*etcdtest.Server, home string) state.Topology {
	topo := state.Topology{
		Regions:   map[string]state.Region{},
		Instances: map[string]string{state.ControllerInstance: home},
	}
	for region, srv := range servers {
		topo.Regions[region] = state.Region{Endpoints: srv.Endpoints()}
		topo.Instances["md-"+region+"-1"] = region
	}
	return topo
}

func applyAuth(t *testing.T, ctx context.Context, topo state.Topology, servers map[string]*etcdtest.Server) {
	t.Helper()
	planes := map[string]state.Plane{}
	for inst := range topo.Instances {
		switch {
		case inst == state.ControllerInstance:
		case strings.HasPrefix(inst, "md-"):
			planes[inst] = state.PlaneMD
		default:
			t.Fatalf("no plane for %s", inst)
		}
	}
	tables, err := state.AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	for region, srv := range servers {
		if err := state.EnsureAuth(ctx, srv.Client(t), tables[region], etcdtest.Password); err != nil {
			t.Fatalf("auth %s: %v", region, err)
		}
	}
}

func config(topo state.Topology, id string, log zerolog.Logger) Config {
	return Config{
		Topology: topo,
		Dial:     state.AuthDial(state.ControllerInstance, etcdtest.Password(state.ControllerInstance)),
		Term:     state.TermOptions{ID: id},
		Release:  "test",
		Log:      log,
	}
}

type result struct {
	c   *Controller
	err error
}

// pending is a controller whose Start runs on its own goroutine, so a follower
// blocked in Campaign does not block the test.
type pending struct {
	t      *testing.T
	cancel context.CancelFunc
	ch     chan result

	mu   sync.Mutex
	got  result
	took bool
}

func startAsync(t *testing.T, parent context.Context, cfg Config) *pending {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	p := &pending{t: t, cancel: cancel, ch: make(chan result, 1)}
	go func() {
		c, err := Start(ctx, cfg)
		p.ch <- result{c, err}
	}()
	t.Cleanup(p.stop)
	return p
}

func (p *pending) ready(within time.Duration) *Controller {
	p.t.Helper()
	r, ok := p.recv(within)
	if !ok {
		p.t.Fatal("controller not elected")
	}
	if r.err != nil {
		p.t.Fatalf("start: %v", r.err)
	}
	return r.c
}

func (p *pending) recv(within time.Duration) (result, bool) {
	p.t.Helper()
	p.mu.Lock()
	if p.took {
		got := p.got
		p.mu.Unlock()
		return got, true
	}
	p.mu.Unlock()
	select {
	case r := <-p.ch:
		p.mu.Lock()
		p.got = r
		p.took = true
		p.mu.Unlock()
		return r, true
	case <-time.After(within):
		return result{}, false
	}
}

// waiting reports whether Start is still blocked. A finished Start is kept for stop.
func (p *pending) waiting() bool {
	p.t.Helper()
	select {
	case r := <-p.ch:
		p.mu.Lock()
		p.got = r
		p.took = true
		p.mu.Unlock()
		return false
	default:
		return true
	}
}

func (p *pending) stop() {
	p.cancel()
	p.mu.Lock()
	took := p.took
	got := p.got
	p.mu.Unlock()
	if !took {
		select {
		case got = <-p.ch:
			p.mu.Lock()
			p.got = got
			p.took = true
			p.mu.Unlock()
		case <-time.After(20 * time.Second):
			p.t.Errorf("controller did not finish starting")
			return
		}
	}
	if got.c == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		got.c.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		p.t.Errorf("controller did not stop")
	}
}

func demand(region string) string {
	return state.MDDemandKey("md-"+region+"-1", "sess", "feed/x")
}

var (
	clientMu sync.Mutex
	clients  = map[*etcdtest.Server]*clientv3.Client{}
)

// client is the server's root client, one per server for the life of the test.
func client(t *testing.T, srv *etcdtest.Server) *clientv3.Client {
	t.Helper()
	clientMu.Lock()
	defer clientMu.Unlock()
	if c := clients[srv]; c != nil {
		return c
	}
	c := srv.Client(t)
	clients[srv] = c
	t.Cleanup(func() {
		clientMu.Lock()
		delete(clients, srv)
		clientMu.Unlock()
	})
	return c
}

func recordOf(t *testing.T, ctx context.Context, srv *etcdtest.Server) (*statev1.AgentRecord, int64, bool) {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, state.AgentKey(state.PlaneCtl, state.ControllerInstance))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, false
	}
	rec := &statev1.AgentRecord{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, rec); err != nil {
		t.Fatal(err)
	}
	return rec, resp.Kvs[0].Lease, true
}

func valueOf(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string) (string, bool) {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return "", false
	}
	return string(resp.Kvs[0].Value), true
}

func electionCount(t *testing.T, ctx context.Context, srv *etcdtest.Server) int {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, state.ElectionPrefix(state.PlaneCtl, state.ControllerInstance)+"/",
		clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs)
}

func epochString(e int64) string { return fmt.Sprintf("%020d", e) }

func TestStartRejects(t *testing.T) {
	ctx := t.Context()
	bare := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: []string{"http://127.0.0.1:1"}}},
		Instances: map[string]string{"md-tw-1": "tw"},
	}
	withCtl := bare
	withCtl.Instances = map[string]string{state.ControllerInstance: "tw", "md-tw-1": "tw"}
	for name, cfg := range map[string]Config{
		"empty":          {},
		"no controller":  {Topology: bare, Dial: state.DefaultDial, Term: state.TermOptions{ID: "a"}},
		"no dial":        {Topology: withCtl, Term: state.TermOptions{ID: "a"}},
		"no id":          {Topology: withCtl, Dial: state.DefaultDial},
		"negative ttl":   {Topology: withCtl, Dial: state.DefaultDial, Term: state.TermOptions{ID: "a", SessionTTL: -time.Second}},
		"empty topology": {Dial: state.DefaultDial, Term: state.TermOptions{ID: "a"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Start(ctx, cfg); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// The leader fences its home region at election and other regions on the first
// write there. Demands outlive the record; a clean stop revokes the record.
func TestLeaderFencesOnFirstWrite(t *testing.T) {
	ctx := t.Context()
	log := testLog(t)
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	p := startAsync(t, ctx, config(topo, t.Name(), log))
	c := p.ready(30 * time.Second)

	rec, lease, ok := recordOf(t, ctx, servers["tw"])
	if !ok || lease == 0 {
		t.Fatal("home record missing")
	}
	if rec.GetPlane() != string(state.PlaneCtl) || rec.GetInstance() != state.ControllerInstance ||
		rec.GetEpoch() < 1 || rec.GetRelease() != "test" || rec.GetStartedAtNs() == 0 {
		t.Fatalf("record %+v", &rec)
	}
	if _, _, ok := recordOf(t, ctx, servers["jp"]); ok {
		t.Fatal("record also in jp")
	}
	if _, ok := valueOf(t, ctx, servers["tw"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); !ok {
		t.Fatal("home writer missing after election")
	}
	if _, ok := valueOf(t, ctx, servers["jp"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); ok {
		t.Fatal("jp fenced before any write there")
	}

	twKey, jpKey := demand("tw"), demand("jp")
	errc := make(chan error, 2)
	go func() { errc <- c.Put(ctx, twKey, "tw") }()
	go func() { errc <- c.Put(ctx, jpKey, "jp") }()
	for range 2 {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if v, ok := valueOf(t, ctx, servers["tw"], twKey); !ok || v != "tw" {
		t.Fatalf("tw demand %q present %v", v, ok)
	}
	if v, ok := valueOf(t, ctx, servers["jp"], jpKey); !ok || v != "jp" {
		t.Fatalf("jp demand %q present %v", v, ok)
	}
	if v, ok := valueOf(t, ctx, servers["jp"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); !ok || v != epochString(rec.GetEpoch()) {
		t.Fatalf("jp writer %q present %v, want epoch %d", v, ok, rec.GetEpoch())
	}

	if err := c.Delete(ctx, twKey); err != nil {
		t.Fatal(err)
	}
	if _, ok := valueOf(t, ctx, servers["tw"], twKey); ok {
		t.Fatal("deleted demand still there")
	}
	if err := c.Put(ctx, state.MDAckKey("md-tw-1", "sess", "feed/x"), "READY"); !errors.Is(err, state.ErrNotWriter) {
		t.Fatalf("ack write: %v, want ErrNotWriter", err)
	}
	if err := c.Put(ctx, twKey, "again"); err != nil {
		t.Fatalf("term ended after a refused write: %v", err)
	}

	p.cancel()
	if err := c.Wait(); err != nil {
		t.Fatalf("clean stop: %v", err)
	}
	until(t, 10*time.Second, "the record is revoked", func() error {
		if _, _, ok := recordOf(t, ctx, servers["tw"]); ok {
			return errors.New("record still there")
		}
		return nil
	})
	if v, ok := valueOf(t, ctx, servers["tw"], twKey); !ok || v != "again" {
		t.Fatalf("demand after revoke %q present %v", v, ok)
	}
	if v, ok := valueOf(t, ctx, servers["jp"], jpKey); !ok || v != "jp" {
		t.Fatalf("jp demand after revoke %q present %v", v, ok)
	}
}

// Two controllers, one home: the follower stays in Campaign and only the leader writes.
// When the leader stops, the follower is elected and is then the one that writes.
func TestOnlyLeaderWrites(t *testing.T) {
	ctx := t.Context()
	log := testLog(t)
	servers := etcdtest.StartRegions(t, "tw")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	key := demand("tw")

	a := startAsync(t, ctx, config(topo, "a", log))
	ac := a.ready(30 * time.Second)
	rec, _, ok := recordOf(t, ctx, servers["tw"])
	if !ok || rec.GetEpoch() != 1 {
		t.Fatalf("leader record %+v present %v", &rec, ok)
	}
	b := startAsync(t, ctx, config(topo, "b", log))
	until(t, 20*time.Second, "the follower is waiting in Campaign", func() error {
		if !b.waiting() {
			return errors.New("follower returned from Start")
		}
		if n := electionCount(t, ctx, servers["tw"]); n != 2 {
			return fmt.Errorf("%d election keys", n)
		}
		got, _, ok := recordOf(t, ctx, servers["tw"])
		if !ok || got.GetEpoch() != 1 || got.GetStartedAtNs() != rec.GetStartedAtNs() {
			return fmt.Errorf("record changed: %+v present %v", &got, ok)
		}
		return nil
	})
	if err := ac.Put(ctx, key, "a"); err != nil {
		t.Fatal(err)
	}
	if !b.waiting() {
		t.Fatal("follower wrote or was elected while the leader held the term")
	}

	a.cancel()
	if err := ac.Wait(); err != nil {
		t.Fatalf("leader stop: %v", err)
	}
	bc := b.ready(30 * time.Second)
	until(t, 20*time.Second, "the follower's record is in place", func() error {
		got, _, ok := recordOf(t, ctx, servers["tw"])
		if !ok || got.GetEpoch() != rec.GetEpoch()+1 {
			return fmt.Errorf("record %+v present %v", &got, ok)
		}
		return nil
	})
	if err := bc.Put(ctx, key, "b"); err != nil {
		t.Fatal(err)
	}
	if v, ok := valueOf(t, ctx, servers["tw"], key); !ok || v != "b" {
		t.Fatalf("demand %q present %v", v, ok)
	}
}

// Homes that disagree each elect in their own cluster. Each can write its home.
// A write into the other's region is ErrSuperseded, leaves that region unchanged,
// and the old term can no longer write anywhere.
func TestInconsistentHomes(t *testing.T) {
	ctx := t.Context()
	log := testLog(t)
	servers := etcdtest.StartRegions(t, "tw", "jp")
	homeTW := topology(servers, "tw")
	homeJP := topology(servers, "jp")
	applyAuth(t, ctx, homeTW, servers)

	a := startAsync(t, ctx, config(homeTW, "a", log))
	b := startAsync(t, ctx, config(homeJP, "b", log))
	ac, bc := a.ready(30*time.Second), b.ready(30*time.Second)

	twRec, _, ok := recordOf(t, ctx, servers["tw"])
	if !ok || twRec.GetEpoch() != 1 {
		t.Fatalf("tw record %+v present %v", &twRec, ok)
	}
	jpRec, _, ok := recordOf(t, ctx, servers["jp"])
	if !ok || jpRec.GetEpoch() != 1 {
		t.Fatalf("jp record %+v present %v", &jpRec, ok)
	}

	twKey, jpKey := demand("tw"), demand("jp")
	if err := ac.Put(ctx, twKey, "a"); err != nil {
		t.Fatal(err)
	}
	if err := bc.Put(ctx, jpKey, "b"); err != nil {
		t.Fatal(err)
	}
	if err := ac.Put(ctx, jpKey, "a-cross"); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("a writing jp: %v", err)
	}
	if err := ac.Wait(); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("a exit: %v", err)
	}
	if err := ac.Put(ctx, twKey, "a-again"); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("a writing tw after being superseded: %v", err)
	}
	if v, ok := valueOf(t, ctx, servers["jp"], jpKey); !ok || v != "b" {
		t.Fatalf("jp demand %q present %v", v, ok)
	}
	if v, ok := valueOf(t, ctx, servers["tw"], twKey); !ok || v != "a" {
		t.Fatalf("tw demand %q present %v", v, ok)
	}

	if err := bc.Put(ctx, twKey, "b-cross"); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("b writing tw: %v", err)
	}
	if err := bc.Wait(); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("b exit: %v", err)
	}
	if err := bc.Put(ctx, jpKey, "b-again"); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("b writing jp after being superseded: %v", err)
	}
	if v, ok := valueOf(t, ctx, servers["tw"], twKey); !ok || v != "a" {
		t.Fatalf("tw demand changed to %q present %v", v, ok)
	}
	if v, ok := valueOf(t, ctx, servers["jp"], jpKey); !ok || v != "b" {
		t.Fatalf("jp demand changed to %q present %v", v, ok)
	}
}

// A newer writer epoch on one region ends the term. Later writes to every region fail
// and change nothing, and Wait returns ErrSuperseded.
func TestSupersededExits(t *testing.T) {
	ctx := t.Context()
	log := testLog(t)
	servers := etcdtest.StartRegions(t, "tw", "jp")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	p := startAsync(t, ctx, config(topo, t.Name(), log))
	c := p.ready(30 * time.Second)
	rec, _, ok := recordOf(t, ctx, servers["tw"])
	if !ok {
		t.Fatal("no record")
	}
	twKey := demand("tw")
	if err := c.Put(ctx, twKey, "orig"); err != nil {
		t.Fatal(err)
	}
	newer := epochString(rec.GetEpoch() + 1)
	jpWriter := state.WriterKey(state.PlaneCtl, state.ControllerInstance)
	if _, err := client(t, servers["jp"]).Put(ctx, jpWriter, newer); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, demand("jp"), "nope"); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("jp write: %v", err)
	}
	if v, ok := valueOf(t, ctx, servers["jp"], jpWriter); !ok || v != newer {
		t.Fatalf("jp writer %q present %v", v, ok)
	}
	if _, ok := valueOf(t, ctx, servers["jp"], demand("jp")); ok {
		t.Fatal("superseded term wrote jp")
	}
	if err := c.Put(ctx, twKey, "later"); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("tw write after the term ended: %v", err)
	}
	if v, ok := valueOf(t, ctx, servers["tw"], twKey); !ok || v != "orig" {
		t.Fatalf("tw demand %q present %v", v, ok)
	}
	if err := c.Wait(); !errors.Is(err, state.ErrSuperseded) {
		t.Fatalf("exit: %v", err)
	}
	until(t, 10*time.Second, "the record is revoked", func() error {
		if _, _, ok := recordOf(t, ctx, servers["tw"]); ok {
			return errors.New("record still there")
		}
		return nil
	})
}

// A lost election session ends the term with ErrSessionLost, and writes stop.
func TestSessionLostExits(t *testing.T) {
	ctx := t.Context()
	log := testLog(t)
	servers := etcdtest.StartRegions(t, "tw")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	p := startAsync(t, ctx, config(topo, t.Name(), log))
	c := p.ready(30 * time.Second)
	_, lease, ok := recordOf(t, ctx, servers["tw"])
	if !ok || lease == 0 {
		t.Fatal("no record")
	}
	if _, err := client(t, servers["tw"]).Revoke(ctx, clientv3.LeaseID(lease)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		if !errors.Is(err, state.ErrSessionLost) {
			t.Fatalf("exit: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("still running after the session was revoked")
	}
	if err := c.Put(ctx, demand("tw"), "x"); !errors.Is(err, state.ErrSessionLost) {
		t.Fatalf("write after session loss: %v", err)
	}
}

// The non-home region accepts no connection. The controller is still elected,
// and a write to the home region completes without waiting for the other.
func TestUnreachableRegion(t *testing.T) {
	ctx := t.Context()
	log := testLog(t)
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	proxy.Cut()
	topo := topology(servers, "tw")
	topo.Regions["jp"] = state.Region{Endpoints: proxy.Endpoints()}
	applyAuth(t, ctx, topo, servers)

	p := startAsync(t, ctx, config(topo, t.Name(), log))
	c := p.ready(30 * time.Second)
	if _, _, ok := recordOf(t, ctx, servers["tw"]); !ok {
		t.Fatal("not elected")
	}
	if _, ok := valueOf(t, ctx, servers["jp"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); ok {
		t.Fatal("jp fenced while unreachable")
	}

	jpErr := make(chan error, 1)
	go func() { jpErr <- c.Put(ctx, demand("jp"), "jp") }()
	if err := c.Put(ctx, demand("tw"), "tw"); err != nil {
		t.Fatalf("home write while jp is unreachable: %v", err)
	}
	if v, ok := valueOf(t, ctx, servers["tw"], demand("tw")); !ok || v != "tw" {
		t.Fatalf("tw demand %q present %v", v, ok)
	}
	select {
	case err := <-jpErr:
		if err == nil || errors.Is(err, state.ErrSuperseded) {
			t.Fatalf("jp write: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("jp write did not return")
	}
	if _, ok := valueOf(t, ctx, servers["jp"], demand("jp")); ok {
		t.Fatal("unreachable region accepted a demand")
	}
	if _, ok := valueOf(t, ctx, servers["jp"], state.WriterKey(state.PlaneCtl, state.ControllerInstance)); ok {
		t.Fatal("unreachable region was fenced")
	}
	if err := c.Put(ctx, demand("tw"), "tw-again"); err != nil {
		t.Fatalf("home write after jp failed: %v", err)
	}
	if _, _, ok := recordOf(t, ctx, servers["tw"]); !ok {
		t.Fatal("term ended")
	}
}
