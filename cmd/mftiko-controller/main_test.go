package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// envChild makes the re-executed test binary run mftiko-controller instead of the tests.
const envChild = "MFTIKO_CONTROLLER_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(envChild) == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

// pollInterval is how often a condition is sampled. It is not the bound.
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

func TestReadDeployment(t *testing.T) {
	const body = `{
	  "regions": {"tw": {"endpoints": ["http://etcd-tw:2379"]}, "jp": {"endpoints": ["http://etcd-jp:2379"]}},
	  "instances": {"controller": "tw", "md-tw-1": "tw", "md-jp-1": "jp"},
	  "planes": {"md-tw-1": "md", "md-jp-1": "md"}
	}`
	topo, err := readDeployment("-", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if topo.Instances[state.ControllerInstance] != "tw" || len(topo.Regions) != 2 {
		t.Fatalf("%+v", topo)
	}
	for name, in := range map[string]string{
		"unknown field": `{"regions":{"tw":{"endpoints":["http://127.0.0.1:1"]}},"instances":{"controller":"tw"},"extra":1}`,
		"not json":      `{`,
		"no endpoints":  `{"regions":{"tw":{"endpoints":[]}},"instances":{"controller":"tw"}}`,
	} {
		if _, err := readDeployment("-", strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := run(t.Context(), "", "pw", zerolog.Nop()); err == nil {
		t.Fatal("missing -in accepted")
	}
}

func TestRunRequiresPassword(t *testing.T) {
	if err := run(context.Background(), "deploy.json", "", zerolog.Nop()); err == nil {
		t.Fatal("missing password accepted")
	}
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
		if inst == state.ControllerInstance {
			continue
		}
		planes[inst] = state.PlaneMD
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

func writeDeploy(t *testing.T, topo state.Topology) string {
	t.Helper()
	b, err := json.Marshal(topo)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "deploy.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
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

type child struct {
	t    *testing.T
	cmd  *exec.Cmd
	out  *safeBuf
	done chan struct{}

	mu       sync.Mutex
	finished bool
	waitErr  error
}

func startChild(t *testing.T, deploy string) *child {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	buf := &safeBuf{}
	cmd := exec.Command(exe, "-in", deploy, "-etcd-password", etcdtest.Password(state.ControllerInstance))
	cmd.Env = append(os.Environ(), envChild+"=1")
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &child{t: t, cmd: cmd, out: buf, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		c.mu.Lock()
		c.finished = true
		c.waitErr = err
		c.mu.Unlock()
		close(c.done)
	}()
	t.Cleanup(func() {
		if c.cmd.Process != nil {
			c.cmd.Process.Kill()
		}
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
		}
		if t.Failed() {
			t.Logf("controller output:\n%s", c.out.String())
		}
	})
	return c
}

func (c *child) signal(sig syscall.Signal) {
	c.t.Helper()
	if err := c.cmd.Process.Signal(sig); err != nil {
		c.t.Fatal(err)
	}
}

func (c *child) result() (error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished {
		return nil, false
	}
	return c.waitErr, true
}

func (c *child) exitCode(within time.Duration) int {
	c.t.Helper()
	select {
	case <-c.done:
	case <-time.After(within):
		c.t.Fatalf("still running after %s\n%s", within, c.out.String())
	}
	err, _ := c.result()
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	c.t.Fatal(err)
	return -1
}

func (c *child) running() bool {
	c.t.Helper()
	_, done := c.result()
	return !done
}

var (
	clientMu sync.Mutex
	clients  = map[*etcdtest.Server]*clientv3.Client{}
)

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

func recordOf(t *testing.T, ctx context.Context, srv *etcdtest.Server) (statev1.AgentRecord, bool) {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, state.AgentKey(state.PlaneCtl, state.ControllerInstance))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return statev1.AgentRecord{}, false
	}
	var rec statev1.AgentRecord
	if err := proto.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		t.Fatal(err)
	}
	return rec, true
}

func recordLease(t *testing.T, ctx context.Context, srv *etcdtest.Server) clientv3.LeaseID {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, state.AgentKey(state.PlaneCtl, state.ControllerInstance))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || resp.Kvs[0].Lease == 0 {
		t.Fatal("record has no lease")
	}
	return clientv3.LeaseID(resp.Kvs[0].Lease)
}

func elections(t *testing.T, ctx context.Context, srv *etcdtest.Server) int {
	t.Helper()
	resp, err := client(t, srv).Get(ctx, state.ElectionPrefix(state.PlaneCtl, state.ControllerInstance)+"/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs)
}

// Two processes, one home: only the leader's AgentRecord is written. The other waits
// in Campaign. SIGTERM revokes and exits 0, and the waiter then becomes leader.
func TestTwoProcessesOneLeader(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	deploy := writeDeploy(t, topo)

	a := startChild(t, deploy)
	var first statev1.AgentRecord
	until(t, 30*time.Second, "the first process writes its record", func() error {
		rec, ok := recordOf(t, ctx, servers["tw"])
		if !ok || rec.GetEpoch() < 1 {
			return errors.New("no record")
		}
		first = rec
		return nil
	})
	b := startChild(t, deploy)
	until(t, 20*time.Second, "the second process is waiting in Campaign", func() error {
		if !b.running() {
			return errors.New("second process exited")
		}
		if n := elections(t, ctx, servers["tw"]); n != 2 {
			return fmt.Errorf("%d election keys", n)
		}
		rec, ok := recordOf(t, ctx, servers["tw"])
		if !ok || rec.GetStartedAtNs() != first.GetStartedAtNs() || rec.GetEpoch() != first.GetEpoch() {
			return fmt.Errorf("record changed: %+v", &rec)
		}
		return nil
	})

	a.signal(syscall.SIGTERM)
	if code := a.exitCode(20 * time.Second); code != 0 {
		t.Fatalf("SIGTERM exit %d", code)
	}
	until(t, 30*time.Second, "the waiting process becomes leader", func() error {
		rec, ok := recordOf(t, ctx, servers["tw"])
		if !ok || rec.GetEpoch() != first.GetEpoch()+1 {
			return fmt.Errorf("record %+v present %v", &rec, ok)
		}
		return nil
	})
	if !b.running() {
		t.Fatal("leader exited")
	}
	b.signal(syscall.SIGTERM)
	if code := b.exitCode(20 * time.Second); code != 0 {
		t.Fatalf("second SIGTERM exit %d", code)
	}
	until(t, 10*time.Second, "the record is revoked", func() error {
		if _, ok := recordOf(t, ctx, servers["tw"]); ok {
			return errors.New("record still there")
		}
		return nil
	})
}

func TestSessionLostExitsNonZero(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw")
	topo := topology(servers, "tw")
	applyAuth(t, ctx, topo, servers)
	c := startChild(t, writeDeploy(t, topo))
	until(t, 30*time.Second, "the process writes its record", func() error {
		if _, ok := recordOf(t, ctx, servers["tw"]); !ok {
			return errors.New("no record")
		}
		return nil
	})
	if _, err := client(t, servers["tw"]).Revoke(ctx, recordLease(t, ctx, servers["tw"])); err != nil {
		t.Fatal(err)
	}
	if code := c.exitCode(20 * time.Second); code == 0 {
		t.Fatal("exited 0 after the session was lost")
	}
	until(t, 10*time.Second, "the record is gone", func() error {
		if _, ok := recordOf(t, ctx, servers["tw"]); ok {
			return errors.New("record still there")
		}
		return nil
	})
}

// Two deployments that disagree on the home region each elect and write a record
// in their own cluster only.
func TestInconsistentHomesBothElect(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	homeTW := topology(servers, "tw")
	homeJP := topology(servers, "jp")
	applyAuth(t, ctx, homeTW, servers)
	a := startChild(t, writeDeploy(t, homeTW))
	b := startChild(t, writeDeploy(t, homeJP))
	until(t, 30*time.Second, "each home has its own record", func() error {
		tw, twOK := recordOf(t, ctx, servers["tw"])
		jp, jpOK := recordOf(t, ctx, servers["jp"])
		if !twOK || !jpOK || tw.GetEpoch() < 1 || jp.GetEpoch() < 1 {
			return fmt.Errorf("tw %+v (%v) jp %+v (%v)", &tw, twOK, &jp, jpOK)
		}
		return nil
	})
	if !a.running() || !b.running() {
		t.Fatal("a process exited")
	}
}
