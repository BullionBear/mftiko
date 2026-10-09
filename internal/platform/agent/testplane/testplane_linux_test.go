package testplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// The test plane end to end: etcd with auth and RBAC, the agent skeleton, real shims (this test binary
// re-executed), fake workers under them (E1-15's done criteria).

const envTestShim = "MFTIKO_TEST_SHIM"

func TestMain(m *testing.M) {
	fakeworker.MaybeMain()
	if os.Getenv(envTestShim) == "1" {
		os.Exit(shim.Run(logger.NewWithWriter("mftiko-shim", os.Stderr)))
	}
	os.Exit(m.Run())
}

const (
	plane = state.PlaneTD
	inst  = "td-tw-1"
)

// syncBuf is the agent's log, shown when a test fails.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type env struct {
	t    *testing.T
	root *clientv3.Client
	cfg  Config
	log  *syncBuf

	mu   sync.Mutex
	pids []int // shims started
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// setup starts etcd, applies the auth tables of a one-region topology with the agent td-tw-1, and configures the
// agent to dial as its own user.
func setup(t *testing.T, regions map[string]state.Region) *env {
	t.Helper()
	s := etcdtest.Start(t)
	if regions == nil {
		regions = map[string]state.Region{}
	}
	regions["tw"] = state.Region{Endpoints: s.Endpoints()}
	topo := state.Topology{Regions: regions, Instances: map[string]string{inst: "tw", state.ControllerInstance: "tw"}}
	tables, err := state.AuthTables(topo, map[string]state.Plane{inst: plane})
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	if err := state.EnsureAuth(testCtx(t), root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "tp-") // short: sockets live under it
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	e := &env{t: t, root: root, log: &syncBuf{}}
	e.cfg = Config{
		Topology: topo, Plane: plane, Instance: inst, Dial: state.AuthDial(inst, etcdtest.Password(inst)),
		Term:    state.TermOptions{ID: t.Name()},
		WorkDir: wd, Shim: self, ShimEnv: []string{envTestShim + "=1"},
		Program: Program{Binary: self, JournalDir: t.TempDir()},
		Resync:  50 * time.Millisecond,
		Started: func(pid int) { e.mu.Lock(); e.pids = append(e.pids, pid); e.mu.Unlock() },
		Log:     zerolog.New(e.log),
	}
	t.Cleanup(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, pid := range e.pids { // the worker follows its shim (Pdeathsig, S2)
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // gone already, mostly
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", e.log.String())
		}
	})
	return e
}

// run starts the agent; done is closed once Run has returned, *err.
func (e *env) run() (done <-chan struct{}, err *error) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	var ret error
	go func() { ret = Run(ctx, e.cfg); close(ch) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			e.t.Error("the agent did not stop")
		}
	})
	return ch, &ret
}

// want writes a demand entry, as the controller would.
func (e *env) want(name string, w Worker) {
	e.t.Helper()
	b, err := json.Marshal(w)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.root.Put(testCtx(e.t), state.DemandPrefix(plane, inst)+name, string(b)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) unwant(name string) {
	e.t.Helper()
	if _, err := e.root.Delete(testCtx(e.t), state.DemandPrefix(plane, inst)+name); err != nil {
		e.t.Fatal(err)
	}
}

func statusKey(name string) string {
	return state.WorkerStatusKey(plane, inst, state.WorkerHash(string(Key(plane, name))))
}

// status is the WorkerStatus of the worker named name, and its key's mod revision; nil if there is none.
func (e *env) status(name string) (*statev1.WorkerStatus, int64) {
	e.t.Helper()
	resp, err := e.root.Get(testCtx(e.t), statusKey(name))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, 0
	}
	st := &statev1.WorkerStatus{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
		e.t.Fatal(err)
	}
	return st, resp.Kvs[0].ModRevision
}

// waitStatus waits for the WorkerStatus of name to satisfy ok (nil for a status that does not exist).
func (e *env) waitStatus(name, what string, ok func(*statev1.WorkerStatus) bool) *statev1.WorkerStatus {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		st, _ := e.status(name)
		if ok(st) {
			return st
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("WorkerStatus of %s: %v, want %s", name, st, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func phase(p string, inc uint32) func(*statev1.WorkerStatus) bool {
	return func(st *statev1.WorkerStatus) bool {
		return st.GetPhase() == p && (inc == 0 || st.GetWorker().GetIncarnation() == inc)
	}
}

func gone(st *statev1.WorkerStatus) bool { return st == nil }

// downs are the messages the worker named name received, every incarnation's.
func (e *env) downs(name string) []*workerv1.WorkerDown {
	e.t.Helper()
	entries, err := fakeworker.ReadJournal(e.cfg.Program.Journal(name))
	if err != nil {
		e.t.Fatal(err)
	}
	var out []*workerv1.WorkerDown
	for _, en := range entries {
		if en.Event == "down" {
			m, err := en.Down()
			if err != nil {
				e.t.Fatal(err)
			}
			out = append(out, m)
		}
	}
	return out
}

func (e *env) waitDown(name, what string, ok func(*workerv1.WorkerDown) bool) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !slices.ContainsFunc(e.downs(name), ok) {
		if time.Now().After(deadline) {
			e.t.Fatalf("worker %s got no %s: %v", name, what, e.downs(name))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) record() *statev1.AgentRecord {
	e.t.Helper()
	resp, err := e.root.Get(testCtx(e.t), state.AgentKey(plane, inst))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	rec := &statev1.AgentRecord{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, rec); err != nil {
		e.t.Fatal(err)
	}
	return rec
}

func (e *env) waitRecord() *statev1.AgentRecord {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if rec := e.record(); rec != nil {
			return rec
		}
		if time.Now().After(deadline) {
			e.t.Fatal("no AgentRecord")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The test plane starts and stops fake workers through the skeleton: a demand entry becomes a worker that is
// initialized and started; a new live config is pushed; WorkerStatus is written as the phase changes and not on
// every Beat; the entry's removal stops the worker, releases its shim and deletes its status.
func TestStartsAndStops(t *testing.T) {
	e := setup(t, nil)
	e.run()
	if rec := e.waitRecord(); rec.GetEpoch() != 1 || rec.GetInstance() != inst || rec.GetPlane() != string(plane) {
		t.Fatalf("AgentRecord %v", rec)
	}

	e.want("a", Worker{Script: fakeworker.Script{BeatMS: 20}, Live: "v1"})
	st := e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	if st.GetRelease() != "fake" || st.GetStartedAtNs() == 0 {
		t.Fatalf("WorkerStatus %v", st)
	}
	e.waitDown("a", "Init with v1", func(m *workerv1.WorkerDown) bool { return string(m.GetInit().GetPayload()) == "v1" })
	e.waitDown("a", "Start", func(m *workerv1.WorkerDown) bool { return m.GetStart() != nil })

	_, rev := e.status("a")
	time.Sleep(time.Second) // 50 Beats, 20 resyncs
	if _, now := e.status("a"); now != rev {
		t.Fatalf("WorkerStatus rewritten while nothing changed: revision %d, then %d", rev, now)
	}

	e.want("a", Worker{Script: fakeworker.Script{BeatMS: 20}, Live: "v2"})
	e.waitDown("a", "Relay with v2", func(m *workerv1.WorkerDown) bool { return string(m.GetRelay().GetPayload()) == "v2" })

	sock := shim.SocketPath(e.cfg.WorkDir, string(plane), string(Key(plane, "a")))
	e.unwant("a")
	e.waitDown("a", "Stop", func(m *workerv1.WorkerDown) bool { return m.GetStop() != nil })
	e.waitStatus("a", "none", gone)
	if _, err := os.Lstat(sock); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the shim's socket is still there: %v", err)
	}
}

// A worker that ends on its own is spawned again, as a new incarnation; a held one is initialized and not started.
func TestRestartsAndHolds(t *testing.T) {
	e := setup(t, nil)
	e.run()
	e.want("crash", Worker{Script: fakeworker.Script{Exit: fakeworker.ExitCrashed, ExitAfterMS: 200}})
	e.want("held", Worker{Hold: true})
	st := e.waitStatus("crash", "a second incarnation", func(st *statev1.WorkerStatus) bool {
		return st.GetWorker().GetIncarnation() >= 2
	})
	if st.GetRestarts() == 0 {
		t.Fatalf("WorkerStatus %v, want restarts counted", st)
	}
	e.waitStatus("held", "STARTING", phase(runner.PhaseStarting, 1))
	time.Sleep(300 * time.Millisecond)
	if slices.ContainsFunc(e.downs("held"), func(m *workerv1.WorkerDown) bool { return m.GetStart() != nil }) {
		t.Fatal("a held worker was started")
	}
	e.want("held", Worker{}) // the controller allows it
	e.waitStatus("held", "RUNNING", phase(runner.PhaseRunning, 1))
}

// When the term ends the agent stops and returns ErrTermEnded; the worker goes on under its shim (D102).
func TestTermEnds(t *testing.T) {
	e := setup(t, nil)
	done, ret := e.run()
	e.waitRecord()
	e.want("a", Worker{})
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))

	resp, err := e.root.Get(testCtx(t), state.AgentKey(plane, inst))
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("record: %v", err)
	}
	if _, err := e.root.Revoke(testCtx(t), clientv3.LeaseID(resp.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if err := *ret; !errors.Is(err, runner.ErrTermEnded) || !errors.Is(err, state.ErrSessionLost) {
			t.Fatalf("Run returned %v, want ErrTermEnded with ErrSessionLost", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the agent went on after its session was lost")
	}

	sock := shim.SocketPath(e.cfg.WorkDir, string(plane), string(Key(plane, "a")))
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Lstat(sock); err != nil {
		t.Fatalf("the shim went with the agent: %v", err)
	}
	if st, _ := e.status("a"); st.GetPhase() != runner.PhaseRunning {
		t.Fatalf("WorkerStatus %v, want RUNNING kept", st)
	}
	if slices.ContainsFunc(e.downs("a"), func(m *workerv1.WorkerDown) bool { return m.GetStop() != nil }) {
		t.Fatal("the worker was stopped")
	}
}

// The agent connects to its own region only: the other cluster in the topology is never dialled.
func TestOwnRegionOnly(t *testing.T) {
	e := setup(t, map[string]state.Region{"jp": {Endpoints: []string{"127.0.0.1:1"}}})
	cl, term, err := runner.Connect(testCtx(t), e.cfg.Topology, plane, inst, e.cfg.Dial, state.TermOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	defer term.Close(context.Background())
	if _, err := cl.Reader("jp"); !errors.Is(err, state.ErrRegionNotAllowed) {
		t.Fatalf("Reader(jp): %v, want ErrRegionNotAllowed", err)
	}
	if _, err := cl.Reader("tw"); err != nil {
		t.Fatal(err)
	}
}

// workerPid is the pid the last incarnation of the worker named name journalled.
func (e *env) workerPid(name string) int {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		entries, err := fakeworker.ReadJournal(e.cfg.Program.Journal(name))
		if err != nil {
			e.t.Fatal(err)
		}
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].Event == "ids" {
				return entries[i].PID
			}
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("worker %s journalled no pid", name)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) shimPids() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.pids)
}

// At most one incarnation of a worker key lives at a time (E1-16): a worker that outlives its killed shim (it
// ignores SIGTERM and EOF, which D104 forbids, so that the case can be made) holds its next incarnation back until
// it is gone; then the next one starts.
func TestOneIncarnationAtATime(t *testing.T) {
	e := setup(t, nil)
	e.run()
	e.want("a", Worker{Script: fakeworker.Script{IgnoreSIGTERM: true, IgnoreEOF: true}})
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	pid := e.workerPid("a")
	t.Cleanup(func() { syscall.Kill(pid, syscall.SIGKILL) }) //nolint:errcheck // killed below, mostly

	if err := syscall.Kill(e.shimPids()[0], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	e.waitStatus("a", "LOST", phase(runner.PhaseLost, 1))
	time.Sleep(time.Second) // a few retries of the spawn, all held back
	if st, _ := e.status("a"); st.GetWorker().GetIncarnation() != 1 || len(e.shimPids()) != 1 {
		t.Fatalf("a second incarnation while the first runs: %v, %d shims", st, len(e.shimPids()))
	}
	if !strings.Contains(e.log.String(), "the last incarnation is still running") {
		t.Fatalf("no spawn held back:\n%s", e.log.String())
	}

	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	e.waitStatus("a", "a second incarnation", phase(runner.PhaseRunning, 2))
}

// A worker that stops beating is reported to the Policy past its timeout (E1-18); this Policy has it stopped, and
// it comes back as the next incarnation.
func TestBeatTimeout(t *testing.T) {
	e := setup(t, nil)
	e.cfg.Policy = agent.Always{Backoff: agent.Backoff{Min: 100 * time.Millisecond, Max: time.Second},
		Beat: 2 * time.Second, StopSilent: true}
	e.run()
	e.want("a", Worker{Script: fakeworker.Script{BeatMS: 100, Beats: 15}}) // 1.5s of Beats, then none
	e.waitStatus("a", "RUNNING", phase(runner.PhaseRunning, 1))
	start := time.Now()
	e.waitDown("a", "Stop for the silence", func(m *workerv1.WorkerDown) bool {
		return m.GetStop().GetReason() == agent.ReasonSilent
	})
	if d := time.Since(start); d < time.Second || d > 8*time.Second { // Beats end ~1.5s in; 2s of silence
		t.Fatalf("stopped %s after RUNNING: want the 2s timeout after the last Beat, within a few resyncs", d)
	}
	e.waitStatus("a", "a second incarnation", phase(runner.PhaseRunning, 2))
}
