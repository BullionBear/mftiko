package agent

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

const (
	envFake     = "MFTIKO_MD_FAKE"
	envReport   = "MFTIKO_MD_REPORT"
	envCrash    = "MFTIKO_MD_CRASH"
	envTestShim = "MFTIKO_TEST_SHIM"
)

func TestMain(m *testing.M) {
	if os.Getenv(envFake) == "1" {
		os.Exit(runFake())
	}
	if os.Getenv(envTestShim) == "1" {
		os.Exit(shim.Run(logger.NewWithWriter("mftiko-shim", os.Stderr)))
	}
	os.Exit(m.Run())
}

// Two feeds land on one conn. Dropping one leaves that worker up past linger.
// Dropping the last owner stops the conn worker only after linger.
func TestFeedShareAndLinger(t *testing.T) {
	e := setup(t)
	stop, done, ret := e.start()
	e.put("s1", "orderbook")
	e.put("s1", "orderbook_delta")
	path := e.waitJournal()
	if got := e.waitLive(path, 2); got.GetAtoms()[0].GetAtomId() != "" {
		t.Fatalf("desired %+v", got)
	}
	if conn := e.waitPlacements(2); conn[0] != "md/Binance/public/0" || conn[1] != "md/Binance/public/0" {
		t.Fatalf("placement %v", conn)
	}
	e.waitAcks(2)
	e.waitRunning("md/Binance/public/0", 1)

	e.del("s1", "orderbook_delta")
	if got := e.waitLive(path, 1); got.GetAtoms()[0].GetFeeds()[0] != feedKey("orderbook") {
		t.Fatalf("after one removal: %+v", got)
	}
	time.Sleep(e.cfg.Linger + 200*time.Millisecond)
	if n := countEvent(t, path, "stop"); n != 0 {
		t.Fatalf("unsubscribed while an owner remained: %d stops", n)
	}
	e.waitAcks(1)
	if conn := e.waitPlacements(1); conn[0] != "md/Binance/public/0" {
		t.Fatalf("placement %v", conn)
	}

	e.del("s1", "orderbook")
	removed := time.Now()
	time.Sleep(200 * time.Millisecond)
	if n := countEvent(t, path, "stop"); n != 0 {
		t.Fatalf("unsubscribed %s after the last owner left, linger is %s", time.Since(removed), e.cfg.Linger)
	}
	e.waitEvent(path, "stop")
	if time.Since(removed) < e.cfg.Linger {
		t.Fatalf("stop arrived after %s, linger is %s", time.Since(removed), e.cfg.Linger)
	}
	e.waitPlacements(0)
	e.waitAcks(0)

	stop()
	waitDone(t, done, ret)
}

// Replacing the agent process leaves the conn worker running. The new term adopts it.
func TestAdoptedConnSurvivesAgentReplacement(t *testing.T) {
	e := setup(t)
	stop, done, ret := e.start()
	e.put("s1", "orderbook")
	path := e.waitJournal()
	e.waitLive(path, 1)
	e.waitRunning("md/Binance/public/0", 1)
	pid := journalPID(t, path)
	if !alive(pid) {
		t.Fatal("worker pid is not alive")
	}

	stop()
	waitDone(t, done, ret)
	if !alive(pid) {
		t.Fatal("the conn worker died with the agent")
	}
	if n := countEvent(t, path, "start"); n != 1 {
		t.Fatalf("starts %d", n)
	}

	cfg := e.cfg
	cfg.Term.ID = t.Name() + "-next"
	stop, _, _ = e.startWith(cfg)
	defer stop()
	deadline := time.Now().Add(15 * time.Second)
	for e.record().GetEpoch() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no epoch 2 record: %+v", e.record())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond)
	if n := countEvent(t, path, "start"); n != 1 {
		t.Fatalf("the adopted worker was spawned again: %d starts", n)
	}
	if n := countEvent(t, path, "init"); n != 1 {
		t.Fatalf("init sent again: %d", n)
	}
	if !alive(pid) {
		t.Fatal("the adopted worker is gone")
	}
	e.waitRunning("md/Binance/public/0", 1)
}

// A crashed conn worker is respawned with the conn_epoch its shim last reported.
func TestRespawnCarriesConnEpoch(t *testing.T) {
	e := setup(t)
	var once atomic.Bool
	e.cfg.ExtraEnv = func(agent.WorkerKey) []string {
		if once.CompareAndSwap(false, true) {
			return []string{envReport + "=9", envCrash + "=1"}
		}
		return nil
	}
	stop, done, ret := e.start()
	defer func() {
		stop()
		waitDone(t, done, ret)
	}()
	e.put("s1", "orderbook")
	path := e.waitJournal()
	var epochs []uint64
	deadline := time.Now().Add(15 * time.Second)
	for len(epochs) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("init epochs %v", epochs)
		}
		epochs = initEpochs(t, path)
		time.Sleep(20 * time.Millisecond)
	}
	if epochs[0] != 0 || epochs[1] != 9 {
		t.Fatalf("init epochs %v, want 0 then 9", epochs)
	}
	e.waitRunning("md/Binance/public/0", 2)
}

type jline struct {
	Event   string `json:"event"`
	PID     int    `json:"pid,omitempty"`
	Payload string `json:"payload,omitempty"`
}

type env struct {
	t    *testing.T
	root *clientv3.Client
	cfg  Config
	log  *syncBuf
}

func setup(t *testing.T) *env {
	t.Helper()
	s := etcdtest.Start(t)
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{testInst: "tw", state.ControllerInstance: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{testInst: state.PlaneMD})
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "md-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	buf := &syncBuf{}
	log := zerolog.New(buf).Level(zerolog.InfoLevel)
	e := &env{t: t, root: root, log: buf}
	e.cfg = Config{
		Topology: topo, Instance: testInst, Dial: state.AuthDial(testInst, etcdtest.Password(testInst)),
		Term:       state.TermOptions{ID: t.Name()},
		WorkDir:    wd,
		Shim:       self,
		Conn:       self,
		ShimEnv:    []string{envTestShim + "=1"},
		WorkerEnv:  []string{envFake + "=1"},
		JournalDir: t.TempDir(),
		Markets:    testMarkets(1024, 1),
		Policy:     agent.Always{Backoff: agent.Backoff{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond}},
		Linger:     500 * time.Millisecond,
		Resync:     25 * time.Millisecond,
		StaleAfter: 5 * time.Second,
		LiveRetry:  30 * time.Second,
		Mirror:     state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Release:    "test",
		Log:        log,
	}
	var mu sync.Mutex
	var pids []int
	e.cfg.Started = func(pid int) {
		mu.Lock()
		pids = append(pids, pid)
		mu.Unlock()
	}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already gone, mostly
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", buf.String())
		}
	})
	return e
}

func (e *env) start() (stop func(), done <-chan struct{}, ret *error) {
	return e.startWith(e.cfg)
}

func (e *env) startWith(cfg Config) (stop func(), done <-chan struct{}, ret *error) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	var err error
	go func() { err = Run(ctx, cfg); close(ch) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			e.t.Error("the agent did not stop")
		}
	})
	return cancel, ch, &err
}

func (e *env) put(session, topic string) {
	e.t.Helper()
	key := feedKey(topic)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.root.Put(ctx, state.MDDemandKey(testInst, session, "feed/"+key), string(mustDemand(key))); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) del(session, topic string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.root.Delete(ctx, state.MDDemandKey(testInst, session, "feed/"+feedKey(topic))); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) waitJournal() string {
	e.t.Helper()
	var path string
	e.wait(func() bool {
		m, err := filepath.Glob(filepath.Join(e.cfg.JournalDir, "*.journal"))
		if err != nil {
			e.t.Fatal(err)
		}
		if len(m) != 1 {
			return false
		}
		path = m[0]
		return true
	})
	return path
}

func (e *env) waitLive(path string, feeds int) *mdctlv1.Desired {
	e.t.Helper()
	var got *mdctlv1.Desired
	e.wait(func() bool {
		last := lastLive(e.t, path)
		if last == nil || len(last.GetAtoms()) != 1 || len(last.GetAtoms()[0].GetFeeds()) != feeds {
			return false
		}
		got = last
		return true
	})
	return got
}

func (e *env) waitPlacements(n int) []string {
	e.t.Helper()
	var conns []string
	e.wait(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := e.root.Get(ctx, state.PlacementPrefix(testInst), clientv3.WithPrefix())
		if err != nil {
			e.t.Fatal(err)
		}
		conns = conns[:0]
		for _, kv := range resp.Kvs {
			msg := &statev1.Placement{}
			if err := proto.Unmarshal(kv.Value, msg); err != nil {
				e.t.Fatal(err)
			}
			conns = append(conns, msg.GetConn())
		}
		return len(conns) == n
	})
	return conns
}

func (e *env) waitAcks(n int) {
	e.t.Helper()
	e.wait(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := e.root.Get(ctx, state.MDAckPrefix(testInst), clientv3.WithPrefix())
		if err != nil {
			e.t.Fatal(err)
		}
		if len(resp.Kvs) != n {
			return false
		}
		for _, kv := range resp.Kvs {
			st := &statev1.EntryStatus{}
			if err := proto.Unmarshal(kv.Value, st); err != nil {
				e.t.Fatal(err)
			}
			if st.GetState() != statev1.EntryState_ENTRY_STATE_PENDING || st.GetSubject() == "" {
				e.t.Fatalf("ack %s: %+v", kv.Key, st)
			}
		}
		return true
	})
}

func (e *env) waitRunning(conn string, inc uint32) {
	e.t.Helper()
	key := state.WorkerStatusKey(state.PlaneMD, testInst, state.WorkerHash(conn))
	e.wait(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := e.root.Get(ctx, key)
		if err != nil {
			e.t.Fatal(err)
		}
		if len(resp.Kvs) != 1 {
			return false
		}
		st := &statev1.WorkerStatus{}
		if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
			e.t.Fatal(err)
		}
		return st.GetPhase() == runner.PhaseRunning && st.GetWorker().GetIncarnation() == inc
	})
}

func (e *env) waitEvent(path, event string) {
	e.t.Helper()
	e.wait(func() bool { return countEvent(e.t, path, event) > 0 })
}

func (e *env) record() *statev1.AgentRecord {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.AgentKey(state.PlaneMD, testInst))
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

func (e *env) wait(ok func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatal("timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitDone(t *testing.T, done <-chan struct{}, ret *error) {
	t.Helper()
	select {
	case <-done:
		if ret == nil || !errors.Is(*ret, context.Canceled) {
			t.Fatalf("agent ended with %v", ret)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the agent did not stop")
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func readJournal(t *testing.T, path string) []jline {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []jline
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line jline
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func countEvent(t *testing.T, path, event string) int {
	t.Helper()
	n := 0
	for _, line := range readJournal(t, path) {
		if line.Event == event {
			n++
		}
	}
	return n
}

func journalPID(t *testing.T, path string) int {
	t.Helper()
	for _, line := range readJournal(t, path) {
		if line.Event == "start" && line.PID != 0 {
			return line.PID
		}
	}
	t.Fatal("no start pid")
	return 0
}

func lastLive(t *testing.T, path string) *mdctlv1.Desired {
	t.Helper()
	var got *mdctlv1.Desired
	for _, line := range readJournal(t, path) {
		if line.Event != "live" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(line.Payload)
		if err != nil {
			t.Fatal(err)
		}
		d := &mdctlv1.Desired{}
		if err := proto.Unmarshal(raw, d); err != nil {
			t.Fatal(err)
		}
		got = d
	}
	return got
}

func initEpochs(t *testing.T, path string) []uint64 {
	t.Helper()
	var out []uint64
	for _, line := range readJournal(t, path) {
		if line.Event != "init" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(line.Payload)
		if err != nil {
			t.Fatal(err)
		}
		p := &mdctlv1.InitPayload{}
		if err := proto.Unmarshal(raw, p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p.GetConnEpoch())
	}
	return out
}

type syncBuf struct {
	mu sync.Mutex
	b  []byte
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b = append(s.b, p...)
	return len(p), nil
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.b)
}

func runFake() int {
	f := &fake{down: make(chan *workerv1.WorkerDown, 16)}
	if path := os.Getenv(envJournal); path != "" {
		j, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			fmtErr(err)
			return 1
		}
		f.j = j
	}
	f.record(jline{Event: "start", PID: os.Getpid()})
	fd := shim.CtlFD
	if v := os.Getenv(shim.EnvCtlFD); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			fd = n
		}
	}
	ctl := os.NewFile(uintptr(fd), "ctl")
	if ctl == nil {
		fmtErr(errors.New("no control fd"))
		return 1
	}
	syscall.CloseOnExec(fd)
	f.out = frame.NewWriter(ctl)
	go f.read(ctl)
	f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Hello{Hello: &workerv1.Hello{
		Kind: "conn", Release: "test", Contracts: []string{conn.Contract}, Pid: int32(os.Getpid()),
	}}})
	stopBeat := make(chan struct{})
	go f.beat(stopBeat)
	defer close(stopBeat)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)
	for {
		select {
		case m, ok := <-f.down:
			if !ok {
				return 0
			}
			switch m := m.Msg.(type) {
			case *workerv1.WorkerDown_Init:
				f.record(jline{Event: "init", Payload: base64.StdEncoding.EncodeToString(m.Init.GetPayload())})
				f.send(doneUp(workerv1.Verb_VERB_INIT, true))
			case *workerv1.WorkerDown_Live:
				f.record(jline{Event: "live", Payload: base64.StdEncoding.EncodeToString(m.Live.GetPayload())})
				f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Applied{Applied: &workerv1.Applied{Digest: m.Live.GetDigest()}}})
			case *workerv1.WorkerDown_Start:
				f.send(doneUp(workerv1.Verb_VERB_START, true))
				f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: "running"}}})
				if os.Getenv(envCrash) == "1" {
					f.reportCrash()
					time.Sleep(150 * time.Millisecond)
					return 20
				}
			case *workerv1.WorkerDown_Stop:
				return f.finish()
			}
		case <-sig:
			return f.finish()
		}
	}
}

type fake struct {
	mu   sync.Mutex
	j    *os.File
	out  *frame.Writer
	down chan *workerv1.WorkerDown
}

func (f *fake) read(ctl *os.File) {
	rd := frame.NewReader(bufio.NewReader(ctl))
	for {
		m := &workerv1.WorkerDown{}
		if err := rd.Read(m); err != nil {
			close(f.down)
			return
		}
		f.down <- m
	}
}

func (f *fake) beat(stop <-chan struct{}) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	var seq uint64
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			seq++
			f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Beat{Beat: &workerv1.Beat{Seq: seq}}})
		}
	}
}

func (f *fake) reportCrash() {
	n, _ := strconv.ParseUint(os.Getenv(envReport), 10, 64)
	body, err := proto.Marshal(&mdctlv1.Report{ConnEpoch: n, State: mdctlv1.ConnState_CONN_STATE_CONNECTING})
	if err != nil {
		return
	}
	f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Report{Report: &workerv1.Report{
		Contract: conn.Contract, Payload: body,
	}}})
}

func (f *fake) finish() int {
	f.record(jline{Event: "stop"})
	f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{
		Outcome: commonv1.Outcome_OUTCOME_DONE, Reason: "stop",
	}}})
	f.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stopped{Stopped: &workerv1.Stopped{OnStopCompleted: true}}})
	return 0
}

func (f *fake) send(m *workerv1.WorkerUp) {
	if f.out == nil {
		return
	}
	f.out.Write(m) //nolint:errcheck // the shim is gone
}

func (f *fake) record(line jline) {
	if f.j == nil {
		return
	}
	b, err := json.Marshal(line)
	if err != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.j.Write(append(b, '\n')) //nolint:errcheck // the test journal
	f.j.Sync()                 //nolint:errcheck
}

func doneUp(v workerv1.Verb, ok bool) *workerv1.WorkerUp {
	return &workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: v, Ok: ok}}}
}

func fmtErr(err error) { os.Stderr.WriteString(err.Error() + "\n") } //nolint:errcheck
