package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/td/account"
)

const (
	testInst = "td-1"

	envWorker   = "MFTIKO_TD_TEST_WORKER" // "1": this process is an account worker
	envJournal  = "MFTIKO_TD_JOURNAL"     // where the worker appends its start
	envCrash    = "MFTIKO_TD_CRASH"       // "1": the worker exits at once, as a crash
	envHedge    = "MFTIKO_TD_HEDGE"       // "1": the venue is in hedge mode
	envTestShim = "MFTIKO_TEST_SHIM"
	apiKey      = "test-key"
)

func TestMain(m *testing.M) {
	if os.Getenv(envWorker) == "1" {
		os.Exit(runWorker())
	}
	if os.Getenv(envTestShim) == "1" {
		os.Exit(shim.Run(logger.NewWithWriter("mftiko-shim", os.Stderr)))
	}
	os.Exit(m.Run())
}

// runWorker is the real account worker against a fake venue in this process: its private stream is
// served here, and NATS and the symbol table are in memory.
func runWorker() int {
	if p := os.Getenv(envJournal); p != "" {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err == nil {
			_ = json.NewEncoder(f).Encode(map[string]any{"event": "start", "pid": os.Getpid()})
			f.Close()
		}
	}
	if os.Getenv(envCrash) == "1" {
		return 20
	}
	v := exchangetest.NewAccountVenue(exchangetest.AccountOptions{Key: apiKey})
	v.SetHedgeMode(os.Getenv(envHedge) == "1")
	srv := httptest.NewServer(v.Handler())
	defer srv.Close()
	kind := v.Kind("Fake")
	open := kind.Account
	kind.Account = func(cfg exchange.AccountConfig) (exchange.Account, error) {
		cfg.StreamEndpoint = "ws" + strings.TrimPrefix(srv.URL, "http") // the host Init named is this server
		return open(cfg)
	}
	fd := os.NewFile(3, "ctl")
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	return account.Serve(ctx, fd, account.ServeConfig{
		Kinds: func(n string) (exchange.AccountKind, bool) { return kind, n == "Fake" },
		Doer:  v,
		Connect: func(context.Context, *tdctlv1.NatsCredentials, string) (*account.Env, error) {
			return &account.Env{Bus: nopBus{}, Instruments: anyInstrument{}}, nil
		},
		RetryAfter: 50 * time.Millisecond, RetryMax: 200 * time.Millisecond,
	})
}

type nopBus struct{}

func (nopBus) Publish(string, []byte) error { return nil }
func (nopBus) Respond(string, func([]byte) []byte) (func(), error) {
	return func() {}, nil
}

type anyInstrument struct{}

func (anyInstrument) Reverse(context.Context, symbol.ExchKey, bool) (*symbol.Info, error) {
	return &symbol.Info{UID: 7, ContractSize: 1}, nil
}

func (anyInstrument) GetByUID(context.Context, uint32, bool) (*symbol.Info, error) {
	return &symbol.Info{UID: 7, ContractSize: 1}, nil
}

type env struct {
	t       *testing.T
	root    *clientv3.Client
	cfg     Config
	key     *sealbox.PrivateKey
	journal string
	log     *syncBuf
}

func setup(t *testing.T) *env {
	t.Helper()
	s := etcdtest.Start(t)
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{testInst: "tw", state.ControllerInstance: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{testInst: state.PlaneTD})
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
	wd, err := os.MkdirTemp("/tmp", "td-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	key, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	buf := &syncBuf{}
	log := zerolog.New(buf).Level(zerolog.InfoLevel)
	e := &env{t: t, root: root, key: key, journal: filepath.Join(t.TempDir(), "starts"), log: buf}
	e.cfg = Config{
		Topology: topo, Instance: testInst, Dial: state.AuthDial(testInst, etcdtest.Password(testInst)),
		Term:      state.TermOptions{ID: t.Name()},
		WorkDir:   wd,
		Shim:      self,
		Account:   self,
		ShimEnv:   []string{envTestShim + "=1"},
		WorkerEnv: []string{envWorker + "=1", envJournal + "=" + e.journal},
		Kinds:     fakeKinds{"Fake": {Name: "Fake", Trades: []string{"Fake"}, Account: func(exchange.AccountConfig) (exchange.Account, error) { return nil, nil }}},
		Endpoints: map[string]Endpoint{"Fake": {Stream: "ws://replaced-by-the-worker"}},
		Open: func(sealed []byte, id int64, kind string) error {
			_, err := sealbox.OpenCredentials(key, sealed, id, kind)
			return err
		},
		Unseal: func(sealed []byte, id int64, kind string) (sealbox.Credentials, error) {
			return sealbox.OpenCredentials(key, sealed, id, kind)
		},
		User:       func(int64) (string, string, string, error) { return "jwt", "seed", "_INBOX_td_42", nil },
		Policy:     Policy{Backoff: agent.Backoff{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond}, Beat: 10 * time.Second, GiveUpAfter: 2},
		Linger:     500 * time.Millisecond,
		Resync:     25 * time.Millisecond,
		StaleAfter: 5 * time.Second,
		Mirror:     state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Release:    "test",
		Log:        log,
	}
	var mu sync.Mutex
	var pids []int
	e.cfg.Started = func(pid int) { mu.Lock(); pids = append(pids, pid); mu.Unlock() }
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already gone, mostly
		}
		for _, pid := range e.workerPIDs() {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already gone, mostly
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", buf.String())
		}
	})
	return e
}

func (e *env) start() (stop func(), done <-chan struct{}, ret *error) { return e.startWith(e.cfg) }

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

func (e *env) put(key string, m proto.Message) {
	e.t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.root.Put(ctx, key, string(b)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) del(key string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.root.Delete(ctx, key); err != nil {
		e.t.Fatal(err)
	}
}

// account declares api_id id of kind Fake, its credentials sealed to this instance's key.
func (e *env) account(id int64, enabled bool) {
	e.t.Helper()
	sealed, err := sealbox.SealCredentials(e.key.Public(), id, "Fake", sealbox.Credentials{KeyType: "hmac", Key: apiKey, Secret: "s-" + strconv.FormatInt(id, 10)})
	if err != nil {
		e.t.Fatal(err)
	}
	e.put(state.AccountKey(testInst, id), &statev1.Account{Name: "a", Kind: "Fake", SealedCredentials: sealed, Enabled: enabled})
}

func (e *env) demand(session string, id int64) {
	e.put(state.TDDemandKey(testInst, session, id), &statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: id}})
}

// ack waits for the ack of (session, id) to be in state st with reason.
func (e *env) ack(session string, id int64, st statev1.EntryState, reason string) {
	e.t.Helper()
	var last *statev1.EntryStatus
	e.waitMsg("ack "+session, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := e.root.Get(ctx, state.TDAckKey(testInst, session, id))
		if err != nil {
			e.t.Fatal(err)
		}
		if len(resp.Kvs) == 0 {
			return false
		}
		last = &statev1.EntryStatus{}
		if err := proto.Unmarshal(resp.Kvs[0].Value, last); err != nil {
			e.t.Fatal(err)
		}
		return last.GetState() == st && last.GetReason() == reason
	}, func() string { return last.String() })
}

func (e *env) acks() int {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.TDAckPrefix(testInst), clientv3.WithPrefix())
	if err != nil {
		e.t.Fatal(err)
	}
	return len(resp.Kvs)
}

// workerStatus is the account worker's WorkerStatus phase, "" when there is none.
func (e *env) workerStatus(id int64) string {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.WorkerStatusKey(state.PlaneTD, testInst, state.WorkerHash(string(WorkerKey(id)))))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	st := &statev1.WorkerStatus{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
		e.t.Fatal(err)
	}
	return st.GetPhase()
}

func (e *env) workerPIDs() []int {
	f, err := os.Open(e.journal)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line struct {
			PID int `json:"pid"`
		}
		if json.Unmarshal(sc.Bytes(), &line) == nil {
			out = append(out, line.PID)
		}
	}
	return out
}

func (e *env) record() *statev1.AgentRecord {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.AgentKey(state.PlaneTD, testInst))
	if err != nil || len(resp.Kvs) == 0 {
		return nil
	}
	rec := &statev1.AgentRecord{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, rec); err != nil {
		return nil
	}
	return rec
}

func (e *env) waitMsg(what string, ok func() bool, last func() string) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s; last %s", what, last())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) wait(what string, ok func() bool) {
	e.t.Helper()
	e.waitMsg(what, ok, func() string { return "" })
}

func waitDone(t *testing.T, done <-chan struct{}, ret *error) {
	t.Helper()
	select {
	case <-done:
		if ret == nil || !errors.Is(*ret, context.Canceled) {
			t.Fatalf("agent ended with %v", *ret)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the agent did not stop")
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

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

// ---------- the acceptance ----------

// Two sessions on one account get one worker. Both acks go READY. The worker stays while one owner
// remains, and stops only linger after the last one leaves.
func TestShareAndLinger(t *testing.T) {
	e := setup(t)
	stop, done, ret := e.start()
	e.account(42, true)
	e.demand("s1", 42)
	e.demand("s2", 42)
	e.ack("s1", 42, statev1.EntryState_ENTRY_STATE_READY, "")
	e.ack("s2", 42, statev1.EntryState_ENTRY_STATE_READY, "")
	if n := len(e.workerPIDs()); n != 1 {
		t.Fatalf("%d workers started", n)
	}

	e.del(state.TDDemandKey(testInst, "s1", 42))
	time.Sleep(e.cfg.Linger + 300*time.Millisecond)
	if ph := e.workerStatus(42); ph != runner.PhaseRunning {
		t.Fatalf("the worker stopped while an owner remained: %q", ph)
	}
	e.wait("one ack", func() bool { return e.acks() == 1 })

	e.del(state.TDDemandKey(testInst, "s2", 42))
	removed := time.Now()
	e.wait("no acks", func() bool { return e.acks() == 0 })
	e.wait("the worker stopped", func() bool { return e.workerStatus(42) == "" })
	if since := time.Since(removed); since < e.cfg.Linger {
		t.Fatalf("stopped %s after the last owner left, linger is %s", since, e.cfg.Linger)
	}
	if n := len(e.workerPIDs()); n != 1 {
		t.Fatalf("%d workers started", n)
	}
	stop()
	waitDone(t, done, ret)
}

// Replacing the agent process leaves the account worker running; the new term adopts it and does
// not start another.
func TestWorkerSurvivesAgentReplacement(t *testing.T) {
	e := setup(t)
	stop, done, ret := e.start()
	e.account(42, true)
	e.demand("s1", 42)
	e.ack("s1", 42, statev1.EntryState_ENTRY_STATE_READY, "")
	pids := e.workerPIDs()
	if len(pids) != 1 {
		t.Fatalf("pids %v", pids)
	}
	stop()
	waitDone(t, done, ret)
	if !alive(pids[0]) {
		t.Fatal("the account worker died with the agent")
	}

	cfg := e.cfg
	cfg.Term.ID = t.Name() + "-next"
	stop, _, _ = e.startWith(cfg)
	defer stop()
	e.wait("the next term", func() bool { return e.record().GetEpoch() == 2 })
	time.Sleep(500 * time.Millisecond)
	if n := len(e.workerPIDs()); n != 1 || !alive(pids[0]) {
		t.Fatalf("workers %v", e.workerPIDs())
	}
	e.ack("s1", 42, statev1.EntryState_ENTRY_STATE_READY, "")
	if ph := e.workerStatus(42); ph != runner.PhaseRunning {
		t.Fatalf("phase %q", ph)
	}
}

// A worker that keeps crashing is restarted until the Policy gives up; the ack is then DEAD (P6).
func TestDeadAfterGivingUp(t *testing.T) {
	e := setup(t)
	e.cfg.ExtraEnv = func(agent.WorkerKey) []string { return []string{envCrash + "=1"} }
	stop, done, ret := e.start()
	e.account(42, true)
	e.demand("s1", 42)
	e.ack("s1", 42, statev1.EntryState_ENTRY_STATE_DEAD, "gave up")
	if n := len(e.workerPIDs()); n != 3 { // the first start and GiveUpAfter restarts
		t.Fatalf("%d starts", n)
	}
	stop()
	waitDone(t, done, ret)
}

// A worker that cannot run in hedge mode is REJECTED and not restarted (D608).
func TestHedgeModeRejectedOnce(t *testing.T) {
	e := setup(t)
	e.cfg.ExtraEnv = func(agent.WorkerKey) []string { return []string{envHedge + "=1"} }
	stop, done, ret := e.start()
	e.account(42, true)
	e.demand("s1", 42)
	e.ack("s1", 42, statev1.EntryState_ENTRY_STATE_REJECTED, account.ReasonHedgeMode)
	time.Sleep(500 * time.Millisecond)
	if n := len(e.workerPIDs()); n != 1 {
		t.Fatalf("restarted: %d starts", n)
	}
	stop()
	waitDone(t, done, ret)
}

// Accounts the agent refuses get no worker.
func TestRejectedAccounts(t *testing.T) {
	e := setup(t)
	stop, done, ret := e.start()
	e.account(2, false)
	other, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealbox.SealCredentials(other.Public(), 3, "Fake", sealbox.Credentials{KeyType: "hmac", Key: apiKey, Secret: "x"})
	if err != nil {
		t.Fatal(err)
	}
	e.put(state.AccountKey(testInst, 3), &statev1.Account{Kind: "Fake", SealedCredentials: sealed, Enabled: true})
	e.put(state.AccountKey(testInst, 4), &statev1.Account{Kind: "Nope", Enabled: true})
	for id := int64(1); id <= 4; id++ {
		e.demand("s", id)
	}
	e.ack("s", 1, statev1.EntryState_ENTRY_STATE_REJECTED, ReasonNoAccount)
	e.ack("s", 2, statev1.EntryState_ENTRY_STATE_REJECTED, ReasonDisabled)
	e.ack("s", 3, statev1.EntryState_ENTRY_STATE_REJECTED, "credentials_other_key")
	e.ack("s", 4, statev1.EntryState_ENTRY_STATE_REJECTED, ReasonUnknownKind)
	if n := len(e.workerPIDs()); n != 0 {
		t.Fatalf("%d workers started", n)
	}
	// Declaring the missing account brings its worker up.
	e.account(1, true)
	e.ack("s", 1, statev1.EntryState_ENTRY_STATE_READY, "")
	stop()
	waitDone(t, done, ret)
}
