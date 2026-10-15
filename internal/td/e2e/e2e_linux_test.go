package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/td/account"
	tdagent "github.com/BullionBear/mftiko/internal/td/agent"
)

const (
	inst    = "td-1"
	apiID   = 42
	apiKey  = "test-key"
	algo    = 3
	uid     = 7
	ticker  = "BTCUSDT"
	session = "s1"

	envWorker   = "MFTIKO_TD_E2E_WORKER" // "1": this process is an account worker
	envJournal  = "MFTIKO_TD_JOURNAL"    // where the worker appends its start
	envCrash    = "MFTIKO_TD_CRASH"      // "1": the worker exits at once, as a crash
	envNATS     = "MFTIKO_NATS_URL"
	envTestShim = "MFTIKO_TEST_SHIM"
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

// ---------- the account worker ----------

// runWorker is mftiko-td-account as its main wires it, but for the symbol table: the venue's REST
// and private stream are the test process's, at the hosts Init names, reached with the real Doer;
// td.order, td.evt and td.view are on the test's NATS.
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
	log := logger.NewWithWriter("mftiko-td-account", os.Stderr)
	inc, _ := strconv.ParseUint(os.Getenv("MFTIKO_INCARNATION"), 10, 32)
	// Only the adapter of this venue is used: the venue the worker talks to is the test's.
	kind := exchangetest.NewAccountVenue(exchangetest.AccountOptions{Key: apiKey}).Kind("Fake")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	return account.Serve(ctx, os.NewFile(3, "ctl"), account.ServeConfig{
		Kinds:       func(n string) (exchange.AccountKind, bool) { return kind, n == "Fake" },
		Doer:        fasthttp.New(nil, fasthttp.Options{}),
		WorkerKey:   os.Getenv("MFTIKO_WORKER_KEY"),
		Incarnation: uint32(inc),
		Connect: func(ctx context.Context, _ *tdctlv1.NatsCredentials, _ string) (*account.Env, error) {
			nc, err := natsx.Connect(os.Getenv(envNATS), natsx.Options{Name: "mftiko-td-account"})
			if err != nil {
				return nil, err
			}
			return &account.Env{Bus: bus{ctx: ctx, nc: nc}, Instruments: table{}, Close: nc.Close}, nil
		},
		RetryAfter: 50 * time.Millisecond, RetryMax: 200 * time.Millisecond,
		Log: &log,
	})
}

// bus is cmd/mftiko-td-account's.
type bus struct {
	ctx context.Context
	nc  *natsx.Conn
}

func (b bus) Publish(subject string, data []byte) error { return b.nc.Publish(subject, data) }

func (b bus) Respond(subject string, h func(req []byte) []byte) (func(), error) {
	sub, err := b.nc.Subscribe(b.ctx, subject, "", func(m natsx.Msg) {
		if m.Reply == "" {
			return
		}
		_ = b.nc.Publish(m.Reply, h(append([]byte(nil), m.Data...)))
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

// table is a symbol table of one tradable perpetual.
type table struct{}

func instrument() *symbol.Info {
	return &symbol.Info{
		UID: uid, ExchTicker: ticker, ContractSize: 1, Status: symbol.Tradable,
		Ticker:  symbol.UniversalTicker{Venue: "Fake", Category: symbol.Perp},
		Filters: symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001, symbol.MinQty: 0.001},
	}
}

func (table) Reverse(_ context.Context, k symbol.ExchKey, _ bool) (*symbol.Info, error) {
	if k.ExchTicker != ticker {
		return nil, errors.New("not found")
	}
	return instrument(), nil
}

func (table) GetByUID(_ context.Context, id uint32, _ bool) (*symbol.Info, error) {
	if id != uid {
		return nil, errors.New("not found")
	}
	return instrument(), nil
}

// ---------- the test's side ----------

type env struct {
	t       *testing.T
	root    *clientv3.Client
	cfg     tdagent.Config
	key     *sealbox.PrivateKey
	venue   *exchangetest.AccountVenue
	nc      *nats.Conn
	journal string
	seq     uint32
}

type kinds map[string]exchange.AccountKind

func (k kinds) AccountKind(n string) (exchange.AccountKind, bool) { x, ok := k[n]; return x, ok }

func setup(t *testing.T) *env {
	t.Helper()
	s := etcdtest.Start(t)
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{inst: "tw", state.ControllerInstance: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{inst: state.PlaneTD})
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}

	ns := natsxtest.Start(t)
	nc, err := nats.Connect(ns.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	venue := exchangetest.NewAccountVenue(exchangetest.AccountOptions{Key: apiKey})
	srv := httptest.NewServer(venue.HTTP())
	t.Cleanup(srv.Close)

	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "td-") // the shims' sockets need a short path
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
	e := &env{t: t, root: root, key: key, venue: venue, nc: nc, journal: filepath.Join(t.TempDir(), "starts")}
	e.cfg = tdagent.Config{
		Topology: topo, Instance: inst, Dial: state.AuthDial(inst, etcdtest.Password(inst)),
		Term:      state.TermOptions{ID: t.Name()},
		WorkDir:   wd,
		Shim:      self,
		Account:   self,
		ShimEnv:   []string{envTestShim + "=1"},
		WorkerEnv: []string{envWorker + "=1", envJournal + "=" + e.journal, envNATS + "=" + ns.URL()},
		Kinds:     kinds{"Fake": venue.Kind("Fake")},
		Endpoints: map[string]tdagent.Endpoint{"Fake": {REST: srv.URL, Stream: "ws" + strings.TrimPrefix(srv.URL, "http")}},
		Open: func(sealed []byte, id int64, kind string) error {
			_, err := sealbox.OpenCredentials(key, sealed, id, kind)
			return err
		},
		Unseal: func(sealed []byte, id int64, kind string) (sealbox.Credentials, error) {
			return sealbox.OpenCredentials(key, sealed, id, kind)
		},
		User:       func(int64) (string, string, string, error) { return "jwt", "seed", "_INBOX_td_42", nil },
		Policy:     tdagent.Policy{Backoff: agent.Backoff{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond}, Beat: 10 * time.Second, GiveUpAfter: 2},
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
		for _, pid := range append(pids, e.workerPIDs()...) {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already gone, mostly
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", buf.String())
		}
	})
	return e
}

func (e *env) start() (stop func(), done <-chan struct{}) { return e.startWith(e.cfg) }

func (e *env) startWith(cfg tdagent.Config) (stop func(), done <-chan struct{}) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	var err error
	go func() { err = tdagent.Run(ctx, cfg); close(ch) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			e.t.Error("the agent did not stop")
		}
	})
	stop = func() {
		e.t.Helper()
		cancel()
		select {
		case <-ch:
			if !errors.Is(err, context.Canceled) {
				e.t.Fatalf("agent ended with %v", err)
			}
		case <-time.After(20 * time.Second):
			e.t.Fatal("the agent did not stop")
		}
	}
	return stop, ch
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

// ready declares the account and the session's demand on it, written by hand as the controller
// would (E13), and waits for the ack to be READY.
func (e *env) ready() {
	e.t.Helper()
	sealed, err := sealbox.SealCredentials(e.key.Public(), apiID, "Fake", sealbox.Credentials{KeyType: "hmac", Key: apiKey, Secret: "s"})
	if err != nil {
		e.t.Fatal(err)
	}
	e.put(state.AccountKey(inst, apiID), &statev1.Account{Name: "a", Kind: "Fake", SealedCredentials: sealed, Enabled: true})
	e.put(state.TDDemandKey(inst, session, apiID), &statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: apiID}})
	e.ack(statev1.EntryState_ENTRY_STATE_READY, "")
}

// ack waits for the session's ack to be in state st with reason, and returns it.
func (e *env) ack(st statev1.EntryState, reason string) *statev1.EntryStatus {
	e.t.Helper()
	var last *statev1.EntryStatus
	e.wait("ack "+st.String(), func() bool {
		last = e.readAck()
		return last.GetState() == st && last.GetReason() == reason
	}, func() string { return last.String() })
	return last
}

func (e *env) readAck() *statev1.EntryStatus {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.TDAckKey(inst, session, apiID))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	st := &statev1.EntryStatus{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *env) record() *statev1.AgentRecord {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.AgentKey(state.PlaneTD, inst))
	if err != nil || len(resp.Kvs) == 0 {
		return nil
	}
	rec := &statev1.AgentRecord{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, rec); err != nil {
		return nil
	}
	return rec
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

// lastPID is the newest account worker's pid.
func (e *env) lastPID() int {
	e.t.Helper()
	pids := e.workerPIDs()
	if len(pids) == 0 {
		e.t.Fatal("no account worker started")
	}
	return pids[len(pids)-1]
}

func (e *env) wait(what string, ok func() bool, last func() string) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s; last %s", what, last())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// ---------- the session ----------

// cid is the session's next client_order_id under algo.
func (e *env) cid() string {
	e.t.Helper()
	e.seq++
	s, err := cloid.Encode(algo, time.Now().UnixMilli(), e.seq)
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func (e *env) order(req *tdv1.OrderRequest) *tdv1.OrderReply {
	e.t.Helper()
	r, err := e.tryOrder(req)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) tryOrder(req *tdv1.OrderRequest) (*tdv1.OrderReply, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	m, err := e.nc.Request("td.order."+strconv.Itoa(apiID), b, 5*time.Second)
	if err != nil {
		return nil, err
	}
	r := &tdv1.OrderReply{}
	return r, proto.Unmarshal(m.Data, r)
}

func submitReq(cid string, qty, price float64) *tdv1.OrderRequest {
	return &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
		SessionId: session, Uid: uid, AlgoId: algo, ClientOrderId: cid,
		Side: commonv1.Side_SIDE_BUY, OrderType: commonv1.OrderType_ORDER_TYPE_LIMIT,
		Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC, Qty: &qty, Price: &price,
	}}}
}

// submit sends a limit buy that rests and checks TD took it.
func (e *env) submit(cid string) {
	e.t.Helper()
	if r := e.order(submitReq(cid, 1, 100)); !r.GetAccepted() {
		e.t.Fatalf("submit %s: %v", cid, r)
	}
}

func cancelReq(cid string) *tdv1.OrderRequest {
	return &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{
		SessionId: session, Uid: uid, AlgoId: algo, ClientOrderId: cid,
	}}}
}

func (e *env) cancel(cid string) {
	e.t.Helper()
	if r := e.order(cancelReq(cid)); !r.GetAccepted() {
		e.t.Fatalf("cancel %s: %v", cid, r)
	}
}

func (e *env) view(req *tdv1.ViewRequest) *tdv1.ViewReply {
	e.t.Helper()
	b, err := proto.Marshal(req)
	if err != nil {
		e.t.Fatal(err)
	}
	m, err := e.nc.Request("td.view."+strconv.Itoa(apiID), b, 5*time.Second)
	if err != nil {
		e.t.Fatal(err)
	}
	r := &tdv1.ViewReply{}
	if err := proto.Unmarshal(m.Data, r); err != nil {
		e.t.Fatal(err)
	}
	if r.GetError() != nil {
		e.t.Fatalf("td.view: %v", r.GetError())
	}
	return r
}

func omsReq() *tdv1.ViewRequest {
	return &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Oms{Oms: &tdv1.OmsViewRequest{}}}
}

func ledgerReq() *tdv1.ViewRequest {
	return &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Ledger{Ledger: &tdv1.LedgerViewRequest{}}}
}

// waitState waits for td.view to say the account is in st.
func (e *env) waitState(st tdv1.AccountState) {
	e.t.Helper()
	var last tdv1.AccountState
	e.wait(st.String(), func() bool { last = e.view(omsReq()).GetState(); return last == st },
		func() string { return last.String() })
}

// events is a subscription to td.evt: every event, in arrival order.
type events struct {
	t   *testing.T
	sub *nats.Subscription
	mu  sync.Mutex
	evs []*tdv1.AccountEvent
}

func (e *env) subscribe() *events {
	e.t.Helper()
	s := &events{t: e.t}
	sub, err := e.nc.Subscribe("td.evt."+strconv.Itoa(apiID), func(m *nats.Msg) {
		ev := &tdv1.AccountEvent{}
		if err := proto.Unmarshal(m.Data, ev); err != nil {
			return
		}
		s.mu.Lock()
		s.evs = append(s.evs, ev)
		s.mu.Unlock()
	})
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.nc.Flush(); err != nil { // the server has the interest before anything is sent
		e.t.Fatal(err)
	}
	s.sub = sub
	e.t.Cleanup(func() { _ = sub.Unsubscribe() })
	return s
}

// close stops the subscription and returns what it got.
func (s *events) close() []*tdv1.AccountEvent {
	_ = s.sub.Unsubscribe()
	return s.all()
}

func (s *events) all() []*tdv1.AccountEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.evs)
}

// await waits for an event that ok accepts and returns it.
func (s *events) await(what string, ok func(*tdv1.AccountEvent) bool) *tdv1.AccountEvent {
	s.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, ev := range s.all() {
			if ok(ev) {
				return ev
			}
		}
		if time.Now().After(deadline) {
			var got []string
			for _, ev := range s.all() {
				got = append(got, ev.String())
			}
			s.t.Fatalf("timed out waiting for %s; got:\n%s", what, strings.Join(got, "\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// status waits for an OrderUpdate that takes cid to st.
func (s *events) status(cid string, st tdv1.OrderStatus) *tdv1.OrderUpdate {
	s.t.Helper()
	return s.await(cid+" "+st.String(), func(ev *tdv1.AccountEvent) bool {
		o := ev.GetOrderUpdate().GetOrder()
		return o.GetClientOrderId() == cid && o.GetStatus() == st
	}).GetOrderUpdate()
}

// rests waits for cid to be at the venue: NEW from Submit's answer, or ACCEPTED from the stream,
// whichever the worker hears first.
func (s *events) rests(cid string) {
	s.t.Helper()
	s.await(cid+" resting", func(ev *tdv1.AccountEvent) bool {
		o := ev.GetOrderUpdate().GetOrder()
		return o.GetClientOrderId() == cid &&
			(o.GetStatus() == tdv1.OrderStatus_ORDER_STATUS_NEW || o.GetStatus() == tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)
	})
}

func (s *events) recon(inc uint32, reason tdv1.ReconReason) *tdv1.AccountEvent {
	s.t.Helper()
	return s.await("Recon "+reason.String(), func(ev *tdv1.AccountEvent) bool {
		r := ev.GetRecon()
		return r != nil && r.GetIncarnation() == inc && r.GetReason() == reason
	})
}

func cloids(orders []*tdv1.Order) []string {
	var out []string
	for _, o := range orders {
		out = append(out, o.GetClientOrderId())
	}
	slices.Sort(out)
	return out
}

func venueCloids(v *exchangetest.AccountVenue) []string {
	var out []string
	for _, o := range v.OpenOrders() {
		out = append(out, o.ClientOrderID)
	}
	slices.Sort(out)
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

// ---------- the scenarios ----------

// A hand-written demand gets a READY ack; the session then submits, cancels, and batch-cancels over
// td.order, and sees each order's way on td.evt and at the venue.
func TestSubmitCancelBatchCancel(t *testing.T) {
	e := setup(t)
	evs := e.subscribe()
	e.start()
	e.ready()
	evs.recon(1, tdv1.ReconReason_RECON_REASON_INCARNATION)

	c1 := e.cid()
	e.submit(c1)
	evs.rests(c1)
	if got := venueCloids(e.venue); !slices.Equal(got, []string{c1}) {
		t.Fatalf("venue holds %v", got)
	}
	e.cancel(c1)
	evs.status(c1, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)

	c2, c3 := e.cid(), e.cid()
	e.submit(c2)
	e.submit(c3)
	evs.rests(c2)
	evs.rests(c3)
	r := e.order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_BatchCancel{BatchCancel: &tdv1.BatchCancel{
		Orders: []*tdv1.BatchCancelOrder{{ClientOrderId: c2}, {ClientOrderId: c3}},
	}}})
	if !r.GetAccepted() {
		t.Fatalf("batch cancel: %v", r)
	}
	evs.status(c2, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	evs.status(c3, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if b := e.venue.Batches(); len(b) != 1 || !slices.Equal(slices.Sorted(slices.Values(b[0])), []string{c2, c3}) {
		t.Fatalf("venue batches %v", b)
	}
	if got := venueCloids(e.venue); len(got) != 0 {
		t.Fatalf("venue still holds %v", got)
	}
	if oms := e.view(omsReq()).GetOms(); len(oms.GetOrders()) != 0 {
		t.Fatalf("td.view still holds %v", cloids(oms.GetOrders()))
	}
}

// Dropping the private stream makes the account DEGRADED: a new order is refused with 102 and a
// cancel goes through. Once the stream is back the worker recons and publishes Recon(RECONNECT).
func TestDegradedAndReconnect(t *testing.T) {
	e := setup(t)
	evs := e.subscribe()
	e.start()
	e.ready()
	inc := e.readAck().GetWorker().GetIncarnation()
	c1 := e.cid()
	e.submit(c1)
	evs.rests(c1)

	e.venue.FailDials(1 << 30)
	e.venue.Disconnect()
	e.waitState(tdv1.AccountState_ACCOUNT_STATE_DEGRADED)

	r := e.order(submitReq(e.cid(), 1, 100))
	if r.GetAccepted() || r.GetCode() != int32(tdv1.RejectCode_REJECT_CODE_ACCOUNT_DEGRADED) {
		t.Fatalf("a new order while DEGRADED: %v", r)
	}
	e.cancel(c1)
	evs.status(c1, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if got := venueCloids(e.venue); len(got) != 0 {
		t.Fatalf("venue still holds %v", got)
	}

	e.venue.FailDials(0)
	rec := evs.recon(inc, tdv1.ReconReason_RECON_REASON_RECONNECT).GetRecon()
	if len(rec.GetOpenOrders()) != 0 {
		t.Fatalf("Recon has open orders %v", cloids(rec.GetOpenOrders()))
	}
	e.waitState(tdv1.AccountState_ACCOUNT_STATE_READY)
	c3 := e.cid()
	e.submit(c3)
	evs.rests(c3)
}

// Replacing the td-agent process leaves the account worker running and its private stream up:
// td.order is answered while no agent runs, and the next term adopts the worker without a new
// incarnation, a new connection, or a Recon.
func TestAgentReplacement(t *testing.T) {
	e := setup(t)
	evs := e.subscribe()
	stop, _ := e.start()
	e.ready()
	evs.recon(1, tdv1.ReconReason_RECON_REASON_INCARNATION)
	pid := e.lastPID()
	_, dials := e.venue.Connections()

	stop()
	if !alive(pid) {
		t.Fatal("the account worker died with the agent")
	}
	c1 := e.cid()
	e.submit(c1)
	evs.rests(c1)

	cfg := e.cfg
	cfg.Term.ID = t.Name() + "-next"
	e.startWith(cfg)
	e.wait("the next term", func() bool { return e.record().GetEpoch() == 2 }, func() string { return e.record().String() })
	e.ack(statev1.EntryState_ENTRY_STATE_READY, "")
	time.Sleep(500 * time.Millisecond) // a respawn or a reconnect would show by now

	if pids := e.workerPIDs(); len(pids) != 1 || !alive(pid) {
		t.Fatalf("workers %v, the first alive: %v", pids, alive(pid))
	}
	if open, total := e.venue.Connections(); open != 1 || total != dials {
		t.Fatalf("private stream: %d open, %d dialled, %d before", open, total, dials)
	}
	if inc := e.readAck().GetWorker().GetIncarnation(); inc != 1 {
		t.Fatalf("incarnation %d", inc)
	}
	c2 := e.cid()
	e.submit(c2)
	evs.rests(c2)
	for _, ev := range evs.all() {
		if r := ev.GetRecon(); r != nil && r.GetReason() != tdv1.ReconReason_RECON_REASON_INCARNATION {
			t.Fatalf("a Recon after the first: %v", r)
		}
	}
}

// An account worker that crashes is started again: the next incarnation's first recon is
// Recon(INCARNATION), with the order the last one left at the venue, and td.order is served again.
func TestWorkerCrashRestarts(t *testing.T) {
	e := setup(t)
	evs := e.subscribe()
	e.start()
	e.ready()
	evs.recon(1, tdv1.ReconReason_RECON_REASON_INCARNATION)
	c1 := e.cid()
	e.submit(c1)
	evs.rests(c1)

	syscall.Kill(e.lastPID(), syscall.SIGKILL) //nolint:errcheck // checked by what follows
	rec := evs.recon(2, tdv1.ReconReason_RECON_REASON_INCARNATION)
	if got := cloids(rec.GetRecon().GetOpenOrders()); !slices.Equal(got, []string{c1}) {
		t.Fatalf("Recon open orders %v", got)
	}
	if rec.GetSeq() != 1 || rec.GetSource().GetIncarnation() != 2 {
		t.Fatalf("Recon seq %d source %v", rec.GetSeq(), rec.GetSource())
	}
	e.wait("ack of incarnation 2", func() bool {
		a := e.readAck()
		return a.GetState() == statev1.EntryState_ENTRY_STATE_READY && a.GetWorker().GetIncarnation() == 2
	}, func() string { return e.readAck().String() })
	if n := len(e.workerPIDs()); n != 2 {
		t.Fatalf("%d starts", n)
	}
	e.cancel(c1)
	evs.status(c1, tdv1.OrderStatus_ORDER_STATUS_CANCELLED)
}

// A worker that keeps crashing is started again until the Policy gives up: the ack is DEAD and
// td.order has no responder (101 at the SDK).
func TestDeadAfterRestarts(t *testing.T) {
	e := setup(t)
	e.start()
	e.ready()
	for i := 1; i <= 3; i++ { // the first incarnation and GiveUpAfter restarts
		syscall.Kill(e.lastPID(), syscall.SIGKILL) //nolint:errcheck // checked by what follows
		if i == 3 {
			break
		}
		e.wait("incarnation "+strconv.Itoa(i+1), func() bool {
			a := e.readAck()
			return a.GetState() == statev1.EntryState_ENTRY_STATE_READY && a.GetWorker().GetIncarnation() == uint32(i+1)
		}, func() string { return e.readAck().String() })
	}
	e.ack(statev1.EntryState_ENTRY_STATE_DEAD, "gave up")
	time.Sleep(300 * time.Millisecond)
	if n := len(e.workerPIDs()); n != 3 {
		t.Fatalf("%d starts", n)
	}
	if _, err := e.tryOrder(submitReq(e.cid(), 1, 100)); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("td.order of a DEAD account: %v", err)
	}
}

// A session that missed part of td.evt takes td.view's snapshot and applies only the events after
// its seq: it ends with the same orders and positions as the worker and the venue.
func TestResumeFromView(t *testing.T) {
	e := setup(t)
	first := e.subscribe()
	e.start()
	e.ready()
	c1, c2, c3 := e.cid(), e.cid(), e.cid()
	for _, c := range []string{c1, c2, c3} {
		e.submit(c)
		first.rests(c)
	}
	seen := first.close()
	lastSeen := seen[len(seen)-1].GetSeq()

	// What the session misses: a fill, a cancel, a new order, a position.
	pos := exchange.Position{Exch: symbol.ExchKey{Venue: "Fake", Category: symbol.Perp, ExchTicker: ticker}, Size: 1, EntryPrice: 100}
	if _, err := e.venue.Fill(c1, 1, 100); err != nil {
		t.Fatal(err)
	}
	e.venue.SetPosition(pos)
	e.cancel(c2)
	c4 := e.cid()
	e.submit(c4)
	e.wait("the worker to have the missed events", func() bool {
		return slices.Equal(cloids(e.view(omsReq()).GetOms().GetOrders()), []string{c3, c4})
	}, func() string { return strings.Join(cloids(e.view(omsReq()).GetOms().GetOrders()), ",") })

	// Resume: subscribe first, then the snapshot; apply the events past its seq.
	again := e.subscribe()
	oms, ledger := e.view(omsReq()), e.view(ledgerReq())
	if oms.GetIncarnation() != ledger.GetIncarnation() {
		t.Fatalf("incarnations %d and %d", oms.GetIncarnation(), ledger.GetIncarnation())
	}
	if oms.GetSeq() <= lastSeen {
		t.Fatalf("the snapshot's seq %d is not past the last seen %d", oms.GetSeq(), lastSeen)
	}
	l := newLocal(oms.GetIncarnation(), min(oms.GetSeq(), ledger.GetSeq()))
	for _, o := range oms.GetOms().GetOrders() {
		l.orders[o.GetClientOrderId()] = o
	}
	for _, p := range ledger.GetLedger().GetPositions() {
		l.positions[p.GetUid()] = p
	}

	// What comes after the resume.
	if _, err := e.venue.Fill(c3, 0.5, 100); err != nil {
		t.Fatal(err)
	}
	pos.Size = 1.5
	e.venue.SetPosition(pos)
	c5 := e.cid()
	e.submit(c5)
	again.rests(c5)
	again.await("position 1.5", func(ev *tdv1.AccountEvent) bool {
		ps := ev.GetPositionUpdate().GetPositions()
		return len(ps) == 1 && ps[0].GetSize() == 1.5
	})
	for _, ev := range again.all() {
		l.apply(ev)
	}

	final, finalLedger := e.view(omsReq()), e.view(ledgerReq())
	if l.seq != final.GetSeq() {
		t.Fatalf("applied up to seq %d, td.view is at %d", l.seq, final.GetSeq())
	}
	if want := final.GetOms().GetOrders(); len(want) != len(l.orders) {
		t.Fatalf("orders: local %v, td.view %v", slices.Sorted(maps.Keys(l.orders)), cloids(want))
	}
	for _, want := range final.GetOms().GetOrders() {
		got := l.orders[want.GetClientOrderId()]
		if got.GetStatus() != want.GetStatus() || got.GetFilledQty() != want.GetFilledQty() {
			t.Fatalf("order %s: local %v, td.view %v", want.GetClientOrderId(), got, want)
		}
	}
	if got, want := slices.Sorted(maps.Keys(l.orders)), venueCloids(e.venue); !slices.Equal(got, want) {
		t.Fatalf("orders: local %v, venue %v", got, want)
	}
	if got := l.orders[c3]; got.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED || got.GetFilledQty() != 0.5 {
		t.Fatalf("c3 %v", got)
	}
	for _, want := range finalLedger.GetLedger().GetPositions() {
		if got := l.positions[want.GetUid()]; got.GetSize() != want.GetSize() {
			t.Fatalf("position %d: local %v, td.view %v", want.GetUid(), got, want)
		}
	}
	if p := l.positions[uid]; p.GetSize() != 1.5 {
		t.Fatalf("position %v", p)
	}
}

// local is a session's copy of the account, as an SDK keeps it: td.view's snapshot, then the
// events of the same incarnation past its seq.
type local struct {
	inc       uint32
	seq       uint64
	orders    map[string]*tdv1.Order
	positions map[uint32]*tdv1.Position
}

func newLocal(inc uint32, seq uint64) *local {
	return &local{inc: inc, seq: seq, orders: map[string]*tdv1.Order{}, positions: map[uint32]*tdv1.Position{}}
}

func (l *local) apply(ev *tdv1.AccountEvent) {
	if ev.GetSource().GetIncarnation() != l.inc || ev.GetSeq() <= l.seq {
		return
	}
	l.seq = ev.GetSeq()
	switch {
	case ev.GetOrderUpdate() != nil:
		o := ev.GetOrderUpdate().GetOrder()
		switch o.GetStatus() {
		case tdv1.OrderStatus_ORDER_STATUS_FILLED, tdv1.OrderStatus_ORDER_STATUS_CANCELLED,
			tdv1.OrderStatus_ORDER_STATUS_REJECTED, tdv1.OrderStatus_ORDER_STATUS_EXPIRED:
			delete(l.orders, o.GetClientOrderId())
		default:
			l.orders[o.GetClientOrderId()] = o
		}
	case ev.GetPositionUpdate() != nil:
		for _, p := range ev.GetPositionUpdate().GetPositions() {
			l.positions[p.GetUid()] = p
		}
	case ev.GetRecon() != nil:
		r := ev.GetRecon()
		l.orders, l.positions = map[string]*tdv1.Order{}, map[uint32]*tdv1.Position{}
		for _, o := range r.GetOpenOrders() {
			l.orders[o.GetClientOrderId()] = o
		}
		for _, p := range r.GetPositions() {
			l.positions[p.GetUid()] = p
		}
	}
}
