package sdk

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

func TestInitStartStop(t *testing.T) {
	url := startNATS(t)
	d := &dials{}
	r := &rec{}
	s := newScript(t, r, host{
		natsURL:     url,
		tryConn:     d.try,
		getenv:      envDirs("/vol", "/in"),
		reportAfter: never,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.gate(r)
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 0))
	waitFor(t, func() bool { return appliedBefore(s.frames(), "L1") })
	s.send(startDown(must(t, &stsctlv1.StartPayload{Bindings: bind(3, 4)})))
	waitFor(t, func() bool {
		ups := s.frames()
		return doneOK(ups, workerv1.Verb_VERB_START) && staged(ups) && reportVersion(ups, 3, 4)
	})
	if !helloSession(s.frames()) {
		t.Fatal("hello")
	}
	if d.url != url || d.jwt != "jwt" || d.seed != "seed" || d.inbox != "_INBOX_s1" || d.n != 1 {
		t.Fatal("dial was not given the session credentials")
	}
	if string(r.params) != "params" || r.vol != "/vol" || r.in != "/in/model" {
		t.Fatalf("context vol=%q in=%q params=%q", r.vol, r.in, r.params)
	}
	if got := r.list(); len(got) < 3 || got[0] != "OnInit" || got[1] != "gate" || got[2] != "OnStart" {
		t.Fatalf("hooks %v", got)
	}
	s.send(stopDown())
	waitFor(t, func() bool {
		ups := s.frames()
		code, got := s.exit()
		return got && code == 0 && exiting(ups, commonv1.Outcome_OUTCOME_DONE, "stop")
	})
	waitFor(t, func() bool {
		got := r.list()
		return len(got) == 4 && got[3] == "OnStop"
	})
	<-s.h.settled
}

func TestCommandFail(t *testing.T) {
	s := running(t, bind(1, 1))
	fail := &stsctlv1.Command{Cmd: &stsctlv1.Command_Fail{Fail: &stsctlv1.Fail{Reason: "agent asked"}}}
	s.send(relayDown(must(t, fail)))
	waitFor(t, func() bool {
		code, got := s.exit()
		ups := s.frames()
		return got && code == 30 && exiting(ups, commonv1.Outcome_OUTCOME_FAILED, "agent asked")
	})
	waitFor(t, func() bool { return contains(s.r.list(), "OnStop") })
}

func TestLiveUpdatesReport(t *testing.T) {
	s := running(t, bind(1, 1))
	waitFor(t, func() bool { return reportVersion(s.frames(), 1, 1) })
	before := reportCount(s.frames(), 1, 1)
	s.send(liveDown(must(t, bind(2, 5)), "L2"))
	waitFor(t, func() bool {
		return applied(s.frames(), "L2") && reportVersion(s.frames(), 2, 5)
	})
	if reportCount(s.frames(), 1, 1) != before {
		t.Fatal("unchanged version was reported again")
	}
	s.send(liveDown(must(t, bind(2, 5)), "L3"))
	waitFor(t, func() bool { return applied(s.frames(), "L3") })
	if reportCount(s.frames(), 2, 5) != 1 {
		t.Fatal("same snapshot sent another report")
	}

	s.send(liveDown(must(t, selectorBind()), "NO"))
	s.send(liveDown(must(t, bind(3, 1)), "YES"))
	waitFor(t, func() bool { return applied(s.frames(), "YES") && reportVersion(s.frames(), 3, 1) })
	if applied(s.frames(), "NO") {
		t.Fatal("selector was applied")
	}
	s.send(stopDown())
	waitFor(t, func() bool {
		code, got := s.exit()
		return got && code == 0
	})
}

func TestReportInterval(t *testing.T) {
	clock := &manual{}
	url := startNATS(t)
	r := &rec{}
	s := newScript(t, r, host{
		natsURL:     url,
		tryConn:     (&dials{}).try,
		reportAfter: clock.After,
		reportEvery: reportInterval,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.gate(r)
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 0))
	waitFor(t, func() bool { return doneOK(s.frames(), workerv1.Verb_VERB_INIT) })
	s.send(startDown(must(t, &stsctlv1.StartPayload{Bindings: bind(1, 1)})))
	waitFor(t, func() bool { return reportVersion(s.frames(), 1, 1) })
	n := reportCount(s.frames(), 1, 1)
	clock.fire()
	waitFor(t, func() bool { return reportCount(s.frames(), 1, 1) > n })
	s.send(stopDown())
	waitFor(t, func() bool {
		code, got := s.exit()
		return got && code == 0
	})
}

func TestNATSRetriesThenConnects(t *testing.T) {
	url := startNATS(t)
	d := &dials{failUntil: 2}
	r := &rec{}
	s := newScript(t, r, host{
		natsURL:     url,
		tryConn:     d.try,
		after:       immediate,
		reportAfter: never,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 0))
	waitFor(t, func() bool { return doneOK(s.frames(), workerv1.Verb_VERB_INIT) })
	if d.calls() < 3 {
		t.Fatalf("dials %d", d.calls())
	}
	s.send(stopDown())
	waitFor(t, func() bool {
		_, got := s.exit()
		return got
	})
	<-s.h.settled
}

func TestNATSDeadline(t *testing.T) {
	r := &rec{}
	s := newScript(t, r, host{
		natsURL: "nats://127.0.0.1:9",
		tryConn: func(string, *stsctlv1.NatsCredentials, time.Duration) (*nats.Conn, error) {
			return nil, errors.New("down")
		},
		after:       immediate,
		reportAfter: never,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 50))
	waitFor(t, func() bool {
		code, got := s.exit()
		why, ok := doneErr(s.frames(), workerv1.Verb_VERB_INIT)
		return got && code == 10 && ok && why == "deadline"
	})
	<-s.h.settled
	if contains(r.list(), "OnInit") {
		t.Fatal("OnInit ran")
	}
}

func TestRealDialDeadline(t *testing.T) {
	r := &rec{}
	init := baseInit()
	init.Nats.Seed = userSeed(t)
	s := newScript(t, r, host{
		natsURL:     "nats://127.0.0.1:1",
		reportAfter: never,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.start()
	s.send(initDown(mustInit(t, init), "L1", 400))
	waitFor(t, func() bool {
		code, got := s.exit()
		why, ok := doneErr(s.frames(), workerv1.Verb_VERB_INIT)
		return got && code == 10 && ok && why == "deadline"
	})
	<-s.h.settled
	if contains(r.list(), "OnInit") {
		t.Fatal("OnInit ran")
	}
}

func TestRejectsBadInit(t *testing.T) {
	var dialed atomicN
	r := &rec{}
	s := newScript(t, r, host{
		natsURL: "nats://127.0.0.1:1",
		tryConn: func(string, *stsctlv1.NatsCredentials, time.Duration) (*nats.Conn, error) {
			dialed.add()
			return nil, errors.New("should not dial")
		},
		c4log:   discardLog,
		log:     func(string) {},
		settled: make(chan struct{}),
	})
	s.start()
	init := baseInit()
	init.RecvWindowMs = 0
	s.send(initDown(mustInit(t, init), "L1", 0))
	waitFor(t, func() bool {
		code, got := s.exit()
		why, ok := doneErr(s.frames(), workerv1.Verb_VERB_INIT)
		return got && code == 10 && ok && why == "recv window"
	})
	<-s.h.settled
	if dialed.n() != 0 || contains(r.list(), "OnInit") {
		t.Fatalf("dialed %d hooks %v", dialed.n(), r.list())
	}
}

func TestSelectorStartFails(t *testing.T) {
	url := startNATS(t)
	r := &rec{}
	s := newScript(t, r, host{
		natsURL:     url,
		tryConn:     (&dials{}).try,
		reportAfter: never,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.gate(r)
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 0))
	waitFor(t, func() bool { return doneOK(s.frames(), workerv1.Verb_VERB_INIT) })
	s.send(startDown(must(t, &stsctlv1.StartPayload{Bindings: selectorBind()})))
	waitFor(t, func() bool {
		code, got := s.exit()
		why, ok := doneErr(s.frames(), workerv1.Verb_VERB_START)
		return got && code == 10 && ok && why != "" && exiting(s.frames(), commonv1.Outcome_OUTCOME_INIT_FAILED, why)
	})
	<-s.h.settled
	got := r.list()
	if !contains(got, "OnInit") || !contains(got, "OnStop") || contains(got, "OnStart") || contains(got, "gate") {
		t.Fatalf("hooks %v", got)
	}
}

func TestCommandSelectorDoesNotEnd(t *testing.T) {
	var mu sync.Mutex
	var notes []string
	url := startNATS(t)
	r := &rec{}
	s := newScript(t, r, host{
		natsURL:     url,
		tryConn:     (&dials{}).try,
		reportAfter: never,
		c4log:       discardLog,
		log: func(msg string) {
			mu.Lock()
			notes = append(notes, msg)
			mu.Unlock()
		},
		settled: make(chan struct{}),
	})
	s.gate(r)
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 0))
	s.send(startDown(must(t, &stsctlv1.StartPayload{Bindings: bind(1, 1)})))
	waitFor(t, func() bool { return staged(s.frames()) })
	cmd := &stsctlv1.Command{Cmd: &stsctlv1.Command_Bindings{Bindings: selectorBind()}}
	s.send(relayDown(must(t, cmd)))
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, n := range notes {
			if strings.Contains(n, "command bindings") {
				return true
			}
		}
		return false
	})
	if _, got := s.exit(); got {
		t.Fatal("session ended")
	}
	if !reportVersion(s.frames(), 1, 1) {
		t.Fatal("bindings changed")
	}
	s.send(stopDown())
	waitFor(t, func() bool {
		code, got := s.exit()
		return got && code == 0
	})
}

type rec struct {
	mu     sync.Mutex
	hooks  []string
	params []byte
	vol    string
	in     string
}

func (r *rec) add(name string) {
	r.mu.Lock()
	r.hooks = append(r.hooks, name)
	r.mu.Unlock()
}

func (r *rec) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.hooks...)
}

func (r *rec) OnInit(ctx Context) error {
	r.mu.Lock()
	r.hooks = append(r.hooks, "OnInit")
	r.params = append([]byte(nil), ctx.Params()...)
	r.vol = ctx.VolumeDir()
	r.in = ctx.InputDir("model")
	r.mu.Unlock()
	return nil
}

func (r *rec) OnStart(Context) error {
	r.add("OnStart")
	return nil
}

func (r *rec) OnStop(Context) { r.add("OnStop") }

type dials struct {
	mu        sync.Mutex
	n         int
	failUntil int
	url       string
	jwt       string
	seed      string
	inbox     string
}

func (d *dials) try(url string, c *stsctlv1.NatsCredentials, _ time.Duration) (*nats.Conn, error) {
	d.mu.Lock()
	d.n++
	n := d.n
	d.url, d.jwt, d.seed, d.inbox = url, c.GetUserJwt(), c.GetSeed(), c.GetInboxPrefix()
	fail := n <= d.failUntil
	d.mu.Unlock()
	if fail {
		return nil, errors.New("down")
	}
	return nats.Connect(url, nats.Name("sdk-test"))
}

func (d *dials) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}

type atomicN struct {
	mu sync.Mutex
	v  int
}

func (a *atomicN) add() {
	a.mu.Lock()
	a.v++
	a.mu.Unlock()
}

func (a *atomicN) n() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.v
}

type manual struct {
	mu    sync.Mutex
	chans []chan time.Time
	fired int
}

func (m *manual) After(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	m.mu.Lock()
	if m.fired > 0 {
		m.fired--
		ch <- time.Now()
	} else {
		m.chans = append(m.chans, ch)
	}
	m.mu.Unlock()
	return ch
}

func (m *manual) fire() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.chans) == 0 {
		m.fired++
		return
	}
	ch := m.chans[0]
	m.chans = m.chans[1:]
	select {
	case ch <- time.Now():
	default:
	}
}

type script struct {
	t     *testing.T
	r     *rec
	strat Strategy // served instead of r when set
	h     host
	w     *frame.Writer
	mu    sync.Mutex
	ups   []*workerv1.WorkerUp
	code  int
	got   bool
}

func newScript(t *testing.T, r *rec, h host) *script {
	t.Helper()
	if h.settled == nil {
		h.settled = make(chan struct{})
	}
	return &script{t: t, r: r, h: h}
}

func (s *script) gate(r *rec) {
	prev := emitGate
	emitGate = func() { r.add("gate") }
	s.t.Cleanup(func() { emitGate = prev })
}

func (s *script) start() {
	s.t.Helper()
	a, b := net.Pipe()
	s.t.Cleanup(func() { a.Close(); b.Close() })
	s.w = frame.NewWriter(a)
	go func() {
		rd := frame.NewReader(bufio.NewReader(a))
		for {
			m := &workerv1.WorkerUp{}
			if err := rd.Read(m); err != nil {
				return
			}
			if m.GetBeat() != nil {
				continue
			}
			s.mu.Lock()
			s.ups = append(s.ups, m)
			s.mu.Unlock()
		}
	}()
	strat := Strategy(s.r)
	if s.strat != nil {
		strat = s.strat
	}
	go func() {
		code := serve(context.Background(), b, strat, s.h)
		s.mu.Lock()
		s.code = code
		s.got = true
		s.mu.Unlock()
	}()
}

func (s *script) send(m proto.Message) {
	s.t.Helper()
	if err := s.w.Write(m); err != nil {
		s.t.Fatal(err)
	}
}

func (s *script) frames() []*workerv1.WorkerUp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*workerv1.WorkerUp(nil), s.ups...)
}

func (s *script) exit() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.code, s.got
}

func running(t *testing.T, b *stsctlv1.Bindings) *script {
	t.Helper()
	url := startNATS(t)
	r := &rec{}
	s := newScript(t, r, host{
		natsURL:     url,
		tryConn:     (&dials{}).try,
		reportAfter: never,
		c4log:       discardLog,
		log:         func(string) {},
		settled:     make(chan struct{}),
	})
	s.gate(r)
	s.start()
	s.send(initDown(mustInit(t, baseInit()), "L1", 0))
	s.send(startDown(must(t, &stsctlv1.StartPayload{Bindings: b})))
	waitFor(t, func() bool {
		ups := s.frames()
		return doneOK(ups, workerv1.Verb_VERB_START) && staged(ups)
	})
	return s
}

func baseInit() *stsctlv1.InitPayload {
	return &stsctlv1.InitPayload{
		SessionId:    "s1",
		RecvWindowMs: 200,
		HookTimeoutS: 2,
		Params:       []byte("params"),
		Nats:         &stsctlv1.NatsCredentials{UserJwt: "jwt", Seed: "seed", InboxPrefix: "_INBOX_s1"},
	}
}

func bind(epoch int64, seq uint64) *stsctlv1.Bindings {
	return &stsctlv1.Bindings{Version: &commonv1.Version{Epoch: epoch, Seq: seq}}
}

func selectorBind() *stsctlv1.Bindings {
	b := bind(9, 9)
	b.Selectors = []*stsctlv1.SelectorBinding{{Name: "roll"}}
	return b
}

func mustInit(t *testing.T, m proto.Message) []byte { return must(t, m) }

func must(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func initDown(payload []byte, digest string, deadline uint32) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{
		Contract: contractSTS, Payload: payload, LiveDigest: digest, DeadlineMs: deadline,
	}}}
}

func startDown(payload []byte) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{
		Contract: contractSTS, Payload: payload,
	}}}
}

func liveDown(payload []byte, digest string) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Live{Live: &workerv1.Live{
		Contract: contractSTS, Payload: payload, Digest: digest,
	}}}
}

func relayDown(payload []byte) *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Relay{Relay: &workerv1.Relay{
		Contract: contractSTS, CorrId: []byte("c"), Payload: payload,
	}}}
}

func stopDown() *workerv1.WorkerDown {
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: "stop", GraceMs: 5000}}}
}

func envDirs(vol, in string) func(string) string {
	return func(k string) string {
		switch k {
		case "MFTIKO_VOLUME_DIR":
			return vol
		case "MFTIKO_INPUT_DIR":
			return in
		default:
			return ""
		}
	}
}

func discardLog(string, error) {}

func immediate(time.Duration) <-chan time.Time {
	ch := make(chan time.Time)
	close(ch)
	return ch
}

func never(time.Duration) <-chan time.Time { return make(chan time.Time) }

func helloSession(ups []*workerv1.WorkerUp) bool {
	if len(ups) == 0 {
		return false
	}
	h := ups[0].GetHello()
	if h == nil || h.GetKind() != "session" || len(h.GetContracts()) != len(helloContracts) {
		return false
	}
	for i := range helloContracts {
		if h.GetContracts()[i] != helloContracts[i] {
			return false
		}
	}
	return true
}

func doneOK(ups []*workerv1.WorkerUp, v workerv1.Verb) bool {
	for _, m := range ups {
		d := m.GetDone()
		if d != nil && d.GetVerb() == v && d.GetOk() {
			return true
		}
	}
	return false
}

func doneErr(ups []*workerv1.WorkerUp, v workerv1.Verb) (string, bool) {
	for _, m := range ups {
		d := m.GetDone()
		if d != nil && d.GetVerb() == v && !d.GetOk() {
			return d.GetError(), true
		}
	}
	return "", false
}

func appliedBefore(ups []*workerv1.WorkerUp, digest string) bool {
	at, done := -1, -1
	for i, m := range ups {
		if a := m.GetApplied(); a != nil && a.GetDigest() == digest && at < 0 {
			at = i
		}
		if d := m.GetDone(); d != nil && d.GetVerb() == workerv1.Verb_VERB_INIT && d.GetOk() && done < 0 {
			done = i
		}
	}
	return at >= 0 && done > at
}

func applied(ups []*workerv1.WorkerUp, digest string) bool {
	for _, m := range ups {
		if a := m.GetApplied(); a != nil && a.GetDigest() == digest {
			return true
		}
	}
	return false
}

func staged(ups []*workerv1.WorkerUp) bool {
	for _, m := range ups {
		if m.GetStage().GetName() == "running" {
			return true
		}
	}
	return false
}

func exiting(ups []*workerv1.WorkerUp, o commonv1.Outcome, reason string) bool {
	for _, m := range ups {
		e := m.GetExiting()
		if e != nil && e.GetOutcome() == o && e.GetReason() == reason {
			return true
		}
	}
	return false
}

func reportsOf(ups []*workerv1.WorkerUp) []*stsctlv1.Report {
	var out []*stsctlv1.Report
	for _, m := range ups {
		raw := m.GetReport()
		if raw == nil {
			continue
		}
		rep := &stsctlv1.Report{}
		if err := proto.Unmarshal(raw.GetPayload(), rep); err != nil {
			continue
		}
		out = append(out, rep)
	}
	return out
}

func reportVersion(ups []*workerv1.WorkerUp, epoch int64, seq uint64) bool {
	for _, rep := range reportsOf(ups) {
		v := rep.GetBindingsApplied()
		if v == nil || v.GetEpoch() != epoch || v.GetSeq() != seq {
			continue
		}
		q := rep.GetQueues()
		if len(q) == 2 && q[0].GetName() == "td" && q[1].GetName() == "md" {
			return true
		}
	}
	return false
}

func reportCount(ups []*workerv1.WorkerUp, epoch int64, seq uint64) int {
	n := 0
	for _, rep := range reportsOf(ups) {
		v := rep.GetBindingsApplied()
		if v != nil && v.GetEpoch() == epoch && v.GetSeq() == seq {
			n++
		}
	}
	return n
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			if cond() {
				return
			}
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func userSeed(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	return string(seed)
}

func startNATS(t *testing.T) string {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: false}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("nats-server did not accept connections")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	addr := ns.Addr()
	if addr == nil {
		t.Fatal("nats-server has no address")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return "nats://" + host + ":" + port
}
