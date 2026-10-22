// The conformance worker (PROTOCOL §9.8, D833, D839). It speaks the session
// worker's C4 and C5 behavior: compliant unless -violate names one check.
// Check id strings are repeated here because this package cannot import the
// harness (internal/sts). sdk.Run is not used. The full Go SDK pass is
// examples/strategies/conform, run from CI (D864). This worker stays the
// violation fixture.

package fakeworker

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

const (
	// tdCap is the TD queue cap (D815). It matches sdk/internal/dispatch.TDCap.
	// The 65537th event ends the session FAILED. This package does not import sdk.
	tdCap = 65536
	// mdCap is the MD ring, 32 MiB (D816). It matches sdk/internal/dispatch.MDBytes.
	mdCap = 32 << 20

	fallbackAPI  int64  = 7
	fallbackAlgo uint32 = 1
	fallbackUID  uint32 = 1001

	beatEvery = 5 * time.Millisecond
)

var (
	conformFlag   = flag.Bool("mftiko-conformance", false, "run the conformance worker")
	violateFlag   = flag.String("violate", "", "one conformance check id to violate")
	contractsFlag = flag.String("contracts", "worker.v1,stsctl.v1,md.v1,td.v1", "comma-separated Hello.contracts")
)

// conformanceMode reports whether this process was started as the conformance worker.
// Flags are registered from init. TestMain runs before the testing harness parses
// flags, so when -mftiko-conformance is present this parses the command line itself.
// A scripted fake (EnvWorker) is often started with arguments this package does not
// define, such as "--mode fake"; those processes are left unparsed.
func conformanceMode() bool {
	if !conformArg(os.Args[1:]) {
		return false
	}
	if !flag.Parsed() {
		flag.Parse()
	}
	return *conformFlag
}

func conformArg(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "-mftiko-conformance" || a == "--mftiko-conformance" ||
			strings.HasPrefix(a, "-mftiko-conformance=") || strings.HasPrefix(a, "--mftiko-conformance=") {
			return true
		}
	}
	return false
}

func knownViolate(id string) bool {
	switch id {
	case "flow", "beat", "deadline", "shutdown", "buffer_priority", "prehook", "td_overflow", "cloid", "subject", "recon", "ctx":
		return true
	default:
		return false
	}
}

func splitContracts(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// observation is one stdout JSON object (PROTOCOL §9.8). Pointers keep false and 0 visible.
type observation struct {
	Hook        string `json:"hook,omitempty"`
	Ctx         *bool  `json:"ctx,omitempty"`
	Seq         uint64 `json:"seq,omitempty"`
	MD          *bool  `json:"md,omitempty"`
	Incarnation uint32 `json:"incarnation,omitempty"`
	View        *bool  `json:"view,omitempty"`
	Fetch       *bool  `json:"fetch,omitempty"`
	Queued      *int   `json:"queued,omitempty"`
}

// conformParams is InitPayload.params (D835). The tags match PROTOCOL §9.8.
type conformParams struct {
	HookSleepMS   int    `json:"hook_sleep_ms,omitempty"`
	OrderCount    int    `json:"order_count,omitempty"`
	InitSleepMS   int    `json:"init_sleep_ms,omitempty"`
	StartSleepMS  int    `json:"start_sleep_ms,omitempty"`
	OnStopSleepMS int    `json:"on_stop_sleep_ms,omitempty"`
	SelfExit      bool   `json:"self_exit,omitempty"`
	ProbeReads    bool   `json:"probe_reads,omitempty"`
	Scenario      string `json:"scenario,omitempty"`
	UntilSeq      uint64 `json:"until_seq,omitempty"`
	APIID         int64  `json:"api_id,omitempty"`
	AlgoID        uint32 `json:"algo_id,omitempty"`
	UID           uint32 `json:"uid,omitempty"`
}

type mdItem struct {
	ts  int64
	seq uint64
	n   int
}

type tdKind int

const (
	tdOther tdKind = iota
	tdOrder
	tdPosition
)

type tdItem struct {
	seq   uint64
	inc   uint32
	kind  tdKind
	cloid string
	uid   uint32
}

type conform struct {
	violate   string
	contracts []string
	params    conformParams

	out   *frame.Writer
	outMu sync.Mutex
	down  chan *workerv1.WorkerDown
	kicks chan struct{}

	nc          *nats.Conn
	feedSubject string
	mdInst      string
	sessionID   string
	recvWindow  uint32
	hookTimeout uint32
	bindings    *stsctlv1.Bindings

	mu          sync.Mutex
	mdq         []mdItem
	mdUsed      int
	mdDropped   uint64
	seenSeq     uint64
	tdq         []tdItem
	tdFull      bool
	orderQueued bool
	posQueued   bool
	orders      map[string]struct{}
	positions   map[uint32]struct{}

	wmInc uint32
	wmSeq uint64

	inited    bool
	started   bool
	reconLive bool
	exitOnce  sync.Once
}

func runConform() int {
	if v := *violateFlag; v != "" && !knownViolate(v) {
		fmt.Fprintf(os.Stderr, "conformance worker: unknown -violate %q\n", v)
		return 2
	}
	ctl := os.NewFile(3, "ctl")
	if ctl == nil {
		fmt.Fprintln(os.Stderr, "conformance worker: no fd 3")
		return 2
	}
	syscall.CloseOnExec(3)
	c := &conform{
		violate:   *violateFlag,
		contracts: splitContracts(*contractsFlag),
		out:       frame.NewWriter(ctl),
		down:      make(chan *workerv1.WorkerDown, 16),
		kicks:     make(chan struct{}, 1),
		orders:    map[string]struct{}{},
		positions: map[uint32]struct{}{},
	}
	go c.readDown(ctl)
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Hello{Hello: &workerv1.Hello{
		Kind: "session", Release: "conform", Contracts: c.contracts, Pid: int32(os.Getpid()),
	}}})
	if c.violate != "beat" {
		go c.beat()
	}
	c.loop()
	return 0
}

func (c *conform) loop() {
	pulse := time.NewTicker(time.Millisecond)
	defer pulse.Stop()
	for {
		select {
		case m, ok := <-c.down:
			if !ok {
				os.Exit(0)
			}
			c.onDown(m)
		case <-c.kicks:
			if c.reconLive {
				c.drainTD()
			}
		case <-pulse.C:
			if c.reconLive {
				c.drainTD()
			}
		}
	}
}

func (c *conform) readDown(r io.Reader) {
	rd := frame.NewReader(bufio.NewReader(r))
	for {
		m := &workerv1.WorkerDown{}
		if err := rd.Read(m); err != nil {
			close(c.down)
			return
		}
		c.down <- m
	}
}

func (c *conform) beat() {
	t := time.NewTicker(beatEvery)
	defer t.Stop()
	var seq uint64
	for range t.C {
		seq++
		c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Beat{Beat: &workerv1.Beat{Seq: seq}}})
	}
}

func (c *conform) onDown(m *workerv1.WorkerDown) {
	switch msg := m.Msg.(type) {
	case *workerv1.WorkerDown_Init:
		c.onInit(msg.Init)
	case *workerv1.WorkerDown_Start:
		c.onStart(msg.Start)
	case *workerv1.WorkerDown_Stop:
		c.onStop(msg.Stop)
	case *workerv1.WorkerDown_Relay:
		c.onRelay(msg.Relay)
	case *workerv1.WorkerDown_Live:
		c.onLive(msg.Live)
	default:
		if !c.inited {
			c.rejectBeforeInit(workerv1.Verb_VERB_INIT)
		}
	}
}

func (c *conform) onInit(m *workerv1.Init) {
	if m == nil {
		c.bail("conformance worker: empty Init")
	}
	if c.inited {
		if c.violate == "flow" {
			c.done(workerv1.Verb_VERB_INIT, true, "")
			return
		}
		c.done(workerv1.Verb_VERB_INIT, false, "duplicate Init")
		c.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, "duplicate Init", false, 0, CodeInitFailed)
		return
	}
	var body stsctlv1.InitPayload
	if err := proto.Unmarshal(m.GetPayload(), &body); err != nil {
		c.bail("conformance worker: init payload: " + err.Error())
	}
	if len(body.GetParams()) > 0 {
		if err := json.Unmarshal(body.GetParams(), &c.params); err != nil {
			c.bail("conformance worker: params: " + err.Error())
		}
	}
	c.sessionID = body.GetSessionId()
	c.recvWindow = body.GetRecvWindowMs()
	c.hookTimeout = body.GetHookTimeoutS()
	// The deadline is armed before any NATS dial. OnInit is printed only after
	// the sleep, so an abort leaves no OnInit line.
	if !c.phaseOK(m.GetDeadlineMs(), c.params.InitSleepMS, workerv1.Verb_VERB_INIT) {
		return
	}
	c.connect(body.GetNats())
	if d := m.GetLiveDigest(); d != "" {
		c.applied(d)
	}
	c.obs(observation{Hook: "OnInit"})
	c.inited = true
	c.done(workerv1.Verb_VERB_INIT, true, "")
}

func (c *conform) onStart(m *workerv1.Start) {
	if m == nil {
		c.bail("conformance worker: empty Start")
	}
	if !c.inited {
		c.rejectBeforeInit(workerv1.Verb_VERB_START)
		return
	}
	if !c.phaseOK(m.GetDeadlineMs(), c.params.StartSleepMS, workerv1.Verb_VERB_START) {
		return
	}
	// Beat prints OnStart before the hook blocks, so a beater on its own
	// goroutine is visible while this one sleeps.
	if c.params.Scenario == "beat" {
		c.obs(observation{Hook: "OnStart"})
		c.sleepMS(c.params.HookSleepMS)
		c.started = true
		c.done(workerv1.Verb_VERB_START, true, "")
		c.stage("running")
		return
	}
	var body stsctlv1.StartPayload
	if err := proto.Unmarshal(m.GetPayload(), &body); err != nil {
		c.bail("conformance worker: start payload: " + err.Error())
	}
	c.bindings = body.GetBindings()
	if err := c.subscribe(); err != nil {
		c.bail("conformance worker: subscribe: " + err.Error())
	}
	if err := c.nc.FlushTimeout(3 * time.Second); err != nil {
		c.bail("conformance worker: flush: " + err.Error())
	}
	c.obs(observation{Hook: "gate"})
	c.runScenario()
	c.obs(observation{Hook: "OnStart"})
	c.started = true
	c.done(workerv1.Verb_VERB_START, true, "")
	c.stage("running")
	if c.params.SelfExit {
		c.finish(commonv1.Outcome_OUTCOME_DONE, "exit", true, time.Duration(c.hookTimeout)*time.Second, CodeDone)
	}
}

func (c *conform) onStop(m *workerv1.Stop) {
	if !c.inited {
		c.rejectBeforeInit(workerv1.Verb_VERB_INIT)
		return
	}
	grace := time.Duration(0)
	if m != nil {
		grace = time.Duration(m.GetGraceMs()) * time.Millisecond
	}
	if !c.started {
		if c.violate == "flow" {
			c.obs(observation{Hook: "OnStop"})
			c.finish(commonv1.Outcome_OUTCOME_DONE, "stop", true, grace, CodeDone)
			return
		}
		c.finish(commonv1.Outcome_OUTCOME_DONE, "stop", false, 0, CodeDone)
		return
	}
	c.finish(commonv1.Outcome_OUTCOME_DONE, "stop", true, grace, CodeDone)
}

func (c *conform) onRelay(m *workerv1.Relay) {
	if m == nil {
		return
	}
	if !c.inited {
		c.rejectBeforeInit(workerv1.Verb_VERB_INIT)
		return
	}
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Relay{Relay: &workerv1.Relay{
		Contract: m.GetContract(),
		CorrId:   append([]byte(nil), m.GetCorrId()...),
		Payload:  append([]byte(nil), m.GetPayload()...),
	}}})
}

func (c *conform) onLive(m *workerv1.Live) {
	if m == nil {
		return
	}
	if !c.inited {
		c.rejectBeforeInit(workerv1.Verb_VERB_INIT)
		return
	}
	c.applied(m.GetDigest())
}

// rejectBeforeInit answers a message that arrived before Init succeeded (D837).
// A flow violation reports Done(ok) and keeps running. Anything else ends
// INIT_FAILED. Start uses its own verb; every other message uses INIT.
func (c *conform) rejectBeforeInit(verb workerv1.Verb) {
	if c.violate == "flow" {
		c.done(workerv1.Verb_VERB_INIT, true, "")
		return
	}
	c.done(verb, false, "before Init")
	c.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, "before Init", false, 0, CodeInitFailed)
}

// phaseOK waits out a hook deadline. deadline_ms > 0 arms a timer before the
// sleep. A deadline violation reports Done(ok) immediately and does not sleep.
// Returning false means finish already exited.
func (c *conform) phaseOK(deadlineMS uint32, sleepMS int, verb workerv1.Verb) bool {
	if c.violate == "deadline" && deadlineMS > 0 {
		c.done(verb, true, "")
		return true
	}
	if deadlineMS == 0 {
		c.sleepMS(sleepMS)
		return true
	}
	if sleepMS <= 0 {
		return true
	}
	limit := time.NewTimer(time.Duration(deadlineMS) * time.Millisecond)
	defer limit.Stop()
	slept := time.NewTimer(time.Duration(sleepMS) * time.Millisecond)
	defer slept.Stop()
	select {
	case <-limit.C:
		c.done(verb, false, "deadline")
		c.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, "deadline", false, 0, CodeInitFailed)
		return false
	case <-slept.C:
		return true
	}
}

func (c *conform) connect(creds *stsctlv1.NatsCredentials) {
	url := os.Getenv("MFTIKO_NATS_URL")
	if creds == nil || creds.GetUserJwt() == "" || creds.GetSeed() == "" || url == "" {
		c.bail("conformance worker: no nats credentials")
	}
	nc, err := nats.Connect(url,
		nats.UserJWTAndSeed(creds.GetUserJwt(), creds.GetSeed()),
		nats.CustomInboxPrefix(creds.GetInboxPrefix()),
		nats.NoReconnect(),
		nats.Timeout(3*time.Second),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	)
	if err != nil {
		c.bail("conformance worker: nats: " + err.Error())
	}
	c.nc = nc
}

func (c *conform) subscribe() error {
	var subject string
	if c.bindings != nil {
		for _, f := range c.bindings.GetFeeds() {
			if f.GetSubject() != "" {
				subject = f.GetSubject()
				c.mdInst = f.GetMdInstance()
				break
			}
		}
	}
	if subject == "" {
		return fmt.Errorf("no feed subject")
	}
	c.feedSubject = subject
	if err := c.listen(subject, c.onMD); err != nil {
		return err
	}
	evt := "td.evt." + strconv.FormatInt(c.api(), 10)
	return c.listen(evt, c.onTD)
}

func (c *conform) listen(subject string, cb nats.MsgHandler) error {
	sub, err := c.nc.Subscribe(subject, cb)
	if err != nil {
		return err
	}
	// The client default pending cap is 65536 messages, which would drop the
	// overflow event before this worker could see it.
	return sub.SetPendingLimits(-1, -1)
}

func (c *conform) runScenario() {
	switch c.params.Scenario {
	case "priority":
		c.runPriority()
	case "prehook", "ring":
		c.runRing()
	case "overflow":
		c.runOverflow()
	case "cloid", "subject":
		c.sendOrders()
	case "recon":
		c.runRecon()
	case "ctx":
		c.runCtx()
	}
	if c.params.ProbeReads {
		c.probeReads()
	}
}

func (c *conform) runPriority() {
	if !c.waitUntil(c.waitCap(), func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.mdq) >= 1 && len(c.tdq) >= 1
	}) {
		return
	}
	if c.violate == "buffer_priority" {
		c.drainMD()
		c.drainTD()
		return
	}
	c.drainTD()
	c.drainMD()
}

func (c *conform) runRing() {
	if c.params.UntilSeq > 0 && !c.waitUntil(c.waitCap(), func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.seenSeq >= c.params.UntilSeq
	}) {
		return
	}
	c.drainMD()
}

func (c *conform) runOverflow() {
	c.waitUntil(60*time.Second, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.tdFull || len(c.tdq) > tdCap
	})
	if c.violate == "td_overflow" {
		return
	}
	c.finish(commonv1.Outcome_OUTCOME_FAILED, "td buffer full", true, 0, CodeFailed)
}

func (c *conform) runRecon() {
	raw, err := proto.Marshal(&tdv1.ViewRequest{Req: &tdv1.ViewRequest_Recon{Recon: &tdv1.ReconRequest{}}})
	if err != nil {
		c.bail("conformance worker: recon request: " + err.Error())
	}
	msg, err := c.nc.Request("td.view."+strconv.FormatInt(c.api(), 10), raw, 3*time.Second)
	if err != nil {
		c.bail("conformance worker: td.view: " + err.Error())
	}
	var reply tdv1.ViewReply
	if err := proto.Unmarshal(msg.Data, &reply); err != nil {
		c.bail("conformance worker: td.view reply: " + err.Error())
	}
	c.wmInc = reply.GetIncarnation()
	c.wmSeq = reply.GetSeq()
	c.drainTD()
	c.obs(observation{Hook: "OnRecon"})
	c.reconLive = true
}

func (c *conform) runCtx() {
	c.waitUntil(c.waitCap(), func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.orderQueued && c.posQueued
	})
	c.drainTD()
}

func (c *conform) probeReads() {
	view := strings.Replace(c.feedSubject, "md.v1.f.", "md.v1.view.", 1)
	raw, err := proto.Marshal(&mdv1.ViewRequest{Limit: 1})
	if err != nil {
		c.bail("conformance worker: view request: " + err.Error())
	}
	_, err = c.nc.Request(view, raw, 3*time.Second)
	ok := err == nil
	c.obs(observation{View: &ok})
	fetch, err := proto.Marshal(&mdv1.FetchRequest{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
		Interval: "1m", Limit: 1, Uid: c.uid(),
	}}})
	if err != nil {
		c.bail("conformance worker: fetch request: " + err.Error())
	}
	_, err = c.nc.Request("md.v1.fetch.Binance", fetch, 3*time.Second)
	ok = err == nil
	c.obs(observation{Fetch: &ok})
}

func (c *conform) sendOrders() {
	subject := "td.order." + strconv.FormatInt(c.api(), 10)
	// D1030: the session may publish td.view, but that is not the order subject,
	// so the order never shows up on td.order. The check fails without a
	// permission error.
	if c.violate == "subject" {
		subject = "td.view." + strconv.FormatInt(c.api(), 10)
	}
	for i := 1; i <= c.params.OrderCount; i++ {
		id := "bad"
		if c.violate != "cloid" {
			var err error
			id, err = cloid.Encode(c.algo(), time.Now().UnixMilli(), uint32(i))
			if err != nil {
				c.bail("conformance worker: cloid: " + err.Error())
			}
		} else if i > 1 {
			id = "nope"
		}
		qty := 1.0
		price := 1.0
		raw, err := proto.Marshal(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{
			SessionId:     c.sessionID,
			Uid:           c.uid(),
			Side:          commonv1.Side_SIDE_BUY,
			OrderType:     commonv1.OrderType_ORDER_TYPE_LIMIT,
			Tif:           commonv1.TimeInForce_TIME_IN_FORCE_GTC,
			Qty:           &qty,
			Price:         &price,
			ClientOrderId: id,
		}}})
		if err != nil {
			c.bail("conformance worker: order: " + err.Error())
		}
		if err := c.nc.Publish(subject, raw); err != nil {
			c.bail("conformance worker: publish order: " + err.Error())
		}
	}
	if err := c.nc.FlushTimeout(3 * time.Second); err != nil {
		c.bail("conformance worker: order flush: " + err.Error())
	}
	// A permissions violation is async: Flush can return nil while LastError
	// is set. Bail so a denied subject is not mistaken for the subject check.
	if err := c.nc.LastError(); err != nil {
		c.bail("conformance worker: order publish: " + err.Error())
	}
}

func (c *conform) onMD(msg *nats.Msg) {
	ts, _, seq, _, _, err := envelope.Parse(msg.Data)
	if err != nil {
		return
	}
	c.pushMD(mdItem{ts: ts, seq: seq, n: len(msg.Data)})
	c.kick()
}

func (c *conform) pushMD(item mdItem) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item.seq > c.seenSeq {
		c.seenSeq = item.seq
	}
	if c.violate == "prehook" {
		c.mdq = append(c.mdq, item)
		c.mdUsed += item.n
		return
	}
	if item.n > mdCap {
		c.mdDropped++
		return
	}
	for c.mdUsed+item.n > mdCap && len(c.mdq) > 0 {
		c.mdUsed -= c.mdq[0].n
		c.mdq = c.mdq[1:]
		c.mdDropped++
	}
	c.mdq = append(c.mdq, item)
	c.mdUsed += item.n
}

func (c *conform) onTD(msg *nats.Msg) {
	var ev tdv1.AccountEvent
	if err := proto.Unmarshal(msg.Data, &ev); err != nil {
		return
	}
	it := tdItem{seq: ev.GetSeq()}
	if src := ev.GetSource(); src != nil {
		it.inc = src.GetIncarnation()
	}
	if o := ev.GetOrderUpdate(); o != nil && o.GetOrder() != nil {
		it.kind = tdOrder
		it.cloid = o.GetOrder().GetClientOrderId()
	} else if p := ev.GetPositionUpdate(); p != nil && len(p.GetPositions()) > 0 {
		it.kind = tdPosition
		it.uid = p.GetPositions()[0].GetUid()
	}
	c.mu.Lock()
	switch it.kind {
	case tdOrder:
		c.orderQueued = true
	case tdPosition:
		c.posQueued = true
	}
	if len(c.tdq) >= tdCap {
		if c.violate == "td_overflow" {
			c.tdq = append(c.tdq, it)
			n := len(c.tdq)
			c.mu.Unlock()
			if n == tdCap+1 {
				q := n
				c.obs(observation{Queued: &q})
			}
			c.kick()
			return
		}
		c.tdFull = true
		c.mu.Unlock()
		c.kick()
		return
	}
	c.tdq = append(c.tdq, it)
	c.mu.Unlock()
	c.kick()
}

func (c *conform) drainMD() {
	for {
		c.mu.Lock()
		if len(c.mdq) == 0 {
			c.mu.Unlock()
			break
		}
		item := c.mdq[0]
		c.mdq = c.mdq[1:]
		c.mdUsed -= item.n
		c.mu.Unlock()
		if c.stale(item.ts) {
			c.mu.Lock()
			c.mdDropped++
			c.mu.Unlock()
			continue
		}
		c.obs(observation{Hook: "OnTrade", Seq: item.seq})
	}
	c.sendReport()
}

func (c *conform) stale(ts int64) bool {
	if c.violate == "prehook" || c.recvWindow == 0 {
		return false
	}
	return time.Now().UnixNano()-ts > int64(c.recvWindow)*int64(time.Millisecond)
}

func (c *conform) drainTD() {
	for {
		c.mu.Lock()
		if len(c.tdq) == 0 {
			c.mu.Unlock()
			return
		}
		it := c.tdq[0]
		c.tdq = c.tdq[1:]
		c.mu.Unlock()
		c.emitTD(it)
	}
}

func (c *conform) emitTD(it tdItem) {
	if c.params.Scenario == "recon" && c.violate != "recon" {
		if it.inc != c.wmInc || it.seq <= c.wmSeq {
			return
		}
	}
	switch it.kind {
	case tdOrder:
		c.mu.Lock()
		c.orders[it.cloid] = struct{}{}
		_, ok := c.orders[it.cloid]
		c.mu.Unlock()
		if c.violate == "ctx" {
			ok = false
		}
		c.obs(observation{Hook: "OnOrder", Ctx: boolPtr(ok), Seq: it.seq, Incarnation: it.inc})
	case tdPosition:
		c.mu.Lock()
		c.positions[it.uid] = struct{}{}
		_, ok := c.positions[it.uid]
		c.mu.Unlock()
		if c.violate == "ctx" {
			ok = false
		}
		c.obs(observation{Hook: "OnPosition", Ctx: boolPtr(ok), Seq: it.seq, Incarnation: it.inc})
	}
}

func (c *conform) sendReport() {
	c.mu.Lock()
	body, err := proto.Marshal(&stsctlv1.Report{Queues: []*stsctlv1.QueueStat{
		{Name: "md", Dropped: c.mdDropped, Depth: uint32(len(c.mdq))},
		{Name: "td", Depth: uint32(len(c.tdq))},
	}})
	c.mu.Unlock()
	if err != nil {
		return
	}
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Report{Report: &workerv1.Report{
		Contract: "stsctl.v1", Payload: body,
	}}})
}

func (c *conform) waitUntil(limit time.Duration, cond func() bool) bool {
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	pulse := time.NewTicker(time.Millisecond)
	defer pulse.Stop()
	for {
		if cond() {
			return true
		}
		select {
		case <-deadline.C:
			return cond()
		case <-c.kicks:
		case <-pulse.C:
		}
	}
}

func (c *conform) waitCap() time.Duration {
	switch c.params.Scenario {
	case "overflow", "ring":
		return 60 * time.Second
	default:
		if c.params.HookSleepMS > 0 {
			return time.Duration(c.params.HookSleepMS) * time.Millisecond
		}
		return 15 * time.Second
	}
}

func (c *conform) finish(outcome commonv1.Outcome, reason string, onStop bool, limit time.Duration, code int) {
	c.exitOnce.Do(func() { c.finishBody(outcome, reason, onStop, limit, code) })
	os.Exit(code)
}

func (c *conform) finishBody(outcome commonv1.Outcome, reason string, onStop bool, limit time.Duration, code int) {
	// flow and shutdown violations write Stopped before Exiting.
	if c.violate == "flow" || c.violate == "shutdown" {
		c.stopped(onStop)
		c.exiting(outcome, reason)
		os.Exit(code)
	}
	c.exiting(outcome, reason)
	if !onStop {
		c.stopped(false)
		os.Exit(code)
	}
	done := make(chan struct{})
	go func() {
		c.sleepMS(c.params.OnStopSleepMS)
		c.obs(observation{Hook: "OnStop"})
		close(done)
	}()
	if limit > 0 {
		timer := time.NewTimer(limit)
		defer timer.Stop()
		select {
		case <-done:
			c.stopped(true)
			os.Exit(code)
		case <-timer.C:
			// Outcome stays whatever Exiting already said. No Stopped frame.
			os.Exit(CodeHung)
		}
	}
	<-done
	c.stopped(true)
	os.Exit(code)
}

func (c *conform) kick() {
	select {
	case c.kicks <- struct{}{}:
	default:
	}
}

func (c *conform) send(m *workerv1.WorkerUp) {
	_ = c.out.Write(m)
}

func (c *conform) done(verb workerv1.Verb, ok bool, err string) {
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: verb, Ok: ok, Error: err}}})
}

func (c *conform) stage(name string) {
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: name}}})
}

func (c *conform) applied(digest string) {
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Applied{Applied: &workerv1.Applied{Digest: digest}}})
}

func (c *conform) exiting(outcome commonv1.Outcome, reason string) {
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{Outcome: outcome, Reason: reason}}})
}

func (c *conform) stopped(on bool) {
	c.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stopped{Stopped: &workerv1.Stopped{OnStopCompleted: on}}})
}

func (c *conform) obs(o observation) {
	raw, err := json.Marshal(o)
	if err != nil {
		return
	}
	c.outMu.Lock()
	defer c.outMu.Unlock()
	_, _ = os.Stdout.Write(append(raw, '\n'))
}

func (c *conform) bail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func (c *conform) sleepMS(ms int) {
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (c *conform) api() int64 {
	if c.params.APIID != 0 {
		return c.params.APIID
	}
	return fallbackAPI
}

func (c *conform) algo() uint32 {
	if c.params.AlgoID != 0 {
		return c.params.AlgoID
	}
	return fallbackAlgo
}

func (c *conform) uid() uint32 {
	if c.params.UID != 0 {
		return c.params.UID
	}
	return fallbackUID
}

func boolPtr(v bool) *bool { return &v }
