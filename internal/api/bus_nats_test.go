package api

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/md/agent"
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Embedded NATS stopped, then the gauge is 0. Restarted, it is 1 (E12-D066).
// The failure timers only bound a missed signal. The pass path is the report.
func TestWorkerBusGaugeFollowsNATS(t *testing.T) {
	if agent.BusDisconnected != condBusDisconnected {
		t.Fatalf("condition type %s", agent.BusDisconnected)
	}
	ns := natsxtest.Start(t)
	nc, err := natsx.Connect(ns.URL(), natsx.Options{Name: "md-conn"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	bus := conn.NewBus(true)
	conn.BindBus(bus, nc.OnDisconnect, nc.OnReconnect)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		for {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	peer, workerEnd := net.Pipe()
	deadline := time.Now().Add(90 * time.Second)
	_ = peer.SetDeadline(deadline)
	_ = workerEnd.SetDeadline(deadline)
	t.Cleanup(func() { peer.Close(); workerEnd.Close() })
	go func() {
		_ = conn.Serve(t.Context(), workerEnd, conn.ServeConfig{
			Lookup: func(string) (exchange.Market, bool) {
				return quietMarket{}, true
			},
			Publish:    nc.Publish,
			Bus:        bus,
			RetryAfter: -1,
		})
	}()
	ups := newBusFeed()
	go func() {
		r := frame.NewReader(bufio.NewReader(peer))
		for {
			m := &workerv1.WorkerUp{}
			if err := r.Read(m); err != nil {
				return
			}
			ups.add(m)
		}
	}()
	down := frame.NewWriter(peer)
	ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetHello() != nil })
	initRaw, err := proto.Marshal(&mdctlv1.InitPayload{
		Venue: "v", Route: "r", MdInstance: "md-a", Url: url,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := down.Write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{
		Contract: conn.Contract, Payload: initRaw,
	}}}); err != nil {
		t.Fatal(err)
	}
	doneInit := ups.next(t, func(m *workerv1.WorkerUp) bool {
		return m.GetDone().GetVerb() == workerv1.Verb_VERB_INIT
	})
	if !doneInit.GetDone().GetOk() {
		t.Fatal(doneInit.GetDone())
	}
	if err := down.Write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{
		Contract: conn.Contract,
	}}}); err != nil {
		t.Fatal(err)
	}

	up := ups.waitBus(t, true)
	assertBusGauge(t, up, 1)
	if err := ns.Halt(); err != nil {
		t.Fatal(err)
	}
	downRep := ups.waitBus(t, false)
	assertBusGauge(t, downRep, 0)
	if err := ns.Restart(); err != nil {
		t.Fatal(err)
	}
	back := ups.waitBus(t, true)
	assertBusGauge(t, back, 1)
}

func assertBusGauge(t *testing.T, rep *mdctlv1.Report, want int) {
	t.Helper()
	body, err := proto.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	conds := agent.BusConditions(&workerv1.Report{Contract: conn.Contract, Payload: body})
	st := &statev1.WorkerStatus{
		Worker:     &commonv1.WorkerRef{Key: "md/v/r/0"},
		Phase:      "RUNNING",
		Conditions: conds,
	}
	raw, err := proto.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	cfg := Config{
		UnavailableAfter: 15 * time.Second,
		Topology:         state.Topology{Instances: map[string]string{"md-a": "tw"}},
		Planes:           map[string]state.Plane{"md-a": state.PlaneMD},
	}
	prefix := state.WorkerStatusPrefix(state.PlaneMD, "md-a")
	text := string(renderMetrics(cfg, now, []regionObs{{
		name: "tw", synced: true, contact: now,
		view: state.View{Entries: map[string]state.Entry{prefix + "h": {Value: raw}}},
	}}, 0, 0))
	line := "mftiko_worker_bus_up{instance=\"md-a\",plane=\"md\",region=\"tw\",worker=\"md/v/r/0\"} " + strconv.Itoa(want) + "\n"
	phase := "mftiko_worker_phase{instance=\"md-a\",phase=\"RUNNING\",plane=\"md\",region=\"tw\"} 1\n"
	if !bytes.Contains([]byte(text), []byte(line)) || !bytes.Contains([]byte(text), []byte(phase)) {
		t.Fatalf("gauge %d missing\n%s", want, text)
	}
}

type busFeed struct {
	mu sync.Mutex
	ms []*workerv1.WorkerUp
	i  int
	ch chan struct{}
}

func newBusFeed() *busFeed { return &busFeed{ch: make(chan struct{}, 1)} }

func (f *busFeed) add(m *workerv1.WorkerUp) {
	f.mu.Lock()
	f.ms = append(f.ms, m)
	f.mu.Unlock()
	select {
	case f.ch <- struct{}{}:
	default:
	}
}

func (f *busFeed) next(t *testing.T, ok func(*workerv1.WorkerUp) bool) *workerv1.WorkerUp {
	t.Helper()
	dead := time.NewTimer(20 * time.Second)
	defer dead.Stop()
	for {
		f.mu.Lock()
		for f.i < len(f.ms) {
			m := f.ms[f.i]
			f.i++
			if ok(m) {
				f.mu.Unlock()
				return m
			}
		}
		f.mu.Unlock()
		select {
		case <-f.ch:
		case <-dead.C:
			t.Fatal("timeout waiting for a control message")
		}
	}
}

func (f *busFeed) waitBus(t *testing.T, want bool) *mdctlv1.Report {
	t.Helper()
	dead := time.NewTimer(20 * time.Second)
	defer dead.Stop()
	for {
		f.mu.Lock()
		for f.i < len(f.ms) {
			m := f.ms[f.i]
			f.i++
			p, ok := busOf(m)
			if ok && p.GetBusUp() == want {
				f.mu.Unlock()
				return p
			}
		}
		n := len(f.ms)
		f.mu.Unlock()
		select {
		case <-f.ch:
		case <-dead.C:
			t.Fatalf("timeout waiting for bus_up=%v after %d messages", want, n)
		}
	}
}

func busOf(m *workerv1.WorkerUp) (*mdctlv1.Report, bool) {
	if m.GetReport() == nil || m.GetReport().GetContract() != conn.Contract {
		return nil, false
	}
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(m.GetReport().GetPayload(), p); err != nil || p.BusUp == nil {
		return nil, false
	}
	return p, true
}

type quietMarket struct{}

func (quietMarket) Topics() []exchange.TopicCaps { return nil }
func (quietMarket) Atom(exchange.Topic, *symbol.Info) (exchange.AtomRef, error) {
	return exchange.AtomRef{}, exchange.ErrNotImplemented
}
func (quietMarket) Routes() []string  { return nil }
func (quietMarket) Budget(string) int { return 0 }
func (quietMarket) Cost(string) int   { return 0 }
func (quietMarket) Endpoint(string, string) (string, error) {
	return "ws://127.0.0.1:9", nil
}
func (quietMarket) NewSession(string) (exchange.Session, error) { return quietSession{}, nil }
func (quietMarket) Book(string) (exchange.BookSync, bool)       { return nil, false }

type quietSession struct{}

func (quietSession) Limits() exchange.SessionLimits {
	return exchange.SessionLimits{ReadLimit: 1 << 20}
}
func (quietSession) Ping() []byte { return nil }
func (quietSession) Subscribe([]string) ([]exchange.Request, error) {
	return nil, nil
}
func (quietSession) Unsubscribe([]string) ([]exchange.Request, error) {
	return nil, nil
}
func (quietSession) Decode([]byte, exchange.Sink) error { return nil }
