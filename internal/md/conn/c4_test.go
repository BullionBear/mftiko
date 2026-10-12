package conn

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/exchange"
)

// TestEpochReportedBeforeDial is the C4 acceptance: Init carries N, the peer has conn_epoch
// N+1 before the websocket handshake completes, and only then does md.f use that number. The
// same process reconnects and the next number arrives the same way. The peer is a pipe, not a
// shim, and the websocket is local.
func TestEpochReportedBeforeDial(t *testing.T) {
	const start uint64 = 7
	open1 := make(chan struct{})
	open2 := make(chan struct{})
	drop := make(chan struct{})
	var gen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := gen.Add(1)
		open := open2
		body := []byte("t2")
		if n == 1 {
			open, body = open1, []byte("t1")
		}
		select {
		case <-open:
		case <-r.Context().Done():
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		go func() {
			for {
				if _, _, err := c.Read(r.Context()); err != nil {
					return
				}
			}
		}()
		if err := c.Write(r.Context(), websocket.MessageText, body); err != nil {
			return
		}
		if n == 1 {
			select {
			case <-drop:
			case <-r.Context().Done():
			}
			_ = c.Close(websocket.StatusNormalClosure, "")
		}
	}))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sess := &tradeSession{}
	sess.lim.ReadLimit = 1 << 20
	rec := newRecorder()
	peer, workerEnd := net.Pipe()
	deadline := time.Now().Add(8 * time.Second)
	_ = peer.SetDeadline(deadline)
	_ = workerEnd.SetDeadline(deadline)
	t.Cleanup(func() { peer.Close(); workerEnd.Close() })

	code := make(chan int, 1)
	go func() {
		code <- Serve(t.Context(), workerEnd, ServeConfig{
			Lookup: func(string) (exchange.Market, bool) {
				return &stubMarket{s: sess}, true
			},
			Publish:    rec.publish,
			Release:    "test",
			RetryAfter: -1,
		})
	}()

	ups := newUpFeed()
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

	hello := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetHello() != nil })
	if hello.GetHello().GetKind() != "conn" || hello.GetHello().GetRelease() != "test" ||
		len(hello.GetHello().GetContracts()) != 1 || hello.GetHello().GetContracts()[0] != Contract {
		t.Fatalf("hello %+v", hello.GetHello())
	}
	initRaw, err := proto.Marshal(&mdctlv1.InitPayload{
		Venue: "V", Route: "r", N: 1, MdInstance: "md1", Url: url, ConnEpoch: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := down.Write(downMsg(&workerv1.WorkerDown_Init{Init: &workerv1.Init{
		Contract: Contract, Payload: initRaw,
	}})); err != nil {
		t.Fatal(err)
	}
	doneInit := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetDone().GetVerb() == workerv1.Verb_VERB_INIT })
	if !doneInit.GetDone().GetOk() {
		t.Fatal(doneInit.GetDone())
	}

	liveRaw, err := proto.Marshal(desired(1, 1, atom("V:r:ch", "trade.T")))
	if err != nil {
		t.Fatal(err)
	}
	if err := down.Write(downMsg(&workerv1.WorkerDown_Live{Live: &workerv1.Live{
		Contract: Contract, Payload: liveRaw, Digest: "d1",
	}})); err != nil {
		t.Fatal(err)
	}
	applied := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetApplied() != nil })
	if applied.GetApplied().GetDigest() != "d1" {
		t.Fatal(applied.GetApplied())
	}
	if err := down.Write(downMsg(&workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: Contract}})); err != nil {
		t.Fatal(err)
	}

	first := readReport(t, ups.next(t, isReport))
	if first.GetConnEpoch() != start+1 || first.GetState() != mdctlv1.ConnState_CONN_STATE_CONNECTING {
		t.Fatalf("first report %+v", first)
	}
	if n := len(rec.snapshot()); n != 0 {
		t.Fatalf("published %d before the first dial", n)
	}
	close(open1)

	got := rec.wait(t, 1)
	_, epoch, seq, ev := openPub(t, got[0])
	if got[0].subject != "md.f.md1.trade.T" || epoch != start+1 || seq != 1 || ev.GetTrade().GetTradeId() != "t1" {
		t.Fatalf("%s epoch %d seq %d id %s", got[0].subject, epoch, seq, ev.GetTrade().GetTradeId())
	}
	close(drop)

	second := readReport(t, ups.next(t, isReport))
	if second.GetConnEpoch() != start+2 || second.GetState() != mdctlv1.ConnState_CONN_STATE_CONNECTING {
		t.Fatalf("second report %+v", second)
	}
	if n := len(rec.snapshot()); n != 1 {
		t.Fatalf("published %d before the second dial", n)
	}
	close(open2)

	got = rec.wait(t, 2)
	_, epoch, seq, ev = openPub(t, got[1])
	if epoch != start+2 || seq != 1 || ev.GetTrade().GetTradeId() != "t2" {
		t.Fatalf("epoch %d seq %d id %s", epoch, seq, ev.GetTrade().GetTradeId())
	}

	if err := down.Write(downMsg(&workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: "test"}})); err != nil {
		t.Fatal(err)
	}
	exiting := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetExiting() != nil })
	if exiting.GetExiting().GetOutcome() != commonv1.Outcome_OUTCOME_DONE || exiting.GetExiting().GetReason() != "test" {
		t.Fatalf("exiting %+v", exiting.GetExiting())
	}
	select {
	case c := <-code:
		if c != 0 {
			t.Fatalf("exit %d", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
	}
}

func TestServeRejectsInit(t *testing.T) {
	peer, workerEnd := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	_ = peer.SetDeadline(deadline)
	_ = workerEnd.SetDeadline(deadline)
	t.Cleanup(func() { peer.Close(); workerEnd.Close() })
	code := make(chan int, 1)
	go func() {
		code <- Serve(t.Context(), workerEnd, ServeConfig{
			Lookup:  func(string) (exchange.Market, bool) { return &stubMarket{}, true },
			Publish: func(string, []byte) error { return nil },
		})
	}()
	ups := newUpFeed()
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
	if err := frame.NewWriter(peer).Write(downMsg(&workerv1.WorkerDown_Init{Init: &workerv1.Init{
		Contract: Contract,
	}})); err != nil {
		t.Fatal(err)
	}
	// Hello is in the feed too. The failing Done is what this checks.
	doneMsg := ups.next(t, func(m *workerv1.WorkerUp) bool {
		return m.GetDone().GetVerb() == workerv1.Verb_VERB_INIT
	})
	if doneMsg.GetDone().GetOk() {
		t.Fatal("init accepted")
	}
	exiting := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetExiting() != nil })
	if exiting.GetExiting().GetOutcome() != commonv1.Outcome_OUTCOME_INIT_FAILED {
		t.Fatalf("exiting %+v", exiting.GetExiting())
	}
	stopped := ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetStopped() != nil })
	if stopped.GetStopped().GetOnStopCompleted() {
		t.Fatal("OnStop ran")
	}
	select {
	case c := <-code:
		if c != exitInit {
			t.Fatalf("exit %d", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
	}
}

func isReport(m *workerv1.WorkerUp) bool { return m.GetReport() != nil }

func readReport(t *testing.T, m *workerv1.WorkerUp) *mdctlv1.Report {
	t.Helper()
	if m.GetReport().GetContract() != Contract {
		t.Fatalf("contract %s", m.GetReport().GetContract())
	}
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(m.GetReport().GetPayload(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func downMsg(msg any) *workerv1.WorkerDown {
	switch msg := msg.(type) {
	case *workerv1.WorkerDown_Init:
		return &workerv1.WorkerDown{Msg: msg}
	case *workerv1.WorkerDown_Start:
		return &workerv1.WorkerDown{Msg: msg}
	case *workerv1.WorkerDown_Live:
		return &workerv1.WorkerDown{Msg: msg}
	case *workerv1.WorkerDown_Stop:
		return &workerv1.WorkerDown{Msg: msg}
	default:
		panic("downMsg")
	}
}

// upFeed is every WorkerUp, read without blocking the peer: a blocked reader would stall
// Report, which holds the same writer as Beat.
type upFeed struct {
	mu sync.Mutex
	ms []*workerv1.WorkerUp
	i  int
	ch chan struct{}
}

func newUpFeed() *upFeed { return &upFeed{ch: make(chan struct{}, 1)} }

func (f *upFeed) add(m *workerv1.WorkerUp) {
	f.mu.Lock()
	f.ms = append(f.ms, m)
	f.mu.Unlock()
	select {
	case f.ch <- struct{}{}:
	default:
	}
}

func (f *upFeed) next(t *testing.T, ok func(*workerv1.WorkerUp) bool) *workerv1.WorkerUp {
	t.Helper()
	dead := time.NewTimer(5 * time.Second)
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

// tradeSession turns one text frame into one trade on channel ch. It is the whole venue.
type tradeSession struct {
	memSession
}

func (s *tradeSession) Decode(frame []byte, sink exchange.Sink) error {
	if len(frame) == 0 {
		return nil
	}
	sink.Trade("ch", &exchange.Trade{TradeID: string(frame), Price: 1, Qty: 1})
	return nil
}
