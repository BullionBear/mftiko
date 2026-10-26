package conn

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/exchange"
)

func TestReportBusUp(t *testing.T) {
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

	bus := NewBus(true)
	peer, workerEnd := net.Pipe()
	deadline := time.Now().Add(15 * time.Second)
	_ = peer.SetDeadline(deadline)
	_ = workerEnd.SetDeadline(deadline)
	t.Cleanup(func() { peer.Close(); workerEnd.Close() })

	code := make(chan int, 1)
	go func() {
		code <- Serve(t.Context(), workerEnd, ServeConfig{
			Lookup: func(string) (exchange.Market, bool) {
				return &stubMarket{s: &memSession{lim: exchange.SessionLimits{ReadLimit: 1 << 20}}}, true
			},
			Publish: func(string, []byte) error { return nil },
			Bus:     bus,
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
	ups.next(t, func(m *workerv1.WorkerUp) bool { return m.GetHello() != nil })
	initRaw, err := proto.Marshal(&mdctlv1.InitPayload{
		Venue: "v", Route: "r", MdInstance: "md-a", Url: url,
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
	if err := down.Write(downMsg(&workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: Contract}})); err != nil {
		t.Fatal(err)
	}
	if !reportBus(t, ups.next(t, func(m *workerv1.WorkerUp) bool { return reportBusIs(m, true) })) {
		t.Fatal("initial bus")
	}
	bus.Set(false)
	if reportBus(t, ups.next(t, func(m *workerv1.WorkerUp) bool { return reportBusIs(m, false) })) {
		t.Fatal("down bus")
	}
	bus.Set(true)
	if !reportBus(t, ups.next(t, func(m *workerv1.WorkerUp) bool { return reportBusIs(m, true) })) {
		t.Fatal("up bus")
	}
	if err := down.Write(downMsg(&workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: "test"}})); err != nil {
		t.Fatal(err)
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

func reportBusIs(m *workerv1.WorkerUp, want bool) bool {
	if m.GetReport() == nil || m.GetReport().GetContract() != Contract {
		return false
	}
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(m.GetReport().GetPayload(), p); err != nil {
		return false
	}
	return p.BusUp != nil && p.GetBusUp() == want
}

func reportBus(t *testing.T, m *workerv1.WorkerUp) bool {
	t.Helper()
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(m.GetReport().GetPayload(), p); err != nil {
		t.Fatal(err)
	}
	if p.BusUp == nil {
		t.Fatal("bus_up unset")
	}
	return p.GetBusUp()
}
