package c3

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// These run the client against a scripted shim in the test process, for what a real shim does not do on demand.

// fakeShim is the shim's end of one connection.
type fakeShim struct {
	t *testing.T
	c net.Conn
	r *frame.Reader
	w *frame.Writer
}

// listenFake accepts one connection at a new socket, answers its Attach with at, and hands it to serve.
func listenFake(t *testing.T, at *shimv1.Attached, serve func(*fakeShim)) string {
	t.Helper()
	path := filepath.Join(shortDir(t), "s.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		f := &fakeShim{t: t, c: c, r: frame.NewReader(c), w: frame.NewWriter(c)}
		req := f.next()
		if req.GetAttach() == nil {
			t.Errorf("first request %v, want Attach", req)
			return
		}
		f.respond(req.GetId(), &shimv1.Response{Ok: true, Attached: at})
		serve(f)
	}()
	return path
}

// next reads the next request; nil once the client has gone.
func (f *fakeShim) next() *shimv1.ShimRequest {
	f.c.SetReadDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck
	req := &shimv1.ShimRequest{}
	if err := f.r.Read(req); err != nil {
		return nil
	}
	return req
}

func (f *fakeShim) send(m *shimv1.ShimMessage) {
	if err := f.w.Write(m); err != nil {
		f.t.Errorf("fake shim: %v", err)
	}
}

func (f *fakeShim) respond(id uint64, r *shimv1.Response) {
	r.Id = id
	f.send(&shimv1.ShimMessage{Msg: &shimv1.ShimMessage_Response{Response: r}})
}

func (f *fakeShim) event(seq uint64) {
	f.send(&shimv1.ShimMessage{Msg: &shimv1.ShimMessage_Event{Event: &shimv1.Event{
		Seq: seq, Kind: &shimv1.Event_Stage{Stage: &workerv1.Stage{Name: "s"}},
	}}})
}

func attached() *shimv1.Attached { return &shimv1.Attached{ShimMajor: 1, NextSeq: 1, OldestSeq: 1} }

// Events read before the shim closes are still delivered, then the channel closes; a burst is acknowledged once,
// for its last event.
func TestEventsAfterEnd(t *testing.T) {
	acks := make(chan uint64, 10)
	path := listenFake(t, attached(), func(f *fakeShim) {
		for seq := uint64(1); seq <= 100; seq++ {
			f.event(seq)
		}
		f.send(&shimv1.ShimMessage{}) // a message this client does not know: ignored
		f.event(0)                    // a Report, Beat or Relay: not acknowledged
		for req := f.next(); req != nil; req = f.next() {
			if a := req.GetAck(); a != nil {
				acks <- a.GetSeq()
				f.respond(req.GetId(), &shimv1.Response{Ok: true})
				f.c.Close()
				return
			}
		}
	})
	c := dial(t, path, cfg(1))
	time.Sleep(100 * time.Millisecond) // everything is in the queue: one burst
	var got []uint64
	for ev := range c.Events() {
		got = append(got, ev.GetSeq())
	}
	if len(got) != 101 || got[0] != 1 || got[99] != 100 || got[100] != 0 {
		t.Fatalf("got %d events %v, want 1-100 and a 0", len(got), got)
	}
	if a := <-acks; a != 100 {
		t.Fatalf("first Ack %d, want 100: one for the burst", a)
	}
	if !errors.Is(c.Err(), ErrClosed) {
		t.Fatalf("ended with %v, want ErrClosed", c.Err())
	}
}

// An agent that does not read Events is cut off at maxQueue, rather than hold them without bound.
func TestBehind(t *testing.T) {
	path := listenFake(t, attached(), func(f *fakeShim) {
		for seq := uint64(1); seq <= maxQueue+1; seq++ {
			f.event(seq)
		}
		f.next() // until the client goes
	})
	c := dial(t, path, cfg(1))
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("not cut off")
	}
	if !errors.Is(c.Err(), ErrBehind) {
		t.Fatalf("ended with %v, want ErrBehind", c.Err())
	}
}

// A request whose response does not come: its context ends the wait, and the connection stays; the shim closing
// fails the ones still waiting.
func TestNoResponse(t *testing.T) {
	closeNow := make(chan struct{})
	path := listenFake(t, attached(), func(f *fakeShim) {
		f.next() // Init: never answered
		f.next() // Start: never answered
		<-closeNow
	})
	c := dial(t, path, cfg(1))
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Init(short, initReq()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Init: %v, want the deadline", err)
	}
	if c.Err() != nil {
		t.Fatalf("ended with %v", c.Err())
	}
	got := make(chan error, 1)
	go func() { got <- c.Start(ctx(t), startReq()) }()
	time.Sleep(50 * time.Millisecond)
	close(closeNow)
	if err := <-got; !errors.Is(err, ErrClosed) {
		t.Fatalf("Start: %v, want ErrClosed", err)
	}
}
