// Package c3 is the agent's end of C3, the unix socket to a worker's shim (PROTOCOL §4): attach, the lifecycle
// requests, and the stream of events.
//
// A Conn is one connection. Dial attaches and returns the shim's Attached, the snapshot an agent decides on
// (DECISIONS D108); events after it come on Events. The connection ends when the shim closes it (after Release, or
// when it exits), when another agent with an epoch at least as large attaches (Preempted), when the agent falls
// behind on events, or on Close. A new connection is a new Dial.
package c3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/shim"
)

const (
	// writeTimeout bounds one write to the shim. A shim that does not read for that long is stuck, and the
	// connection ends: the shim's own writes to the agent have a timeout too (PROTOCOL §4.1).
	writeTimeout = 5 * time.Second
	// maxQueue is how many events the Conn holds for an agent that is not reading Events. The shim sends at most
	// the 1024 of its ring on attach, then one event per worker message: an agent this far behind is stuck, and
	// the connection ends with ErrBehind rather than grow without bound. Nothing is lost by it that matters: the
	// next Dial's Attached has the state (D108).
	maxQueue = 4 * 1024
	// redial is how long Dial waits between attempts to connect while the socket is not there yet.
	redial = 20 * time.Millisecond
)

var (
	// ErrClosed: the connection has ended, closed by the shim (after Release, or because it exited) or by Close.
	ErrClosed = errors.New("c3: connection closed")
	// ErrBehind: the agent did not read Events, and the Conn stopped holding them (maxQueue).
	ErrBehind = errors.New("c3: events not read; the agent fell behind")
)

// PreemptedError ends a connection whose agent was replaced: another connection attached with an epoch at least as
// large (PROTOCOL §4.2 step 3).
type PreemptedError struct {
	ByEpoch int64
}

func (e *PreemptedError) Error() string { return fmt.Sprintf("c3: preempted by epoch %d", e.ByEpoch) }

// Error is a request the shim refused: Response.error. Code is one of the shim's codes (shim.CodeExited, ...;
// PROTOCOL §4.2).
type Error struct {
	Op      string // the request: "attach", "init", "release"
	Code    string
	Message string
	Details map[string]string // stale_epoch: "epoch"; bad_stage: "phase"
}

func (e *Error) Error() string { return fmt.Sprintf("c3: %s refused: %s: %s", e.Op, e.Code, e.Message) }

// CodeOf is the shim's code if err is a refused request, "" otherwise.
func CodeOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// Config is who attaches: the agent that spawned the shim, at its epoch.
type Config struct {
	Plane    string // Attach.plane and agent_instance: must be the Launch's
	Instance string
	Epoch    int64    // §3.2 W2; from 1
	Release  string   // Attach.agent_release, for the shim's log
	Majors   []uint32 // the shim majors the agent speaks; nil is shim.Major
	// ResumeFrom is the first event wanted (Attach.resume_from); 0 is the oldest the shim kept. The Attached
	// covers every event before its next_seq (D108), so an agent that wants no history may ignore those.
	ResumeFrom uint64
	Log        zerolog.Logger
}

// Conn is an attached connection to a shim. Its methods are safe for concurrent use. Close it when done with it,
// whatever ended it.
type Conn struct {
	op       string // what the Conn is to, for errors: the socket's path
	c        net.Conn
	r        *frame.Reader
	log      zerolog.Logger
	attached *shimv1.Attached

	wmu sync.Mutex // one write at a time, with its deadline
	w   *frame.Writer

	mu      sync.Mutex
	nextID  uint64
	pending map[uint64]func(*shimv1.Response) // by request id: what to do with the response
	err     error                             // why the connection ended; nil while it is up
	done    chan struct{}                     // closed when err is set

	qmu    sync.Mutex
	queue  []*shimv1.Event // read from the shim, not yet taken from events
	wake   chan struct{}   // a new event in queue, or the reader has ended; capacity 1
	events chan *shimv1.Event

	readerDone chan struct{} // the reader has ended: nothing more comes into queue
	closed     chan struct{} // Close was called: stop delivering
	closeOnce  sync.Once
}

// Dial connects to the shim's socket at path, waiting for it to appear, and attaches (PROTOCOL §4.2). A refused
// Attach is an *Error: shim.CodeWrongAgent, CodeStaleEpoch (Details["epoch"] is the epoch to beat), CodeBadRequest.
// ctx bounds the wait for the socket and the Attach; the Conn outlives it.
func Dial(ctx context.Context, path string, cfg Config) (*Conn, error) {
	nc, err := connect(ctx, path)
	if err != nil {
		return nil, err
	}
	majors := cfg.Majors
	if majors == nil {
		majors = []uint32{shim.Major}
	}
	c := &Conn{
		op: path, c: nc, r: frame.NewReader(nc), w: frame.NewWriter(nc),
		log:     cfg.Log.With().Str("shim_socket", path).Logger(),
		pending: map[uint64]func(*shimv1.Response){}, done: make(chan struct{}),
		wake: make(chan struct{}, 1), events: make(chan *shimv1.Event),
		readerDone: make(chan struct{}), closed: make(chan struct{}),
	}
	go c.read()
	r, err := c.call(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Attach{Attach: &shimv1.Attach{
		Plane: cfg.Plane, AgentInstance: cfg.Instance, Epoch: cfg.Epoch, AgentRelease: cfg.Release,
		ShimMajors: majors, ResumeFrom: cfg.ResumeFrom,
	}}})
	if err != nil {
		c.Close()
		return nil, err
	}
	if r.GetAttached() == nil {
		c.Close()
		return nil, fmt.Errorf("c3: %s: Attach answered without Attached", path)
	}
	c.attached = r.GetAttached()
	go c.pump(c.attached.GetShimMajor() != 0)
	return c, nil
}

// connect dials path until it answers: the shim listens only once it has read its Launch (PROTOCOL §4.1), and a
// socket left by a shim that was killed refuses until the next one replaces it.
func connect(ctx context.Context, path string) (net.Conn, error) {
	var d net.Dialer
	for {
		nc, err := d.DialContext(ctx, "unix", path)
		if err == nil {
			return nc, nil
		}
		if !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("c3: dial %s: %w", path, err)
		}
		t := time.NewTimer(redial)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, fmt.Errorf("c3: dial %s: %w (last: %w)", path, ctx.Err(), err)
		case <-t.C:
		}
	}
}

// Attached is the shim's answer to Dial's Attach: the state of the worker at one moment, covering every event
// before its next_seq (PROTOCOL §4.4, D108). With shim_major 0 the Conn may only Signal, and gets no events.
func (c *Conn) Attached() *shimv1.Attached { return c.attached }

// Events are the shim's events from Attached on, in the order sent: first the latest Report, then the kept events
// from ResumeFrom, then each new one (PROTOCOL §4.4). An event taken from it is acknowledged to the shim, so that
// it frees its ring early; Ack is not a commit (D108). The channel is closed after the connection has ended and the
// events read before that are taken, or on Close.
func (c *Conn) Events() <-chan *shimv1.Event { return c.events }

// Done is closed when the connection has ended; Err says why.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err is why the connection ended: ErrClosed, ErrBehind, a *PreemptedError, or an I/O error. nil while it is up.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close ends the connection, if it has not ended, and stops delivering events. The shim goes on: the worker's
// lifecycle is not tied to the connection (PROTOCOL §4.3).
func (c *Conn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	c.fail(ErrClosed)
	return nil
}

// Signal sends signo to the worker's process group. shim.CodeExited once the worker has been reaped.
func (c *Conn) Signal(ctx context.Context, sig syscall.Signal) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Signal{Signal: &shimv1.Signal{Signo: int32(sig)}}})
}

// Init passes init to the worker. shim.CodeBadStage unless the phase is NEW.
func (c *Conn) Init(ctx context.Context, init *workerv1.Init) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Init{Init: init}})
}

// Start passes start to the worker. shim.CodeBadStage unless the phase is INITIALIZED.
func (c *Conn) Start(ctx context.Context, start *workerv1.Start) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Start{Start: start}})
}

// Stop asks the worker to stop. The shim sends SIGTERM to its group after grace, and SIGKILL after killAfter more;
// 0 is no wait (PROTOCOL §4.3). Only the first Stop sets the timers.
func (c *Conn) Stop(ctx context.Context, reason string, grace, killAfter time.Duration) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Stop{Stop: &shimv1.Stop{
		Reason: reason, GraceMs: millis(grace), KillAfterMs: millis(killAfter),
	}}})
}

// Relay passes a C5 command to the worker, or answers one of its Relays by corr_id.
func (c *Conn) Relay(ctx context.Context, relay *workerv1.Relay) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Relay{Relay: relay}})
}

// Live passes a live config to the worker (PROTOCOL §4.3). The worker answers with Applied when it has applied it
// (DECISIONS D111), as an event, and in Attached.applied_digest.
func (c *Conn) Live(ctx context.Context, live *workerv1.Live) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Live{Live: live}})
}

// Release ends the shim, once the worker's end is recorded (shim.CodeRunning before that): the shim removes the
// exit file and its socket, and exits (PROTOCOL §4.3). The connection then ends with ErrClosed.
func (c *Conn) Release(ctx context.Context) error {
	return c.do(ctx, &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Release{Release: &shimv1.Release{}}})
}

func millis(d time.Duration) uint32 {
	return uint32(min(max(d.Milliseconds(), 0), math.MaxUint32))
}

// do is call for a request answered by ok or an error.
func (c *Conn) do(ctx context.Context, req *shimv1.ShimRequest) error {
	_, err := c.call(ctx, req)
	return err
}

// call sends req and waits for its response. A refused request is an *Error. If ctx ends first the request may
// still be carried out: the shim does what it has read.
func (c *Conn) call(ctx context.Context, req *shimv1.ShimRequest) (*shimv1.Response, error) {
	ch := make(chan *shimv1.Response, 1)
	id, err := c.send(req, func(r *shimv1.Response) { ch <- r })
	if err != nil {
		return nil, err
	}
	select {
	case r := <-ch:
		return r, refused(req, r)
	case <-c.done:
		select { // the response may have come just before the end
		case r := <-ch:
			return r, refused(req, r)
		default:
			return nil, c.Err()
		}
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// send writes req with the next id, and has then called with its response.
func (c *Conn) send(req *shimv1.ShimRequest, then func(*shimv1.Response)) (uint64, error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return 0, c.err
	}
	c.nextID++
	id := c.nextID
	c.pending[id] = then
	c.mu.Unlock()

	req.Id = id
	c.wmu.Lock()
	c.c.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck // a closed conn fails the write
	err := c.w.Write(req)
	c.wmu.Unlock()
	if err != nil {
		c.fail(fmt.Errorf("c3: %s: write: %w", c.op, err))
		return 0, c.Err()
	}
	return id, nil
}

func refused(req *shimv1.ShimRequest, r *shimv1.Response) error {
	if r.GetOk() {
		return nil
	}
	e := r.GetError()
	return &Error{Op: opName(req), Code: e.GetCode(), Message: e.GetMessage(), Details: e.GetDetails()}
}

// opName is the request's operation as the proto spells it: "attach", "init".
func opName(req *shimv1.ShimRequest) string {
	m := req.ProtoReflect()
	if f := m.WhichOneof(m.Descriptor().Oneofs().ByName("op")); f != nil {
		return string(f.Name())
	}
	return "request"
}

// fail ends the connection with err, the first time.
func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	c.pending = map[uint64]func(*shimv1.Response){}
	close(c.done)
	c.mu.Unlock()
	c.c.Close()
}

// read reads the shim's messages until the connection ends.
func (c *Conn) read() {
	defer func() {
		close(c.readerDone)
		c.poke()
	}()
	for {
		m := &shimv1.ShimMessage{}
		if err := c.r.Read(m); err != nil {
			c.fail(c.readErr(err))
			return
		}
		switch msg := m.GetMsg().(type) {
		case *shimv1.ShimMessage_Response:
			c.mu.Lock()
			then := c.pending[msg.Response.GetId()]
			delete(c.pending, msg.Response.GetId())
			c.mu.Unlock()
			if then != nil {
				then(msg.Response)
			}
		case *shimv1.ShimMessage_Event:
			if !c.enqueue(msg.Event) {
				c.fail(ErrBehind)
				return
			}
		case *shimv1.ShimMessage_Preempted:
			c.fail(&PreemptedError{ByEpoch: msg.Preempted.GetByEpoch()})
			return
		default: // a message of a later minor: not for this agent
			c.log.Debug().Msg("ignored an unknown message from the shim")
		}
	}
}

func (c *Conn) readErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return fmt.Errorf("c3: %s: read: %w", c.op, err)
}

// enqueue holds ev for Events; false when maxQueue are held already, and the queue is dropped.
func (c *Conn) enqueue(ev *shimv1.Event) bool {
	c.qmu.Lock()
	if len(c.queue) >= maxQueue {
		c.queue = nil
		c.qmu.Unlock()
		return false
	}
	c.queue = append(c.queue, ev)
	c.qmu.Unlock()
	c.poke()
	return true
}

func (c *Conn) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// pump hands the queued events to Events, one at a time, and acknowledges them: once the queue is empty, it sends
// one Ack for the last taken, so a burst costs one Ack. With no common major there are no events to acknowledge.
func (c *Conn) pump(ack bool) {
	defer close(c.events)
	var taken, acked uint64
	for {
		c.qmu.Lock()
		if len(c.queue) == 0 {
			c.qmu.Unlock()
			if ack && taken > acked {
				c.ack(taken)
				acked = taken
			}
			select {
			case <-c.readerDone:
				c.qmu.Lock()
				empty := len(c.queue) == 0
				c.qmu.Unlock()
				if empty {
					return
				}
			case <-c.wake:
			case <-c.closed:
				return
			}
			continue
		}
		ev := c.queue[0]
		c.qmu.Unlock()
		select {
		case c.events <- ev:
			c.qmu.Lock()
			if len(c.queue) > 0 && c.queue[0] == ev { // not dropped by enqueue meanwhile
				c.queue[0] = nil
				c.queue = c.queue[1:]
			}
			c.qmu.Unlock()
			if ev.GetSeq() > 0 {
				taken = ev.GetSeq()
			}
		case <-c.closed:
			return
		}
	}
}

// ack sends Ack{seq} without waiting for the answer; a refusal is only logged: Ack frees the shim's ring early, and
// nothing depends on it (D108).
func (c *Conn) ack(seq uint64) {
	_, err := c.send(&shimv1.ShimRequest{Op: &shimv1.ShimRequest_Ack{Ack: &shimv1.Ack{Seq: seq}}},
		func(r *shimv1.Response) {
			if !r.GetOk() {
				c.log.Warn().Uint64("seq", seq).Str("code", r.GetError().GetCode()).
					Str("error", r.GetError().GetMessage()).Msg("ack refused")
			}
		})
	if err != nil && !errors.Is(err, ErrClosed) {
		c.log.Debug().Err(err).Uint64("seq", seq).Msg("ack not sent")
	}
}
