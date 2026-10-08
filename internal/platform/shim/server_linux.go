package shim

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

const (
	// outQueue is how many messages a connection may have waiting to be written. An agent that lets more pile up
	// is not reading, and its connection is closed.
	outQueue = 256
	// writeTimeout bounds one write to an agent, so that a connection that stopped reading ends.
	writeTimeout = 5 * time.Second
	// maxSignal is SIGRTMAX on Linux: signals are 1 to 64.
	maxSignal = 64
)

// supportedMajors are the C3 majors this shim speaks (PROTOCOL §1).
var supportedMajors = []uint32{Major}

// controlled is what the C3 server needs of the worker: the view in view_linux.go, or a fake in tests.
type controlled interface {
	// Signal sends sig to the worker's process group; ErrExited once the worker has been reaped.
	Signal(sig syscall.Signal) error
	// Attached is what Attached says of the worker now: its state, last stage, pid, pgid, start time, Hello and
	// last Beat. The server fills in the rest.
	Attached() *shimv1.Attached
}

// server is the shim's C3 server (PROTOCOL §4.1, §4.2). It accepts any number of connections from the agent's uid,
// fences them by epoch, and runs what the controller asks; the others get not_controller.
//
// Requests are handled one at a time under mu, each connection's in the order it sent them. What goes back to a
// connection is queued, and written by that connection's own goroutine with a timeout: an agent that stops reading
// holds up only itself, and loses its connection.
type server struct {
	log    zerolog.Logger
	ln     *net.UnixListener
	launch *shimv1.Launch
	worker controlled
	uid    int // the one peer uid accepted: the shim's own, which is the agent's (§4.1)

	wg sync.WaitGroup // the accept loop, and each connection's reader and writer

	mu     sync.Mutex
	conns  map[*agentConn]struct{}
	ctl    *agentConn // the controller; nil when there is none
	epoch  int64      // the largest epoch attached so far, 0 before the first. Kept when its agent goes (§4.2)
	nextID uint64
	closed bool
}

// agentConn is one connection from an agent. Its fields other than id and nc are guarded by server.mu.
type agentConn struct {
	id  uint64
	nc  *net.UnixConn
	out chan *shimv1.ShimMessage // closed when the connection starts closing; the writer drains it, then closes nc

	closing bool
	epoch   int64  // of the Attach that made it the controller
	major   uint32 // negotiated by that Attach; 0 when there was no common major, which allows only Signal
}

func newServer(log zerolog.Logger, ln *net.UnixListener, l *shimv1.Launch, w controlled, uid int) *server {
	return &server{log: log, ln: ln, launch: l, worker: w, uid: uid, conns: map[*agentConn]struct{}{}}
}

// start accepts connections until close.
func (s *server) start() { s.wg.Go(s.serve) }

// close stops accepting, closes every connection and the listener, which removes the socket, and waits for the
// server's goroutines.
func (s *server) close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		s.closeLocked(c, false)
	}
	s.mu.Unlock()
	s.ln.Close()
	s.wg.Wait()
}

func (s *server) serve() {
	backoff := 5 * time.Millisecond
	for {
		nc, err := s.ln.AcceptUnix()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil { // out of fds, most likely: wait and try again, as net/http does
			s.log.Warn().Err(err).Dur("retry_in", backoff).Msg("accept")
			time.Sleep(backoff)
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 5 * time.Millisecond
		s.admit(nc)
	}
}

// admit takes a new connection if it comes from the agent's uid (§4.1).
func (s *server) admit(nc *net.UnixConn) {
	peer, err := peerCred(nc)
	if err != nil {
		s.log.Warn().Err(err).Msg("connection refused: no peer credentials")
		nc.Close()
		return
	}
	if int(peer.Uid) != s.uid {
		s.log.Warn().Int32("peer_pid", peer.Pid).Uint32("peer_uid", peer.Uid).Int("uid", s.uid).
			Msg("connection refused: not the agent's uid")
		nc.Close()
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		nc.Close()
		return
	}
	s.nextID++
	c := &agentConn{id: s.nextID, nc: nc, out: make(chan *shimv1.ShimMessage, outQueue)}
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	s.log.Info().Uint64("conn", c.id).Int32("peer_pid", peer.Pid).Msg("agent connected")
	s.wg.Go(func() { s.write(c) })
	s.wg.Go(func() { s.read(c) })
}

// read handles c's requests in order until the connection ends. A frame that does not decode as a ShimRequest is
// a broken agent, and ends the connection.
func (s *server) read(c *agentConn) {
	r := frame.NewReader(bufio.NewReader(c.nc))
	for {
		req := &shimv1.ShimRequest{}
		if err := r.Read(req); err != nil {
			s.drop(c, err)
			return
		}
		s.handle(c, req)
	}
}

// write writes what is queued for c until the queue is closed, then closes the connection.
func (s *server) write(c *agentConn) {
	defer c.nc.Close()
	w := frame.NewWriter(c.nc)
	for m := range c.out {
		c.nc.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck // fails only once nc is closed
		if err := w.Write(m); err != nil {
			s.drop(c, fmt.Errorf("write: %w", err))
			return
		}
	}
}

// drop ends c after its reader or writer failed. An end that the server started is not news.
func (s *server) drop(c *agentConn, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.closing {
		return
	}
	ev := s.log.Info()
	if !errors.Is(err, io.EOF) {
		ev = s.log.Warn().Err(err)
	}
	ev.Uint64("conn", c.id).Bool("controller", s.ctl == c).Msg("agent disconnected")
	s.closeLocked(c, false)
}

// closeLocked ends c. With flush the queue is written first, a Preempted last; without, the connection closes now.
// A controller that goes leaves no controller, and the epoch it attached with stays the one to beat.
func (s *server) closeLocked(c *agentConn, flush bool) {
	if c.closing {
		return
	}
	c.closing = true
	close(c.out)
	delete(s.conns, c)
	if s.ctl == c {
		s.ctl = nil
	}
	if !flush {
		c.nc.Close()
	}
}

// sendLocked queues m for c. A connection whose queue is full is not reading, and is closed.
func (s *server) sendLocked(c *agentConn, m *shimv1.ShimMessage) {
	if c.closing {
		return
	}
	select {
	case c.out <- m:
	default:
		s.log.Warn().Uint64("conn", c.id).Int("queued", outQueue).Msg("agent not reading: closing its connection")
		s.closeLocked(c, false)
	}
}

// handle runs one request and queues its response.
func (s *server) handle(c *agentConn, req *shimv1.ShimRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.closing { // preempted, or the server is closing: nothing it asks runs any more
		return
	}
	resp := s.respondLocked(c, req)
	resp.Id = req.GetId()
	s.sendLocked(c, &shimv1.ShimMessage{Msg: &shimv1.ShimMessage_Response{Response: resp}})
}

func (s *server) respondLocked(c *agentConn, req *shimv1.ShimRequest) *shimv1.Response {
	if a := req.GetAttach(); a != nil {
		return s.attachLocked(c, a)
	}
	if s.ctl != c {
		return fail(CodeNotController, "this connection is not the controller: attach first")
	}
	switch op := req.GetOp().(type) {
	case *shimv1.ShimRequest_Signal:
		return s.signal(c, op.Signal) // whatever the major (§4.2)
	case nil:
		return fail(CodeUnimplemented, "an operation this shim does not know")
	}
	if c.major == 0 {
		return fail(CodeNoCommonMajor, "no common shim major: only Signal")
	}
	// Init, Start, Stop, Relay and Release come with E1-11, Ack with E1-12.
	return fail(CodeUnimplemented, opName(req)+" is not implemented yet")
}

// attachLocked is PROTOCOL §4.2 steps 2-5: a refused Attach changes nothing; an accepted one makes c the
// controller and ends the one before it.
func (s *server) attachLocked(c *agentConn, a *shimv1.Attach) *shimv1.Response {
	log := s.log.With().Uint64("conn", c.id).Int64("epoch", a.GetEpoch()).Logger()
	switch {
	case a.GetPlane() != s.launch.GetPlane() || a.GetAgentInstance() != s.launch.GetAgentInstance():
		log.Warn().Str("agent_plane", a.GetPlane()).Str("agent_instance", a.GetAgentInstance()).
			Msg("attach refused: wrong agent")
		return fail(CodeWrongAgent, fmt.Sprintf("this shim belongs to %s %s, not %s %s",
			s.launch.GetPlane(), s.launch.GetAgentInstance(), a.GetPlane(), a.GetAgentInstance()))
	case a.GetEpoch() < 1:
		log.Warn().Msg("attach refused: no epoch")
		return fail(CodeBadRequest, "epoch must be at least 1")
	case a.GetEpoch() < s.epoch:
		log.Info().Int64("controller_epoch", s.epoch).Msg("attach refused: stale epoch")
		r := fail(CodeStaleEpoch, fmt.Sprintf("epoch %d is older than %d", a.GetEpoch(), s.epoch))
		r.Error.Details = map[string]string{"epoch": strconv.FormatInt(s.epoch, 10)}
		return r
	}
	if old := s.ctl; old != nil && old != c {
		s.sendLocked(old, &shimv1.ShimMessage{Msg: &shimv1.ShimMessage_Preempted{
			Preempted: &shimv1.Preempted{ByEpoch: a.GetEpoch()},
		}})
		s.closeLocked(old, true)
		log.Info().Uint64("preempted_conn", old.id).Int64("preempted_epoch", old.epoch).Msg("controller preempted")
	}
	s.ctl, s.epoch = c, a.GetEpoch()
	c.epoch, c.major = a.GetEpoch(), negotiate(a.GetShimMajors())
	log.Info().Uint32("shim_major", c.major).Str("agent_release", a.GetAgentRelease()).Msg("agent attached")

	at := s.worker.Attached()
	at.ShimRelease = buildinfo.Version
	at.ShimMajor = c.major
	at.Worker = proto.CloneOf(s.launch.GetWorker())
	at.SpecHash = s.launch.GetSpecHash()
	return &shimv1.Response{Ok: true, Attached: at}
}

func (s *server) signal(c *agentConn, sg *shimv1.Signal) *shimv1.Response {
	n := sg.GetSigno()
	if n < 1 || n > maxSignal {
		return fail(CodeBadRequest, fmt.Sprintf("signo %d is not 1-%d", n, maxSignal))
	}
	sig := syscall.Signal(n)
	err := s.worker.Signal(sig)
	switch {
	case errors.Is(err, ErrExited):
		return fail(CodeExited, "the worker has exited")
	case err != nil:
		s.log.Warn().Err(err).Uint64("conn", c.id).Str("signal", sig.String()).Msg("signal failed")
		return fail(CodeSignalFailed, err.Error())
	}
	s.log.Info().Uint64("conn", c.id).Str("signal", sig.String()).Msg("signalled the worker's group")
	return &shimv1.Response{Ok: true}
}

// negotiate is the largest major both the agent and the shim speak, 0 when there is none.
func negotiate(agent []uint32) uint32 {
	var best uint32
	for _, m := range agent {
		for _, ours := range supportedMajors {
			if m == ours && m > best {
				best = m
			}
		}
	}
	return best
}

func fail(code, msg string) *shimv1.Response {
	return &shimv1.Response{Error: &commonv1.Error{Code: code, Message: msg}}
}

// opName is the name of the request's operation, as the proto spells it: "init", "release".
func opName(req *shimv1.ShimRequest) string {
	m := req.ProtoReflect()
	if f := m.WhichOneof(m.Descriptor().Oneofs().ByName("op")); f != nil {
		return string(f.Name())
	}
	return "an unknown operation"
}
