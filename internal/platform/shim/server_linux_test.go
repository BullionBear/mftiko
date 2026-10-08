package shim

import (
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

// The agent the test shims belong to.
const (
	testPlane    = "td"
	testInstance = "td-tw-1"
)

// agent is a test's end of a C3 connection.
type agent struct {
	t      *testing.T
	c      net.Conn
	w      *frame.Writer
	r      *frame.Reader
	nextID uint64
}

// dial connects to the shim's socket, waiting for it to appear.
func dial(t *testing.T, path string) *agent {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.Dial("unix", path)
		if err == nil {
			t.Cleanup(func() { c.Close() })
			return &agent{t: t, c: c, w: frame.NewWriter(c), r: frame.NewReader(c)}
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// send writes req with the next id, and returns the id.
func (a *agent) send(req *shimv1.ShimRequest) uint64 {
	a.t.Helper()
	a.nextID++
	req.Id = a.nextID
	if err := a.w.Write(req); err != nil {
		a.t.Fatalf("send: %v", err)
	}
	return req.Id
}

// next reads the next message from the shim.
func (a *agent) next() *shimv1.ShimMessage {
	a.t.Helper()
	a.c.SetReadDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck
	m := &shimv1.ShimMessage{}
	if err := a.r.Read(m); err != nil {
		a.t.Fatalf("read: %v", err)
	}
	return m
}

// call sends req and returns the response to it, which must be the next message.
func (a *agent) call(req *shimv1.ShimRequest) *shimv1.Response {
	a.t.Helper()
	id := a.send(req)
	m := a.next()
	r := m.GetResponse()
	if r == nil || r.GetId() != id {
		a.t.Fatalf("want the response to %d, got %v", id, m)
	}
	return r
}

func attachReq(plane, instance string, epoch int64, majors ...uint32) *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Attach{Attach: &shimv1.Attach{
		Plane: plane, AgentInstance: instance, Epoch: epoch, AgentRelease: "test", ShimMajors: majors,
	}}}
}

// attach attaches as the shim's own agent.
func (a *agent) attach(epoch int64, majors ...uint32) *shimv1.Response {
	a.t.Helper()
	return a.call(attachReq(testPlane, testInstance, epoch, majors...))
}

func (a *agent) signal(sig syscall.Signal) *shimv1.Response {
	a.t.Helper()
	return a.call(&shimv1.ShimRequest{Op: &shimv1.ShimRequest_Signal{Signal: &shimv1.Signal{Signo: int32(sig)}}})
}

func initReq() *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Init{Init: &workerv1.Init{Contract: "fake.v1"}}}
}

// eof waits for the shim to close the connection, skipping whatever comes before. A shim that closes with requests
// of ours still unread resets the connection, and one that closes in the middle of a write cuts a frame short.
func (a *agent) eof() {
	a.t.Helper()
	a.c.SetReadDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck
	for {
		if _, err := a.r.Next(); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, frame.ErrTruncated) {
				a.t.Fatalf("want the shim to close the connection, got %v", err)
			}
			return
		}
	}
}

// preempted reads the next message, which must be a Preempted by epoch, and then the end of the connection.
func (a *agent) preempted(by int64) {
	a.t.Helper()
	m := a.next()
	if m.GetPreempted() == nil || m.GetPreempted().GetByEpoch() != by {
		a.t.Fatalf("want Preempted by %d, got %v", by, m)
	}
	a.eof()
}

func wantOK(t *testing.T, r *shimv1.Response) {
	t.Helper()
	if !r.GetOk() || r.GetError() != nil {
		t.Fatalf("want ok, got %v", r)
	}
}

func wantErr(t *testing.T, r *shimv1.Response, code string) {
	t.Helper()
	if r.GetOk() || r.GetError().GetCode() != code {
		t.Fatalf("want %s, got %v", code, r)
	}
}

// fakeWorker stands in for the worker behind an in-process server.
type fakeWorker struct {
	mu      sync.Mutex
	signals []syscall.Signal
	err     error // what Signal returns, instead of recording the signal
	verbs   []string
	verbErr error // what Init, Start, Relay, Stop and Release return
	stops   []fakeStop
}

type fakeStop struct {
	reason           string
	grace, killAfter time.Duration
}

func (f *fakeWorker) Signal(sig syscall.Signal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.signals = append(f.signals, sig)
	return nil
}

func (f *fakeWorker) Attached() *shimv1.Attached {
	return &shimv1.Attached{State: shimv1.WorkerState_WORKER_STATE_RUNNING, Pid: 4242, Pgid: 4242, StartTime: 77,
		Stage: "running", LastBeatAtNs: 5, Phase: shimv1.Phase_PHASE_STARTED}
}

func (f *fakeWorker) verb(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.verbErr != nil {
		return f.verbErr
	}
	f.verbs = append(f.verbs, name)
	return nil
}

func (f *fakeWorker) Init(*workerv1.Init) error   { return f.verb("init") }
func (f *fakeWorker) Start(*workerv1.Start) error { return f.verb("start") }
func (f *fakeWorker) Relay(*workerv1.Relay) error { return f.verb("relay") }
func (f *fakeWorker) Release() error              { return f.verb("release") }

func (f *fakeWorker) Stop(reason string, grace, killAfter time.Duration) (bool, error) {
	if err := f.verb("stop"); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, fakeStop{reason, grace, killAfter})
	return len(f.stops) == 1, nil
}

func (f *fakeWorker) setVerbErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verbErr = err
}

func (f *fakeWorker) gotVerbs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.verbs)
}

func (f *fakeWorker) got() []syscall.Signal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.signals)
}

func testLaunch(workDir string) *shimv1.Launch {
	return &shimv1.Launch{Plane: testPlane, AgentInstance: testInstance, SpecHash: "spec-1",
		Worker: &commonv1.WorkerRef{Key: "td/42", Incarnation: 3}, WorkDir: workDir}
}

// startServer runs a C3 server in the test process, for a fake worker, accepting the peer uid given. It returns the
// socket's path.
func startServer(t *testing.T, uid int) (string, *fakeWorker) {
	t.Helper()
	path, fw, _ := startServerAt(t, uid)
	return path, fw
}

// startServerAt is startServer, returning the server too.
func startServerAt(t *testing.T, uid int) (string, *fakeWorker, *server) {
	t.Helper()
	l := testLaunch(workDir(t))
	ln, err := listen(l)
	if err != nil {
		t.Fatal(err)
	}
	fw := &fakeWorker{}
	s := newServer(zerolog.New(zerolog.NewTestWriter(t)), ln, l, fw, uid)
	s.start()
	t.Cleanup(func() { // close ends every goroutine of the server, and promptly
		closed := make(chan struct{})
		go func() { s.close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Error("the server did not close")
		}
	})
	return SocketPath(l.GetWorkDir(), l.GetPlane(), l.GetWorker().GetKey()), fw, s
}

// Done criterion 1: agents with different epochs contend for one shim, and only the largest epoch takes effect.
// The smaller is refused or preempted, and nothing it asks runs once a larger one has attached.
func TestAttachLargerEpochWins(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())

	a := dial(t, path)
	wantOK(t, a.attach(5, Major))
	wantOK(t, a.signal(syscall.SIGUSR1))

	b := dial(t, path) // older than the controller: refused, and changes nothing
	r := b.attach(4, Major)
	wantErr(t, r, CodeStaleEpoch)
	if got := r.GetError().GetDetails()["epoch"]; got != "5" {
		t.Errorf("stale_epoch details %v, want epoch 5", r.GetError().GetDetails())
	}
	wantErr(t, b.signal(syscall.SIGKILL), CodeNotController)
	wantOK(t, a.signal(syscall.SIGUSR2))

	c := dial(t, path) // newer: takes over, and the old controller is told and closed
	wantOK(t, c.attach(6, Major))
	a.preempted(6)
	wantErr(t, b.signal(syscall.SIGKILL), CodeNotController)
	wantOK(t, c.signal(syscall.SIGTERM))

	if got, want := fw.got(), []syscall.Signal{syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM}; !slices.Equal(got, want) {
		t.Fatalf("signals sent %v, want %v: only the controller's", got, want)
	}
}

// Two agents attach at the same moment, many times over. Whichever the shim handles first, the larger epoch ends
// as the controller; the smaller is refused, or accepted and then preempted, and runs nothing after.
func TestAttachRace(t *testing.T) {
	for range 50 {
		path, fw := startServer(t, os.Geteuid())
		lo, hi := dial(t, path), dial(t, path)
		var wg sync.WaitGroup
		wg.Go(func() { lo.send(attachReq(testPlane, testInstance, 7, Major)) })
		wg.Go(func() { hi.send(attachReq(testPlane, testInstance, 8, Major)) })
		wg.Wait()

		if r := hi.next().GetResponse(); !r.GetOk() {
			t.Fatalf("epoch 8 refused: %v", r)
		}
		switch r := lo.next().GetResponse(); {
		case r.GetOk(): // first in: then preempted
			lo.preempted(8)
		case r.GetError().GetCode() == CodeStaleEpoch: // second in
			wantErr(t, lo.signal(syscall.SIGKILL), CodeNotController)
		default:
			t.Fatalf("epoch 7: %v", r)
		}
		wantOK(t, hi.signal(syscall.SIGTERM))
		if got := fw.got(); !slices.Equal(got, []syscall.Signal{syscall.SIGTERM}) {
			t.Fatalf("signals sent %v, want only the controller's SIGTERM", got)
		}
	}
}

// The largest epoch stays the one to beat after its agent is gone: the agent it replaced cannot come back. An
// equal epoch is the same agent reconnecting, and takes over.
func TestFenceOutlivesController(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	a := dial(t, path)
	wantOK(t, a.attach(5, Major))
	a.c.Close()

	old := dial(t, path)
	wantErr(t, old.attach(4, Major), CodeStaleEpoch)
	wantErr(t, old.signal(syscall.SIGKILL), CodeNotController)

	again := dial(t, path)
	wantOK(t, again.attach(5, Major))
	other := dial(t, path) // the same epoch once more, on another connection: the newest connection wins
	wantOK(t, other.attach(5, Major))
	again.preempted(5)
	wantOK(t, other.attach(5, Major)) // the controller attaching again is not preempted
	wantOK(t, other.signal(syscall.SIGTERM))
	if got := fw.got(); !slices.Equal(got, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals sent %v", got)
	}
}

// Done criterion 2: an Attach from another plane or instance is refused, takes nothing from the controller, and
// leaves its connection unable to do anything.
func TestAttachWrongAgent(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	ctl := dial(t, path)
	wantOK(t, ctl.attach(1, Major))

	for _, who := range []struct{ plane, instance string }{
		{"md", testInstance},   // another plane
		{testPlane, "td-tw-2"}, // another instance of the plane
		{"", ""},
	} {
		x := dial(t, path)
		wantErr(t, x.call(attachReq(who.plane, who.instance, 9, Major)), CodeWrongAgent)
		wantErr(t, x.signal(syscall.SIGKILL), CodeNotController)
	}
	wantOK(t, ctl.signal(syscall.SIGTERM)) // still the controller, and never preempted
	if got := fw.got(); !slices.Equal(got, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals sent %v", got)
	}

	// The controller itself sending a wrong Attach changes nothing either.
	wantErr(t, ctl.call(attachReq("md", testInstance, 9, Major)), CodeWrongAgent)
	wantErr(t, ctl.attach(0, Major), CodeBadRequest)
	wantOK(t, ctl.signal(syscall.SIGHUP))
}

// Done criterion 3: with no major in common the agent still attaches, is fenced like any other, and can Signal;
// nothing else.
func TestNoCommonMajor(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	a := dial(t, path)
	r := a.attach(1, Major+1, Major+7)
	wantOK(t, r)
	if r.GetAttached().GetShimMajor() != 0 {
		t.Fatalf("shim_major %d with no major in common", r.GetAttached().GetShimMajor())
	}
	wantOK(t, a.signal(syscall.SIGTERM))
	wantErr(t, a.call(initReq()), CodeNoCommonMajor)
	wantErr(t, a.call(&shimv1.ShimRequest{Op: &shimv1.ShimRequest_Ack{Ack: &shimv1.Ack{Seq: 1}}}), CodeNoCommonMajor)
	if got := fw.got(); !slices.Equal(got, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals sent %v", got)
	}

	// No major listed at all is no major in common.
	b := dial(t, path)
	r = b.attach(2)
	wantOK(t, r)
	a.preempted(2)
	if r.GetAttached().GetShimMajor() != 0 {
		t.Fatalf("shim_major %d with no major listed", r.GetAttached().GetShimMajor())
	}

	// One in common among others: that one, and the requests of v1 go past the major check.
	c := dial(t, path)
	r = c.attach(3, Major+1, Major)
	wantOK(t, r)
	if r.GetAttached().GetShimMajor() != Major {
		t.Fatalf("shim_major %d, want %d", r.GetAttached().GetShimMajor(), Major)
	}
	wantOK(t, c.call(initReq()))
}

// Attached carries the shim's release and major, the worker of the Launch, its spec hash and what the worker view
// says.
func TestAttached(t *testing.T) {
	path, _ := startServer(t, os.Geteuid())
	a := dial(t, path)
	r := a.attach(1, Major)
	wantOK(t, r)
	at := r.GetAttached()
	if at.GetShimRelease() != buildinfo.Version || at.GetShimMajor() != Major || at.GetSpecHash() != "spec-1" ||
		at.GetWorker().GetKey() != "td/42" || at.GetWorker().GetIncarnation() != 3 {
		t.Errorf("attached %v", at)
	}
	if at.GetPid() != 4242 || at.GetPgid() != 4242 || at.GetStartTime() != 77 || at.GetStage() != "running" ||
		at.GetState() != shimv1.WorkerState_WORKER_STATE_RUNNING || at.GetLastBeatAtNs() != 5 ||
		at.GetPhase() != shimv1.Phase_PHASE_STARTED {
		t.Errorf("attached %v: not the worker's", at)
	}
}

// Before an Attach, and after a refused one, a connection runs nothing.
func TestNotController(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	a := dial(t, path)
	wantErr(t, a.signal(syscall.SIGKILL), CodeNotController)
	wantErr(t, a.call(initReq()), CodeNotController)
	wantErr(t, a.call(&shimv1.ShimRequest{}), CodeNotController)
	if got := fw.got(); len(got) != 0 {
		t.Fatalf("signals sent %v", got)
	}
}

// What the controller may ask, and what comes back when it cannot be done.
func TestControllerRequests(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	a := dial(t, path)
	wantOK(t, a.attach(1, Major))
	wantErr(t, a.call(&shimv1.ShimRequest{}), CodeUnimplemented) // an op this shim does not know
	r := a.call(&shimv1.ShimRequest{Op: &shimv1.ShimRequest_Ack{Ack: &shimv1.Ack{Seq: 1}}})
	wantErr(t, r, CodeUnimplemented)
	if r.GetError().GetMessage() != "ack is not implemented yet" {
		t.Errorf("message %q", r.GetError().GetMessage())
	}
	wantErr(t, a.signal(0), CodeBadRequest)
	wantErr(t, a.signal(65), CodeBadRequest)
	wantOK(t, a.signal(64))

	fw.mu.Lock()
	fw.err = ErrExited
	fw.mu.Unlock()
	wantErr(t, a.signal(syscall.SIGKILL), CodeExited)
	fw.mu.Lock()
	fw.err = syscall.EPERM
	fw.mu.Unlock()
	if r := a.signal(syscall.SIGKILL); r.GetError().GetCode() != CodeSignalFailed || r.GetError().GetMessage() == "" {
		t.Fatalf("want %s with the reason, got %v", CodeSignalFailed, r)
	}
}

// A connection from another uid is closed unanswered (§4.1). The test cannot connect as another uid, so the
// server is told to accept another one instead.
func TestOtherUIDRefused(t *testing.T) {
	path, _ := startServer(t, os.Geteuid()+1)
	a := dial(t, path)
	a.w.Write(attachReq(testPlane, testInstance, 1, Major)) //nolint:errcheck // the shim may have closed it already
	a.eof()
}

// A frame that is not a ShimRequest ends the connection.
func TestGarbageEndsConnection(t *testing.T) {
	path, _ := startServer(t, os.Geteuid())
	a := dial(t, path)
	wantOK(t, a.attach(1, Major))
	if _, err := a.c.Write([]byte{0, 0, 0, 2, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	a.eof()
	b := dial(t, path) // the controller is gone; its epoch is not
	wantErr(t, b.attach(0, Major), CodeBadRequest)
	wantOK(t, b.attach(1, Major))
}

// An agent that sends and never reads loses its connection, and holds up no one: a newer agent attaches at once.
func TestAgentNotReading(t *testing.T) {
	path, _ := startServer(t, os.Geteuid())
	stuck := dial(t, path)
	wantOK(t, stuck.attach(1, Major))
	flooded := make(chan struct{})
	go func() {
		defer close(flooded)
		w := frame.NewWriter(stuck.c)
		for i := range 200000 {
			if w.Write(&shimv1.ShimRequest{Id: uint64(i), Op: initReq().Op}) != nil {
				return // the shim closed it
			}
		}
	}()
	select {
	case <-flooded:
	case <-time.After(20 * time.Second):
		t.Fatal("the shim kept reading from an agent that does not read")
	}
	next := dial(t, path)
	start := time.Now()
	wantOK(t, next.attach(2, Major))
	wantOK(t, next.signal(syscall.SIGTERM))
	if d := time.Since(start); d > writeTimeout/2 {
		t.Errorf("attach took %v", d)
	}
	stuck.eof()
}

func startReq() *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Start{Start: &workerv1.Start{Contract: "fake.v1"}}}
}

func relayReq(payload string) *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Relay{Relay: &workerv1.Relay{
		Contract: "fake.v1", CorrId: []byte("c1"), Payload: []byte(payload),
	}}}
}

func stopReq(graceMs, killAfterMs uint32) *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Stop{Stop: &shimv1.Stop{
		Reason: "test", GraceMs: graceMs, KillAfterMs: killAfterMs,
	}}}
}

func releaseReq() *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Release{Release: &shimv1.Release{}}}
}

// The lifecycle requests reach the worker, in the order sent, and its refusals come back as the codes of §4.2.
func TestLifecycleRequests(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	a := dial(t, path)
	wantOK(t, a.attach(1, Major))

	wantOK(t, a.call(initReq()))
	wantOK(t, a.call(startReq()))
	wantOK(t, a.call(relayReq("x")))
	wantOK(t, a.call(stopReq(1500, 2500)))
	wantOK(t, a.call(stopReq(1, 1))) // a second Stop is accepted, and keeps the first one's timers
	if got, want := fw.gotVerbs(), []string{"init", "start", "relay", "stop", "stop"}; !slices.Equal(got, want) {
		t.Fatalf("verbs %v, want %v", got, want)
	}
	if fw.stops[0] != (fakeStop{"test", 1500 * time.Millisecond, 2500 * time.Millisecond}) {
		t.Errorf("stop %+v", fw.stops[0])
	}

	for _, c := range []struct {
		err  error
		code string
	}{
		{ErrExited, CodeExited},
		{ErrRunning, CodeRunning},
		{ErrWorkerBusy, CodeWorkerBusy},
	} {
		fw.setVerbErr(c.err)
		for _, req := range []*shimv1.ShimRequest{initReq(), startReq(), relayReq("x"), stopReq(1, 1), releaseReq()} {
			wantErr(t, a.call(req), c.code)
		}
	}
	fw.setVerbErr(&stageError{verb: "start", phase: shimv1.Phase_PHASE_INITIALIZING})
	r := a.call(startReq())
	wantErr(t, r, CodeBadStage)
	if got := r.GetError().GetDetails()["phase"]; got != "initializing" {
		t.Errorf("bad_stage details %v", r.GetError().GetDetails())
	}
}

// Only the controller's lifecycle requests run.
func TestLifecycleNotController(t *testing.T) {
	path, fw := startServer(t, os.Geteuid())
	ctl := dial(t, path)
	wantOK(t, ctl.attach(2, Major))
	old := dial(t, path)
	wantErr(t, old.attach(1, Major), CodeStaleEpoch)
	for _, req := range []*shimv1.ShimRequest{initReq(), startReq(), relayReq("x"), stopReq(1, 1), releaseReq()} {
		wantErr(t, old.call(req), CodeNotController)
	}
	if got := fw.gotVerbs(); len(got) != 0 {
		t.Fatalf("verbs %v", got)
	}
}

// A Release accepted closes Released, and the response reaches the agent even though the shim closes the server
// right after.
func TestReleaseEndsServer(t *testing.T) {
	path, fw, srv := startServerAt(t, os.Geteuid())
	a := dial(t, path)
	wantOK(t, a.attach(1, Major))

	fw.setVerbErr(ErrRunning)
	wantErr(t, a.call(releaseReq()), CodeRunning)
	select {
	case <-srv.Released():
		t.Fatal("released while the worker runs")
	default:
	}

	fw.setVerbErr(nil)
	id := a.send(releaseReq())
	select {
	case <-srv.Released():
	case <-time.After(5 * time.Second):
		t.Fatal("Released not closed")
	}
	srv.close() // as Run does
	if r := a.next().GetResponse(); r.GetId() != id || !r.GetOk() {
		t.Fatalf("release response %v", r)
	}
	a.eof()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket left behind: %v", err)
	}
}
