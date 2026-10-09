package c3

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// These run the client against a real shim, the test binary re-executed, with the fake worker under it.

// envTestShim makes the re-executed test binary a shim; fakeworker's variables make it a worker.
const envTestShim = "MFTIKO_TEST_SHIM"

func TestMain(m *testing.M) {
	fakeworker.MaybeMain()
	if os.Getenv(envTestShim) == "1" {
		os.Exit(shim.Run(logger.NewWithWriter("mftiko-shim", os.Stderr)))
	}
	os.Exit(m.Run())
}

const (
	testPlane    = "td"
	testInstance = "td-tw-1"
	testKey      = "td/42"
)

// shimProc is a shim the test started, as an agent does.
type shimProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	socket string
	exited chan struct{} // closed when the shim has exited; code is then set
	code   int

	mu     sync.Mutex
	stderr bytes.Buffer
}

func (p *shimProc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stderr.Write(b)
}

// shortDir is a new WORK_DIR, short enough for a socket path under it (sun_path is 108 bytes).
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "c3-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// socketOf is where the shim for the test worker in workDir listens.
func socketOf(workDir string) string { return shim.SocketPath(workDir, testPlane, testKey) }

// startShim starts a shim for a fake worker running s, in workDir ("" for a new one).
func startShim(t *testing.T, s fakeworker.Script, workDir string) *shimProc {
	t.Helper()
	if workDir == "" {
		workDir = shortDir(t)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fake, err := fakeworker.Env(s, filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	env := append([]string{"MFTIKO_PLANE=" + testPlane, "MFTIKO_INSTANCE=" + testInstance,
		"MFTIKO_WORKER_KEY=" + testKey, "MFTIKO_INCARNATION=3"}, fake...)
	l := &shimv1.Launch{
		Plane: testPlane, AgentInstance: testInstance, SpecHash: "spec-1",
		Worker: &commonv1.WorkerRef{Key: testKey, Incarnation: 3},
		Path:   self, Env: env, Dir: "/", WorkDir: workDir,
	}
	p := &shimProc{t: t, socket: socketOf(workDir), exited: make(chan struct{})}
	p.cmd = exec.Command(self)
	p.cmd.Env = append(os.Environ(), envTestShim+"=1")
	p.cmd.Stderr = p
	stdin, err := p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		p.cmd.Wait() //nolint:errcheck // the code says
		p.code = p.cmd.ProcessState.ExitCode()
		close(p.exited)
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill() //nolint:errcheck // the worker follows by Pdeathsig (S2)
		<-p.exited
		if t.Failed() {
			p.mu.Lock()
			t.Logf("shim log:\n%s", p.stderr.String())
			p.mu.Unlock()
		}
	})
	if err := frame.NewWriter(stdin).Write(l); err != nil {
		t.Fatal(err)
	}
	stdin.Close()
	return p
}

// wantExit waits for the shim to exit with code.
func (p *shimProc) wantExit(code int) {
	p.t.Helper()
	select {
	case <-p.exited:
		if p.code != code {
			p.t.Fatalf("shim exited %d, want %d", p.code, code)
		}
	case <-time.After(10 * time.Second):
		p.t.Fatal("shim still running")
	}
}

func cfg(epoch int64) Config {
	return Config{Plane: testPlane, Instance: testInstance, Epoch: epoch, Release: "test", Log: zerolog.Nop()}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func dial(t *testing.T, path string, c Config) *Conn {
	t.Helper()
	conn, err := Dial(ctx(t), path, c)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// until takes events until one matches, failing if the channel closes or nothing comes in time.
func until(t *testing.T, c *Conn, what string, ok func(*shimv1.Event) bool) *shimv1.Event {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, open := <-c.Events():
			if !open {
				t.Fatalf("events ended before %s: %v", what, c.Err())
			}
			if ok(ev) {
				return ev
			}
		case <-deadline:
			t.Fatalf("no %s", what)
		}
	}
}

func isDone(verb workerv1.Verb) func(*shimv1.Event) bool {
	return func(ev *shimv1.Event) bool { return ev.GetDone().GetVerb() == verb && ev.GetDone().GetOk() }
}

func isExited(ev *shimv1.Event) bool { return ev.GetExited() != nil }

func initReq() *workerv1.Init { return &workerv1.Init{Contract: fakeworker.Contract} }

func startReq() *workerv1.Start { return &workerv1.Start{Contract: fakeworker.Contract} }

// A worker's whole life through the client: attach, Init, Start, Stop, the events of each, Release; then the
// connection ends with ErrClosed and the shim exits 0.
func TestLifecycle(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "")
	c := dial(t, p.socket, cfg(1))
	at := c.Attached()
	if at.GetShimMajor() != shim.Major || at.GetWorker().GetKey() != testKey || at.GetSpecHash() != "spec-1" ||
		at.GetPid() == 0 || at.GetState() == shimv1.WorkerState_WORKER_STATE_EXITED {
		t.Fatalf("Attached %v", at)
	}
	if ev := until(t, c, "Started", func(ev *shimv1.Event) bool { return ev.GetStarted() != nil }); ev.GetSeq() != 1 ||
		ev.GetStarted().GetPid() != at.GetPid() {
		t.Fatalf("Started %v, want seq 1 and pid %d", ev, at.GetPid())
	}
	until(t, c, "Hello", func(ev *shimv1.Event) bool { return ev.GetHello() != nil })

	if err := c.Init(ctx(t), initReq()); err != nil {
		t.Fatal(err)
	}
	until(t, c, "Done(INIT)", isDone(workerv1.Verb_VERB_INIT))
	if err := c.Start(ctx(t), startReq()); err != nil {
		t.Fatal(err)
	}
	until(t, c, "Done(START)", isDone(workerv1.Verb_VERB_START))
	if err := c.Release(ctx(t)); CodeOf(err) != shim.CodeRunning {
		t.Fatalf("Release while running: %v, want %s", err, shim.CodeRunning)
	}
	if err := c.Stop(ctx(t), "test", time.Second, time.Second); err != nil {
		t.Fatal(err)
	}
	ex := until(t, c, "Exited", isExited).GetExited().GetExit()
	if ex.GetOutcome() != commonv1.Outcome_OUTCOME_DONE || ex.GetReason() != "stop: test" || !ex.GetOnStopCompleted() {
		t.Fatalf("Exited %v", ex)
	}
	if err := c.Init(ctx(t), initReq()); CodeOf(err) != shim.CodeExited {
		t.Fatalf("Init after the exit: %v, want %s", err, shim.CodeExited)
	}
	if err := c.Release(ctx(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the connection outlived Release")
	}
	if !errors.Is(c.Err(), ErrClosed) {
		t.Fatalf("ended with %v, want ErrClosed", c.Err())
	}
	for range c.Events() { // closes once the rest is taken
	}
	p.wantExit(0)
	if err := c.Signal(ctx(t), syscall.SIGTERM); !errors.Is(err, ErrClosed) {
		t.Fatalf("Signal after the end: %v, want ErrClosed", err)
	}
}

// The shim's refusals come back as *Error, with their details.
func TestRefusals(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "")
	wrong := cfg(1)
	wrong.Instance = "td-tw-2"
	if _, err := Dial(ctx(t), p.socket, wrong); CodeOf(err) != shim.CodeWrongAgent {
		t.Fatalf("Dial as another instance: %v, want %s", err, shim.CodeWrongAgent)
	}

	c := dial(t, p.socket, cfg(2))
	_, err := Dial(ctx(t), p.socket, cfg(1))
	var e *Error
	if !errors.As(err, &e) || e.Code != shim.CodeStaleEpoch || e.Details["epoch"] != "2" || e.Op != "attach" {
		t.Fatalf("Dial at an older epoch: %#v, want %s with epoch 2", err, shim.CodeStaleEpoch)
	}
	select { // a refused Attach changes nothing (§4.2 step 2)
	case <-c.Done():
		t.Fatalf("the controller ended: %v", c.Err())
	default:
	}

	err = c.Start(ctx(t), startReq())
	if !errors.As(err, &e) || e.Code != shim.CodeBadStage || e.Details["phase"] != "new" || e.Op != "start" {
		t.Fatalf("Start before Init: %#v, want %s in phase new", err, shim.CodeBadStage)
	}
}

// Another agent at an epoch as large takes over: the old connection ends with PreemptedError, and its requests
// fail with it.
func TestPreempted(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "")
	old := dial(t, p.socket, cfg(1))
	dial(t, p.socket, cfg(2))
	select {
	case <-old.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the old controller was not preempted")
	}
	var pe *PreemptedError
	if !errors.As(old.Err(), &pe) || pe.ByEpoch != 2 {
		t.Fatalf("ended with %v, want preempted by epoch 2", old.Err())
	}
	if err := old.Init(ctx(t), initReq()); !errors.As(err, &pe) {
		t.Fatalf("Init after the end: %v, want the PreemptedError", err)
	}
}

// Dial waits for the socket: the shim listens only once it has read its Launch.
func TestDialWaitsForSocket(t *testing.T) {
	wd := shortDir(t)
	got := make(chan error, 1)
	go func() {
		c, err := Dial(ctx(t), socketOf(wd), cfg(1))
		if err == nil {
			c.Close()
		}
		got <- err
	}()
	time.Sleep(100 * time.Millisecond)
	startShim(t, fakeworker.Script{}, wd)
	if err := <-got; err != nil {
		t.Fatal(err)
	}

	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := Dial(short, socketOf(shortDir(t)), cfg(1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial with no shim: %v, want the deadline", err)
	}
}

// Events taken are acknowledged: the shim frees them, and the next Attached keeps none.
func TestEventsAcked(t *testing.T) {
	p := startShim(t, fakeworker.Script{Stages: 50}, "")
	isLast := func(ev *shimv1.Event) bool { return ev.GetStage().GetName() == "stage-50" }
	c := dial(t, p.socket, cfg(1))
	last := until(t, c, "stage-50", isLast).GetSeq()

	// The Ack goes out once Events is drained, without a reply to wait for; the next Dial preempts c, so give it a
	// moment, and have each Conn that still sees the events take them in turn.
	deadline := time.Now().Add(10 * time.Second)
	for epoch := int64(2); ; epoch++ {
		time.Sleep(20 * time.Millisecond)
		c = dial(t, p.socket, cfg(epoch))
		if at := c.Attached(); at.GetOldestSeq() > last {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("still kept from %d after taking %d", at.GetOldestSeq(), last)
		}
		until(t, c, "stage-50 again", isLast)
	}
}

// Without a common major the connection may only Signal, and gets no events.
func TestNoCommonMajor(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "")
	old := cfg(1)
	old.Majors = []uint32{shim.Major + 1}
	c := dial(t, p.socket, old)
	if c.Attached().GetShimMajor() != 0 {
		t.Fatalf("shim_major %d, want 0", c.Attached().GetShimMajor())
	}
	if err := c.Init(ctx(t), initReq()); CodeOf(err) != shim.CodeNoCommonMajor {
		t.Fatalf("Init: %v, want %s", err, shim.CodeNoCommonMajor)
	}
	if err := c.Signal(ctx(t), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case ev, open := <-c.Events():
		t.Fatalf("an event without a common major: %v %v", ev, open)
	case <-time.After(200 * time.Millisecond):
	}
	ex := until(t, dial(t, p.socket, cfg(2)), "Exited", isExited).GetExited().GetExit()
	if ex.GetSignal() != int32(syscall.SIGKILL) {
		t.Fatalf("Exited %v, want killed by the Signal", ex)
	}
}
