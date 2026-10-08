package fakeworker_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

func TestMain(m *testing.M) {
	fakeworker.MaybeMain()
	os.Exit(m.Run())
}

// proc is a fake worker started as a shim would start it: fd 3 is our socketpair's other end. No shim yet: the
// test plays it.
type proc struct {
	t       *testing.T
	cmd     *exec.Cmd
	ctl     *os.File
	out     *frame.Writer
	up      chan *workerv1.WorkerUp // closed at EOF
	journal string
	stdout  *lockedBuffer
	stderr  *lockedBuffer
	exited  chan struct{}
	waitErr error
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Len()
}

func start(t *testing.T, s fakeworker.Script) *proc {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	ours, theirs := os.NewFile(uintptr(fds[0]), "shim"), os.NewFile(uintptr(fds[1]), "worker")
	p := &proc{
		t: t, ctl: ours, out: frame.NewWriter(ours), up: make(chan *workerv1.WorkerUp, 1024),
		journal: filepath.Join(t.TempDir(), "journal"), stdout: &lockedBuffer{}, stderr: &lockedBuffer{},
		exited: make(chan struct{}),
	}
	env, err := fakeworker.Env(s, p.journal)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd = exec.Command(os.Args[0])
	p.cmd.Env = append(env, "MFTIKO_CTL_FD=3", "MFTIKO_WORKER_KEY=fake/1", "MFTIKO_INCARNATION=7")
	p.cmd.ExtraFiles = []*os.File{theirs}
	p.cmd.Stdout, p.cmd.Stderr = p.stdout, p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	theirs.Close()
	go func() { p.waitErr = p.cmd.Wait(); close(p.exited) }()
	go func() {
		r := frame.NewReader(ours)
		for {
			m := &workerv1.WorkerUp{}
			if err := r.Read(m); err != nil {
				close(p.up)
				return
			}
			p.up <- m
		}
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		<-p.exited
		ours.Close()
	})
	return p
}

func (p *proc) send(m *workerv1.WorkerDown) {
	p.t.Helper()
	if err := p.out.Write(m); err != nil {
		p.t.Fatal(err)
	}
}

// next is the next message other than a Beat, or nil at EOF.
func (p *proc) next() *workerv1.WorkerUp {
	p.t.Helper()
	for {
		select {
		case m, ok := <-p.up:
			if !ok {
				return nil
			}
			if m.GetBeat() == nil {
				return m
			}
		case <-time.After(5 * time.Second):
			p.t.Fatal("no message in 5s")
		}
	}
}

// quiet fails if a message other than a Beat arrives within d.
func (p *proc) quiet(d time.Duration) {
	p.t.Helper()
	deadline := time.After(d)
	for {
		select {
		case m, ok := <-p.up:
			if ok && m.GetBeat() == nil {
				p.t.Fatalf("unexpected %v", m)
			}
		case <-deadline:
			return
		}
	}
}

// beats counts the Beats that arrive within d, skipping Reports, and fails on anything else.
func (p *proc) beats(d time.Duration) int {
	p.t.Helper()
	n := 0
	deadline := time.After(d)
	for {
		select {
		case m, ok := <-p.up:
			if !ok {
				return n
			}
			switch {
			case m.GetBeat() != nil:
				n++
			case m.GetReport() == nil:
				p.t.Fatalf("unexpected %v", m)
			}
		case <-deadline:
			return n
		}
	}
}

// wait returns the exit code, or -1 and the signal for a process a signal ended.
func (p *proc) wait() (int, syscall.Signal) {
	p.t.Helper()
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		p.t.Fatal("worker did not exit")
	}
	var ee *exec.ExitError
	if p.waitErr == nil {
		return 0, 0
	}
	if !errors.As(p.waitErr, &ee) {
		p.t.Fatal(p.waitErr)
	}
	ws := ee.Sys().(syscall.WaitStatus)
	if ws.Signaled() {
		return -1, ws.Signal()
	}
	return ws.ExitStatus(), 0
}

func (p *proc) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (p *proc) entries() []fakeworker.Entry {
	p.t.Helper()
	es, err := fakeworker.ReadJournal(p.journal)
	if err != nil {
		p.t.Fatal(err)
	}
	return es
}

func (p *proc) hello() {
	p.t.Helper()
	if h := p.next().GetHello(); h == nil || h.GetKind() != "fake" || !slices.Equal(h.GetContracts(), []string{fakeworker.Contract}) ||
		int(h.GetPid()) != p.cmd.Process.Pid {
		p.t.Fatalf("hello: %v", h)
	}
}

func (p *proc) initAndStart() {
	p.t.Helper()
	p.hello()
	p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{Contract: fakeworker.Contract}}})
	if d := p.next().GetDone(); d.GetVerb() != workerv1.Verb_VERB_INIT || !d.GetOk() {
		p.t.Fatalf("done init: %v", d)
	}
	p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{Contract: fakeworker.Contract}}})
	if d := p.next().GetDone(); d.GetVerb() != workerv1.Verb_VERB_START || !d.GetOk() {
		p.t.Fatalf("done start: %v", d)
	}
	if s := p.next().GetStage(); s.GetName() != "running" {
		p.t.Fatalf("stage: %v", s)
	}
}

// ending checks Exiting(outcome), then Stopped(onStop) and the exit code.
func (p *proc) ending(outcome commonv1.Outcome, onStop bool, code int) {
	p.t.Helper()
	if e := p.next().GetExiting(); e.GetOutcome() != outcome {
		p.t.Fatalf("exiting: %v, want %v", e, outcome)
	}
	if s := p.next().GetStopped(); s == nil || s.GetOnStopCompleted() != onStop {
		p.t.Fatalf("stopped: %v, want on_stop_completed %v", s, onStop)
	}
	if got, sig := p.wait(); got != code || sig != 0 {
		p.t.Fatalf("exit %d (signal %v), want %d", got, sig, code)
	}
}

// The well-behaved worker: the whole of PROTOCOL §5.2, and the journal of what it saw.
func TestNormalLife(t *testing.T) {
	p := start(t, fakeworker.Script{BeatMS: 20, ReportMS: 50, OnStopMS: 50})
	p.initAndStart()
	if r := p.next().GetReport(); r.GetContract() != fakeworker.Contract {
		t.Fatalf("report: %v", r)
	}
	relay := &workerv1.Relay{Contract: fakeworker.Contract, CorrId: []byte("c1"), Payload: []byte("ping")}
	p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Relay{Relay: relay}})
	for {
		m := p.next()
		if m.GetReport() != nil {
			continue
		}
		if r := m.GetRelay(); string(r.GetCorrId()) != "c1" || string(r.GetPayload()) != "ping" {
			t.Fatalf("relay echo: %v", m)
		}
		break
	}
	if n := p.beats(200 * time.Millisecond); n == 0 {
		t.Fatal("no beats while running")
	}
	p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{Reason: "test"}}})
	for {
		m := p.next()
		if m.GetReport() != nil {
			continue
		}
		if e := m.GetExiting(); e.GetOutcome() != commonv1.Outcome_OUTCOME_DONE {
			t.Fatalf("exiting: %v", m)
		}
		break
	}
	if s := p.next().GetStopped(); !s.GetOnStopCompleted() {
		t.Fatalf("stopped: %v", s)
	}
	if code, sig := p.wait(); code != 0 || sig != 0 {
		t.Fatalf("exit %d %v", code, sig)
	}

	es := p.entries()
	var downs []string
	for _, e := range es {
		switch e.Event {
		case "env":
			if e.Env["MFTIKO_CTL_FD"] != "3" || e.Env["MFTIKO_WORKER_KEY"] != "fake/1" || e.Env["MFTIKO_INCARNATION"] != "7" {
				t.Errorf("env %v", e.Env)
			}
			if _, leaked := e.Env[fakeworker.EnvScript]; leaked {
				t.Error("the fake's own variables are journalled as the worker's environment")
			}
		case "fd3":
			if !e.Socket {
				t.Error("fd 3 is not a socket")
			}
		case "ids":
			if e.PID != p.cmd.Process.Pid || e.UID != os.Getuid() || e.PPID != os.Getpid() {
				t.Errorf("ids %+v", e)
			}
		case "down":
			m, err := e.Down()
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case m.GetInit() != nil:
				downs = append(downs, "init")
			case m.GetStart() != nil:
				downs = append(downs, "start")
			case m.GetRelay() != nil:
				downs = append(downs, "relay")
			case m.GetStop() != nil:
				downs = append(downs, "stop")
			}
		}
	}
	if !slices.Equal(downs, []string{"init", "start", "relay", "stop"}) {
		t.Fatalf("journalled %v", downs)
	}
	if last := es[len(es)-1]; last.Event != "exit" || last.Detail != "0" {
		t.Fatalf("last entry %+v", last)
	}
}

// Every ending of PROTOCOL §5.3 the worker declares.
func TestDeclaredEndings(t *testing.T) {
	for _, c := range []struct {
		name    string
		script  fakeworker.Script
		start   bool // get as far as running before it ends
		outcome commonv1.Outcome
		onStop  bool
		code    int
	}{
		{"exit", fakeworker.Script{Exit: fakeworker.ExitDone}, true, commonv1.Outcome_OUTCOME_DONE, true, fakeworker.CodeDone},
		{"fail", fakeworker.Script{Exit: fakeworker.ExitFailed}, true, commonv1.Outcome_OUTCOME_FAILED, true, fakeworker.CodeFailed},
		{"crash", fakeworker.Script{Exit: fakeworker.ExitCrashed, ExitAfterMS: 50}, true, commonv1.Outcome_OUTCOME_CRASHED, true, fakeworker.CodeCrashed},
		{"hung", fakeworker.Script{Exit: fakeworker.ExitHung}, true, commonv1.Outcome_OUTCOME_HUNG, false, fakeworker.CodeHung},
		{"crash in Start", fakeworker.Script{Exit: fakeworker.ExitCrashed, ExitAt: fakeworker.AtStart}, false, commonv1.Outcome_OUTCOME_CRASHED, true, fakeworker.CodeCrashed},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := start(t, c.script)
			if c.start {
				p.initAndStart()
			} else {
				p.hello()
				p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{}}})
				p.next() // Done(INIT)
				p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{}}})
			}
			p.ending(c.outcome, c.onStop, c.code)
		})
	}
}

func TestInitAndStartFail(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		p := start(t, fakeworker.Script{FailInit: "bad params"})
		p.hello()
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{}}})
		if d := p.next().GetDone(); d.GetVerb() != workerv1.Verb_VERB_INIT || d.GetOk() || d.GetError() != "bad params" {
			t.Fatalf("done: %v", d)
		}
		p.ending(commonv1.Outcome_OUTCOME_INIT_FAILED, false, fakeworker.CodeInitFailed)
	})
	t.Run("start", func(t *testing.T) {
		p := start(t, fakeworker.Script{FailStart: "no data"})
		p.hello()
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{}}})
		p.next()
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{}}})
		if d := p.next().GetDone(); d.GetVerb() != workerv1.Verb_VERB_START || d.GetOk() {
			t.Fatalf("done: %v", d)
		}
		p.ending(commonv1.Outcome_OUTCOME_INIT_FAILED, true, fakeworker.CodeInitFailed) // OnStop runs after a failed Start
	})
}

// Endings without Exiting: killed by a signal, a panic, death before Hello.
func TestUndeclaredEndings(t *testing.T) {
	t.Run("killed", func(t *testing.T) {
		p := start(t, fakeworker.Script{Exit: fakeworker.ExitKilled})
		p.initAndStart()
		if m := p.next(); m != nil {
			t.Fatalf("sent %v before dying", m)
		}
		if code, sig := p.wait(); code != -1 || sig != syscall.SIGKILL {
			t.Fatalf("exit %d %v, want SIGKILL", code, sig)
		}
	})
	t.Run("panic", func(t *testing.T) {
		p := start(t, fakeworker.Script{Exit: fakeworker.ExitPanic, ExitAt: fakeworker.AtInit})
		p.hello()
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{}}})
		if m := p.next(); m != nil {
			t.Fatalf("sent %v", m)
		}
		if code, _ := p.wait(); code != fakeworker.CodePanic {
			t.Fatalf("exit %d", code)
		}
	})
	t.Run("before hello", func(t *testing.T) {
		p := start(t, fakeworker.Script{Exit: fakeworker.ExitKilled, ExitAt: fakeworker.AtHello})
		if m := p.next(); m != nil {
			t.Fatalf("sent %v", m)
		}
		if _, sig := p.wait(); sig != syscall.SIGKILL {
			t.Fatalf("signal %v", sig)
		}
	})
}

// A hung hook stops the main thread, not the heartbeat; what arrives meanwhile is still journalled.
func TestHang(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		p := start(t, fakeworker.Script{Hang: fakeworker.AtInit, BeatMS: 20})
		p.hello()
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{}}})
		if n := p.beats(300 * time.Millisecond); n < 5 {
			t.Fatalf("%d beats while Init hangs", n)
		}
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{}}})
		p.quiet(200 * time.Millisecond)
		stops := 0
		for _, e := range p.entries() {
			if m, _ := e.Down(); e.Event == "down" && m.GetStop() != nil {
				stops++
			}
		}
		if stops != 1 || !p.alive() {
			t.Fatalf("stop journalled %d times, alive %v", stops, p.alive())
		}
	})
	t.Run("stop", func(t *testing.T) {
		p := start(t, fakeworker.Script{Hang: fakeworker.AtStop, BeatMS: 20})
		p.initAndStart()
		p.send(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{}}})
		if e := p.next().GetExiting(); e.GetOutcome() != commonv1.Outcome_OUTCOME_DONE {
			t.Fatalf("exiting: %v", e)
		}
		if n := p.beats(300 * time.Millisecond); n < 5 {
			t.Fatalf("%d beats while OnStop hangs", n) // and no Stopped: beats fails on anything else
		}
	})
}

func TestBeatsStop(t *testing.T) {
	p := start(t, fakeworker.Script{BeatMS: 20, Beats: 3})
	p.hello()
	if n := p.beats(500 * time.Millisecond); n != 3 {
		t.Fatalf("%d beats, want 3", n)
	}
	if !p.alive() {
		t.Fatal("stopped beating by dying")
	}
}

func TestSIGTERM(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		p := start(t, fakeworker.Script{})
		p.hello()
		p.cmd.Process.Signal(syscall.SIGTERM)
		if _, sig := p.wait(); sig != syscall.SIGTERM {
			t.Fatalf("signal %v", sig)
		}
	})
	t.Run("ignored", func(t *testing.T) {
		p := start(t, fakeworker.Script{IgnoreSIGTERM: true})
		p.hello()
		p.cmd.Process.Signal(syscall.SIGTERM)
		time.Sleep(200 * time.Millisecond)
		if !p.alive() {
			t.Fatal("died of an ignored SIGTERM")
		}
	})
}

// The shim's end of fd 3 closes, as when the shim dies.
func TestEOF(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		p := start(t, fakeworker.Script{})
		p.hello()
		p.ctl.Close()
		if code, sig := p.wait(); code != 0 || sig != 0 {
			t.Fatalf("exit %d %v", code, sig)
		}
		es := p.entries()
		if !slices.ContainsFunc(es, func(e fakeworker.Entry) bool { return e.Event == "eof" }) {
			t.Fatal("eof not journalled")
		}
	})
	t.Run("ignored", func(t *testing.T) {
		p := start(t, fakeworker.Script{IgnoreEOF: true})
		p.hello()
		p.ctl.Close()
		time.Sleep(200 * time.Millisecond)
		if !p.alive() {
			t.Fatal("exited at EOF")
		}
	})
}

func TestFlood(t *testing.T) {
	const size = 1 << 20
	p := start(t, fakeworker.Script{FloodBytes: size})
	p.initAndStart()
	deadline := time.Now().Add(10 * time.Second)
	for p.stdout.Len() < size || p.stderr.Len() < size {
		if time.Now().After(deadline) {
			t.Fatalf("stdout %d, stderr %d bytes", p.stdout.Len(), p.stderr.Len())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The grandchild outlives the worker; the shim, as subreaper, will reap it (E1-10).
func TestGrandchild(t *testing.T) {
	p := start(t, fakeworker.Script{Grandchild: true})
	p.hello()
	var pid int
	for _, e := range p.entries() {
		if e.Event == "grandchild" {
			pid = e.PID
		}
	}
	if pid == 0 {
		t.Fatal("no grandchild journalled")
	}
	t.Cleanup(func() { syscall.Kill(pid, syscall.SIGKILL) })
	p.cmd.Process.Kill()
	p.wait()
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("grandchild gone with the worker: %v", err)
	}
}
