package shim

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// These run a real shim, with the fake worker under it, and play the agent: E1-11's done criteria end to end.

// waitPhase attaches until the shim reports phase.
func (a *agent) waitPhase(epoch int64, phase shimv1.Phase) *shimv1.Attached {
	a.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := a.attach(epoch, Major)
		wantOK(a.t, r)
		if r.GetAttached().GetPhase() == phase {
			return r.GetAttached()
		}
		if time.Now().After(deadline) {
			a.t.Fatalf("phase %v, want %v", r.GetAttached().GetPhase(), phase)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// started attaches, sends Init and then Start, each once the shim says the one before is done.
func (a *agent) started(epoch int64) {
	a.t.Helper()
	wantOK(a.t, a.attach(epoch, Major))
	wantOK(a.t, a.call(initReq()))
	a.waitPhase(epoch, shimv1.Phase_PHASE_INITIALIZED)
	wantOK(a.t, a.call(startReq()))
	a.waitPhase(epoch, shimv1.Phase_PHASE_STARTED)
}

// downs is what the fake worker received, in order.
func (p *shimProc) downs() []*workerv1.WorkerDown {
	p.t.Helper()
	entries, err := fakeworker.ReadJournal(p.journal)
	if err != nil {
		p.t.Fatal(err)
	}
	var out []*workerv1.WorkerDown
	for _, e := range entries {
		if e.Event == "down" {
			m, err := e.Down()
			if err != nil {
				p.t.Fatal(err)
			}
			out = append(out, m)
		}
	}
	return out
}

func downKinds(ms []*workerv1.WorkerDown) []string {
	var out []string
	for _, m := range ms {
		out = append(out, string(m.ProtoReflect().WhichOneof(m.ProtoReflect().Descriptor().Oneofs().ByName("msg")).Name()))
	}
	return out
}

// stopped is the Stopped the fake worker sent, which says whether its OnStop ran.
func (p *shimProc) stopped() *workerv1.Stopped {
	p.t.Helper()
	entries, err := fakeworker.ReadJournal(p.journal)
	if err != nil {
		p.t.Fatal(err)
	}
	for _, e := range entries {
		if e.Event != "sent" {
			continue
		}
		if m, err := e.Up(); err == nil && m.GetStopped() != nil {
			return m.GetStopped()
		}
	}
	return nil
}

func (p *shimProc) wantExit(code int) {
	p.t.Helper()
	err := p.cmd.Wait()
	var exit *exec.ExitError
	switch {
	case code == 0 && err != nil:
		p.t.Fatalf("shim ended with %v, want exit 0", err)
	case code != 0 && (!errors.As(err, &exit) || exit.ExitCode() != code):
		p.t.Fatalf("shim ended with %v, want exit %d", err, code)
	}
}

// The verbs in their order: what each phase allows, what reaches the worker, and Release, which ends the shim once
// the worker is gone.
func TestC3Lifecycle(t *testing.T) {
	p := startShim(t, fakeworker.Script{OnStopMS: 50}, "", "", nil)
	pid := p.workerPid()
	a := dial(t, p.socket())
	if at := a.attachRunning(1, Major); at.GetPhase() != shimv1.Phase_PHASE_NEW {
		t.Fatalf("phase %v at spawn", at.GetPhase())
	}
	r := a.call(startReq())
	wantErr(t, r, CodeBadStage)
	if r.GetError().GetDetails()["phase"] != "new" {
		t.Errorf("bad_stage details %v", r.GetError().GetDetails())
	}
	wantErr(t, a.call(releaseReq()), CodeRunning)

	wantOK(t, a.call(initReq()))
	a.waitPhase(1, shimv1.Phase_PHASE_INITIALIZED)
	wantErr(t, a.call(initReq()), CodeBadStage)
	wantOK(t, a.call(relayReq("ping")))
	wantOK(t, a.call(startReq()))
	a.waitPhase(1, shimv1.Phase_PHASE_STARTED)

	wantOK(t, a.call(stopReq(5000, 5000)))
	p.waitLog("worker exited", map[string]any{"code": float64(fakeworker.CodeDone), "signal": float64(0)})
	waitGone(t, pid)
	if got, want := downKinds(p.downs()), []string{"init", "relay", "start", "stop"}; !slices.Equal(got, want) {
		t.Fatalf("the worker got %v, want %v", got, want)
	}
	if st := p.downs()[3].GetStop(); st.GetReason() != "test" || st.GetGraceMs() != 5000 {
		t.Errorf("stop passed as %v", st)
	}
	if s := p.stopped(); !s.GetOnStopCompleted() {
		t.Errorf("stopped %v: OnStop did not run", s)
	}

	at := a.waitPhase(1, shimv1.Phase_PHASE_STOPPING)
	if at.GetState() != shimv1.WorkerState_WORKER_STATE_EXITED {
		t.Errorf("state %v", at.GetState())
	}
	wantErr(t, a.call(initReq()), CodeExited)
	wantErr(t, a.call(stopReq(1, 1)), CodeExited)
	wantErr(t, a.call(relayReq("late")), CodeExited)
	wantOK(t, a.call(releaseReq()))
	a.eof()
	p.wantExit(0)
	if _, err := os.Lstat(p.socket()); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket left behind: %v", err)
	}
}

// Done criterion 1: the agent sends Stop and is gone at once. The worker hangs in OnStop and shrugs off SIGTERM;
// the shim's timers still send SIGTERM when the grace is over and SIGKILL after, and a new agent finds it exited.
func TestC3StopOutlivesAgent(t *testing.T) {
	p := startShim(t, fakeworker.Script{Hang: fakeworker.AtStop}, "", "", nil)
	pid := p.workerPid()
	a := dial(t, p.socket())
	a.started(1)

	a.send(stopReq(200, 300))
	a.c.Close() // not even reading the response
	p.waitJournal("hang")
	term := p.waitLog("stop: signalled the worker's group", map[string]any{"signal": syscall.SIGTERM.String()})
	kill := p.waitLog("stop: signalled the worker's group", map[string]any{"signal": syscall.SIGKILL.String()})
	if term["after"].(float64) != 200 || kill["after"].(float64) != 300 {
		t.Errorf("SIGTERM %v, SIGKILL %v", term, kill)
	}
	p.waitLog("worker exited", map[string]any{"signal": float64(syscall.SIGKILL)})
	waitGone(t, pid)
	if _, ok := p.logged("stop: signal failed"); ok {
		t.Error("a signal of the Stop failed")
	}

	b := dial(t, p.socket())
	at := b.waitPhase(2, shimv1.Phase_PHASE_STOPPING)
	if at.GetState() != shimv1.WorkerState_WORKER_STATE_EXITED {
		t.Errorf("state %v", at.GetState())
	}
	wantOK(t, b.call(releaseReq()))
	p.wantExit(0)
}

// Done criterion 2: the host sends the shim SIGTERM. The shim does not go first: it stops the worker, which runs
// its OnStop to the end, and ends once the worker has been reaped.
func TestShimSIGTERMStopsWorkerFirst(t *testing.T) {
	const onStop = 500 * time.Millisecond
	p := startShim(t, fakeworker.Script{OnStopMS: int(onStop / time.Millisecond)}, "", "", nil)
	pid := p.workerPid()
	a := dial(t, p.socket())
	a.started(1)
	a.c.Close() // no agent: the shim acts alone

	sent := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.wantExit(0)
	if d := time.Since(sent); d < onStop {
		t.Errorf("the shim ended %v after SIGTERM, before the worker's OnStop of %v could finish", d, onStop)
	}
	if _, ok := p.logged("worker exited"); !ok {
		t.Fatal("the shim ended before reaping the worker")
	}
	waitGone(t, pid)

	downs := p.downs()
	if got := downKinds(downs); !slices.Equal(got, []string{"init", "start", "stop"}) {
		t.Fatalf("the worker got %v", got)
	}
	st := downs[2].GetStop()
	if st.GetReason() != "shim: "+syscall.SIGTERM.String() || st.GetGraceMs() != uint32(shutdownGrace.Milliseconds()) {
		t.Errorf("stop passed as %v", st)
	}
	if s := p.stopped(); !s.GetOnStopCompleted() {
		t.Errorf("stopped %v: OnStop did not complete", s)
	}
	if e, _ := p.journalEntry("exit"); e.Detail != "0" {
		t.Errorf("worker exit %q", e.Detail)
	}
}

// A SIGTERM that finds the worker already stopping keeps the agent's timers, and one that finds it gone ends the
// shim at once.
func TestShimSIGTERMAfterStop(t *testing.T) {
	p := startShim(t, fakeworker.Script{Hang: fakeworker.AtStop}, "", "", nil)
	a := dial(t, p.socket())
	a.started(1)
	wantOK(t, a.call(stopReq(100, 100)))
	p.waitJournal("hang")
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitLog("shim stopping: the worker is already stopping", nil)
	p.waitLog("worker exited", map[string]any{"signal": float64(syscall.SIGKILL)}) // after 200 ms, not 15 s
	p.wantExit(0)

	q := startShim(t, fakeworker.Script{Exit: fakeworker.ExitDone, ExitAt: fakeworker.AtHello}, "", "", nil)
	q.waitLog("worker exited", nil)
	if err := q.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	q.wantExit(0)
}

// worldDir is a directory any uid may use, holding a copy of this test binary: a worker run as nobody must reach
// its executable and write its journal.
func worldDir(t *testing.T) (dir, bin string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "shim-cred-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	self, _ := os.Executable()
	bin = filepath.Join(dir, "worker")
	copyFile(t, self, bin, 0o755)
	return dir, bin
}

// Done criterion 3, without root: a cred with another uid and no CAP_KILL is refused before the spawn.
func TestSpawnNoCapKill(t *testing.T) {
	if ok, err := hasEffectiveCap(unix.CAP_KILL); err != nil || ok {
		t.Skipf("needs a process without CAP_KILL (%v)", err)
	}
	uid := uint32(0)
	if os.Geteuid() == 0 || os.Getuid() == 0 {
		uid = 65534
	}
	p := startShim(t, fakeworker.Script{}, "", "", &shimv1.Credential{Uid: uid, Gid: uint32(os.Getegid())})
	wantNoCapKill(t, p)
}

func wantNoCapKill(t *testing.T, p *shimProc) {
	t.Helper()
	failed := p.waitLog("spawn failed", nil)
	if msg, _ := failed["error"].(string); !strings.Contains(msg, "CAP_KILL") {
		t.Errorf("spawn failed with %q, want the CAP_KILL check", msg)
	}
	p.wantExit(1)
	if _, ok := p.logged("worker started"); ok {
		t.Fatal("spawned a worker it could not signal")
	}
	if _, err := os.Lstat(p.journal); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the worker ran: it wrote its journal")
	}
	if _, err := os.Lstat(p.socket()); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket left behind: %v", err)
	}
}

// Done criterion 3, as root: a shim that may change ids but lacks CAP_KILL does not spawn a nobody worker; with
// CAP_KILL, and no other capability, its Signal and the SIGTERM and SIGKILL of its Stop all reach that worker.
func TestCapKill(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to change ids")
	}
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Skip("needs setpriv to limit the shim's capabilities")
	}
	const nobody = 65534
	cred := &shimv1.Credential{Uid: nobody, Gid: nobody}
	without := []string{setpriv, "--bounding-set=-all,+setuid,+setgid", "--"}
	with := []string{setpriv, "--bounding-set=-all,+setuid,+setgid,+kill", "--"}

	t.Run("without", func(t *testing.T) {
		dir, bin := worldDir(t)
		l, journal := fakeLaunch(t, fakeworker.Script{}, bin, dir, cred)
		wantNoCapKill(t, runShimVia(t, without, l, journal))
	})

	t.Run("signal", func(t *testing.T) {
		dir, bin := worldDir(t)
		l, journal := fakeLaunch(t, fakeworker.Script{}, bin, dir, cred)
		p := runShimVia(t, with, l, journal)
		pid := p.workerPid()
		if ids := p.waitJournal("ids"); ids.UID != nobody {
			t.Fatalf("worker uid %d", ids.UID)
		}
		a := dial(t, p.socket())
		a.attachRunning(1, Major)
		wantOK(t, a.signal(syscall.SIGTERM))
		p.waitJournal("sigterm")
		waitGone(t, pid)
	})

	t.Run("stop", func(t *testing.T) {
		dir, bin := worldDir(t)
		l, journal := fakeLaunch(t, fakeworker.Script{Hang: fakeworker.AtStop}, bin, dir, cred)
		p := runShimVia(t, with, l, journal)
		pid := p.workerPid()
		a := dial(t, p.socket())
		a.started(1)
		wantOK(t, a.call(stopReq(100, 100)))
		p.waitJournal("hang")
		p.waitLog("stop: signalled the worker's group", map[string]any{"signal": syscall.SIGTERM.String()})
		p.waitLog("worker exited", map[string]any{"signal": float64(syscall.SIGKILL)})
		waitGone(t, pid)
		if f, ok := p.logged("stop: signal failed"); ok {
			t.Fatalf("a signal of the Stop failed: %v", f)
		}
	})
}
