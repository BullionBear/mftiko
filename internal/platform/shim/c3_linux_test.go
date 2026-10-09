package shim

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// These run a real shim, a re-executed test binary, with the fake worker under it, and play the agent over its
// socket.

// attachRunning attaches until the shim has seen the worker's Hello.
func (a *agent) attachRunning(epoch int64, majors ...uint32) *shimv1.Attached {
	a.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := a.attach(epoch, majors...)
		wantOK(a.t, r)
		if r.GetAttached().GetState() == shimv1.WorkerState_WORKER_STATE_RUNNING {
			return r.GetAttached()
		}
		if time.Now().After(deadline) {
			a.t.Fatalf("worker not running: %v", r.GetAttached())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Done criterion 3, through a real shim: an agent with no major in common attaches, reads where the worker is,
// and stops it with a Signal to its group. Once the worker is reaped, a Signal is refused.
func TestC3SignalWithoutCommonMajor(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "", "", nil)
	started := p.waitLog("worker started", nil)
	pid := int(started["pid"].(float64))

	a := dial(t, p.socket())
	at := a.attachRunning(1, Major+1)
	if at.GetShimMajor() != 0 {
		t.Fatalf("shim_major %d", at.GetShimMajor())
	}
	if int(at.GetPid()) != pid || int(at.GetPgid()) != pid || at.GetStartTime() != uint64(started["start_time"].(float64)) {
		t.Errorf("attached pid %d pgid %d start %d; the shim logged %v", at.GetPid(), at.GetPgid(), at.GetStartTime(), started)
	}
	if at.GetWorker().GetKey() != "td/42" || at.GetWorker().GetIncarnation() != 3 || at.GetHello().GetKind() != "fake" {
		t.Errorf("attached %v", at)
	}
	wantErr(t, a.call(initReq()), CodeNoCommonMajor)

	wantOK(t, a.signal(syscall.SIGTERM))
	p.waitJournal("sigterm")
	p.waitLog("worker exited", map[string]any{"code": float64(fakeworker.CodeDone)})
	waitGone(t, pid)

	a.waitExited(1, Major+1) // once its exit is recorded
	wantErr(t, a.signal(syscall.SIGKILL), CodeExited)
}

// Done criteria 1 and 2, through a real shim: the wrong agent and an older epoch are refused and cannot signal;
// a newer epoch preempts the controller; only the controller's Signal reaches the worker.
func TestC3Fencing(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "", "", nil)
	pid := p.workerPid()

	ctl := dial(t, p.socket())
	wantOK(t, ctl.attach(2, Major))

	wrong := dial(t, p.socket())
	wantErr(t, wrong.call(attachReq("md", testInstance, 9, Major)), CodeWrongAgent)
	wantErr(t, wrong.call(attachReq(testPlane, "td-tw-2", 9, Major)), CodeWrongAgent)
	wantErr(t, wrong.signal(syscall.SIGKILL), CodeNotController)

	old := dial(t, p.socket())
	wantErr(t, old.attach(1, Major), CodeStaleEpoch)
	wantErr(t, old.signal(syscall.SIGKILL), CodeNotController)

	time.Sleep(100 * time.Millisecond)
	if gone(pid) {
		t.Fatal("the worker died from a refused Signal")
	}

	next := dial(t, p.socket())
	wantOK(t, next.attach(3, Major))
	ctl.preempted(3)
	wantOK(t, next.signal(syscall.SIGTERM))
	p.waitJournal("sigterm")
	waitGone(t, pid)
}

// The socket is the agent's alone, the worker does not hold it, and the shim removes it when it ends.
func TestC3Socket(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "", "", nil)
	pid := p.workerPid()
	p.waitLog("listening", map[string]any{"socket": p.socket()})
	fi, err := os.Lstat(p.socket())
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Type() != fs.ModeSocket || fi.Mode().Perm() != 0o600 {
		t.Errorf("socket mode %v", fi.Mode())
	}
	if fi, err := os.Stat(filepath.Dir(p.socket())); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("run dir mode %v (%v)", fi.Mode(), err)
	}
	wantOK(t, dial(t, p.socket()).attach(1, Major))
	if socks := socketFDs(t, pid); len(socks) != 1 {
		t.Errorf("worker sockets %v: want only its end of the socketpair", socks)
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("shim: %v", err)
	}
	if _, err := os.Lstat(p.socket()); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket left behind: %v", err)
	}
}

// A second shim for a worker key whose shim is still serving does not start, and spawns nothing; the first is
// untouched. A socket left by a killed shim is no obstacle.
func TestC3OneShimPerWorkerKey(t *testing.T) {
	first := startShim(t, fakeworker.Script{}, "", "", nil)
	first.workerPid()
	first.waitLog("listening", nil)

	second := runShim(t, proto.CloneOf(first.launch), first.journal)
	second.waitLog("no socket", nil)
	var exit *exec.ExitError
	if err := second.cmd.Wait(); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("second shim ended with %v, want exit 1", err)
	}
	if _, ok := second.logged("worker started"); ok {
		t.Fatal("the second shim spawned a worker")
	}
	wantOK(t, dial(t, first.socket()).attach(1, Major))

	first.cmd.Process.Kill() //nolint:errcheck // leaves its socket behind
	first.cmd.Wait()         //nolint:errcheck
	if _, err := os.Lstat(first.socket()); err != nil {
		t.Fatalf("the killed shim's socket: %v", err)
	}
	third := runShim(t, proto.CloneOf(first.launch), first.journal)
	third.workerPid()
	wantOK(t, dial(t, third.socket()).attach(1, Major))
}

// A shim whose worker cannot be exec'd leaves no socket.
func TestC3NoSocketAfterFailedSpawn(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "/nonexistent/worker", "", nil)
	p.waitLog("spawn failed", nil)
	p.cmd.Wait() //nolint:errcheck
	if _, err := os.Lstat(p.socket()); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket left behind: %v", err)
	}
}
