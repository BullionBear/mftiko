package shim

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// shimProc is a shim run as a re-executed test binary, with its log lines collected.
type shimProc struct {
	t       *testing.T
	cmd     *exec.Cmd
	launch  *shimv1.Launch
	journal string

	mu   sync.Mutex
	logs []map[string]any
}

// identity is the §5.4 environment the tests give a worker.
var identity = []string{"MFTIKO_PLANE=td", "MFTIKO_INSTANCE=td-tw-1", "MFTIKO_WORKER_KEY=td/42", "MFTIKO_INCARNATION=3"}

// startShim starts a shim and hands it a Launch for a fake worker running s, with the identity environment. path
// is the worker's executable, this test binary if empty; dir holds the journal, t.TempDir() if empty. The shim
// inherits extra from fd 3 on, as if the agent had left them open.
func startShim(t *testing.T, s fakeworker.Script, path, dir string, cred *shimv1.Credential, extra ...*os.File) *shimProc {
	t.Helper()
	l, journal := fakeLaunch(t, s, path, dir, cred)
	return runShim(t, l, journal, extra...)
}

// fakeLaunch is the Launch startShim gives, and the journal its fake worker writes.
func fakeLaunch(t *testing.T, s fakeworker.Script, path, dir string, cred *shimv1.Credential) (*shimv1.Launch, string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		path = self
	}
	if dir == "" {
		dir = t.TempDir()
	}
	journal := filepath.Join(dir, "journal")
	fake, err := fakeworker.Env(s, journal)
	if err != nil {
		t.Fatal(err)
	}
	return &shimv1.Launch{
		Plane: "td", AgentInstance: "td-tw-1", Worker: &commonv1.WorkerRef{Key: "td/42", Incarnation: 3},
		Path: path, Env: append(append([]string{}, identity...), fake...), Dir: "/", Cred: cred,
	}, journal
}

// runShim starts a shim, the way an agent does, and writes l on its stdin. journal is where the fake worker of l
// writes. A Launch without a work dir gets a new one.
func runShim(t *testing.T, l *shimv1.Launch, journal string, extra ...*os.File) *shimProc {
	t.Helper()
	return runShimVia(t, nil, l, journal, extra...)
}

// runShimVia is runShim, with the shim started through wrap: a command that execs the rest of its arguments, such
// as setpriv.
func runShimVia(t *testing.T, wrap []string, l *shimv1.Launch, journal string, extra ...*os.File) *shimProc {
	t.Helper()
	if l.WorkDir == "" {
		l.WorkDir = workDir(t)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self)
	if len(wrap) > 0 {
		cmd = exec.Command(wrap[0], append(append([]string{}, wrap[1:]...), self)...)
	}
	cmd.ExtraFiles = extra
	// The shim's own environment, which the worker must not see.
	cmd.Env = append(os.Environ(), envTestShim+"=1", "MFTIKO_SHIM_ONLY=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &shimProc{t: t, cmd: cmd, launch: l, journal: journal}
	go p.collect(stderr)
	t.Cleanup(func() {
		if pid, ok := p.logged("worker started"); ok {
			syscall.Kill(-int(pid["pid"].(float64)), syscall.SIGKILL) //nolint:errcheck // gone already, mostly
		}
		cmd.Process.Kill() //nolint:errcheck
		cmd.Wait()         //nolint:errcheck
	})
	if err := frame.NewWriter(stdin).Write(l); err != nil {
		t.Fatal(err)
	}
	stdin.Close()
	return p
}

// workDir is a new WORK_DIR, short enough for a socket path under it.
func workDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if len(SocketPath(dir, "plane", "")) <= maxSocketPath {
		return dir
	}
	dir, err := os.MkdirTemp("/tmp", "shim-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// socket is the path of the shim's C3 socket.
func (p *shimProc) socket() string {
	return SocketPath(p.launch.GetWorkDir(), p.launch.GetPlane(), p.launch.GetWorker().GetKey())
}

func (p *shimProc) collect(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var line map[string]any
		if json.Unmarshal(sc.Bytes(), &line) != nil {
			line = map[string]any{"message": sc.Text()}
		}
		p.mu.Lock()
		p.logs = append(p.logs, line)
		p.mu.Unlock()
	}
}

// logged is the first log line with this message.
func (p *shimProc) logged(msg string) (map[string]any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.logs {
		if l["message"] == msg {
			return l, true
		}
	}
	return nil, false
}

func (p *shimProc) dumpLogs() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var b strings.Builder
	for _, l := range p.logs {
		j, _ := json.Marshal(l)
		b.Write(append(j, '\n'))
	}
	return b.String()
}

// waitLog waits for a log line with msg that also matches want's fields.
func (p *shimProc) waitLog(msg string, want map[string]any) map[string]any {
	p.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for _, l := range p.logs {
			if l["message"] != msg {
				continue
			}
			match := true
			for k, v := range want {
				if l[k] != v {
					match = false
				}
			}
			if match {
				p.mu.Unlock()
				return l
			}
		}
		p.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	p.t.Fatalf("no log %q with %v; logs:\n%s", msg, want, p.dumpLogs())
	return nil
}

// waitJournal waits for the worker's journal to hold an entry with this event.
func (p *shimProc) waitJournal(event string) fakeworker.Entry {
	p.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := p.journalEntry(event); ok {
			return e
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.t.Fatalf("no %q in the journal; shim logs:\n%s", event, p.dumpLogs())
	return fakeworker.Entry{}
}

func (p *shimProc) journalEntry(event string) (fakeworker.Entry, bool) {
	entries, _ := fakeworker.ReadJournal(p.journal)
	for _, e := range entries {
		if e.Event == event {
			return e, true
		}
	}
	return fakeworker.Entry{}, false
}

// workerPid is the worker's pid as the shim logged it.
func (p *shimProc) workerPid() int {
	p.t.Helper()
	return int(p.waitLog("worker started", nil)["pid"].(float64))
}

// gone reports whether pid has ended: no such process, or a zombie no one has reaped yet (in a container, init
// may not reap).
func gone(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	i := strings.LastIndexByte(string(b), ')')
	return i >= 0 && strings.HasPrefix(string(b[i+1:]), " Z")
}

func waitGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !gone(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("process %d still running", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// procStatus is one field of /proc/<pid>/status.
func procStatus(t *testing.T, pid int, field string) string {
	t.Helper()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && k == field {
			return strings.TrimSpace(v)
		}
	}
	t.Fatalf("no %s in /proc/%d/status", field, pid)
	return ""
}

// The worker gets fd 3, the §5.4 environment and nothing of the shim's, its own process group in the shim's
// session, the shim as parent, /dev/null on fd 0 and pipes to the shim on fds 1 and 2.
func TestSpawnEnvironment(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "", "", nil)
	shimPid := p.cmd.Process.Pid
	started := p.waitLog("worker started", nil)
	pid := int(started["pid"].(float64))
	if started["start_time"].(float64) == 0 {
		t.Errorf("start_time 0 in %v", started)
	}

	env := p.waitJournal("env").Env
	want := map[string]string{EnvCtlFD: "3"}
	for _, kv := range identity {
		k, v, _ := strings.Cut(kv, "=")
		want[k] = v
	}
	if !maps.Equal(env, want) {
		t.Errorf("worker env %v, want %v", env, want)
	}
	if !p.waitJournal("fd3").Socket {
		t.Error("fd 3 is not a socket")
	}
	ids := p.waitJournal("ids")
	if ids.PID != pid || ids.PPID != shimPid || ids.PGID != pid || ids.SID != shimPid {
		t.Errorf("ids %+v: want pid %d, the shim %d as parent and session, a group of its own", ids, pid, shimPid)
	}
	if target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/0", pid)); err != nil || target != os.DevNull {
		t.Errorf("fd 0 is %q (%v), want /dev/null", target, err)
	}
	// fds 1 and 2 are pipes, each its own, and the shim holds their read ends (S4).
	shimFDs := fdTargets(t, shimPid)
	var pipes []string
	for _, fd := range []int{1, 2} {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", pid, fd))
		if err != nil || !strings.HasPrefix(target, "pipe:") {
			t.Errorf("fd %d is %q (%v), want a pipe", fd, target, err)
			continue
		}
		if !slices.Contains(shimFDs, target) {
			t.Errorf("fd %d is %s, which the shim does not hold", fd, target)
		}
		pipes = append(pipes, target)
	}
	if len(pipes) == 2 && pipes[0] == pipes[1] {
		t.Errorf("stdout and stderr share %s", pipes[0])
	}
	// The shim reads the worker's side: the Hello it sent went through.
	if e := p.waitJournal("sent"); !strings.Contains(string(e.Msg), "hello") {
		t.Errorf("first message sent: %s", e.Msg)
	}
	// The thread that forked the worker lives on: were it to end, Pdeathsig would stop the worker now.
	time.Sleep(300 * time.Millisecond)
	if e, ok := p.journalEntry("sigterm"); ok || gone(pid) {
		t.Errorf("worker stopped (%+v) while the shim lives", e)
	}
	// The shim leads its own session, apart from the test's.
	if sid, _ := unix.Getsid(shimPid); sid != shimPid {
		t.Errorf("shim session %d, want its own %d", sid, shimPid)
	}
}

// The shim is killed: the worker reads EOF on fd 3 and stops on its own (S2). It ignores SIGTERM here, so that
// Pdeathsig, which comes at the same moment, does not stop it first.
func TestShimKilledWorkerSeesEOF(t *testing.T) {
	p := startShim(t, fakeworker.Script{IgnoreSIGTERM: true}, "", "", nil)
	pid := p.workerPid()
	p.waitJournal("sent") // Hello: the worker is up
	p.cmd.Process.Kill()  //nolint:errcheck
	p.waitJournal("eof")
	if e := p.waitJournal("exit"); e.Detail != "eof" {
		t.Errorf("worker exit %+v, want on EOF", e)
	}
	waitGone(t, pid)
}

// A worker that ignores the EOF still stops with the shim: Pdeathsig sends it SIGTERM (S2), which a worker takes
// as a Stop (PROTOCOL §9.2 rule 6).
func TestShimKilledPdeathsig(t *testing.T) {
	p := startShim(t, fakeworker.Script{IgnoreEOF: true}, "", "", nil)
	pid := p.workerPid()
	p.waitJournal("sent")
	if got := procStatus(t, pid, "State"); strings.HasPrefix(got, "Z") {
		t.Fatalf("worker already gone: %s", got)
	}
	p.cmd.Process.Kill() //nolint:errcheck
	p.waitJournal("sigterm")
	if e := p.waitJournal("exit"); e.Detail != strconv.Itoa(fakeworker.CodeDone) {
		t.Errorf("worker exit %+v, want a graceful stop", e)
	}
	waitGone(t, pid)
}

// The worker runs as the Launch's uid and gid, with only the groups it lists.
func TestSpawnCredential(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to change ids")
	}
	const nobody = 65534
	dir, bin := worldDir(t)

	for _, groups := range [][]uint32{nil, {100}} {
		p := startShim(t, fakeworker.Script{}, bin, dir, &shimv1.Credential{Uid: nobody, Gid: nobody, Groups: groups})
		pid := p.workerPid()
		ids := p.waitJournal("ids")
		if ids.UID != nobody || ids.GID != nobody {
			t.Errorf("worker uid %d gid %d, want %d", ids.UID, ids.GID, nobody)
		}
		want := strings.Trim(fmt.Sprint(groups), "[]")
		if got := procStatus(t, pid, "Groups"); got != want {
			t.Errorf("worker groups %q, want %q", got, want)
		}
		p.cmd.Process.Kill() //nolint:errcheck
		p.cmd.Wait()         //nolint:errcheck
		os.Remove(p.journal)
	}
}

// Without the capability to change ids, a credential fails the spawn and the shim exits 1. The uid is the shim's
// own, so that the CAP_KILL check passes and setgid is what fails.
func TestSpawnCredentialRefused(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root may change ids")
	}
	if os.Getegid() == 0 {
		t.Skip("needs a gid other than 0 to ask for")
	}
	p := startShim(t, fakeworker.Script{}, "", "", &shimv1.Credential{Uid: uint32(os.Geteuid()), Gid: 0})
	p.waitLog("spawn failed", nil)
	var exit *exec.ExitError
	if err := p.cmd.Wait(); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("shim ended with %v, want exit 1", err)
	}
}

// A worker that cannot be exec'd fails the spawn: the shim logs it and exits 1.
func TestSpawnExecFails(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "/nonexistent/worker", "", nil)
	p.waitLog("spawn failed", nil)
	var exit *exec.ExitError
	if err := p.cmd.Wait(); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("shim ended with %v, want exit 1", err)
	}
}

// The worker dies and leaves a grandchild: the shim, as subreaper, becomes its parent and reaps it when it ends
// (S1). Before that it reaped the worker and logged how it ended.
func TestGrandchildReaped(t *testing.T) {
	p := startShim(t, fakeworker.Script{Grandchild: true, Exit: fakeworker.ExitKilled, ExitAt: fakeworker.AtHello}, "", "", nil)
	shimPid := p.cmd.Process.Pid
	gc := p.waitJournal("grandchild")
	if gc.PID == 0 {
		t.Fatalf("no grandchild: %+v", gc)
	}
	t.Cleanup(func() { syscall.Kill(gc.PID, syscall.SIGKILL) }) //nolint:errcheck
	p.waitLog("worker exited", map[string]any{"signal": float64(syscall.SIGKILL)})

	if ppid := procStatus(t, gc.PID, "PPid"); ppid != strconv.Itoa(shimPid) {
		t.Fatalf("grandchild's parent %s, want the shim %d", ppid, shimPid)
	}
	if fds := socketFDs(t, gc.PID); len(fds) != 0 {
		t.Errorf("grandchild holds sockets %v: the worker's fd 3 leaked to it", fds)
	}
	if err := syscall.Kill(gc.PID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitLog("reaped", map[string]any{"pid": float64(gc.PID), "signal": float64(syscall.SIGTERM)})
	if _, err := os.Stat("/proc/" + strconv.Itoa(gc.PID)); !os.IsNotExist(err) {
		t.Fatalf("grandchild %d still in /proc after the shim reaped it", gc.PID)
	}
}

// An fd the agent left open in the shim does not reach the worker.
func TestSpawnInheritsNoOtherFD(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	leak, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", r.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	// From fd 4 on in the shim: fd 3 the spawn overwrites anyway, with the worker's end of the socketpair.
	p := startShim(t, fakeworker.Script{}, "", "", nil, w, r)
	pid := p.workerPid()
	p.waitJournal("sent")
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if target, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, e.Name())); target == leak {
			t.Errorf("worker fd %s is the shim's inherited %s", e.Name(), leak)
		}
	}
}

// socketFDs lists the fds of pid that are sockets.
func socketFDs(t *testing.T, pid int) []string {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	var socks []string
	for _, e := range entries {
		if target, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, e.Name())); strings.HasPrefix(target, "socket:") {
			socks = append(socks, e.Name()+" "+target)
		}
	}
	return socks
}

// A worker started the way every worker is (PROTOCOL §4.6, §8.3): from an unpacked rootfs, with the Launch built
// from its manifest. Everything but fetching and unpacking the artifact: the rootfs is made by hand.
func TestSpawnFromRootfs(t *testing.T) {
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	if err := os.MkdirAll(filepath.Join(rootfs, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	self, _ := os.Executable()
	copyFile(t, self, filepath.Join(rootfs, "bin", "worker"), 0o755)

	var m artifact.Manifest
	if err := json.Unmarshal([]byte(`{"schema": "mftiko.worker.v1", "kind": "td.account", "os": "linux",
		"entrypoint": ["bin/worker", "--mode", "fake"], "env": {"ZETA": "z", "ALPHA": "a"}}`), &m); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(t.TempDir(), "journal")
	fake, err := fakeworker.Env(fakeworker.Script{}, journal) // the test's own variables, passed as the platform's
	if err != nil {
		t.Fatal(err)
	}
	platform := append(append([]string{}, identity...), fake...)
	l, err := m.Launch(rootfs, platform)
	if err != nil {
		t.Fatal(err)
	}
	l.Plane, l.AgentInstance, l.Worker = "td", "td-tw-1", &commonv1.WorkerRef{Key: "td/42", Incarnation: 3}

	p := runShim(t, l, journal)
	pid := p.workerPid()
	if e := p.waitJournal("sent"); !strings.Contains(string(e.Msg), "hello") {
		t.Fatalf("first message sent: %s", e.Msg)
	}

	proc := func(name string) string {
		s, err := os.Readlink(fmt.Sprintf("/proc/%d/%s", pid, name))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if cwd := proc("cwd"); cwd != rootfs {
		t.Errorf("worker cwd %q, want the rootfs %q", cwd, rootfs)
	}
	if exe := proc("exe"); exe != filepath.Join(rootfs, "bin", "worker") {
		t.Errorf("worker executable %q, want the entrypoint in the rootfs", exe)
	}
	nulSplit := func(name string) []string {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(strings.TrimSuffix(string(b), "\x00"), "\x00")
	}
	wantArgv := []string{filepath.Join(rootfs, "bin", "worker"), "--mode", "fake"}
	if argv := nulSplit("cmdline"); !slices.Equal(argv, wantArgv) {
		t.Errorf("worker argv %q, want %q", argv, wantArgv)
	}
	wantEnv := append(append([]string{"ALPHA=a", "ZETA=z"}, platform...), EnvCtlFD+"=3")
	if env := nulSplit("environ"); !slices.Equal(env, wantEnv) {
		t.Errorf("worker environment, in order:\n%q\nwant the manifest's, the platform's, then the control fd:\n%q", env, wantEnv)
	}
}

func copyFile(t *testing.T, from, to string, mode os.FileMode) {
	t.Helper()
	in, err := os.Open(from)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// fdTargets is what every fd of pid points to.
func fdTargets(t *testing.T, pid int) []string {
	t.Helper()
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if target, err := os.Readlink(filepath.Join(dir, e.Name())); err == nil {
			out = append(out, target)
		}
	}
	return out
}
