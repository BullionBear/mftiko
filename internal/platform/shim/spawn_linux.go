package shim

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
)

// Worker is the one worker of this shim, from its exec until it is reaped.
type Worker struct {
	Pid       int
	Pgid      int
	StartTime uint64   // /proc/<pid>/stat starttime, which tells this process from a later one with the same pid
	Conn      *os.File // the shim's end of the socketpair

	done   chan struct{}
	status syscall.WaitStatus
}

// Done is closed once the worker has been reaped.
func (w *Worker) Done() <-chan struct{} { return w.done }

// Status is the worker's wait status. Valid once Done is closed.
func (w *Worker) Status() syscall.WaitStatus {
	<-w.done
	return w.status
}

// Reaped is a process other than the worker that the shim reaped: a grandchild it inherited as subreaper.
type Reaped struct {
	Pid    int
	Status syscall.WaitStatus
}

// Become is step 1 of PROTOCOL §4.6: the shim leads its own session, so the agent's session ending does not reach
// it, and it is the subreaper of everything below it, so a grandchild the worker leaves behind is reaped here
// instead of by init (S1).
//
// It also marks every fd from 3 up close-on-exec: whatever the agent let the shim inherit must not reach the worker.
func Become() error {
	if sid, err := unix.Getsid(0); err != nil || sid != os.Getpid() {
		if _, err := syscall.Setsid(); err != nil {
			if errors.Is(err, syscall.EPERM) {
				return fmt.Errorf("shim: setsid: %w: the shim leads a process group but not a session; start it with Setsid, not Setpgid", err)
			}
			return fmt.Errorf("shim: setsid: %w", err)
		}
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("shim: become subreaper: %w", err)
	}
	return cloexecFrom(3)
}

// cloexecFrom marks every fd from first up close-on-exec: with close_range where the kernel has it (5.11), else
// one by one.
func cloexecFrom(first int) error {
	if err := unix.CloseRange(uint(first), ^uint(0), unix.CLOSE_RANGE_CLOEXEC); err == nil {
		return nil
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return fmt.Errorf("shim: list fds: %w", err)
	}
	for _, e := range entries {
		if fd, err := strconv.Atoi(e.Name()); err == nil && fd >= first {
			unix.CloseOnExec(fd) // the fd ReadDir itself had open may be gone by now
		}
	}
	return nil
}

var spawned atomic.Bool

// Spawn execs the worker of l (PROTOCOL §4.6 step 4). stdout and stderr become its fd 1 and 2; nil is /dev/null.
//
// From then on Spawn's goroutine reaps every child of the process, not only the worker, so a shim spawns once and
// starts no other child: one that did would be reaped from under it. Each grandchild reaped goes to reaped, which
// may be nil; it is called on the reaping goroutine and must not block.
//
// That goroutine is locked to the thread that forks the worker and does not return before the worker is reaped.
// Pdeathsig fires when the forking thread ends, not the process, and a Go thread ends when a goroutine locked to it
// returns: forking on a thread another goroutine had locked would tie the worker to that goroutine.
func Spawn(l *shimv1.Launch, stdout, stderr *os.File, reaped func(Reaped)) (*Worker, error) {
	if err := CheckLaunch(l); err != nil {
		return nil, err
	}
	if !spawned.CompareAndSwap(false, true) {
		return nil, errors.New("shim: a shim spawns one worker")
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("shim: socketpair: %w", err)
	}
	ours, theirs := os.NewFile(uintptr(fds[0]), "ctl"), os.NewFile(uintptr(fds[1]), "worker-ctl")

	attr := &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
	if c := l.GetCred(); c != nil {
		attr.Credential = &syscall.Credential{Uid: c.GetUid(), Gid: c.GetGid(), Groups: append([]uint32{}, c.GetGroups()...)}
		// The shim's own ids need no capability, but setgroups always does: skip it when nothing changes.
		attr.Credential.NoSetGroups = c.GetUid() == uint32(os.Geteuid()) && c.GetGid() == uint32(os.Getegid()) &&
			sameGroups(c.GetGroups())
	}
	cmd := &exec.Cmd{
		Path:        l.GetPath(),
		Args:        append([]string{l.GetPath()}, l.GetArgs()...),
		Env:         workerEnv(l),
		Dir:         l.GetDir(),
		ExtraFiles:  []*os.File{theirs}, // the first extra file is fd 3
		SysProcAttr: attr,
	}
	if stdout != nil {
		cmd.Stdout = stdout // an *os.File is passed as is, with no copying goroutine
	}
	if stderr != nil {
		cmd.Stderr = stderr
	}

	w := &Worker{Conn: ours, done: make(chan struct{})}
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread() // never unlocked: see the doc comment
		err := cmd.Start()
		theirs.Close()
		if err != nil {
			started <- err
			return
		}
		w.Pid = cmd.Process.Pid
		w.Pgid = cmd.Process.Pid // Setpgid with Pgid 0: a group of its own
		w.StartTime, _ = procStartTime(w.Pid)
		cmd.Process.Release() //nolint:errcheck // the reaper below waits for it, not os.Process
		started <- nil
		reap(w, reaped)
	}()
	if err := <-started; err != nil {
		ours.Close()
		spawned.Store(false)
		return nil, fmt.Errorf("shim: exec %s: %w", l.GetPath(), err)
	}
	return w, nil
}

// reap waits for every child of the process until none is left. A subreaper only inherits orphans from below it,
// so once the worker and every grandchild are reaped no child can appear again.
func reap(w *Worker, reaped func(Reaped)) {
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, 0, nil)
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.ECHILD):
			return // the worker was reaped before: it was a child
		case err != nil:
			panic(fmt.Sprintf("shim: wait4: %v", err)) // EINVAL or EFAULT: a bug here
		case pid == w.Pid:
			w.status = ws
			close(w.done)
		case reaped != nil:
			reaped(Reaped{Pid: pid, Status: ws})
		}
	}
}

// sameGroups reports whether groups are the process's supplementary groups, in any order.
func sameGroups(groups []uint32) bool {
	have, err := os.Getgroups()
	if err != nil || len(have) != len(groups) {
		return false
	}
	set := map[uint32]bool{}
	for _, g := range have {
		set[uint32(g)] = true
	}
	for _, g := range groups {
		if !set[g] {
			return false
		}
	}
	return true
}

// procStartTime is field 22 of /proc/<pid>/stat: when the process started, in clock ticks after boot.
func procStartTime(pid int) (uint64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	// The command name is field 2, in parentheses, and may hold spaces or parentheses itself: count from the last ")".
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0, fmt.Errorf("shim: /proc/%d/stat: no command name", pid)
	}
	fields := bytes.Fields(b[i+1:]) // field 3 onwards
	const idx = 22 - 3
	if len(fields) <= idx {
		return 0, fmt.Errorf("shim: /proc/%d/stat: %d fields", pid, len(fields)+2)
	}
	return strconv.ParseUint(string(fields[idx]), 10, 64)
}
