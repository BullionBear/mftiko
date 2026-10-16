package conformance

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// obs is one stdout JSON object from the worker (PROTOCOL §9.8).
type obs struct {
	Hook        string `json:"hook,omitempty"`
	Ctx         *bool  `json:"ctx,omitempty"`
	Seq         uint64 `json:"seq,omitempty"`
	MD          *bool  `json:"md,omitempty"`
	Incarnation uint32 `json:"incarnation,omitempty"`
	View        *bool  `json:"view,omitempty"`
	Fetch       *bool  `json:"fetch,omitempty"`
	Queued      *int   `json:"queued,omitempty"`
}

func (o obs) any() bool {
	return o.Hook != "" || o.Ctx != nil || o.MD != nil || o.View != nil || o.Fetch != nil || o.Queued != nil || o.Seq != 0 || o.Incarnation != 0
}

// proc is one worker process. The parent end of the socketpair is fd 3 in the child.
type proc struct {
	cmd  *exec.Cmd
	ctl  *os.File
	wr   *frame.Writer
	done chan struct{}

	mu     sync.Mutex
	frames []*workerv1.WorkerUp
	lines  []obs
	exited bool
	code   int
	stderr string
}

func startProc(argv, env []string) (*proc, error) {
	if len(argv) == 0 || argv[0] == "" {
		return nil, fmt.Errorf("conformance: empty worker command")
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("conformance: socketpair: %w", err)
	}
	ours := os.NewFile(uintptr(fds[0]), "ctl")
	theirs := os.NewFile(uintptr(fds[1]), "worker-ctl")
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{theirs} // fd 3
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		ours.Close()
		theirs.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		ours.Close()
		theirs.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		ours.Close()
		theirs.Close()
		return nil, fmt.Errorf("conformance: start worker: %w", err)
	}
	theirs.Close() // the child holds the dup
	p := &proc{cmd: cmd, ctl: ours, wr: frame.NewWriter(ours), done: make(chan struct{})}
	outDone := make(chan struct{})
	errDone := make(chan struct{})
	go p.scanOut(stdout, outDone)
	go p.scanErr(stderr, errDone)
	go p.readFrames()
	go func() {
		<-outDone
		<-errDone
		err := cmd.Wait()
		p.mu.Lock()
		p.exited = true
		if ee, ok := err.(*exec.ExitError); ok {
			p.code = ee.ExitCode()
		}
		p.mu.Unlock()
		close(p.done)
	}()
	return p, nil
}

func (p *proc) scanOut(r io.Reader, done chan struct{}) {
	defer close(done)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var o obs
		if json.Unmarshal(line, &o) != nil || !o.any() {
			continue
		}
		p.mu.Lock()
		p.lines = append(p.lines, o)
		p.mu.Unlock()
	}
}

func (p *proc) scanErr(r io.Reader, done chan struct{}) {
	defer close(done)
	buf := make([]byte, 512)
	var all []byte
	for {
		n, err := r.Read(buf)
		if n > 0 {
			all = append(all, buf[:n]...)
			if len(all) > 2048 {
				all = append([]byte(nil), all[len(all)-2048:]...)
			}
			p.mu.Lock()
			p.stderr = string(all)
			p.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (p *proc) readFrames() {
	rd := frame.NewReader(bufio.NewReader(p.ctl))
	for {
		body, err := rd.Next()
		if err != nil {
			return
		}
		m := &workerv1.WorkerUp{}
		if proto.Unmarshal(body, m) != nil {
			continue
		}
		p.mu.Lock()
		p.frames = append(p.frames, m)
		p.mu.Unlock()
	}
}

func (p *proc) write(m proto.Message) error { return p.wr.Write(m) }

// stop kills the process and waits until stdout and stderr have been drained.
func (p *proc) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	<-p.done
	_ = p.ctl.Close()
}

type shot struct {
	frames []*workerv1.WorkerUp
	lines  []obs
	exited bool
	code   int
	stderr string
}

func (p *proc) snap() shot {
	p.mu.Lock()
	defer p.mu.Unlock()
	frames := make([]*workerv1.WorkerUp, len(p.frames))
	copy(frames, p.frames)
	lines := make([]obs, len(p.lines))
	copy(lines, p.lines)
	return shot{frames: frames, lines: lines, exited: p.exited, code: p.code, stderr: p.stderr}
}

func (s shot) hello() *workerv1.Hello {
	for _, f := range s.frames {
		if h := f.GetHello(); h != nil {
			return h
		}
	}
	return nil
}

func (s shot) beats() int {
	n := 0
	for _, f := range s.frames {
		if f.GetBeat() != nil {
			n++
		}
	}
	return n
}

func (s shot) hook(name string) bool {
	for _, l := range s.lines {
		if l.Hook == name {
			return true
		}
	}
	return false
}

func (s shot) seqs(hook string) []uint64 {
	var out []uint64
	for _, l := range s.lines {
		if l.Hook == hook {
			out = append(out, l.Seq)
		}
	}
	return out
}

func (s shot) queued() (int, bool) {
	for i := len(s.lines) - 1; i >= 0; i-- {
		if s.lines[i].Queued != nil {
			return *s.lines[i].Queued, true
		}
	}
	return 0, false
}

func (s shot) mdDropped() (uint64, bool) {
	var dropped uint64
	found := false
	for _, f := range s.frames {
		rep := f.GetReport()
		if rep == nil || len(rep.GetPayload()) == 0 {
			continue
		}
		var body stsctlv1.Report
		if proto.Unmarshal(rep.GetPayload(), &body) != nil {
			continue
		}
		for _, q := range body.GetQueues() {
			if q.GetName() == "md" {
				dropped = q.GetDropped()
				found = true
			}
		}
	}
	return dropped, found
}

func (s shot) exiting() (commonv1.Outcome, string, bool) {
	for _, f := range s.frames {
		if e := f.GetExiting(); e != nil {
			return e.GetOutcome(), e.GetReason(), true
		}
	}
	return 0, "", false
}

func (s shot) stopped() (on bool, ok bool) {
	for _, f := range s.frames {
		if st := f.GetStopped(); st != nil {
			return st.GetOnStopCompleted(), true
		}
	}
	return false, false
}

// exitingBeforeStopped reports whether Exiting was framed before Stopped.
// ok is false when either frame is missing.
func (s shot) exitingBeforeStopped() (before bool, ok bool) {
	ex, st := -1, -1
	for i, f := range s.frames {
		if ex < 0 && f.GetExiting() != nil {
			ex = i
		}
		if st < 0 && f.GetStopped() != nil {
			st = i
		}
	}
	if ex < 0 || st < 0 {
		return false, false
	}
	return ex < st, true
}

func (s shot) appliedBeforeDoneInit(digest string) bool {
	applied, done := -1, -1
	for i, f := range s.frames {
		if applied < 0 && f.GetApplied() != nil && f.GetApplied().GetDigest() == digest {
			applied = i
		}
		if d := f.GetDone(); done < 0 && d != nil && d.GetVerb() == workerv1.Verb_VERB_INIT && d.GetOk() {
			done = i
		}
	}
	return applied >= 0 && done >= 0 && applied < done
}

func (s shot) done(verb workerv1.Verb, ok bool) bool {
	for _, f := range s.frames {
		if d := f.GetDone(); d != nil && d.GetVerb() == verb && d.GetOk() == ok {
			return true
		}
	}
	return false
}

func (s shot) anyDoneOK() bool {
	for _, f := range s.frames {
		if d := f.GetDone(); d != nil && d.GetOk() {
			return true
		}
	}
	return false
}

func (s shot) stage(name string) bool {
	for _, f := range s.frames {
		if st := f.GetStage(); st != nil && st.GetName() == name {
			return true
		}
	}
	return false
}

func (s shot) relay(corr string) bool {
	for _, f := range s.frames {
		if r := f.GetRelay(); r != nil && string(r.GetCorrId()) == corr {
			return true
		}
	}
	return false
}

func (s shot) applied(digest string) bool {
	for _, f := range s.frames {
		if a := f.GetApplied(); a != nil && a.GetDigest() == digest {
			return true
		}
	}
	return false
}

// poll waits until cond is true or the deadline passes. The wait is the
// deadline of an observable condition, not a fixed pause: cond is checked
// again every millisecond, and a true cond returns at once.
func poll(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return cond()
		}
		time.Sleep(time.Millisecond)
	}
}
