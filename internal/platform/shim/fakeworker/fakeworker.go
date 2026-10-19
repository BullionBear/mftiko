// Package fakeworker is a worker for tests of the shim and the agent skeleton (E1-19): it speaks C4 on fd 3
// (PROTOCOL §5) and nothing else. It runs no strategy, connects to no NATS and knows no C5 payload: its contract is
// "fake.v1" and its payloads are opaque. A Script makes it misbehave in every way the shim must handle: each ending
// of PROTOCOL §5.3, a hook that hangs, a heartbeat that stops, a flood of output, a grandchild left behind, SIGTERM
// or the shim's EOF ignored.
//
// It is deliberately not built on the SDK (E8): the shim's tests do not wait for it, and the fake can break the
// rules an SDK keeps.
//
// It runs as a re-executed test binary: a test package calls MaybeMain from TestMain, and spawns os.Args[0] with
// Env in its environment. It writes what it saw (the messages it received, its environment, fd 3, its ids) to a
// journal of JSON lines that the test reads back with ReadJournal.
package fakeworker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

// Environment variables. EnvWorker marks a process as a fake worker; the others configure it.
const (
	EnvWorker  = "MFTIKO_FAKE_WORKER"  // "1": MaybeMain runs the worker
	EnvScript  = "MFTIKO_FAKE_SCRIPT"  // the Script, as JSON; empty for the defaults
	EnvJournal = "MFTIKO_FAKE_JOURNAL" // where to append the journal; empty for none
	envSleeper = "MFTIKO_FAKE_SLEEPER" // "1": the grandchild, which only sleeps
)

// Contract is the C5 contract the fake declares and sends.
const Contract = "fake.v1"

// Exit codes (PROTOCOL §5.3).
const (
	CodeDone       = 0
	CodeInitFailed = 10
	CodeCrashed    = 20
	CodeHung       = 21
	CodeFailed     = 30
	CodePanic      = 2 // what the Go runtime exits with on an unrecovered panic
)

// Ways the fake ends on its own (Script.Exit).
const (
	ExitDone    = "done"    // Exiting(DONE), OnStop, exit 0: the strategy called Exit
	ExitFailed  = "failed"  // Exiting(FAILED), OnStop, exit 30
	ExitCrashed = "crashed" // Exiting(CRASHED), OnStop, exit 20
	ExitHung    = "hung"    // Exiting(HUNG), no OnStop, exit 21: its own watchdog gave up on a hook
	ExitKilled  = "killed"  // no Exiting: SIGKILL to itself, as the OOM killer would
	ExitPanic   = "panic"   // no Exiting: exit 2, as an unrecovered panic would
)

// Points where Script.Exit or Script.Hang happens.
const (
	AtHello   = "hello"   // before Hello (Exit); right after it (FloodAt)
	AtInit    = "init"    // on Init, instead of Done
	AtStart   = "start"   // on Start, instead of Done
	AtRunning = "running" // ExitAfterMS after Start succeeded (Exit, FloodAt; the default)
	AtStop    = "stop"    // in OnStop (Hang only): Exiting is sent, Stopped never is
)

// Script is how the fake behaves; the zero value is a well-behaved worker. Times are in milliseconds.
type Script struct {
	Kind    string `json:"kind,omitempty"`    // Hello.kind; default "fake"
	Release string `json:"release,omitempty"` // Hello.release; default "fake"

	FailInit  string `json:"fail_init,omitempty"`  // Done(INIT, false, FailInit), Exiting(INIT_FAILED), exit 10, no OnStop
	FailStart string `json:"fail_start,omitempty"` // Done(START, false, FailStart), Exiting(INIT_FAILED), OnStop, exit 10

	Exit        string `json:"exit,omitempty"`          // end on its own: ExitDone, ExitFailed, ...
	ExitAt      string `json:"exit_at,omitempty"`       // AtHello, AtInit, AtStart or AtRunning (default)
	ExitAfterMS int    `json:"exit_after_ms,omitempty"` // with AtRunning

	Hang string `json:"hang,omitempty"` // AtInit, AtStart or AtStop: that hook never returns; Beat goes on

	BeatMS   int `json:"beat_ms,omitempty"`   // Beat interval, from Hello on; default 100
	Beats    int `json:"beats,omitempty"`     // stop beating after this many; 0 = never
	ReportMS int `json:"report_ms,omitempty"` // Report interval once running; 0 = no Reports
	Reports  int `json:"reports,omitempty"`   // stop reporting after this many; 0 = never. The payload is the count
	Stages   int `json:"stages,omitempty"`    // send this many Stage("stage-<n>") right after Hello
	AskMS    int `json:"ask_ms,omitempty"`    // send the agent a Relay (corr_id "ask-<n>") this often; 0 = never
	OnStopMS int `json:"on_stop_ms,omitempty"`

	IgnoreSIGTERM bool   `json:"ignore_sigterm,omitempty"` // by default SIGTERM is a Stop, as the SDK makes it
	IgnoreLive    bool   `json:"ignore_live,omitempty"`    // take a Live without applying it: no Applied, as when applying fails
	IgnoreEOF     bool   `json:"ignore_eof,omitempty"`     // keep running when fd 3 ends, so only Pdeathsig kills it
	Grandchild    bool   `json:"grandchild,omitempty"`     // start a process that outlives the worker
	FloodBytes    int    `json:"flood_bytes,omitempty"`    // write this much to stdout and to stderr once running
	FloodAt       string `json:"flood_at,omitempty"`       // AtRunning (default), or AtHello: right after Hello

	// VolumeFile is a file under MFTIKO_VOLUME_DIR. After Stage(running) the fake
	// reads it, journals the previous contents, then writes VolumeText or VolumeBytes
	// bytes of 0x01 (D977). The read happens before the write so the next incarnation can
	// show what the previous one left.
	VolumeFile  string `json:"volume_file,omitempty"`
	VolumeText  string `json:"volume_text,omitempty"`
	VolumeBytes int    `json:"volume_bytes,omitempty"`
}

// Env is the environment that makes a re-executed test binary this fake: append it to the shim's spawn
// environment. journal may be empty.
func Env(s Script, journal string) ([]string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return []string{EnvWorker + "=1", EnvScript + "=" + string(b), EnvJournal + "=" + journal}, nil
}

// MaybeMain runs the fake and exits if this process was started as one; otherwise it returns at once.
// Call it first thing in TestMain.
//
// EnvWorker runs the shim's scripted fake. -mftiko-conformance runs the conformance worker
// (PROTOCOL §9.8). The testing harness invokes TestMain before flag.Parse, so conformanceMode
// parses the conformance flags itself when that switch is present.
func MaybeMain() {
	if os.Getenv(envSleeper) == "1" {
		select {} // the grandchild: lives until killed
	}
	if conformanceMode() {
		os.Exit(runConform())
	}
	if os.Getenv(EnvWorker) != "1" {
		return
	}
	os.Exit(run())
}

// Entry is one journal line. Event says which fields are set.
type Entry struct {
	Event  string            `json:"event"`            // start, env, fd3, ids, down, sent, eof, sigterm, grandchild, flooded, exit
	Env    map[string]string `json:"env,omitempty"`    // env: every MFTIKO_ variable
	Socket bool              `json:"socket,omitempty"` // fd3: fd 3 is a socket
	UID    int               `json:"uid,omitempty"`    // ids
	GID    int               `json:"gid,omitempty"`
	PID    int               `json:"pid,omitempty"` // ids; grandchild: the grandchild's
	PGID   int               `json:"pgid,omitempty"`
	PPID   int               `json:"ppid,omitempty"`
	SID    int               `json:"sid,omitempty"`
	Msg    json.RawMessage   `json:"msg,omitempty"`    // down: the WorkerDown received; sent: the WorkerUp sent
	Detail string            `json:"detail,omitempty"` // exit: the code; others: free text
}

// Down decodes a down entry's message.
func (e Entry) Down() (*workerv1.WorkerDown, error) {
	m := &workerv1.WorkerDown{}
	return m, protojson.Unmarshal(e.Msg, m)
}

// Up decodes a sent entry's message.
func (e Entry) Up() (*workerv1.WorkerUp, error) {
	m := &workerv1.WorkerUp{}
	return m, protojson.Unmarshal(e.Msg, m)
}

// ReadJournal reads every entry written so far. A missing journal reads as empty.
func ReadJournal(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Entry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return out, fmt.Errorf("fakeworker: journal line %q: %w", sc.Text(), err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// worker is one run of the fake.
type worker struct {
	s       Script
	journal *os.File
	jmu     sync.Mutex
	out     *frame.Writer
	down    chan *workerv1.WorkerDown // closed at EOF
	running chan struct{}             // closed once Start succeeded
	sigterm chan os.Signal            // nil when SIGTERM is ignored
	beats   int
}

func run() int {
	w := &worker{down: make(chan *workerv1.WorkerDown, 16), running: make(chan struct{})}
	if path := os.Getenv(EnvJournal); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakeworker: journal:", err)
			return 1
		}
		w.journal = f
	}
	if raw := os.Getenv(EnvScript); raw != "" {
		if err := json.Unmarshal([]byte(raw), &w.s); err != nil {
			fmt.Fprintln(os.Stderr, "fakeworker: script:", err)
			return 1
		}
	}
	w.record(Entry{Event: "start"})
	w.recordEnv()

	if w.s.IgnoreSIGTERM {
		signal.Ignore(syscall.SIGTERM)
	} else { // SIGTERM is a Stop (PROTOCOL §9.2 rule 6)
		w.sigterm = make(chan os.Signal, 1)
		signal.Notify(w.sigterm, syscall.SIGTERM)
	}
	fd := 3
	if v := os.Getenv("MFTIKO_CTL_FD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			fd = n
		}
	}
	ctl := os.NewFile(uintptr(fd), "ctl")
	if ctl == nil {
		fmt.Fprintln(os.Stderr, "fakeworker: no fd", fd)
		return 1
	}
	syscall.CloseOnExec(fd) // the grandchild must not hold the control socket (PROTOCOL §9.2 rule 1)
	if w.s.Grandchild {
		w.startGrandchild()
	}
	w.out = frame.NewWriter(ctl)
	if w.s.Exit != "" && w.s.ExitAt == AtHello { // after w.out: end may send Exiting and Stopped
		return w.end()
	}
	go w.read(ctl)

	kind, release := w.s.Kind, w.s.Release
	if kind == "" {
		kind = "fake"
	}
	if release == "" {
		release = "fake"
	}
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Hello{Hello: &workerv1.Hello{
		Kind: kind, Release: release, Contracts: []string{Contract}, Pid: int32(os.Getpid()),
	}}})
	go w.beat()
	for i := 1; i <= w.s.Stages; i++ {
		w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: "stage-" + strconv.Itoa(i)}}})
	}
	if w.s.AskMS > 0 {
		go w.ask()
	}
	if w.s.FloodBytes > 0 && w.s.FloodAt == AtHello {
		w.flood()
	}
	return w.loop()
}

// loop is the worker's main thread: the hooks run here, so a hung hook stops it while Beat goes on.
func (w *worker) loop() int {
	started := false
	reported := 0
	var exitTimer <-chan time.Time
	var reports <-chan time.Time
	for {
		select {
		case m, ok := <-w.down:
			if !ok {
				if w.s.IgnoreEOF {
					w.down = nil // keep running until killed
					continue
				}
				w.record(Entry{Event: "exit", Detail: "eof"})
				return CodeDone
			}
			switch m := m.Msg.(type) {
			case *workerv1.WorkerDown_Init:
				if code, done := w.onInit(m.Init); done {
					return code
				}
			case *workerv1.WorkerDown_Start:
				if code, done := w.onStart(); done {
					return code
				}
				started = true
				close(w.running)
				if w.s.Exit != "" && (w.s.ExitAt == "" || w.s.ExitAt == AtRunning) {
					exitTimer = time.After(time.Duration(w.s.ExitAfterMS) * time.Millisecond)
				}
				if w.s.ReportMS > 0 {
					t := time.NewTicker(time.Duration(w.s.ReportMS) * time.Millisecond)
					defer t.Stop()
					reports = t.C
				}
				if w.s.FloodBytes > 0 && (w.s.FloodAt == "" || w.s.FloodAt == AtRunning) {
					w.flood()
				}
			case *workerv1.WorkerDown_Stop:
				return w.finish(commonv1.Outcome_OUTCOME_DONE, "stop: "+m.Stop.GetReason(), started, CodeDone)
			case *workerv1.WorkerDown_Relay:
				w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Relay{Relay: m.Relay}}) // echo
			case *workerv1.WorkerDown_Live:
				if !w.s.IgnoreLive { // the fake applies any live config at once
					w.applied(m.Live.GetDigest())
				}
			}
		case <-exitTimer:
			return w.end()
		case <-w.sigterm:
			w.record(Entry{Event: "sigterm"})
			return w.finish(commonv1.Outcome_OUTCOME_DONE, "SIGTERM", started, CodeDone)
		case <-reports:
			reported++
			w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Report{Report: &workerv1.Report{
				Contract: Contract, Payload: []byte(strconv.Itoa(reported)),
			}}})
			if w.s.Reports > 0 && reported >= w.s.Reports {
				reports = nil
			}
		}
	}
}

// applied tells the agent which live config the worker has (D111).
func (w *worker) applied(digest string) {
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Applied{Applied: &workerv1.Applied{Digest: digest}}})
}

func (w *worker) onInit(m *workerv1.Init) (code int, done bool) {
	if w.s.Hang == AtInit {
		w.hang()
	}
	if w.s.Exit != "" && w.s.ExitAt == AtInit {
		return w.end(), true
	}
	if w.s.FailInit != "" {
		w.send(doneMsg(workerv1.Verb_VERB_INIT, false, w.s.FailInit))
		return w.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, w.s.FailInit, false, CodeInitFailed), true
	}
	if d := m.GetLiveDigest(); d != "" { // the initial live config is applied: say so before Done
		w.applied(d)
	}
	w.send(doneMsg(workerv1.Verb_VERB_INIT, true, ""))
	return 0, false
}

func (w *worker) onStart() (code int, done bool) {
	if w.s.Hang == AtStart {
		w.hang()
	}
	if w.s.Exit != "" && w.s.ExitAt == AtStart {
		return w.end(), true
	}
	if w.s.FailStart != "" {
		w.send(doneMsg(workerv1.Verb_VERB_START, false, w.s.FailStart))
		return w.finish(commonv1.Outcome_OUTCOME_INIT_FAILED, w.s.FailStart, true, CodeInitFailed), true
	}
	w.send(doneMsg(workerv1.Verb_VERB_START, true, ""))
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stage{Stage: &workerv1.Stage{Name: "running"}}})
	w.touchVolume()
	return 0, false
}

// touchVolume reads the volume file and then replaces it. It runs after Stage(running)
// and before onStart returns, so a later Stop still finds a started worker and runs OnStop.
func (w *worker) touchVolume() {
	if w.s.VolumeFile == "" && w.s.VolumeText == "" && w.s.VolumeBytes == 0 {
		return
	}
	dir := os.Getenv("MFTIKO_VOLUME_DIR")
	name := w.s.VolumeFile
	if name == "" {
		name = "note"
	}
	if dir == "" || strings.Contains(name, "/") || strings.Contains(name, `\`) {
		w.record(Entry{Event: "volume", Detail: "bad path"})
		return
	}
	path := filepath.Join(dir, name)
	prev, err := os.ReadFile(path)
	detail := ""
	if err == nil {
		detail = string(prev)
	} else if !os.IsNotExist(err) {
		detail = err.Error()
	}
	w.record(Entry{Event: "volume", Detail: detail})
	var body []byte
	switch {
	case w.s.VolumeBytes > 0:
		body = bytes.Repeat([]byte{1}, w.s.VolumeBytes)
	case w.s.VolumeText != "" || w.s.VolumeFile != "":
		body = []byte(w.s.VolumeText)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		w.record(Entry{Event: "volume-write", Detail: err.Error()})
		return
	}
	w.record(Entry{Event: "volume-write", Detail: strconv.Itoa(len(body))})
}

// end is Script.Exit: the worker ends on its own.
func (w *worker) end() int {
	switch w.s.Exit {
	case ExitDone:
		return w.finish(commonv1.Outcome_OUTCOME_DONE, "exit", true, CodeDone)
	case ExitFailed:
		return w.finish(commonv1.Outcome_OUTCOME_FAILED, "fail", true, CodeFailed)
	case ExitCrashed:
		return w.finish(commonv1.Outcome_OUTCOME_CRASHED, "crash", true, CodeCrashed)
	case ExitHung:
		return w.finish(commonv1.Outcome_OUTCOME_HUNG, "hook timed out", false, CodeHung)
	case ExitKilled:
		w.record(Entry{Event: "exit", Detail: "SIGKILL"})
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	case ExitPanic:
		w.record(Entry{Event: "exit", Detail: strconv.Itoa(CodePanic)})
		return CodePanic
	default:
		fmt.Fprintln(os.Stderr, "fakeworker: unknown exit", w.s.Exit)
		return 1
	}
}

// finish sends Exiting, runs OnStop if it would, sends Stopped and returns the exit code (PROTOCOL §5.2).
func (w *worker) finish(outcome commonv1.Outcome, reason string, onStop bool, code int) int {
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Exiting{Exiting: &workerv1.Exiting{Outcome: outcome, Reason: reason}}})
	if onStop {
		if w.s.Hang == AtStop {
			w.hang()
		}
		time.Sleep(time.Duration(w.s.OnStopMS) * time.Millisecond)
	}
	w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Stopped{Stopped: &workerv1.Stopped{OnStopCompleted: onStop}}})
	w.record(Entry{Event: "exit", Detail: strconv.Itoa(code)})
	return code
}

// hang blocks the main thread for good. The reader keeps journaling what arrives, and Beat keeps beating.
func (w *worker) hang() {
	w.record(Entry{Event: "hang"})
	select {}
}

// read journals every WorkerDown and hands it to the main thread; at EOF it closes the channel.
func (w *worker) read(ctl io.Reader) {
	r := frame.NewReader(bufio.NewReader(ctl))
	for {
		m := &workerv1.WorkerDown{}
		if err := r.Read(m); err != nil {
			w.record(Entry{Event: "eof", Detail: err.Error()})
			close(w.down)
			return
		}
		b, _ := protojson.Marshal(m)
		w.record(Entry{Event: "down", Msg: b})
		w.down <- m
	}
}

// ask sends the agent a Relay every AskMS: the shim passes it on, or answers Undeliverable when no agent is there.
func (w *worker) ask() {
	t := time.NewTicker(time.Duration(w.s.AskMS) * time.Millisecond)
	defer t.Stop()
	for n := 1; ; n++ {
		<-t.C
		w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Relay{Relay: &workerv1.Relay{
			Contract: Contract, CorrId: []byte("ask-" + strconv.Itoa(n)),
		}}})
	}
}

func (w *worker) beat() {
	interval := time.Duration(w.s.BeatMS) * time.Millisecond
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for seq := uint64(1); ; seq++ {
		<-t.C
		if w.s.Beats > 0 && int(seq) > w.s.Beats {
			w.record(Entry{Event: "beats stopped"})
			return
		}
		w.send(&workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Beat{Beat: &workerv1.Beat{Seq: seq}}})
	}
}

// send writes m on fd 3 and journals it, except Beats, which would drown the journal. A write error means the shim
// is gone; the reader sees the EOF and the main thread acts on it.
func (w *worker) send(m *workerv1.WorkerUp) {
	err := w.out.Write(m)
	if _, beat := m.Msg.(*workerv1.WorkerUp_Beat); beat {
		return
	}
	b, _ := protojson.Marshal(m)
	e := Entry{Event: "sent", Msg: b}
	if err != nil {
		e.Detail = err.Error()
	}
	w.record(e)
}

func (w *worker) flood() {
	chunk := []byte(strings.Repeat("fakeworker flood\n", 1024)) // 17 KiB
	for _, f := range []*os.File{os.Stdout, os.Stderr} {
		for n := 0; n < w.s.FloodBytes; n += len(chunk) {
			if _, err := f.Write(chunk); err != nil {
				w.record(Entry{Event: "flooded", Detail: err.Error()})
				return
			}
		}
	}
	w.record(Entry{Event: "flooded", Detail: strconv.Itoa(w.s.FloodBytes)})
}

func (w *worker) startGrandchild() {
	self, err := os.Executable()
	if err != nil {
		w.record(Entry{Event: "grandchild", Detail: err.Error()})
		return
	}
	cmd := exec.Command(self)
	cmd.Env = []string{envSleeper + "=1"}
	if err := cmd.Start(); err != nil {
		w.record(Entry{Event: "grandchild", Detail: err.Error()})
		return
	}
	w.record(Entry{Event: "grandchild", PID: cmd.Process.Pid})
	go cmd.Wait() //nolint:errcheck // reaped by us while we live, by the subreaper after
}

func (w *worker) recordEnv() {
	env := map[string]string{}
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, "MFTIKO_") && !strings.HasPrefix(k, "MFTIKO_FAKE_") {
			env[k] = v
		}
	}
	w.record(Entry{Event: "env", Env: env})
	var st syscall.Stat_t
	socket := syscall.Fstat(3, &st) == nil && st.Mode&syscall.S_IFMT == syscall.S_IFSOCK
	w.record(Entry{Event: "fd3", Socket: socket})
	pgid, _ := syscall.Getpgid(0)
	sid, _, _ := syscall.RawSyscall(syscall.SYS_GETSID, 0, 0, 0)
	w.record(Entry{Event: "ids", UID: os.Getuid(), GID: os.Getgid(), PID: os.Getpid(), PGID: pgid, PPID: os.Getppid(), SID: int(sid)})
}

func (w *worker) record(e Entry) {
	if w.journal == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	w.jmu.Lock()
	defer w.jmu.Unlock()
	w.journal.Write(append(b, '\n')) //nolint:errcheck // a test reads what made it
}

func doneMsg(v workerv1.Verb, ok bool, errText string) *workerv1.WorkerUp {
	return &workerv1.WorkerUp{Msg: &workerv1.WorkerUp_Done{Done: &workerv1.Done{Verb: v, Ok: ok, Error: errText}}}
}
