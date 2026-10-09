package shim

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// floodLine is what the fake worker floods with.
const floodLine = "fakeworker flood\n"

// outputShim starts a shim for a fake worker running s, in workDir, with the output limits o and the given
// incarnation.
func outputShim(t *testing.T, s fakeworker.Script, workDir string, incarnation uint32, o *shimv1.Output) *shimProc {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(t.TempDir(), "journal")
	fake, err := fakeworker.Env(s, journal)
	if err != nil {
		t.Fatal(err)
	}
	l := &shimv1.Launch{
		Plane: "td", AgentInstance: "td-tw-1", Worker: &commonv1.WorkerRef{Key: "td/42", Incarnation: incarnation},
		Path: self, Env: append(append([]string{}, identity...), fake...), Dir: "/", WorkDir: workDir, Output: o,
	}
	return runShim(t, l, journal)
}

func (p *shimProc) outputPath(stream string) string {
	return OutputPath(p.launch.GetWorkDir(), p.launch.GetPlane(), p.launch.GetWorker().GetKey(), stream)
}

// Done criteria 1 and 2: a worker that floods both streams is never held up, rotation keeps exactly keep files of
// each, none over max_bytes, and the shim keeps serving agents meanwhile.
func TestWorkerOutputFlood(t *testing.T) {
	const maxBytes, keep, flood = 1 << 20, 3, 32 << 20
	p := outputShim(t, fakeworker.Script{FloodBytes: flood, FloodAt: fakeworker.AtHello}, workDir(t), 3,
		&shimv1.Output{MaxBytes: maxBytes, Keep: keep})
	p.workerPid()
	start := time.Now()
	a := dial(t, p.socket())
	wantOK(t, a.attach(1, Major)) // the shim answers while the worker floods

	done := p.waitJournal("flooded")
	if done.Detail != strconv.Itoa(flood) {
		t.Fatalf("flood ended with %q", done.Detail)
	}
	if d := time.Since(start); d > 20*time.Second {
		t.Errorf("flooding %d bytes on each stream took %v", flood, d)
	}
	wantOK(t, a.signal(syscall.SIGTERM))
	p.waitLog("worker exited", nil)
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.cmd.Wait() //nolint:errcheck

	for _, stream := range []string{"stdout", "stderr"} {
		names, contents := files(t, p.outputPath(stream))
		if len(names) != keep {
			t.Errorf("%s: files %v, want %d", stream, names, keep)
		}
		for i, c := range contents {
			if len(c) > maxBytes {
				t.Errorf("%s is %d bytes, over %d", names[i], len(c), maxBytes)
			}
		}
		last := contents[len(contents)-1]
		if !bytes.HasSuffix(last, []byte(floodLine)) && !bytes.Contains(last, []byte("lost here")) {
			t.Errorf("%s: the newest file does not end with the flood: %q", stream, last[max(0, len(last)-80):])
		}
	}
	dir := LogDir(p.launch.GetWorkDir(), "td")
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("log dir %v (%v), want 0700", fi.Mode(), err)
	}
	if fi, err := os.Stat(p.outputPath("stdout")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("stdout file %v (%v), want 0600", fi.Mode(), err)
	}
}

// Each incarnation of a worker key appends to the same files after a header naming it, and the shim writes out the
// worker's last output before it ends.
func TestWorkerOutputIncarnations(t *testing.T) {
	const flood = 200 << 10
	wd := workDir(t)
	for _, inc := range []uint32{3, 4} {
		p := outputShim(t, fakeworker.Script{FloodBytes: flood, FloodAt: fakeworker.AtHello}, wd, inc, nil)
		p.workerPid()
		p.waitJournal("flooded")
		a := dial(t, p.socket())
		wantOK(t, a.attach(1, Major))
		wantOK(t, a.signal(syscall.SIGTERM))
		p.waitLog("worker exited", nil)
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := p.cmd.Wait(); err != nil {
			t.Fatalf("shim: %v", err)
		}
		// No Release: the exit file stays, and the next shim of the key will not start until the agent collects it.
		exit := ExitPath(wd, "td", "td/42")
		if rec, err := ReadExitFile(exit); err != nil || rec.GetWorker().GetIncarnation() != inc {
			t.Fatalf("exit file %v (%v), want incarnation %d", rec, err, inc)
		}
		if err := os.Remove(exit); err != nil {
			t.Fatal(err)
		}
	}
	for _, stream := range []string{"stdout", "stderr"} {
		b, err := os.ReadFile(OutputPath(wd, "td", "td/42", stream))
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(string(b), "[mftiko-shim] ")
		if len(parts) != 3 || parts[0] != "" {
			t.Fatalf("%s: %d headers, want 2", stream, len(parts)-1)
		}
		for i, inc := range []string{"3", "4"} {
			header, body, _ := strings.Cut(parts[i+1], "\n")
			if !strings.HasSuffix(header, "worker td/42 incarnation "+inc) {
				t.Errorf("%s: header %q", stream, header)
			}
			const chunk = 1024 * len(floodLine) // the fake floods in whole chunks
			if want := (flood + chunk - 1) / chunk * chunk; len(body) != want {
				t.Errorf("%s: incarnation %s has %d bytes, want all %d", stream, inc, len(body), want)
			}
		}
	}
}

// A Launch whose output limits are out of range spawns nothing.
func TestWorkerOutputBadLaunch(t *testing.T) {
	p := outputShim(t, fakeworker.Script{}, workDir(t), 3, &shimv1.Output{MaxBytes: 1})
	p.waitLog("no launch", nil)
	p.cmd.Wait() //nolint:errcheck
	if _, ok := p.logged("worker started"); ok {
		t.Fatal("spawned with a bad output")
	}
}
