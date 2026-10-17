package conformance

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// TestExampleLifecycles runs hello and tape-keeper through Init, Start, and
// Stop on the fake shim. Empty params are {}. Neither strategy has to print a
// hook line: tape-keeper prints nothing (D862).
func TestExampleLifecycles(t *testing.T) {
	root := moduleRoot(t)
	for _, name := range []string{"hello", "tape-keeper"} {
		t.Run(name, func(t *testing.T) {
			bin := filepath.Join(t.TempDir(), name)
			cmd := exec.Command("go", "build", "-o", bin, "./examples/strategies/"+name)
			cmd.Dir = root
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("build: %v\n%s", err, out)
			}
			b, err := startBus()
			if err != nil {
				t.Fatal(err)
			}
			defer b.close()
			p, err := startProc([]string{bin}, b.workerEnv())
			if err != nil {
				t.Fatal(err)
			}
			defer p.stop()
			if _, detail := waitHello(p); detail != "" {
				t.Fatal(detail)
			}
			initMsg, err := b.initDown(Params{}, 0, "L1")
			if err != nil {
				t.Fatal(err)
			}
			if err := p.write(initMsg); err != nil {
				t.Fatal(err)
			}
			if !poll(opTimeout, func() bool {
				s := p.snap()
				return s.done(workerv1.Verb_VERB_INIT, true) || s.exited
			}) || !p.snap().done(workerv1.Verb_VERB_INIT, true) {
				t.Fatalf("Init: exit %d %s", p.snap().code, p.snap().stderr)
			}
			if !p.snap().appliedBeforeDoneInit("L1") {
				t.Fatal("Applied L1 was not before Done INIT")
			}
			startMsg, err := b.startDown(0)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.write(startMsg); err != nil {
				t.Fatal(err)
			}
			if !poll(opTimeout, func() bool {
				s := p.snap()
				return (s.done(workerv1.Verb_VERB_START, true) && s.stage("running")) || s.exited
			}) || !p.snap().done(workerv1.Verb_VERB_START, true) || !p.snap().stage("running") {
				t.Fatalf("Start: exit %d %s", p.snap().code, p.snap().stderr)
			}
			if err := p.write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{GraceMs: 5000}}}); err != nil {
				t.Fatal(err)
			}
			if !poll(opTimeout, func() bool {
				s := p.snap()
				_, ok := s.exitingBeforeStopped()
				return s.exited && ok
			}) {
				t.Fatalf("Stop: exit %d %s", p.snap().code, p.snap().stderr)
			}
			s := p.snap()
			before, orderOK := s.exitingBeforeStopped()
			outcome, _, hasEx := s.exiting()
			on, hasSt := s.stopped()
			if !s.exited || s.code != 0 || !orderOK || !before || !hasEx || outcome != commonv1.Outcome_OUTCOME_DONE || !hasSt || !on {
				t.Fatalf("exit %d outcome %s order %v on_stop %v stderr %s", s.code, outcome, before, on, s.stderr)
			}
		})
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal(err)
	}
	return root
}
