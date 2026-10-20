package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestHostLeftoverWaitsForSpecView(t *testing.T) {
	data := t.TempDir()
	const id = "a1b2c3"
	sess := filepath.Join(data, "sessions", id)
	if err := os.MkdirAll(sess, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestTerminal(t, data, id)
	p := &plane{prog: &Program{DataDir: data}, workDir: t.TempDir()}
	if p.hostLeftover(id) {
		t.Fatal("phase 2 ran before the first spec list")
	}
	p.specReady = true
	p.specs = map[string]struct{}{}
	if !p.hostLeftover(id) {
		t.Fatal("spec gone and phase 1 done")
	}
	vol, _ := sessionDirs(data, id)
	if err := os.Mkdir(vol, 0o700); err != nil {
		t.Fatal(err)
	}
	if p.hostLeftover(id) {
		t.Fatal("volume still on disk")
	}
	if err := os.Remove(vol); err != nil {
		t.Fatal(err)
	}
	p.specs = map[string]struct{}{id: {}}
	if p.hostLeftover(id) {
		t.Fatal("spec still in the reconcile view")
	}
	p.specs = map[string]struct{}{}
	if err := os.MkdirAll(filepath.Join(data, "sessions", "never"), 0o755); err != nil {
		t.Fatal(err)
	}
	if p.hostLeftover("never") {
		t.Fatal("directory that never became terminal")
	}
	if err := os.WriteFile(filepath.Join(sess, terminalName), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if p.hostLeftover(id) {
		t.Fatal("corrupt terminal.json")
	}
}

func TestReleaseHostOrder(t *testing.T) {
	data := t.TempDir()
	work := t.TempDir()
	const id = "a1b2c3"
	sess := filepath.Join(data, "sessions", id)
	if err := os.MkdirAll(sess, 0o755); err != nil {
		t.Fatal(err)
	}
	term := writeTestTerminal(t, data, id)
	h := state.WorkerHash(string(WorkerKey(id)))
	logDir := filepath.Join(work, "log", string(state.PlaneSTS))
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logDir, h+".stdout")
	shimPath := filepath.Join(logDir, h+".shim.log")
	other := filepath.Join(logDir, "other.stdout")
	for _, path := range []string{logPath, shimPath, other} {
		if err := os.WriteFile(path, []byte("x"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	p := &plane{
		prog: &Program{DataDir: data}, workDir: work,
		specReady: true, specs: map[string]struct{}{id: {}},
	}
	if err := p.releaseHost(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{logPath, shimPath, term, other} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	p.specs = map[string]struct{}{}
	err := p.releaseHost(context.Background(), id)
	if err == nil || !strings.Contains(err.Error(), "incarnations") {
		t.Fatalf("err %v", err)
	}
	for _, path := range []string{logPath, shimPath} {
		if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
			t.Fatalf("%s still there: %v", path, statErr)
		}
	}
	if _, err := os.Lstat(other); err != nil {
		t.Fatalf("other log: %v", err)
	}
	if _, err := os.Lstat(term); err != nil {
		t.Fatalf("terminal.json: %v", err)
	}
	if _, err := os.Lstat(sess); err != nil {
		t.Fatalf("session dir: %v", err)
	}
}

func writeTestTerminal(t *testing.T, data, id string) string {
	t.Helper()
	body, err := json.Marshal(terminalMeta{SessionID: id, Incarnation: 3, Clean: true})
	if err != nil {
		t.Fatal(err)
	}
	path := terminalPath(data, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
