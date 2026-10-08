package shim

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
)

func TestCheckLaunch(t *testing.T) {
	ok := func() *shimv1.Launch {
		return &shimv1.Launch{Path: "/bin/true", Dir: "/", Env: []string{"MFTIKO_PLANE=td", "EMPTY="}}
	}
	if err := CheckLaunch(ok()); err != nil {
		t.Fatalf("a good launch: %v", err)
	}
	for name, mut := range map[string]func(*shimv1.Launch){
		"no path":       func(l *shimv1.Launch) { l.Path = "" },
		"relative path": func(l *shimv1.Launch) { l.Path = "bin/true" },
		"relative dir":  func(l *shimv1.Launch) { l.Dir = "tmp" },
		"no =":          func(l *shimv1.Launch) { l.Env = append(l.Env, "MFTIKO_PLANE") },
		"empty key":     func(l *shimv1.Launch) { l.Env = append(l.Env, "=x") },
		"ctl fd":        func(l *shimv1.Launch) { l.Env = append(l.Env, EnvCtlFD+"=4") },
		"twice":         func(l *shimv1.Launch) { l.Env = append(l.Env, "MFTIKO_PLANE=md") },
	} {
		l := ok()
		mut(l)
		if err := CheckLaunch(l); !errors.Is(err, ErrBadLaunch) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// ReadLaunch takes one frame and not a byte more: whatever follows stays in the stream.
func TestReadLaunch(t *testing.T) {
	want := &shimv1.Launch{Plane: "td", AgentInstance: "td-tw-1", Path: "/bin/true",
		Worker: &commonv1.WorkerRef{Key: "td/42", Incarnation: 3}}
	var buf bytes.Buffer
	if err := frame.NewWriter(&buf).Write(want); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("rest")
	got, err := ReadLaunch(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetWorker().GetIncarnation() != 3 || got.GetAgentInstance() != "td-tw-1" {
		t.Fatalf("read %v", got)
	}
	if buf.String() != "rest" {
		t.Fatalf("left %q in the stream", buf.String())
	}

	if _, err := ReadLaunch(strings.NewReader("")); err == nil {
		t.Fatal("no frame read as a launch")
	}
	buf.Reset()
	if err := frame.NewWriter(&buf).Write(&shimv1.Launch{Path: "true"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadLaunch(&buf); !errors.Is(err, ErrBadLaunch) {
		t.Fatalf("a bad launch: %v", err)
	}
}

func TestWorkerEnv(t *testing.T) {
	l := &shimv1.Launch{Env: []string{"A=1"}}
	got := strings.Join(workerEnv(l), " ")
	if got != "A=1 MFTIKO_CTL_FD=3" {
		t.Fatalf("env %q", got)
	}
	if len(l.Env) != 1 {
		t.Fatal("workerEnv changed the launch")
	}
}
