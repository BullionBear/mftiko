package fleet

import (
	"testing"
	"time"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

type hashed string

func (h hashed) Hash() string { return string(h) }

type stubProgram struct {
	grace time.Duration
}

func (stubProgram) Launch(agent.WorkerKey, hashed) (*shimv1.Launch, error) {
	return &shimv1.Launch{}, nil
}
func (stubProgram) Init(agent.WorkerKey, hashed, hashed) (*workerv1.Init, error) {
	return &workerv1.Init{}, nil
}
func (stubProgram) Start(agent.WorkerKey, hashed) (*workerv1.Start, error) {
	return &workerv1.Start{}, nil
}
func (stubProgram) Push(agent.WorkerKey, hashed) (*workerv1.Live, error) {
	return &workerv1.Live{}, nil
}
func (p stubProgram) StopGrace(agent.WorkerKey) time.Duration { return p.grace }

// noGrace implements Program and not StopGrace, as the MD and TD programs do.
type noGrace struct{}

func (noGrace) Launch(agent.WorkerKey, hashed) (*shimv1.Launch, error) { return &shimv1.Launch{}, nil }
func (noGrace) Init(agent.WorkerKey, hashed, hashed) (*workerv1.Init, error) {
	return &workerv1.Init{}, nil
}
func (noGrace) Start(agent.WorkerKey, hashed) (*workerv1.Start, error) {
	return &workerv1.Start{}, nil
}
func (noGrace) Push(agent.WorkerKey, hashed) (*workerv1.Live, error) { return &workerv1.Live{}, nil }

// StopGrace is optional. A positive value replaces the fleet default; 0 and a
// program that does not implement it keep the fleet value (D962). MD and TD
// programs do not implement it.
func TestStopGraceOptional(t *testing.T) {
	fallback := 10 * time.Second
	with := New(Config[hashed, hashed]{Program: stubProgram{grace: 2500 * time.Millisecond}, StopGrace: fallback})
	if got := stopGraceOf(with.cfg.Program, "k", with.cfg.StopGrace); got != 2500*time.Millisecond {
		t.Fatalf("spec grace %s", got)
	}
	zero := New(Config[hashed, hashed]{Program: stubProgram{}, StopGrace: fallback})
	if got := stopGraceOf(zero.cfg.Program, "k", zero.cfg.StopGrace); got != fallback {
		t.Fatalf("zero grace %s", got)
	}
	plain := New(Config[hashed, hashed]{Program: noGrace{}, StopGrace: fallback})
	if _, ok := plain.cfg.Program.(interface {
		StopGrace(agent.WorkerKey) time.Duration
	}); ok {
		t.Fatal("program implements StopGrace")
	}
	if got := stopGraceOf(plain.cfg.Program, "k", plain.cfg.StopGrace); got != fallback {
		t.Fatalf("plain program grace %s", got)
	}
}
