package agent

import (
	"errors"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
)

// workerContract is the C5 contract Init and Start name. The test worker is
// fakeworker; #284 replaces the payload and the contract with the artifact's.
// Production code does not import the fake worker.
const workerContract = "fake.v1"

// Program builds the launch of the worker executable named at deployment (D943).
type Program struct {
	Worker string   // executable every new spawn runs
	Env    []string // the plane's variables; the shim adds the control fd and the agent the identity
	// ExtraEnv is appended to one worker's environment. Tests use it.
	ExtraEnv func(agent.WorkerKey) []string
}

// CarriesLive is false: Init's payload is empty, not a live config, so the
// skeleton does not treat the spawn as a push.
func (*Program) CarriesLive() bool { return false }

// Launch is the worker path. Dir is the root so any absolute path is inside it;
// #284 switches this to the artifact's rootfs and Manifest.Launch.
func (p *Program) Launch(key agent.WorkerKey, _ Spec) (*shimv1.Launch, error) {
	if p.Worker == "" {
		return nil, errors.New("sts: no worker executable")
	}
	env := append([]string(nil), p.Env...)
	if p.ExtraEnv != nil {
		env = append(env, p.ExtraEnv(key)...)
	}
	return &shimv1.Launch{Path: p.Worker, Dir: "/", Env: env}, nil
}

// Init has an empty payload. #284 fills it from the spec.
func (p *Program) Init(agent.WorkerKey, Spec, Live) (*workerv1.Init, error) {
	return &workerv1.Init{Contract: workerContract}, nil
}

// Start has an empty payload. The skeleton sends it once Plan.Start is set.
func (p *Program) Start(agent.WorkerKey, Spec) (*workerv1.Start, error) {
	return &workerv1.Start{Contract: workerContract}, nil
}

// Push is unused while Live's hash is always empty.
func (p *Program) Push(agent.WorkerKey, Live) (*workerv1.Live, error) {
	return &workerv1.Live{Contract: workerContract}, nil
}
