package agent

import (
	"errors"
	"strings"

	"google.golang.org/protobuf/proto"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/td/account"
)

// Unsealer opens an account's credentials with this instance's key (E06-D013).
type Unsealer func(sealed []byte, apiID int64, kind string) (sealbox.Credentials, error)

// NATSUser signs a NATS user for the account worker of apiID (E06-D011): its JWT, seed and inbox prefix.
type NATSUser func(apiID int64) (userJWT, seed, inbox string, err error)

// Program builds the account worker's launch and its Init.
type Program struct {
	Account  string   // mftiko-td-account, the binary every new spawn runs (P9)
	Instance string   // td_instance
	Env      []string // MFTIKO_NATS_URL; the shim adds the control fd and the agent the identity
	Unseal   Unsealer
	User     NATSUser
	// ExtraEnv is appended to one worker's environment. Tests use it.
	ExtraEnv func(agent.WorkerKey) []string
}

// CarriesLive is false: TD has no live config, and Init has no live_digest to answer.
func (*Program) CarriesLive() bool { return false }

// Launch is the repo-built account binary.
func (p *Program) Launch(key agent.WorkerKey, _ Spec) (*shimv1.Launch, error) {
	env := append([]string(nil), p.Env...)
	if p.ExtraEnv != nil {
		env = append(env, p.ExtraEnv(key)...)
	}
	return &shimv1.Launch{Path: p.Account, Dir: "/", Env: env}, nil
}

// Init unseals the credentials and signs a NATS user for this spawn. The plaintext is in the
// returned message only, which the fleet sends and drops; an error never quotes it.
func (p *Program) Init(_ agent.WorkerKey, spec Spec, _ Live) (*workerv1.Init, error) {
	if p.Unseal == nil || p.User == nil {
		return nil, errors.New("td: program has no sealing key or NATS signer")
	}
	c, err := p.Unseal(spec.Sealed, spec.APIID, spec.Kind)
	if err != nil {
		return nil, errors.New("td: credentials: " + sealbox.Reason(err))
	}
	jwt, seed, inbox, err := p.User(spec.APIID)
	if err != nil {
		return nil, errors.New("td: nats user: " + strings.TrimSpace(err.Error()))
	}
	b, err := proto.Marshal(&tdctlv1.InitPayload{
		ApiId: spec.APIID, Kind: spec.Kind, TdInstance: p.Instance, CancelOnDisconnectS: spec.CancelOnDisconnectS,
		Credentials: &tdctlv1.Credentials{KeyType: c.KeyType, Key: c.Key, Secret: c.Secret, Passphrase: c.Passphrase},
		Nats:        &tdctlv1.NatsCredentials{UserJwt: jwt, Seed: seed, InboxPrefix: inbox},
		RestUrl:     spec.Endpoint.REST, StreamUrl: spec.Endpoint.Stream, Simulated: spec.Endpoint.Simulated,
	})
	if err != nil {
		return nil, errors.New("td: init payload does not marshal")
	}
	return &workerv1.Init{Contract: account.Contract, Payload: b}, nil
}

// Start has an empty payload: the worker connects and recons, then answers Done(START).
func (p *Program) Start(agent.WorkerKey, Spec) (*workerv1.Start, error) {
	return &workerv1.Start{Contract: account.Contract}, nil
}

// Push is never called: Live's hash is always the one the worker reports.
func (p *Program) Push(agent.WorkerKey, Live) (*workerv1.Live, error) {
	return &workerv1.Live{Contract: account.Contract}, nil
}
