package agent

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/sts"
)

// workerContract is the C5 contract Init, Start, and Command name (stsctl.v1).
const workerContract = "stsctl.v1"

// SessionSigner signs one session user (E09-D024). jwt and seed go into InitPayload.nats
// and are not written anywhere else.
type SessionSigner func(sessionID string, perms sts.Permissions) (userJWT, seed string, err error)

// Program builds Launch, Init, and Start from an unpacked strategy artifact (E09-D060, E09-D061).
// Its methods run on the agent loop. The puller does not touch it.
type Program struct {
	Env      []string // the plane's variables; the shim adds the control fd and the agent the identity
	ExtraEnv func(agent.WorkerKey) []string
	DataDir  string
	Instance string
	Epoch    int64
	Sign     SessionSigner

	RecvWindowMS  int
	HookTimeoutS  int
	StartTimeoutS int

	mem    map[string]sessionMem
	tracks map[string]bindTrack
	// pendingInc is the incarnation StampIncarnation last wrote for a session.
	// Start uses it to keep seq moving across incarnations of one epoch (E09-D071).
	pendingInc map[string]uint32
	policy     *Policy
}

// sessionMem is the spec, bindings, and unpacked image of one session this round.
type sessionMem struct {
	spec      *sessionv1.SessionSpec
	binds     *stsctlv1.Bindings // version left unset; Start and Command fill it
	rootfs    string
	manifest  artifact.Manifest
	haveImage bool
}

// bindTrack is the bindings version this process has published for one session (E09-D061).
// seq is memory only. A new process, including after a rotation, starts again at 1.
type bindTrack struct {
	spec string
	sent bool
	seq  uint64
	hash string
	inc  uint32 // incarnation this seq was published to; 0 before StampIncarnation
}

// CarriesLive is false: Init's payload is stsctl.InitPayload, not a live config, so the
// skeleton does not treat the spawn as a push. Later bindings go out as Command (E09-D061).
func (*Program) CarriesLive() bool { return false }

// note records the spec and the bindings Start or Command will carry. A new spec encoding
// resets the published version so the replacement incarnation's Start is seq 1 (E09-D061).
func (p *Program) note(id, specHash string, spec *sessionv1.SessionSpec, binds *stsctlv1.Bindings) {
	if p.mem == nil {
		p.mem = map[string]sessionMem{}
	}
	if p.tracks == nil {
		p.tracks = map[string]bindTrack{}
	}
	mem := p.mem[id]
	t := p.tracks[id]
	if t.spec != specHash {
		mem.haveImage = false
		mem.rootfs = ""
		t = bindTrack{spec: specHash}
		p.tracks[id] = t
	}
	mem.spec = spec
	if binds == nil {
		binds = &stsctlv1.Bindings{}
	}
	mem.binds = binds
	p.mem[id] = mem
}

// setImage records a rootfs that is ready to launch.
func (p *Program) setImage(id, rootfs string, m artifact.Manifest) {
	if p.mem == nil {
		p.mem = map[string]sessionMem{}
	}
	mem := p.mem[id]
	mem.rootfs, mem.manifest, mem.haveImage = rootfs, m, true
	p.mem[id] = mem
}

// Launch is the artifact's Manifest.Launch (E09-D060). Dir is the rootfs.
func (p *Program) Launch(key agent.WorkerKey, _ Spec) (*shimv1.Launch, error) {
	id, err := sessionOf(key)
	if err != nil {
		return nil, err
	}
	mem := p.mem[id]
	if !mem.haveImage {
		return nil, errors.New("sts: rootfs is not ready")
	}
	env := p.platformEnv(id, mem.spec)
	if p.ExtraEnv != nil {
		env = append(env, p.ExtraEnv(key)...)
	}
	return mem.manifest.Launch(mem.rootfs, env)
}

func (p *Program) platformEnv(id string, spec *sessionv1.SessionSpec) []string {
	env := append([]string(nil), p.Env...)
	env = append(env,
		"MFTIKO_VOLUME_DIR="+filepath.Join(p.DataDir, "sessions", id, "volume"),
		"MFTIKO_INPUT_DIR="+filepath.Join(p.DataDir, "sessions", id, "inputs"),
	)
	if mb := spec.GetLimits().GetMemoryMb(); mb > 0 {
		env = append(env, fmt.Sprintf("GOMEMLIMIT=%dMiB", mb))
	}
	if n := spec.GetLimits().GetGomaxprocs(); n > 0 {
		env = append(env, fmt.Sprintf("GOMAXPROCS=%d", n))
	}
	return env
}

// Init fills InitPayload from the spec (E09-D018, E09-D024). incarnation is 0 here; StampIncarnation
// writes it once the fleet has allocated the number.
func (p *Program) Init(key agent.WorkerKey, spec Spec, _ Live) (*workerv1.Init, error) {
	id, err := sessionOf(key)
	if err != nil {
		return nil, err
	}
	msg := p.mem[id].spec
	if msg == nil {
		msg, err = parseSpec(spec.Raw)
		if err != nil {
			return nil, err
		}
	}
	perms, err := sts.SessionPermissions(id, msg, feedInstance(msg))
	if err != nil {
		return nil, err
	}
	if p.Sign == nil {
		return nil, errors.New("sts: no session signer")
	}
	userJWT, seed, err := p.Sign(id, perms)
	if err != nil {
		return nil, err
	}
	var strategy *sessionv1.StrategyRef
	if msg.GetStrategy() != nil {
		strategy = proto.Clone(msg.GetStrategy()).(*sessionv1.StrategyRef)
	}
	payload := &stsctlv1.InitPayload{
		SessionId: id, StsInstance: p.Instance, Name: msg.GetName(),
		Strategy: strategy,
		Params:   append([]byte(nil), msg.GetParams()...),
		Feeds:    cloneSlice(msg.GetFeeds()), Accounts: cloneSlice(msg.GetAccounts()),
		Nats: &stsctlv1.NatsCredentials{
			UserJwt: userJWT, Seed: seed, InboxPrefix: "_INBOX_" + id,
		},
		RecvWindowMs: orU32(msg.GetRecvWindowMs(), p.RecvWindowMS),
		HookTimeoutS: hookTimeoutS(msg, p.HookTimeoutS),
	}
	raw, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &workerv1.Init{Contract: workerContract, Payload: raw, DeadlineMs: p.deadline(msg)}, nil
}

// StampIncarnation writes the fleet's incarnation into an Init built before the number existed.
func (p *Program) StampIncarnation(init *workerv1.Init, n uint32) error {
	msg := &stsctlv1.InitPayload{}
	if err := proto.Unmarshal(init.GetPayload(), msg); err != nil {
		return err
	}
	msg.Incarnation = n
	raw, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	init.Payload = raw
	if id := msg.GetSessionId(); id != "" && n != 0 {
		if p.pendingInc == nil {
			p.pendingInc = map[string]uint32{}
		}
		p.pendingInc[id] = n
	}
	return nil
}

// KillSilent tells the fleet to SIGKILL this worker without C4 Stop when a beat
// times out (E09-D066). inc is remembered so that exit is HUNG / no_beat (E09-D067).
func (p *Program) KillSilent(key agent.WorkerKey, inc uint32) bool {
	if p.policy != nil {
		p.policy.noteKill(key, inc)
	}
	return true
}

// Start carries the first bindings, or the current ones when a previous Start did not land,
// with the version E09-D061 assigns. Marking it published keeps the same round from also sending
// a Command.
func (p *Program) Start(key agent.WorkerKey, spec Spec) (*workerv1.Start, error) {
	id, err := sessionOf(key)
	if err != nil {
		return nil, err
	}
	b, hash, seq := p.versioned(id)
	p.markSent(id, hash, seq)
	raw, err := proto.Marshal(&stsctlv1.StartPayload{Bindings: b})
	if err != nil {
		return nil, err
	}
	msg := p.mem[id].spec
	if msg == nil {
		msg, err = parseSpec(spec.Raw)
		if err != nil {
			return nil, err
		}
	}
	return &workerv1.Start{Contract: workerContract, Payload: raw, DeadlineMs: p.deadline(msg)}, nil
}

// command is the next bindings Command, when the content differs from what this process
// last published. The caller marks it sent only after the relay succeeds.
func (p *Program) command(id string) (payload []byte, hash string, seq uint64, ok bool) {
	b, hash, seq := p.versioned(id)
	t := p.tracks[id]
	if t.sent && t.hash == hash {
		return nil, "", 0, false
	}
	raw, err := proto.Marshal(&stsctlv1.Command{Cmd: &stsctlv1.Command_Bindings{Bindings: b}})
	if err != nil {
		return nil, "", 0, false
	}
	return raw, hash, seq, true
}

// markSent records a bindings snapshot this process has handed to the worker.
func (p *Program) markSent(id, hash string, seq uint64) {
	if p.tracks == nil {
		p.tracks = map[string]bindTrack{}
	}
	t := p.tracks[id]
	t.sent, t.hash, t.seq = true, hash, seq
	if inc := p.pendingInc[id]; inc != 0 {
		t.inc = inc
	}
	p.tracks[id] = t
}

// versioned is the bindings to publish now and the seq E09-D061 gives them.
// Nothing published yet is seq 1, including a content change that only Start will carry.
// After a publish, the same content keeps its seq and a new content takes the next one.
func (p *Program) versioned(id string) (b *stsctlv1.Bindings, hash string, seq uint64) {
	mem := p.mem[id]
	if mem.binds != nil {
		b = proto.Clone(mem.binds).(*stsctlv1.Bindings)
	} else {
		b = &stsctlv1.Bindings{}
	}
	b.Version = nil
	hash = contentHash(b)
	t := p.tracks[id]
	inc := p.pendingInc[id]
	switch {
	case !t.sent:
		seq = 1
	case inc != 0 && t.inc != inc:
		// A new incarnation in this epoch takes the next seq even when the
		// bindings did not change (E09-D071). A spec change clears the track first,
		// so that replacement's Start is seq 1 again (E09-D061).
		seq = t.seq + 1
	case t.hash == hash:
		seq = t.seq
	case t.seq > 0:
		seq = t.seq + 1
	default:
		seq = 1
	}
	b.Version = &commonv1.Version{Epoch: p.Epoch, Seq: seq}
	return b, hash, seq
}

// StopGrace is the spec's grace_ms. 0 asks the fleet to use its deployment default (E09-D018, E09-D062).
func (p *Program) StopGrace(key agent.WorkerKey) time.Duration {
	id, err := sessionOf(key)
	if err != nil {
		return 0
	}
	ms := p.mem[id].spec.GetGraceMs()
	if ms == 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// Push is unused while Live's hash is always empty. Bindings are not a live config (E09-D061).
func (p *Program) Push(agent.WorkerKey, Live) (*workerv1.Live, error) {
	return &workerv1.Live{Contract: workerContract}, nil
}

func (p *Program) deadline(spec *sessionv1.SessionSpec) uint32 {
	return startTimeoutS(spec, p.StartTimeoutS) * 1000
}

// hookTimeoutS is the resolved hook limit in seconds (E09-D018). 0 in the spec takes def.
func hookTimeoutS(spec *sessionv1.SessionSpec, def int) uint32 {
	return orU32(spec.GetLimits().GetHookTimeoutS(), def)
}

// startTimeoutS is the resolved start limit in seconds, also Init and Start's
// deadline_ms once multiplied by 1000 (E09-D018).
func startTimeoutS(spec *sessionv1.SessionSpec, def int) uint32 {
	s := spec.GetStartTimeoutS()
	if s == 0 {
		s = orU32(0, def)
		if s == 0 {
			s = DefaultStartTimeoutS
		}
	}
	if s > math.MaxUint32/1000 {
		s = math.MaxUint32 / 1000
	}
	return s
}

func orU32(v uint32, def int) uint32 {
	if v != 0 {
		return v
	}
	if def <= 0 {
		return 0
	}
	return uint32(def)
}

func feedInstance(spec *sessionv1.SessionSpec) func(string) string {
	return func(feed string) string {
		for _, f := range spec.GetFeeds() {
			if f.GetFeed() == feed {
				return f.GetMdInstance()
			}
		}
		return ""
	}
}

func sessionOf(key agent.WorkerKey) (string, error) {
	id, ok := idFrom("sts/", string(key))
	if !ok {
		return "", fmt.Errorf("sts: worker key %s", key)
	}
	return id, nil
}

func cloneSlice[T proto.Message](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	for i, m := range in {
		out[i] = proto.Clone(m).(T)
	}
	return out
}
