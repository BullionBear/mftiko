package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/latest"
)

// envJournal is where a conn worker appends a test journal. Empty in production.
const envJournal = "MFTIKO_MD_JOURNAL"

// reportWait is how long a respawn waits for a report that has not landed yet. The shim's follow
// starts asynchronously, so an exit can be observed a moment before the report that preceded it.
const reportWait = 200 * time.Millisecond

// Spec is one conn worker's identity. The binary path is not part of it: a new agent binary does
// not respawn a worker that is already running (P9). The URL is: a different host is a new
// incarnation (E04-D003). conn_epoch is not part of it; it rides in Init.
type Spec struct {
	Venue string
	Route string
	N     uint32
	URL   string
}

// Hash is a stable identity of the spec.
func (s Spec) Hash() string {
	sum := sha256.Sum256([]byte(s.Venue + "\n" + s.Route + "\n" + strconv.FormatUint(uint64(s.N), 10) + "\n" + s.URL))
	return hex.EncodeToString(sum[:8])
}

// Live is the Desired of one conn worker, without its version. Feeds are topic.UniversalTicker.
// The worker turns them into atoms. The version is stamped at push time, so two identical
// placements hash equal across an agent restart and are not pushed again.
type Live struct {
	Feeds []string
}

// Hash is the digest of the Desired with no version.
func (l Live) Hash() string {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(l.desired())
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func (l Live) desired() *mdctlv1.Desired {
	d := &mdctlv1.Desired{}
	if len(l.Feeds) == 0 {
		return d
	}
	d.Atoms = []*mdctlv1.AtomAssignment{{
		Feeds: append([]string(nil), l.Feeds...),
	}}
	return d
}

// Program builds the conn worker's launch and its C5 messages.
type Program struct {
	Worker     latest.Launcher
	Instance   string
	Env        []string
	JournalDir string
	ExtraEnv   func(agent.WorkerKey) []string
	Epoch      func() int64

	epochs *epochTable
	seq    atomic.Uint64
}

// CarriesLive is false: Init is mdctl.InitPayload, and Desired arrives later as a Live. The fleet
// would otherwise treat the spawn as a push and wait out LiveRetry before the first Desired.
func (*Program) CarriesLive() bool { return false }

// Launch is the conn worker the Worker has ready: the latest artifact, or a local binary in development
// (E12-D010). Env is the process environment the worker needs (MFTIKO_NATS_URL, MFTIKO_NATS_CREDS); the shim
// adds the control fd.
func (p *Program) Launch(key agent.WorkerKey, _ Spec) (*shimv1.Launch, error) {
	env := append([]string(nil), p.Env...)
	if p.JournalDir != "" {
		env = append(env, envJournal+"="+journalFile(p.JournalDir, key))
	}
	if p.ExtraEnv != nil {
		env = append(env, p.ExtraEnv(key)...)
	}
	return p.Worker.Launch(env)
}

func journalFile(dir string, key agent.WorkerKey) string {
	return filepath.Join(dir, strings.ReplaceAll(string(key), "/", "_")+".journal")
}

// Init carries the last conn_epoch reported for key, copied as-is. The worker dials with the next
// number and reports that before the dial (E04-D009).
func (p *Program) Init(key agent.WorkerKey, spec Spec, _ Live) (*workerv1.Init, error) {
	b, err := proto.Marshal(&mdctlv1.InitPayload{
		Venue: spec.Venue, Route: spec.Route, N: spec.N, MdInstance: p.Instance, Url: spec.URL,
		ConnEpoch: p.epochOf(key),
	})
	if err != nil {
		return nil, err
	}
	return &workerv1.Init{Contract: conn.Contract, Payload: b}, nil
}

// Start has an empty payload: the worker connects and waits for Desired.
func (p *Program) Start(agent.WorkerKey, Spec) (*workerv1.Start, error) {
	return &workerv1.Start{Contract: conn.Contract}, nil
}

// Push is one Desired. Seq starts at 1 for the term; the hash ignores it.
func (p *Program) Push(_ agent.WorkerKey, live Live) (*workerv1.Live, error) {
	d := live.desired()
	var epoch int64
	if p.Epoch != nil {
		epoch = p.Epoch()
	}
	d.Version = &commonv1.Version{Epoch: epoch, Seq: p.seq.Add(1)}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &workerv1.Live{Contract: conn.Contract, Payload: b}, nil
}

func (p *Program) epochOf(key agent.WorkerKey) uint64 {
	if p.epochs == nil {
		return 0
	}
	return p.epochs.get(key)
}

// epochTable is the conn_epoch sampled before Fleet.Spawn, which releases the old worker before
// Init runs.
type epochTable struct {
	mu sync.Mutex
	v  map[agent.WorkerKey]uint64
}

func (e *epochTable) set(key agent.WorkerKey, n uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.v == nil {
		e.v = map[agent.WorkerKey]uint64{}
	}
	e.v[key] = n
}

func (e *epochTable) get(key agent.WorkerKey) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.v[key]
}

// reporting is the fleet with conn_epoch filled in. Spawn reads the latest report before the
// inner Spawn releases the old shim.
type reporting struct {
	*fleet.Fleet[Spec, Live]
	epochs *epochTable
}

func (r *reporting) Spawn(ctx context.Context, key agent.WorkerKey, spec Spec, live Live) error {
	r.epochs.set(key, r.lookup(ctx, key))
	return r.Fleet.Spawn(ctx, key, spec, live)
}

// lookup is the conn_epoch the next Init should carry. A lost shim, or a worker that never
// reported, starts at 0. A number already observed is copied even after the process has exited.
func (r *reporting) lookup(ctx context.Context, key agent.WorkerKey) uint64 {
	obs, ok := r.Observe()[key]
	if !ok || obs.State == agent.StateLost {
		return 0
	}
	if n, ok := connEpoch(obs.Report); ok {
		return n
	}
	if !obs.State.Ended() {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, reportWait)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			obs, ok = r.Observe()[key]
			if !ok || obs.State == agent.StateLost {
				return 0
			}
			n, _ := connEpoch(obs.Report)
			return n
		case <-r.Changed():
			obs, ok = r.Observe()[key]
			if !ok || obs.State == agent.StateLost {
				return 0
			}
			if n, ok := connEpoch(obs.Report); ok {
				return n
			}
		}
	}
}

// connEpoch reads mdctl.Report.conn_epoch. A report that does not parse is not a number, so the
// caller can still wait for one. Zero is a real report when the payload parses.
func connEpoch(rep *workerv1.Report) (uint64, bool) {
	if rep == nil || rep.GetContract() != conn.Contract || len(rep.GetPayload()) == 0 {
		return 0, false
	}
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(rep.GetPayload(), p); err != nil {
		return 0, false
	}
	return p.GetConnEpoch(), true
}
