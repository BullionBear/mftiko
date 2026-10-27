package agent

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/sdk"
)

// pullSlots is how many image and input pulls and unpacks run at once (E09-D058, E09-D074).
const pullSlots = 4

// pullBackoff is the wait after a temporary registry or unpack error (E09-D058, E08-D003).
var pullBackoff = agent.Backoff{Min: time.Second, Max: 60 * time.Second}

// pullSnap is what commit knows about one digest.
type pullSnap struct {
	ready    bool
	rootfs   string
	manifest artifact.Manifest
	perm     string
}

// puller fetches and unpacks strategy images and session inputs outside reconcile (E09-D058, E09-D074).
// One image digest has one job. One session input has one job, keyed separately so retaining
// image digests does not cancel an input pull.
type puller struct {
	client   *artifact.Client
	dataDir  string
	maxBytes int64
	// limits caps one rootfs and each input, compared separately (E15-D012–E15-D014).
	// Zero fields are the defaults. Run passes Config.UnpackLimits after applyDefaults.
	limits artifact.UnpackLimits
	sem    chan struct{}
	log    zerolog.Logger
	now    func() time.Time
	base   context.Context

	mu     sync.Mutex
	jobs   map[string]*pullJob
	inputs map[string]*inputJob

	// afterHold, when set, runs after a strategy Pull's layers are held and before
	// UnpackImage. Tests GC in that window (E09-D073). Production leaves it nil.
	afterHold func(*artifact.Fetched)
}

type pullJob struct {
	ready    bool
	rootfs   string
	manifest artifact.Manifest
	perm     string
	fails    uint32
	next     time.Time
	running  bool
	cancel   context.CancelFunc
	// streak* is the continuous temporary-failure signal (E12-D068). It is not
	// the backoff counter: fails and next stay as they are across success and
	// permanent rejection (E12-D067).
	streakFails  uint32
	streakSince  time.Time
	streakReason string
}

func newPuller(ctx context.Context, client *artifact.Client, dataDir string, maxBytes int64, limits artifact.UnpackLimits, log zerolog.Logger, now func() time.Time) *puller {
	if now == nil {
		now = time.Now
	}
	return &puller{
		client: client, dataDir: dataDir, maxBytes: maxBytes, limits: limits, sem: make(chan struct{}, pullSlots),
		log: log, now: now, base: ctx, jobs: map[string]*pullJob{}, inputs: map[string]*inputJob{},
	}
}

// ensure starts a pull of req when one is not already running, ready, or waiting out a backoff.
func (p *puller) ensure(req artifact.Request) pullSnap {
	p.mu.Lock()
	defer p.mu.Unlock()
	job := p.jobs[req.Digest]
	if job == nil {
		job = &pullJob{}
		p.jobs[req.Digest] = job
	}
	if job.ready {
		if _, err := os.Stat(job.rootfs); err == nil {
			return pullSnap{ready: true, rootfs: job.rootfs, manifest: job.manifest}
		}
		job.ready = false
	}
	if job.perm != "" {
		return pullSnap{perm: job.perm}
	}
	if job.running || p.now().Before(job.next) {
		return pullSnap{}
	}
	ctx, cancel := context.WithCancel(p.base)
	job.running = true
	job.cancel = cancel
	go p.run(req, ctx)
	return pullSnap{}
}

// retain cancels pulls no planned session still wants. A job stays until its goroutine
// finishes, so GC can keep the digest for that long (E09-D059).
func (p *puller) retain(want map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for dig, job := range p.jobs {
		if _, ok := want[dig]; ok || job.cancel == nil {
			continue
		}
		job.cancel()
		job.cancel = nil
	}
}

// inflight is the digests whose pull or unpack has not finished.
func (p *puller) inflight() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for dig, job := range p.jobs {
		if job.running && digestOK(dig) {
			out = append(out, dig)
		}
	}
	slices.Sort(out)
	return out
}

func (p *puller) run(req artifact.Request, ctx context.Context) {
	dig := req.Digest
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		p.finish(dig, "", artifact.Manifest{}, ctx.Err())
		return
	}
	if err := ctx.Err(); err != nil {
		p.finish(dig, "", artifact.Manifest{}, err)
		return
	}
	req.Kind = artifact.KindStrategy
	req.Contracts = sdk.Contracts()
	req.MaxBytes = p.maxBytes
	fetched, err := p.client.Pull(ctx, req)
	if err != nil {
		p.finish(dig, "", artifact.Manifest{}, err)
		return
	}
	// Hold the layers Pull just wrote. Until UnpackImage records them, GC has no
	// rootfs reference and would delete them (E09-D073). The download itself is not held.
	release, err := p.client.HoldLayers(layerDigests(fetched.Layers))
	if err != nil {
		p.finish(dig, "", artifact.Manifest{}, err)
		return
	}
	defer release()
	if p.afterHold != nil {
		p.afterHold(fetched)
	}
	root, err := p.client.UnpackImageFrom(ctx, fetched.Digest, req.Repository, fetched.Layers, p.limits)
	if err != nil {
		p.finish(dig, "", artifact.Manifest{}, err)
		return
	}
	p.finish(dig, root, fetched.Manifest, nil)
}

func layerDigests(layers []artifact.Layer) []string {
	out := make([]string, len(layers))
	for i, layer := range layers {
		out[i] = layer.Digest
	}
	return out
}

func (p *puller) finish(dig, rootfs string, m artifact.Manifest, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	job := p.jobs[dig]
	if job == nil {
		return
	}
	job.running = false
	job.cancel = nil
	if err == nil {
		job.ready, job.rootfs, job.manifest, job.perm = true, rootfs, m, ""
		clearStreak(&job.streakFails, &job.streakSince, &job.streakReason)
		p.log.Info().Str("digest", dig).Str("rootfs", rootfs).Msg("artifact ready")
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	if artifact.Permanent(err) {
		job.perm = artifact.ReasonOf(err)
		clearStreak(&job.streakFails, &job.streakSince, &job.streakReason)
		p.log.Warn().Str("digest", dig).Str("reason", job.perm).Err(err).Msg("artifact rejected")
		return
	}
	now := p.now()
	job.fails++
	job.next = now.Add(pullBackoff.After(job.fails - 1))
	recordStreak(&job.streakFails, &job.streakSince, &job.streakReason, now, err)
	p.log.Warn().Str("digest", dig).Uint32("fails", job.fails).Time("retry_at", job.next).Err(err).Msg("artifact pull will retry")
}

// recordStreak counts one temporary failure. since stays at the first failure of this streak.
func recordStreak(fails *uint32, since *time.Time, reason *string, now time.Time, err error) {
	if since.IsZero() {
		*since = now
		*fails = 0
	}
	*fails++
	r := artifact.ReasonOf(err)
	if r == "" {
		r = artifact.ReasonUnavailable
	}
	*reason = r
}

// clearStreak drops the signal. The backoff counter is left alone (E12-D067).
func clearStreak(fails *uint32, since *time.Time, reason *string) {
	*fails = 0
	*since = time.Time{}
	*reason = ""
}

func digestOK(s string) bool {
	d, err := digest.Parse(s)
	return err == nil && d.Algorithm() == digest.SHA256 && d.String() == s
}

// keepDigests is what GC retains: every non-terminal session's strategy digest, plus
// digests still being pulled or unpacked (E09-D059, E09-D056). A value that is not a sha256
// digest is left out; GC would otherwise delete nothing.
func keepDigests(sessions map[string]*sess, inflight []string) []string {
	set := map[string]struct{}{}
	add := func(d string) {
		if digestOK(d) {
			set[d] = struct{}{}
		}
	}
	for _, s := range sessions {
		if s.terminal() || s.parsed == nil {
			continue
		}
		add(s.parsed.GetStrategy().GetCode())
	}
	for _, d := range inflight {
		add(d)
	}
	return slices.Sorted(maps.Keys(set))
}

func sessionDirs(dataDir, id string) (volume, inputs string) {
	base := filepath.Join(dataDir, "sessions", id)
	return filepath.Join(base, "volume"), filepath.Join(base, "inputs")
}

func ensureSessionDirs(dataDir, id string) error {
	volume, inputs := sessionDirs(dataDir, id)
	if err := os.MkdirAll(volume, 0o700); err != nil {
		return err
	}
	// MkdirAll leaves an existing directory's mode alone. The volume is 0700 (E09-D077).
	if err := os.Chmod(volume, 0o700); err != nil {
		return err
	}
	return os.MkdirAll(inputs, 0o700)
}
