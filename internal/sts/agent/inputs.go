package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// inputSnap is what commit knows about one session's inputs.
type inputSnap struct {
	ready bool
	perm  string
}

// inputJob is one session input's pull and unpack (E09-D074).
type inputJob struct {
	ready   bool
	perm    string
	fails   uint32
	next    time.Time
	running bool
	cancel  context.CancelFunc
	layers  []string // layer digests held while this job is pulling or unpacking (E09-D073)
	// streak* matches pullJob: the artifact_pull signal, separate from backoff (E12-D067, E12-D068).
	streakFails  uint32
	streakSince  time.Time
	streakReason string
}

// inputKey separates input jobs from image jobs. A name cannot contain NUL.
func inputKey(id, name string) string { return id + "\x00" + name }

func inputPath(dataDir, id, name string) string {
	_, inputs := sessionDirs(dataDir, id)
	return filepath.Join(inputs, name)
}

// inputNameOK is InputDecl.name (session.proto): ^[a-z0-9][a-z0-9_-]{0,63}$.
func inputNameOK(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case i > 0 && (c == '_' || c == '-'):
		default:
			return false
		}
	}
	return true
}

// validateInputs rejects a name, digest, or repository the puller must not use.
// The failure is permanent: invalid_input, and nothing is fetched.
func validateInputs(decls []*sessionv1.InputDecl) error {
	seen := make(map[string]struct{}, len(decls))
	for _, d := range decls {
		name := d.GetName()
		if !inputNameOK(name) {
			return fmt.Errorf("name %q", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate name %q", name)
		}
		seen[name] = struct{}{}
		if !digestOK(d.GetDigest()) {
			return fmt.Errorf("digest %q", d.GetDigest())
		}
		if d.GetRepository() == "" {
			return fmt.Errorf("repository")
		}
	}
	return nil
}

// ensureInputs starts a pull for each input that is not ready. A directory that is
// already there is reused; a missing directory is unpacked again (E09-D074).
func (p *puller) ensureInputs(id string, decls []*sessionv1.InputDecl) inputSnap {
	if len(decls) == 0 {
		return inputSnap{ready: true}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ready := true
	var perm string
	for _, d := range decls {
		key := inputKey(id, d.GetName())
		job := p.inputs[key]
		if job == nil {
			job = &inputJob{}
			p.inputs[key] = job
		}
		if job.ready {
			if st, err := os.Stat(inputPath(p.dataDir, id, d.GetName())); err == nil && st.IsDir() {
				continue
			}
			job.ready = false
		}
		if job.perm != "" {
			if perm == "" {
				perm = job.perm
			}
			ready = false
			continue
		}
		ready = false
		if job.running || p.now().Before(job.next) {
			continue
		}
		ctx, cancel := context.WithCancel(p.base)
		job.running = true
		job.cancel = cancel
		go p.runInput(id, d.GetName(), d.GetRepository(), d.GetDigest(), ctx)
	}
	if perm != "" {
		return inputSnap{perm: perm}
	}
	return inputSnap{ready: ready}
}

// retainInputs cancels input pulls no planned session still wants (E09-D059, E09-D074).
func (p *puller) retainInputs(want map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, job := range p.inputs {
		if _, ok := want[key]; ok || job.cancel == nil {
			continue
		}
		job.cancel()
		job.cancel = nil
	}
}

// inflightLayers is the input-layer digests a pull or unpack is still using (E09-D073).
func (p *puller) inflightLayers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	set := map[string]struct{}{}
	for _, job := range p.inputs {
		if !job.running {
			continue
		}
		for _, d := range job.layers {
			if digestOK(d) {
				set[d] = struct{}{}
			}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

func (p *puller) noteInputLayers(key string, layers []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	job := p.inputs[key]
	if job == nil {
		return
	}
	job.layers = append([]string(nil), layers...)
}

func (p *puller) runInput(id, name, repository, digest string, ctx context.Context) {
	key := inputKey(id, name)
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		p.finishInput(key, ctx.Err())
		return
	}
	var release func()
	defer func() {
		if release != nil {
			release()
		}
	}()
	dest := inputPath(p.dataDir, id, name)
	if st, err := os.Stat(dest); err == nil && st.IsDir() {
		p.finishInput(key, artifact.MakeReadOnly(dest))
		return
	} else if err != nil && !os.IsNotExist(err) {
		p.finishInput(key, err)
		return
	}
	fetched, err := p.client.PullData(ctx, artifact.DataRequest{
		Repository: repository,
		Digest:     digest,
		MaxBytes:   p.maxBytes,
		OnLayers: func(digests []string) error {
			rel, err := p.client.HoldLayers(digests)
			if err != nil {
				return err
			}
			release = rel
			p.noteInputLayers(key, digests)
			return nil
		},
	})
	if err != nil {
		p.finishInput(key, err)
		return
	}
	if err := ctx.Err(); err != nil {
		p.finishInput(key, err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		p.finishInput(key, err)
		return
	}
	if st, err := os.Stat(dest); err == nil && st.IsDir() {
		p.finishInput(key, artifact.MakeReadOnly(dest))
		return
	}
	// Read-only before the rename, so dest never shows the extraction's permissions (E09-D075, E09-D096).
	if err := artifact.UnpackReadOnly(ctx, []string{fetched.Layer.Path}, dest, artifact.UnpackLimits{}); err != nil {
		p.finishInput(key, err)
		return
	}
	p.finishInput(key, nil)
}

func (p *puller) finishInput(key string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	job := p.inputs[key]
	if job == nil {
		return
	}
	job.running = false
	job.cancel = nil
	job.layers = nil
	if err == nil {
		job.ready, job.perm = true, ""
		clearStreak(&job.streakFails, &job.streakSince, &job.streakReason)
		p.log.Info().Str("input", key).Msg("input ready")
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	if artifact.Permanent(err) {
		job.perm = artifact.ReasonOf(err)
		clearStreak(&job.streakFails, &job.streakSince, &job.streakReason)
		p.log.Warn().Str("input", key).Str("reason", job.perm).Err(err).Msg("input rejected")
		return
	}
	now := p.now()
	job.fails++
	job.next = now.Add(pullBackoff.After(job.fails - 1))
	recordStreak(&job.streakFails, &job.streakSince, &job.streakReason, now, err)
	p.log.Warn().Str("input", key).Uint32("fails", job.fails).Time("retry_at", job.next).Err(err).Msg("input pull will retry")
}
