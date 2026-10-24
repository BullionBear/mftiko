// Package latest keeps the newest conn or account worker artifact ready on the host, for the MD and TD agents
// (PROTOCOL §8.1, §8.3; DECISIONS E12-D008–E12-D010). One goroutine resolves the latest tag every poll interval,
// outside reconcile, and pulls and unpacks a digest it has not seen. Launch uses the last digest that passed every
// check. A worker that already runs keeps the binary it was started with (P9, E01-D003). GC keeps that running
// digest, a digest being pulled or unpacked, and the last digest that passed verification (E12-D014).
package latest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// Tag is the tag the release workflow moves on every v* tag (E09-D087).
const Tag = "latest"

// DefaultPoll is how often the tag is resolved when the deployment sets nothing (E12-D008).
const DefaultPoll = time.Minute

// ReasonPulling is the reason while the first pull has not finished and nothing failed yet.
const ReasonPulling = "artifact_pulling"

// retry is the wait after a temporary registry or unpack error (E09-D058).
var retry = agent.Backoff{Min: time.Second, Max: time.Minute}

// ErrNotReady is a Launch with no artifact on disk yet. Its message carries Pending.
var ErrNotReady = errors.New("latest: no worker artifact is ready")

// Config is one worker kind in one repository.
type Config struct {
	Registry   artifact.Registry
	DataDir    string // ${DATA}: layers/ and artifacts/
	Repository string // without the registry, for example bullionbear/mftiko-md-conn
	Kind       string // artifact.KindMDConn or artifact.KindTDAccount
	OS, Arch   string // the host when empty
	MaxBytes   int64  // artifact.DefaultMaxBytes when 0
	Poll       time.Duration
	Log        zerolog.Logger
	Now        func() time.Time
}

// ready is one unpacked digest.
type ready struct {
	digest   string
	rootfs   string
	manifest artifact.Manifest
}

// Source is the tracker. The zero value is not usable; call New.
type Source struct {
	cfg       Config
	contracts []string
	client    *artifact.Client
	catalog   *artifact.Catalog
	kick      chan struct{}

	mu       sync.Mutex
	cur      *ready
	inflight string            // digest being pulled or unpacked; empty when idle (E12-D014)
	rejected map[string]string // digest -> what is wrong with its content; a digest's content never changes
	reason   string            // why there is no newer digest ready: the last error, or "" when cur is the tag
}

// New checks the settings and opens the data directory. Nothing is dialled until Run.
func New(cfg Config) (*Source, error) {
	contracts, ok := artifact.WorkerContracts(cfg.Kind)
	if !ok || cfg.Kind == artifact.KindStrategy {
		return nil, fmt.Errorf("latest: kind %q is not a conn or account worker", cfg.Kind)
	}
	if cfg.Repository == "" {
		return nil, errors.New("latest: repository is required")
	}
	if cfg.Poll <= 0 {
		cfg.Poll = DefaultPoll
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	client, err := artifact.Open(cfg.Registry, cfg.DataDir)
	if err != nil {
		return nil, err
	}
	catalog, err := artifact.OpenCatalog(cfg.Registry)
	if err != nil {
		return nil, err
	}
	return &Source{
		cfg: cfg, contracts: contracts, client: client, catalog: catalog, kick: make(chan struct{}, 1),
		rejected: map[string]string{}, reason: ReasonPulling,
	}, nil
}

// Run resolves the tag at once, then every poll interval, until ctx is done. A temporary error is retried
// sooner, with a backoff of 1 to 60 seconds.
func (s *Source) Run(ctx context.Context) {
	var fails uint32
	for {
		wait := s.cfg.Poll
		if err := s.step(ctx); err != nil && ctx.Err() == nil {
			if artifact.Permanent(err) {
				fails = 0
			} else {
				wait = min(retry.After(fails), s.cfg.Poll)
				fails++
			}
		} else {
			fails = 0
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-s.kick:
		case <-t.C:
		}
		t.Stop()
	}
}

// Refresh asks Run to resolve the tag now instead of at the end of the interval.
func (s *Source) Refresh() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// step resolves the tag and, for a digest that is neither current nor rejected, pulls and unpacks it.
func (s *Source) step(ctx context.Context) error {
	dig, err := s.catalog.Resolve(ctx, s.cfg.Repository, Tag)
	if err != nil {
		return s.fail("", err)
	}
	s.mu.Lock()
	cur, why := s.cur, s.rejected[dig]
	s.mu.Unlock()
	if cur != nil && cur.digest == dig {
		s.settle("")
		return nil
	}
	if why != "" {
		s.settle(why)
		return nil
	}
	s.mu.Lock()
	s.inflight = dig
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.inflight == dig {
			s.inflight = ""
		}
		s.mu.Unlock()
	}()
	fetched, err := s.client.Pull(ctx, artifact.Request{
		Repository: s.cfg.Repository, Digest: dig, Kind: s.cfg.Kind, OS: s.cfg.OS, Arch: s.cfg.Arch,
		Contracts: s.contracts, MaxBytes: s.cfg.MaxBytes,
	})
	if err != nil {
		return s.fail(dig, err)
	}
	if err := fetched.Manifest.CheckLaunchFields(); err != nil {
		return s.fail(dig, &artifact.Error{Reason: artifact.ReasonBadManifest, Permanent: true, Err: err})
	}
	// Hold the layers Pull just wrote. Until UnpackImage records them, GC has no rootfs reference
	// and would delete them (E09-D073, E12-D014). The download itself is not held.
	release, err := s.client.HoldLayers(layerDigests(fetched.Layers))
	if err != nil {
		return s.fail(dig, err)
	}
	defer release()
	root, err := s.client.UnpackImageFrom(ctx, fetched.Digest, s.cfg.Repository, fetched.Layers, artifact.UnpackLimits{})
	if err != nil {
		return s.fail(dig, err)
	}
	s.mu.Lock()
	s.cur = &ready{digest: dig, rootfs: root, manifest: fetched.Manifest}
	s.reason = ""
	s.mu.Unlock()
	s.cfg.Log.Info().Str("kind", s.cfg.Kind).Str("digest", dig).Str("rootfs", root).Msg("worker artifact ready")
	return nil
}

// fail records err. A permanent error about a digest's content marks that digest rejected, so it is not pulled
// again; 401, 403 and 404 are about the registry and are retried at the next poll. The current digest, if any,
// stays in use.
func (s *Source) fail(dig string, err error) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	reason := artifact.ReasonOf(err)
	if reason == "" {
		reason = artifact.ReasonUnavailable
	}
	s.mu.Lock()
	if dig != "" && artifact.Permanent(err) && reason != artifact.ReasonUnauthorized && reason != artifact.ReasonNotFound {
		s.rejected[dig] = reason
	}
	s.reason = reason
	kept := ""
	if s.cur != nil {
		kept = s.cur.digest
	}
	s.mu.Unlock()
	s.cfg.Log.Warn().Err(err).Str("kind", s.cfg.Kind).Str("digest", dig).Str("reason", reason).
		Str("in_use", kept).Bool("permanent", artifact.Permanent(err)).Msg("worker artifact not updated")
	return err
}

func layerDigests(layers []artifact.Layer) []string {
	out := make([]string, len(layers))
	for i, layer := range layers {
		out[i] = layer.Digest
	}
	return out
}

func (s *Source) settle(reason string) {
	s.mu.Lock()
	s.reason = reason
	s.mu.Unlock()
}

// Pending is why a spawn cannot start a worker now: the reason of the last failure, or ReasonPulling before the
// first pull ends. It is empty once a digest is ready, even when a newer one failed: spawns use the ready one.
func (s *Source) Pending() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur != nil {
		return ""
	}
	return s.reason
}

// GC deletes unpacked images this source does not still need (E12-D014, E12-D015). It keeps the last
// digest that passed verification, a digest being pulled or unpacked, and digests of processes running
// from this data directory. It does not call the registry. The caller logs a failure.
func (s *Source) GC() error {
	running, err := artifact.RunningDigests(s.cfg.DataDir)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client.GC(s.keepLocked(running))
}

func (s *Source) keepLocked(extra []string) []string {
	set := map[string]struct{}{}
	add := func(d string) {
		if d != "" {
			set[d] = struct{}{}
		}
	}
	if s.cur != nil {
		add(s.cur.digest)
	}
	add(s.inflight)
	for _, d := range extra {
		add(d)
	}
	return slices.Sorted(maps.Keys(set))
}

// Digest is the image digest new workers start from, or "" before the first pull.
func (s *Source) Digest() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil {
		return ""
	}
	return s.cur.digest
}

// Launch is the worker's path, args, dir and env from the ready artifact (PROTOCOL §4.6): the manifest's env,
// then env. With nothing ready it returns ErrNotReady, and the spawn waits out the restart backoff.
func (s *Source) Launch(env []string) (*shimv1.Launch, error) {
	s.mu.Lock()
	cur, reason := s.cur, s.reason
	s.mu.Unlock()
	if cur == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotReady, reason)
	}
	return cur.manifest.Launch(cur.rootfs, env)
}

// Local is a worker binary on the host, for development and tests (E12-D010). Production uses Source.
type Local struct{ Path string }

// Launch runs Path with env, in /.
func (l Local) Launch(env []string) (*shimv1.Launch, error) {
	return &shimv1.Launch{Path: l.Path, Dir: "/", Env: env}, nil
}

// Pending is always empty: the binary is there.
func (Local) Pending() string { return "" }

// Launcher is either source of the worker binary.
type Launcher interface {
	Launch(env []string) (*shimv1.Launch, error)
	Pending() string
}

// Repositories of the release workflow under ghcr.io/bullionbear (docs/release.md).
const (
	RepoMDConn    = "bullionbear/mftiko-md-conn"
	RepoTDAccount = "bullionbear/mftiko-td-account"
)

// Flags are the agent flags that pick the worker binary (E12-D010): -registry for the latest artifact, or the
// plane's local-binary flag for development. Exactly one of the two.
type Flags struct {
	Registry, Credentials, Repository, DataDir string
	PlainHTTP                                  bool
	MaxBytes                                   int64
	Poll                                       time.Duration
}

// Register adds the flags to fs. repo is the plane's repository default.
func (f *Flags) Register(fs *flag.FlagSet, repo string) {
	fs.StringVar(&f.Registry, "registry", "", "OCI registry address, host[:port], to run the latest worker artifact from")
	fs.StringVar(&f.Credentials, "registry-creds", "", "Docker config.json for the registry")
	fs.StringVar(&f.Repository, "repository", repo, "worker artifact repository in the registry")
	fs.StringVar(&f.DataDir, "data", "", "artifact layers and unpacked workers (${DATA}); required with -registry")
	fs.BoolVar(&f.PlainHTTP, "plain-http", false, "talk to the registry over plain HTTP")
	fs.Int64Var(&f.MaxBytes, "max-artifact-bytes", artifact.DefaultMaxBytes, "worker artifact size limit")
	fs.DurationVar(&f.Poll, "artifact-poll", DefaultPoll, "how often to resolve the latest tag")
}

// Launcher is Local{local} when the local flag (named localFlag) is set, and a Source for kind when -registry is.
func (f Flags) Launcher(kind, localFlag, local string, log zerolog.Logger) (Launcher, error) {
	switch {
	case local != "" && f.Registry != "":
		return nil, fmt.Errorf("-%s and -registry are exclusive", localFlag)
	case local != "":
		abs, err := filepath.Abs(local)
		if err != nil {
			return nil, err
		}
		return Local{Path: filepath.Clean(abs)}, nil
	case f.Registry == "":
		return nil, fmt.Errorf("-registry (or -%s, for development) is required", localFlag)
	case f.DataDir == "":
		return nil, errors.New("-data is required with -registry")
	}
	data, err := filepath.Abs(f.DataDir)
	if err != nil {
		return nil, err
	}
	return New(Config{
		Registry: artifact.Registry{Address: f.Registry, Credentials: f.Credentials, PlainHTTP: f.PlainHTTP},
		DataDir:  filepath.Clean(data), Repository: f.Repository, Kind: kind, MaxBytes: f.MaxBytes, Poll: f.Poll,
		Log: log,
	})
}
