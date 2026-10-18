package daemon

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// shutdownTimeout bounds the revoke and the cluster close after the term ends.
const shutdownTimeout = 5 * time.Second

// Config is one controller process.
type Config struct {
	Topology state.Topology
	// Planes is the deployment's plane of every instance except the controller,
	// the same map mftiko-etcd-roles reads (D1317).
	Planes  map[string]state.Plane
	Dial    state.DialFunc // authenticated as the etcd user "controller"
	Term    state.TermOptions
	Release string // AgentRecord.release
	// StaleAfter is how old a region's LastContact may be before the view marks
	// it stale. Zero selects DefaultStaleAfter (D1318).
	StaleAfter time.Duration
	// Resync is the full-resync interval. Zero selects DefaultResync (D1319).
	Resync time.Duration
	// Mirror tunes the instance mirrors. Zero selects the Mirror defaults.
	Mirror state.MirrorOptions
	Log    zerolog.Logger
	Now    func() time.Time // time.Now if nil
}

// Controller is a running term. Put and Delete are the test write entry
// (D1313). Derivation puts through the same queues.
type Controller struct {
	ctx    context.Context
	term   *state.Term
	cl     *state.Clusters
	writes *queues
	intake *intake
	log    zerolog.Logger

	once sync.Once
	err  error
}

// Run campaigns, writes the AgentRecord, and blocks until ctx is canceled or
// the term ends. A canceled ctx revokes and returns nil (D1315). ErrSuperseded
// and ErrSessionLost are returned as themselves.
func Run(ctx context.Context, cfg Config) error {
	c, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	return c.Wait()
}

// Start campaigns in the home region and writes the AgentRecord once (D1314).
// It returns while the term is running. ctx bounds the campaign; canceling it
// later ends the term cleanly. The caller must Wait, once.
func Start(ctx context.Context, cfg Config) (*Controller, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	allowed, err := state.AgentRegions(cfg.Topology, state.PlaneCtl, state.ControllerInstance)
	if err != nil {
		return nil, fmt.Errorf("controller: %w", err)
	}
	cl, err := state.NewClusters(cfg.Topology, allowed, cfg.Dial)
	if err != nil {
		return nil, fmt.Errorf("controller: %w", err)
	}
	term, err := state.Campaign(ctx, cl, state.PlaneCtl, state.ControllerInstance, cfg.Term)
	if err != nil {
		cl.Close()
		return nil, err
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	log := cfg.Log.With().Str("plane", string(state.PlaneCtl)).Str("instance", state.ControllerInstance).
		Int64("epoch", term.Epoch()).Logger()
	rec := controller.Record(term.Epoch(), cfg.Release, now().UnixNano())
	if err := term.PutRecord(ctx, controller.Encode(rec)); err != nil {
		release(term, cl)
		return nil, err
	}
	log.Info().Str("release", cfg.Release).Msg("controller record written")
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	if cfg.Resync == 0 {
		cfg.Resync = DefaultResync
	}
	in := newIntake(ctx, term, cl, cfg, now, log)
	c := &Controller{
		ctx: ctx, term: term, cl: cl, log: log,
		writes: newQueues(term, cl.Topology(), log, in.writable),
		intake: in,
	}
	in.derive = c.apply
	in.start()
	return c, nil
}

func (cfg Config) validate() error {
	if err := cfg.Topology.Validate(); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	if _, err := cfg.Topology.RegionOf(state.ControllerInstance); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	if err := cfg.checkPlanes(); err != nil {
		return err
	}
	switch {
	case cfg.Dial == nil:
		return errors.New("controller: dial is required")
	case cfg.Term.ID == "":
		return errors.New("controller: term ID is required")
	case cfg.Term.SessionTTL < 0:
		return fmt.Errorf("controller: negative session TTL %s", cfg.Term.SessionTTL)
	case cfg.StaleAfter < 0:
		return fmt.Errorf("controller: negative stale threshold %s", cfg.StaleAfter)
	case cfg.Resync < 0:
		return fmt.Errorf("controller: negative resync interval %s", cfg.Resync)
	default:
		return nil
	}
}

// checkPlanes requires an agent plane for every instance except the controller.
func (cfg Config) checkPlanes() error {
	for _, inst := range slices.Sorted(maps.Keys(cfg.Topology.Instances)) {
		if inst == state.ControllerInstance {
			continue
		}
		p, ok := cfg.Planes[inst]
		if !ok || !agentPlane(p) {
			return fmt.Errorf("controller: instance %s has plane %q, want sts, md or td", inst, p)
		}
	}
	for _, inst := range slices.Sorted(maps.Keys(cfg.Planes)) {
		if _, ok := cfg.Topology.Instances[inst]; !ok {
			return fmt.Errorf("controller: plane given for %s, which is not in the topology", inst)
		}
	}
	return nil
}

func agentPlane(p state.Plane) bool {
	return p == state.PlaneSTS || p == state.PlaneMD || p == state.PlaneTD
}

// Put writes key on its region's queue and returns that write's result.
// A region whose mirrors have not finished the first list is not written:
// Put returns ErrUnsynced and does not fence it (D1318). Regions do not wait
// for each other. ErrSuperseded ends the whole term.
func (c *Controller) Put(ctx context.Context, key, val string) error {
	return c.writes.submit(ctx, key, val, false)
}

// Delete removes key on its region's queue. Same isolation as Put.
func (c *Controller) Delete(ctx context.Context, key string) error {
	return c.writes.submit(ctx, key, "", true)
}

// Wait blocks until the term ends. Canceling the Start ctx revokes the session
// and returns nil. ErrSuperseded or a lost session is returned to the caller,
// which exits non-zero (D1315). A second call returns the same error.
func (c *Controller) Wait() error {
	c.once.Do(func() {
		c.err = c.outcome()
		c.shutdown()
	})
	return c.err
}

func (c *Controller) outcome() error {
	select {
	case <-c.term.Done():
		return clean(c.term.Err())
	case <-c.ctx.Done():
		if err := c.term.Err(); err != nil {
			return clean(err)
		}
		return nil
	}
}

func clean(err error) error {
	if errors.Is(err, state.ErrClosed) {
		return nil
	}
	return err
}

func (c *Controller) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := c.term.Close(ctx); err != nil {
		c.log.Warn().Err(err).Msg("revoke")
	}
	if c.intake != nil {
		c.intake.stop()
	}
	c.writes.stop()
	if err := c.cl.Close(); err != nil {
		c.log.Warn().Err(err).Msg("close clusters")
	}
}

func release(term *state.Term, cl *state.Clusters) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	term.Close(ctx)
	cl.Close()
}
