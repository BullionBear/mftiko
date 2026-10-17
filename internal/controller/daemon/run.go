package daemon

import (
	"context"
	"errors"
	"fmt"
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
	Dial     state.DialFunc // authenticated as the etcd user "controller"
	Term     state.TermOptions
	Release  string // AgentRecord.release
	Log      zerolog.Logger
	Now      func() time.Time // time.Now if nil
}

// Controller is a running term. Put and Delete are the test write entry
// (D1313): production does not call them until derivation is connected.
type Controller struct {
	ctx    context.Context
	term   *state.Term
	cl     *state.Clusters
	writes *queues
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
	return &Controller{
		ctx: ctx, term: term, cl: cl, log: log,
		writes: newQueues(term, cl.Topology(), log),
	}, nil
}

func (cfg Config) validate() error {
	if err := cfg.Topology.Validate(); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	if _, err := cfg.Topology.RegionOf(state.ControllerInstance); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	switch {
	case cfg.Dial == nil:
		return errors.New("controller: dial is required")
	case cfg.Term.ID == "":
		return errors.New("controller: term ID is required")
	case cfg.Term.SessionTTL < 0:
		return fmt.Errorf("controller: negative session TTL %s", cfg.Term.SessionTTL)
	default:
		return nil
	}
}

// Put writes key on its region's queue and returns that write's result.
// Regions do not wait for each other. ErrSuperseded ends the whole term.
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
