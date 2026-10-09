package fleet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/c3"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const (
	// adoptTimeout bounds one Attach to a shim found at startup.
	adoptTimeout = 5 * time.Second
	// retryUnreachable is how often a shim that answered but could not be attached is tried again.
	retryUnreachable = 2 * time.Second
	// probeTimeout bounds the connect that tells a live shim from a socket left behind.
	probeTimeout = 200 * time.Millisecond
)

// ErrUnreachable: a shim of this worker key answers on its socket but could not be attached. Its worker may well
// be running: no new incarnation until the shim is attached or gone (D106).
var ErrUnreachable = errors.New("fleet: a shim of this worker key answers but could not be attached")

// answers reports whether something listens on the socket: a shim, or one that is exiting.
func answers(path string) bool {
	c, err := net.DialTimeout("unix", path, probeTimeout)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// Adopt takes over the shims a previous term left (D102, PROTOCOL §4.2): every socket and exit file under
// run/{plane}, before the first reconcile (E1-17).
//
//   - A shim that answers is attached at this term's epoch, which preempts the old term if it is somehow still
//     there; the worker is observed from Attached, which holds everything the agent decides on (D108).
//   - A shim that answers but cannot be attached (it does not reply in time) is held as unreachable: its
//     WorkerStatus stays (W7.1), no new incarnation is spawned for it, and it is tried again every few seconds.
//   - An exit file with no shim answering is a worker that ended while no agent was there: it is observed as
//     exited, and Release removes the file.
//   - A socket that refuses, with no exit file, is a shim killed with its worker before it recorded an end; with
//     the worker's key unknown it cannot be observed. Its WorkerStatus goes unless the demand still wants it.
//
// An Attach refused for a stale epoch means a newer term exists: Adopt fails, and so does the agent.
func (f *Fleet[S, L]) Adopt(ctx context.Context) error {
	dir := shim.RunDir(f.cfg.WorkDir, string(f.cfg.Plane))
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("fleet: adopt: %w", err)
	}
	var hashes []string
	for _, e := range entries {
		name := e.Name()
		for _, suffix := range []string{".sock", ".exit.pb"} {
			if h, ok := strings.CutSuffix(name, suffix); ok && !slices.Contains(hashes, h) {
				hashes = append(hashes, h)
			}
		}
	}
	slices.Sort(hashes)
	adopted, held := 0, 0
	for _, h := range hashes {
		switch err := f.adopt(ctx, dir, h); {
		case c3.CodeOf(err) == shim.CodeStaleEpoch:
			return fmt.Errorf("fleet: adopt: %w", err)
		case errors.Is(err, ErrUnreachable):
			held++
		case err != nil:
			return fmt.Errorf("fleet: adopt %s: %w", h, err)
		default:
			adopted++
		}
	}
	f.cfg.Log.Info().Int("adopted", adopted).Int("unreachable", held).Msg("shims adopted")
	if held > 0 {
		go f.retryHeld(ctx, dir)
	}
	f.signal()
	return nil
}

// adopt takes over the shim, or the exit file, of worker hash h. ErrUnreachable holds it.
func (f *Fleet[S, L]) adopt(ctx context.Context, dir, h string) error {
	sock := filepath.Join(dir, h+".sock")
	if answers(sock) {
		actx, cancel := context.WithTimeout(ctx, adoptTimeout)
		defer cancel()
		conn, err := c3.Dial(actx, sock, f.attachConfig())
		if c3.CodeOf(err) == shim.CodeStaleEpoch || ctx.Err() != nil {
			return err
		}
		if err != nil {
			f.hold(h, sock, err)
			return ErrUnreachable
		}
		f.adoptConn(h, sock, conn)
		return nil
	}
	rec, err := shim.ReadExitFile(filepath.Join(dir, h+".exit.pb"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		f.cfg.Log.Warn().Str("worker_hash", h).Msg("shim gone with no exit recorded")
		return nil
	case err != nil:
		f.hold(h, sock, err) // unreadable: keep its status, spawn nothing over it
		return ErrUnreachable
	}
	key := agent.WorkerKey(rec.GetWorker().GetKey())
	if state.WorkerHash(string(key)) != h {
		f.hold(h, sock, fmt.Errorf("exit file of %q under the hash %s", key, h))
		return ErrUnreachable
	}
	w := &worker[S]{socket: sock, pid: int(rec.GetPid()), startTime: rec.GetStartTime(), obs: agent.Observed{
		Key: key, Incarnation: rec.GetWorker().GetIncarnation(), SpecHash: rec.GetSpecHash(),
		State: agent.StateExited, Exit: rec.GetExit(),
	}}
	f.mu.Lock()
	f.workers[key] = w
	delete(f.unreachable, h)
	f.mu.Unlock()
	f.cfg.Log.Info().Str("worker", string(key)).Uint32("incarnation", w.obs.Incarnation).
		Msg("adopted the exit of a worker whose shim is gone")
	return nil
}

func (f *Fleet[S, L]) hold(h, sock string, err error) {
	f.mu.Lock()
	f.unreachable[h] = sock
	f.mu.Unlock()
	f.cfg.Log.Warn().Err(err).Str("worker_hash", h).Msg("shim not adopted; held")
}

// adoptConn observes the worker of an attached shim from its Attached, and follows its events.
func (f *Fleet[S, L]) adoptConn(h, sock string, conn *c3.Conn) {
	at := conn.Attached()
	key := agent.WorkerKey(at.GetWorker().GetKey())
	w := &worker[S]{conn: conn, socket: sock, pid: int(at.GetPid()), startTime: at.GetStartTime(),
		obs: observedOf(at)}
	f.mu.Lock()
	f.workers[key] = w
	delete(f.unreachable, h)
	f.mu.Unlock()
	go f.follow(key, w)
	f.cfg.Log.Info().Str("worker", string(key)).Uint32("incarnation", w.obs.Incarnation).
		Stringer("state", w.obs.State).Msg("adopted")
}

// observedOf is a worker as its shim's Attached has it, the live config it has applied included (D111): nothing
// is sent again that it already has.
func observedOf(at *shimv1.Attached) agent.Observed {
	o := agent.Observed{
		Key: agent.WorkerKey(at.GetWorker().GetKey()), Incarnation: at.GetWorker().GetIncarnation(),
		SpecHash: at.GetSpecHash(), LiveHash: at.GetAppliedDigest(), Stage: at.GetStage(), Release: at.GetHello().GetRelease(),
		ShimRelease: at.GetShimRelease(), Exit: at.GetExit(),
	}
	if ns := at.GetLastBeatAtNs(); ns != 0 {
		o.LastBeat = time.Unix(0, ns) // the Beat timeout goes on from the last Beat the shim saw (E1-18)
	}
	switch {
	case at.GetState() == shimv1.WorkerState_WORKER_STATE_EXITED:
		o.State = agent.StateExited
	case at.GetPhase() == shimv1.Phase_PHASE_INITIALIZED:
		o.State = agent.StateInitialized
	case at.GetPhase() == shimv1.Phase_PHASE_STARTING:
		o.State = agent.StateStarting
	case at.GetPhase() == shimv1.Phase_PHASE_STARTED:
		o.State = agent.StateRunning
	case at.GetPhase() == shimv1.Phase_PHASE_STOPPING:
		o.State = agent.StateStopping
	default: // NEW, INITIALIZING
		o.State = agent.StateInitializing
	}
	return o
}

// retryHeld tries the held shims again until each is attached or gone, or ctx ends.
func (f *Fleet[S, L]) retryHeld(ctx context.Context, dir string) {
	t := time.NewTicker(retryUnreachable)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		f.mu.Lock()
		var hs []string
		for h := range f.unreachable {
			hs = append(hs, h)
		}
		f.mu.Unlock()
		if len(hs) == 0 {
			return
		}
		for _, h := range hs {
			if err := f.adopt(ctx, dir, h); err == nil {
				f.mu.Lock()
				delete(f.unreachable, h) // attached, an exit adopted, or the shim gone
				f.mu.Unlock()
				f.signal()
			}
		}
	}
}

// Unreachable are the worker hashes h of the shims held: their WorkerStatus is kept (W7.1).
func (f *Fleet[S, L]) Unreachable() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.unreachable))
	for h := range f.unreachable {
		out = append(out, h)
	}
	slices.Sort(out)
	return out
}
