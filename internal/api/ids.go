package api

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// idAttempts is how many ids one allocation draws before it gives up (E10-D007, E10-D008).
const idAttempts = 5

// maxAPIID is 2⁵³ − 1, the largest integer a JavaScript number holds exactly (E10-D008).
const maxAPIID = 1<<53 - 1

// newSessionID draws a session_id: 6 lowercase hex digits, 24 bits from r (E10-D007).
func newSessionID(r io.Reader) (string, error) {
	var b [3]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", fmt.Errorf("api: draw a session_id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// newAPIID draws an api_id uniformly from 1 to 2⁵³ − 1 (E10-D008): 53 bits from r, drawn again on 0.
func newAPIID(r io.Reader) (int64, error) {
	var b [8]byte
	for {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, fmt.Errorf("api: draw an api_id: %w", err)
		}
		if id := int64(binary.BigEndian.Uint64(b[:]) & maxAPIID); id != 0 {
			return id, nil
		}
	}
}

// allocate draws ids until try writes one. An id that taken reports is skipped; one that try finds existing after
// all (errConflict: another replica wrote it since the check, or the mirror had not seen it yet) is given up. Both
// count as a draw, and after attempts draws allocate is errIDsExhausted. Any other error, from draw or try, ends it.
func allocate[T comparable](attempts int, draw func() (T, error), taken func(T) bool, try func(T) error) (T, error) {
	var zero T
	for range attempts {
		id, err := draw()
		if err != nil {
			return zero, err
		}
		if taken(id) {
			continue
		}
		err = try(id)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, errConflict) {
			return zero, err
		}
	}
	return zero, fmt.Errorf("%w after %d draws", errIDsExhausted, attempts)
}

// createSession allocates a session_id and creates the spec of it under stsInst, create-only (W10, E10-D007, E10-D042). It
// returns the id and the spec's revision.
//
// The region of stsInst must be available (E10-D014), or it is errRegionUnavailable before any write: its mirror is what
// the check of that region's ids reads. Other regions are checked when available and skipped when not; an id that
// lives only in an unavailable region can be drawn again, at odds of the order E10-D007 accepts for two replicas racing.
// An id is taken while any key names it (state.SessionOf): its spec, and also a status, demand or ack left without
// one, such as a status the STS put back after W5 deleted the spec.
func (s *Server) createSession(ctx context.Context, stsInst string, spec []byte) (string, int64, error) {
	if p := s.cfg.Planes[stsInst]; p != state.PlaneSTS {
		return "", 0, fmt.Errorf("api: %q is not an STS instance", stsInst)
	}
	views, err := s.availableViews(stsInst)
	if err != nil {
		return "", 0, err
	}
	taken := sessionIDs(views)
	src := s.ids()
	w := s.cl.APIWriter()
	var rev int64
	id, err := allocate(idAttempts,
		func() (string, error) { return newSessionID(src) },
		func(id string) bool { _, ok := taken[id]; return ok },
		func(id string) (err error) {
			rev, err = create(ctx, w, state.SessionSpecKey(stsInst, id), spec)
			return err
		})
	if err != nil {
		return "", 0, err
	}
	return id, rev, nil
}

// availableViews is the mirror copy of every available region, by name. It is errRegionUnavailable when the region of
// inst is not one of them.
func (s *Server) availableViews(inst string) (map[string]state.View, error) {
	home, err := s.cfg.Topology.RegionOf(inst)
	if err != nil {
		return nil, err
	}
	now := s.now()
	views := make(map[string]state.View, len(s.clusters))
	for _, c := range s.clusters {
		view, contact, synced := c.observe()
		if (regionSnap{synced: synced, contact: contact}).available(now, s.cfg.UnavailableAfter) {
			views[c.name] = view
		}
	}
	if _, ok := views[home]; !ok {
		return nil, fmt.Errorf("%w: %s, the region of %s", errRegionUnavailable, home, inst)
	}
	return views, nil
}

// sessionIDs is every session_id a key names in views.
func sessionIDs(views map[string]state.View) map[string]struct{} {
	out := map[string]struct{}{}
	for _, v := range views {
		for key := range v.Entries {
			if id, ok := state.SessionOf(key); ok {
				out[id] = struct{}{}
			}
		}
	}
	return out
}
