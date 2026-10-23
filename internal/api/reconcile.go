package api

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/puddle/v2"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// runReconcile compares etcd and session_records on a timer and when a mirror changes (D1070).
// Every replica runs it. Writes are conditional. There is no leader.
func (s *Server) runReconcile() {
	defer s.wg.Done()
	every := s.cfg.ReconcileEvery
	if every <= 0 {
		every = reconcileEveryDefault
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	s.reconcileOnce(s.ctx)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.reconcileOnce(s.ctx)
		case <-s.reconcileKick:
			s.reconcileOnce(s.ctx)
		}
	}
}

// watchMirror turns mirror notifications into one coalesced reconcile. The channel is never closed
// (state.Mirror); canceling ctx is what stops the watch, which runMirror does when Run returns.
func (s *Server) watchMirror(ctx context.Context, m *state.Mirror) {
	defer s.wg.Done()
	notify := m.Notify()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.ctx.Done():
			return
		case <-notify:
			s.kickReconcile()
		}
	}
}

func (s *Server) kickReconcile() {
	if s.reconcileKick == nil {
		return
	}
	select {
	case s.reconcileKick <- struct{}{}:
	default:
	}
}

type liveSpec struct {
	sts  string
	id   string
	rev  int64
	raw  []byte
	ok   bool
	view state.View
}

// reconcileOnce is one full compare (D1068–D1072). A direct etcd spec with no row is logged and
// counted, not inserted (D1072). Tests call it; the loop calls it too, so the mutex keeps one pass.
func (s *Server) reconcileOnce(ctx context.Context) {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if err := ctx.Err(); err != nil || s.stopped() {
		return
	}
	cutoff := s.now().UTC().Add(-idempotencyTTL)
	if err := s.expireIdempotency(ctx, cutoff); err != nil {
		s.logDB(err, "", "", "expire idempotency keys")
		return
	}
	records, err := s.loadLiveRecords(ctx)
	if err != nil {
		s.logDB(err, "", "", "load session records")
		return
	}
	specs := s.liveSpecs()
	matched := map[string]bool{}
	for _, hit := range specs {
		idx := indexByRevision(records, hit.id, hit.rev)
		if idx < 0 && hit.ok && !revisionTaken(records, hit.rev) {
			idx = pickAdoptIndex(records, hit.id, hit.raw)
		}
		if idx < 0 {
			if !hit.ok {
				s.log.Error().Str("session_id", hit.id).Str("sts_instance", hit.sts).Msg("session spec could not be decoded")
			}
			s.warnMissing(hit.id, hit.sts)
			continue
		}
		matched[records[idx].ID] = true
		if !hit.ok {
			s.log.Error().Str("session_id", hit.id).Str("sts_instance", hit.sts).Msg("session spec could not be decoded")
			continue
		}
		row := &records[idx]
		if specContentEqual(row.Spec, hit.raw) {
			if err := s.markCreated(ctx, row, hit.rev, hit.raw); err != nil {
				if _, unique := pgConstraint(err); unique {
					s.log.Error().Err(err).Str("session_id", hit.id).Str("sts_instance", hit.sts).Msg("session record revive collided")
					continue
				}
				if s.quiet(err) {
					return
				}
				s.logDB(err, hit.id, hit.sts, "sync session record")
			}
			continue
		}
		s.warnHash(hit.id, hit.sts)
		reason, ended := s.outcomeOf(hit)
		if err := s.syncDesired(ctx, *row, desiredOfSpec(hit.raw), reason, ended); err != nil {
			if s.quiet(err) {
				return
			}
			s.logDB(err, hit.id, hit.sts, "sync session desired state")
		}
	}
	abandonBefore := s.now().Add(-pendingTimeout)
	for i := range records {
		row := &records[i]
		if matched[row.ID] || row.State != recPending || row.CreateRevision != nil {
			continue
		}
		home, err := s.cfg.Topology.RegionOf(row.STS)
		if err != nil || !s.regionAvailable(home) {
			continue
		}
		// Exactly pendingTimeout old is not yet past the limit (D1069: 超過).
		if !row.CreatedAt.Before(abandonBefore) {
			continue
		}
		if err := s.abandonPending(ctx, row.ID); err != nil {
			if s.quiet(err) {
				return
			}
			s.logDB(err, row.SessionID, row.STS, "abandon pending session")
		}
	}
}

func (s *Server) liveSpecs() []liveSpec {
	now := s.now()
	var out []liveSpec
	for _, c := range s.clusters {
		view, contact, synced := c.observe()
		if !(regionSnap{synced: synced, contact: contact}).available(now, s.cfg.UnavailableAfter) {
			continue
		}
		for key, entry := range view.Entries {
			plane, inst, err := state.Owner(key)
			if err != nil || plane != state.PlaneSTS {
				continue
			}
			id := sessionIDOfSpec(key, inst)
			if id == "" || key != state.SessionSpecKey(inst, id) {
				continue
			}
			raw := append([]byte(nil), entry.Value...)
			hit := liveSpec{sts: inst, id: id, rev: entry.CreateRevision, raw: raw, view: view}
			var spec sessionv1.SessionSpec
			if proto.Unmarshal(raw, &spec) == nil {
				hit.ok = true
			}
			out = append(out, hit)
		}
	}
	return out
}

func (s *Server) regionAvailable(name string) bool {
	c := s.byName[name]
	if c == nil {
		return false
	}
	_, contact, synced := c.observe()
	return (regionSnap{synced: synced, contact: contact}).available(s.now(), s.cfg.UnavailableAfter)
}

func indexByRevision(records []sessionRecord, sessionID string, rev int64) int {
	for i := range records {
		r := &records[i]
		if r.SessionID == sessionID && r.CreateRevision != nil && *r.CreateRevision == rev {
			return i
		}
	}
	return -1
}

func revisionTaken(records []sessionRecord, rev int64) bool {
	for i := range records {
		if records[i].CreateRevision != nil && *records[i].CreateRevision == rev {
			return true
		}
	}
	return false
}

func pickAdoptIndex(records []sessionRecord, sessionID string, spec []byte) int {
	best := -1
	for i := range records {
		r := &records[i]
		if r.CreateRevision != nil || r.SessionID != sessionID {
			continue
		}
		if r.State != recPending && r.State != recAbandoned {
			continue
		}
		if !specContentEqual(r.Spec, spec) {
			continue
		}
		if best < 0 || adoptBetter(r, &records[best]) {
			best = i
		}
	}
	return best
}

func adoptBetter(a, b *sessionRecord) bool {
	if a.State == recPending && b.State != recPending {
		return true
	}
	if b.State == recPending && a.State != recPending {
		return false
	}
	return a.CreatedAt.After(b.CreatedAt)
}

// outcomeOf is the terminal columns reconcile may fill when the immutable spec does not match (D1071).
// The first stored reason stays; callers pass this through COALESCE.
func (s *Server) outcomeOf(hit liveSpec) (string, *time.Time) {
	desired := desiredOfSpec(hit.raw)
	reason := ""
	if entry, ok := hit.view.Entries[state.SessionDemandKey(hit.sts, hit.id)]; ok {
		var demand statev1.SessionDemand
		if proto.Unmarshal(entry.Value, &demand) == nil && demand.GetEndReason() != "" {
			reason = demand.GetEndReason()
		}
	}
	terminal := false
	if st := statusOf(hit.view, hit.sts, hit.id); sessionTerminal(st) {
		terminal = true
		if reason == "" {
			reason = st.GetReason()
		}
	}
	if desired == desEnded && reason == "" {
		reason = endReasonSpec
	}
	if desired != desEnded && !terminal {
		return "", nil
	}
	at := s.now().UTC()
	return reason, &at
}

func (s *Server) warnMissing(sessionID, sts string) {
	n := s.missingRecord.Add(1)
	s.log.Error().Str("session_id", sessionID).Str("sts_instance", sts).Int64("count", n).Msg("session spec has no session_records row")
}

func (s *Server) warnHash(sessionID, sts string) {
	n := s.hashMismatch.Add(1)
	s.log.Error().Str("session_id", sessionID).Str("sts_instance", sts).Int64("count", n).Msg("session spec hash does not match session_records")
}

func (s *Server) quiet(err error) bool {
	// puddle.ErrClosedPool is what pgxpool.Acquire returns after Close. Drain closes the audit
	// pool before stop cancels s.ctx, so a closed pool is a quiet stop even while s.ctx is live.
	return err == nil || s.stopped() || errors.Is(err, context.Canceled) || errors.Is(err, puddle.ErrClosedPool)
}

func (s *Server) logDB(err error, sessionID, sts, msg string) {
	if s.quiet(err) {
		return
	}
	ev := s.log.Error().Err(err)
	if sessionID != "" {
		ev = ev.Str("session_id", sessionID)
	}
	if sts != "" {
		ev = ev.Str("sts_instance", sts)
	}
	ev.Msg(msg)
}
