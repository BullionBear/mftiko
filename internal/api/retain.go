package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// retain is W5 (D1017, D1031, D1080–D1083). It runs inside reconcileOnce, after the
// mirror compare and the pending sweep, so it has no loop of its own (D1083).
// Postgres is written before etcd is deleted. A database error returns and deletes
// nothing further (D1080).
func (s *Server) retain(ctx context.Context) {
	if s.cl == nil {
		return
	}
	items := s.retentionItems()
	if len(items) == 0 {
		return
	}
	ids := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		if _, ok := seen[item.id]; ok {
			continue
		}
		seen[item.id] = struct{}{}
		ids = append(ids, item.id)
	}
	rows, err := s.recordsBySessionIDs(ctx, ids)
	if err != nil {
		s.logDB(err, "", "", "load session records for retention")
		return
	}
	byID := map[string][]sessionRecord{}
	for _, row := range rows {
		byID[row.SessionID] = append(byID[row.SessionID], row)
	}
	cutoff := s.now().Add(-s.retention())
	for i := range items {
		if err := s.retainItem(ctx, &items[i], byID[items[i].id], cutoff); err != nil {
			if s.quiet(err) {
				return
			}
			if errors.Is(err, errStorage) {
				s.logDB(err, items[i].id, items[i].sts, "retention close")
				return
			}
			s.log.Warn().Err(err).Str("session_id", items[i].id).Str("sts_instance", items[i].sts).Msg("retention delete")
		}
	}
}

func (s *Server) retention() time.Duration {
	if s.cfg.Retention > 0 {
		return s.cfg.Retention
	}
	return DefaultRetention
}

// retainItem is one session in one STS instance, as the mirror currently shows it.
type retainItem struct {
	sts        string
	id         string
	hasSpec    bool
	hasStatus  bool
	spec       state.Entry
	status     state.Entry
	phaseKnown bool
	terminal   bool
	reason     string
	digest     string
	updated    int64
}

func (s *Server) retentionItems() []retainItem {
	type slot struct{ sts, id string }
	index := map[slot]int{}
	var out []retainItem
	for _, c := range s.clusters {
		if !s.regionAvailable(c.name) {
			continue
		}
		view, _, _ := c.observe()
		for key, entry := range view.Entries {
			plane, inst, err := state.Owner(key)
			if err != nil || plane != state.PlaneSTS {
				continue
			}
			id, ok := state.SessionOf(key)
			if !ok || id == "" {
				continue
			}
			specKey := state.SessionSpecKey(inst, id)
			statusKey := state.SessionStatusKey(inst, id)
			if key != specKey && key != statusKey {
				continue
			}
			at := slot{inst, id}
			i, seen := index[at]
			if !seen {
				i = len(out)
				index[at] = i
				out = append(out, retainItem{sts: inst, id: id})
			}
			copied := entry
			if key == specKey {
				out[i].hasSpec = true
				out[i].spec = copied
				continue
			}
			out[i].hasStatus = true
			out[i].status = copied
			st := &statev1.SessionStatus{}
			if proto.Unmarshal(copied.Value, st) != nil {
				s.log.Error().Str("session_id", id).Str("sts_instance", inst).Msg("session status decode failed")
				continue
			}
			out[i].phaseKnown = true
			out[i].terminal = sessionTerminal(st)
			out[i].reason = st.GetReason()
			out[i].digest = st.GetStateOut().GetDigest()
			out[i].updated = st.GetUpdatedAtNs()
		}
	}
	return out
}

// pastRetention is true when the status is DONE or FAILED and its updated_at_ns is
// strictly older than the retention window (D1017). A missing timestamp has not
// started the clock: STS writes updated_at_ns whenever it writes a status.
func pastRetention(updated int64, terminal bool, cutoff time.Time) bool {
	if !terminal || updated <= 0 {
		return false
	}
	return time.Unix(0, updated).Before(cutoff)
}

func (s *Server) retainItem(ctx context.Context, item *retainItem, rows []sessionRecord, cutoff time.Time) error {
	if item.hasSpec && !item.hasStatus {
		return s.retainClosedSpec(ctx, item, rows)
	}
	if !item.phaseKnown || !pastRetention(item.updated, item.terminal, cutoff) {
		return nil
	}
	if item.hasSpec {
		return s.retainSpec(ctx, item, rows)
	}
	return s.retainStatusOnly(ctx, item, rows)
}

// retainClosedSpec finishes a delete whose status is already gone. Only a closed
// row with the same create_revision is that session (D1042). Anything else is
// not known to be terminal and stays.
func (s *Server) retainClosedSpec(ctx context.Context, item *retainItem, rows []sessionRecord) error {
	row := rowForSpec(rows, item.sts, item.spec.CreateRevision)
	if row == nil || row.State != recClosed || row.CreateRevision == nil || *row.CreateRevision != item.spec.CreateRevision {
		return nil
	}
	if s.haltBeforeDelete.Load() {
		return nil
	}
	_, err := deleteIf(ctx, s.cl.APIWriter(), cond{key: state.SessionSpecKey(item.sts, item.id), rev: item.spec.ModRevision})
	return ignoreDeleteRace(s, item.id, err)
}

// retainSpec closes the row for this spec's create_revision, then deletes the spec
// and the status together. A closed row whose create_revision still matches is the
// crash between those two steps: only the etcd delete is repeated (D1042).
func (s *Server) retainSpec(ctx context.Context, item *retainItem, rows []sessionRecord) error {
	row := rowForSpec(rows, item.sts, item.spec.CreateRevision)
	if row == nil {
		s.noteMissing(item.id, item.sts)
		return nil
	}
	rev := item.spec.CreateRevision
	if err := s.finishClose(ctx, row, item, &rev); err != nil {
		return err
	}
	if s.haltBeforeDelete.Load() {
		return nil
	}
	_, err := deleteIf(ctx, s.cl.APIWriter(),
		cond{key: state.SessionSpecKey(item.sts, item.id), rev: item.spec.ModRevision},
		cond{key: state.SessionStatusKey(item.sts, item.id), rev: item.status.ModRevision},
	)
	return ignoreDeleteRace(s, item.id, err)
}

// retainStatusOnly is the status STS wrote after W5 removed the spec (D1082).
// The delete is one compare-and-delete on that status key. A spec that exists in
// etcd but is not in this mirror yet is left alone.
func (s *Server) retainStatusOnly(ctx context.Context, item *retainItem, rows []sessionRecord) error {
	present, err := s.specPresent(ctx, item.sts, item.id)
	if err != nil || present {
		return err
	}
	row := rowForStatusOnly(rows, item.sts)
	if row == nil {
		s.noteMissing(item.id, item.sts)
		return nil
	}
	if err := s.finishClose(ctx, row, item, nil); err != nil {
		return err
	}
	if s.haltBeforeDelete.Load() {
		return nil
	}
	_, err = deleteIf(ctx, s.cl.APIWriter(), cond{key: state.SessionStatusKey(item.sts, item.id), rev: item.status.ModRevision})
	return ignoreDeleteRace(s, item.id, err)
}

func (s *Server) specPresent(ctx context.Context, sts, id string) (bool, error) {
	resp, err := txn(ctx, s.cl.APIWriter(), nil, []clientv3.Op{clientv3.OpGet(state.SessionSpecKey(sts, id))}, nil)
	if err != nil {
		return false, err
	}
	return firstKV(resp, 0) != nil, nil
}

func (s *Server) finishClose(ctx context.Context, row *sessionRecord, item *retainItem, createRev *int64) error {
	digest := item.digest
	if row.State == recClosed && (digest == "" || (row.StateOut != nil && *row.StateOut != "")) {
		return nil
	}
	ended := s.now().UTC()
	if item.updated > 0 {
		ended = time.Unix(0, item.updated).UTC()
	}
	return s.closeRecord(ctx, row, item.reason, digest, ended, createRev)
}

func (s *Server) noteMissing(id, sts string) {
	n := s.missingRecord.Add(1)
	s.log.Error().Str("session_id", id).Str("sts_instance", sts).Int64("count", n).Msg("terminal session has no record; not deleted")
}

func ignoreDeleteRace(s *Server, id string, err error) error {
	if errors.Is(err, errConflict) {
		s.log.Info().Str("session_id", id).Msg("session delete lost the revision race")
		return nil
	}
	return err
}

// rowForSpec is the session_records generation of this spec. create_revision is the
// identity (D1042). A row that has not stored one yet can still be that generation.
func rowForSpec(rows []sessionRecord, sts string, createRev int64) *sessionRecord {
	var open *sessionRecord
	for i := range rows {
		row := &rows[i]
		if row.STS != sts {
			continue
		}
		if row.CreateRevision != nil && *row.CreateRevision == createRev {
			return row
		}
		if row.CreateRevision == nil && row.State != recClosed {
			if open == nil || adoptBetter(row, open) {
				open = row
			}
		}
	}
	return open
}

// rowForStatusOnly is the row of a status whose spec is already gone. A pending row
// never had a spec, so it is not this status. An abandoned row counts only when it
// recorded a create_revision.
func rowForStatusOnly(rows []sessionRecord, sts string) *sessionRecord {
	var open, closed *sessionRecord
	for i := range rows {
		row := &rows[i]
		if row.STS != sts || row.State == recPending {
			continue
		}
		if row.State == recAbandoned && row.CreateRevision == nil {
			continue
		}
		if row.State == recClosed {
			if closed == nil || row.CreatedAt.After(closed.CreatedAt) {
				closed = row
			}
			continue
		}
		if open == nil || adoptBetter(row, open) {
			open = row
		}
	}
	if open != nil {
		return open
	}
	return closed
}

// DeleteSession removes one terminal session now (D1017, D1084). A session that is
// already gone, including a repeated DELETE, is 204. A live session is 409 not_terminal.
func (s *Server) DeleteSession(w http.ResponseWriter, r *http.Request, sessionID apiv1.SessionID) {
	if err := s.deleteSession(r.Context(), string(sessionID)); err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteSession(ctx context.Context, id string) error {
	rows, err := s.recordsForSessionAll(ctx, id)
	if err != nil {
		return err
	}
	instances, err := s.locateSession(id)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		if hasClosedRecord(rows) {
			return nil
		}
		if len(rows) > 0 {
			return errNotTerminal
		}
		return errNotFound
	}
	for _, sts := range instances {
		if err := s.deleteOnSTS(ctx, id, sts, rows, 0); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) deleteOnSTS(ctx context.Context, id, sts string, rows []sessionRecord, attempt int) error {
	if attempt >= endAttempts {
		return fmt.Errorf("%w: %s kept changing", errConflict, id)
	}
	specKey := state.SessionSpecKey(sts, id)
	statusKey := state.SessionStatusKey(sts, id)
	resp, err := txn(ctx, s.cl.APIWriter(), nil, []clientv3.Op{clientv3.OpGet(specKey), clientv3.OpGet(statusKey)}, nil)
	if err != nil {
		return err
	}
	specKV := firstKV(resp, 0)
	statusKV := firstKV(resp, 1)
	if specKV == nil && statusKV == nil {
		return nil
	}
	st := &statev1.SessionStatus{}
	if statusKV != nil {
		if err := proto.Unmarshal(statusKV.Value, st); err != nil {
			return err
		}
	}
	if !sessionTerminal(st) {
		return errNotTerminal
	}
	item := &retainItem{
		sts: sts, id: id, terminal: true, phaseKnown: true,
		reason: st.GetReason(), digest: st.GetStateOut().GetDigest(), updated: st.GetUpdatedAtNs(),
	}
	var row *sessionRecord
	var rev *int64
	if specKV != nil {
		row = rowForSpec(rows, sts, specKV.CreateRevision)
		v := specKV.CreateRevision
		rev = &v
	} else {
		row = rowForStatusOnly(rows, sts)
	}
	if row == nil {
		s.noteMissing(id, sts)
		return errNoRecord
	}
	if err := s.finishClose(ctx, row, item, rev); err != nil {
		return err
	}
	var conds []cond
	if specKV != nil {
		conds = append(conds, cond{key: specKey, rev: specKV.ModRevision})
	}
	if statusKV != nil {
		conds = append(conds, cond{key: statusKey, rev: statusKV.ModRevision})
	}
	if _, err := deleteIf(ctx, s.cl.APIWriter(), conds...); err != nil {
		if errors.Is(err, errConflict) {
			return s.deleteOnSTS(ctx, id, sts, rows, attempt+1)
		}
		return err
	}
	return nil
}

func (s *Server) locateSession(id string) ([]string, error) {
	down := false
	var out []string
	seen := map[string]struct{}{}
	for _, c := range s.clusters {
		if !s.regionAvailable(c.name) {
			down = true
			continue
		}
		view, _, _ := c.observe()
		for key := range view.Entries {
			plane, inst, err := state.Owner(key)
			if err != nil || plane != state.PlaneSTS {
				continue
			}
			sid, ok := state.SessionOf(key)
			if !ok || sid != id {
				continue
			}
			if key != state.SessionSpecKey(inst, id) && key != state.SessionStatusKey(inst, id) {
				continue
			}
			if _, ok := seen[inst]; ok {
				continue
			}
			seen[inst] = struct{}{}
			out = append(out, inst)
		}
	}
	if len(out) == 0 && down {
		return nil, fmt.Errorf("%w: session %s", errRegionUnavailable, id)
	}
	return out, nil
}

func hasClosedRecord(rows []sessionRecord) bool {
	for i := range rows {
		if rows[i].State == recClosed {
			return true
		}
	}
	return false
}
