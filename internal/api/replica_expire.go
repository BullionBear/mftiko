package api

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// replicaGrace is the configured grace, or the default when unset (E21-D050).
func (c Config) replicaGrace() time.Duration {
	if c.TDReplicaGrace > 0 {
		return c.TDReplicaGrace
	}
	return DefaultTDReplicaGrace
}

// warnShortReplicaGrace logs when grace is below the TD default linger (E21-D058).
// Startup continues. Equal to the default linger is not a warning.
func warnShortReplicaGrace(log zerolog.Logger, grace time.Duration) {
	if grace >= tdDefaultLinger {
		return
	}
	log.Warn().
		Str("td_replica_grace", grace.String()).
		Str("td_default_linger", tdDefaultLinger.String()).
		Msg("td_replica_grace is below the TD default linger")
}

// replicaUse is what session history says about one (td_instance, api_id).
type replicaUse struct {
	live     bool
	ended    time.Time
	hasEnded bool
}

// replicaDue is true when the replica is not referenced and the last session end
// is strictly past td_replica_grace (E21-D056, E21-D062). Exactly the grace is
// not yet past. With no ended session, the replica row's created_at is the clock.
func (s *Server) replicaDue(row replicaView, uses map[replicaKey]replicaUse) bool {
	u := uses[replicaKey{inst: row.Inst, id: row.ID}]
	if u.live {
		return false
	}
	last := row.replicaCreated.UTC()
	if u.hasEnded {
		last = u.ended.UTC()
	}
	return last.Before(s.now().UTC().Add(-s.cfg.replicaGrace()))
}

// replicaUses loads every session_records row and the non-terminal specs the
// API can see (E21-D056, E21-D062). An unreadable spec is skipped.
func (s *Server) replicaUses(ctx context.Context) (map[replicaKey]replicaUse, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT session_id, sts_instance, state, ended_at, spec_bytes
FROM session_records`)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()

	views, unavail := s.regionViews()
	down := make(map[string]struct{}, len(unavail))
	for _, name := range unavail {
		down[name] = struct{}{}
	}
	uses := map[replicaKey]replicaUse{}
	for rows.Next() {
		var (
			sessionID, sts, state string
			ended                 *time.Time
			spec                  []byte
		)
		if err := rows.Scan(&sessionID, &sts, &state, &ended, &spec); err != nil {
			return nil, storageErr(err)
		}
		spec = append([]byte(nil), spec...)
		var parsed sessionv1.SessionSpec
		if proto.Unmarshal(spec, &parsed) != nil {
			continue
		}
		live := blocksReplica(state, ended, s.sessionRunning(views, down, sts, sessionID))
		for _, a := range parsed.GetAccounts() {
			if a.GetApiId() <= 0 || a.GetTdInstance() == "" {
				continue
			}
			k := replicaKey{inst: a.GetTdInstance(), id: a.GetApiId()}
			u := uses[k]
			if live {
				u.live = true
			}
			if ended != nil && (!u.hasEnded || ended.After(u.ended)) {
				u.ended = ended.UTC()
				u.hasEnded = true
			}
			uses[k] = u
		}
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	for k := range liveEtcdReplicas(views) {
		u := uses[k]
		u.live = true
		uses[k] = u
	}
	return uses, nil
}

// blocksReplica reports a session_records row that still references its accounts (E21-D062).
// pending always does. created with no ended_at does, so the clock cannot fall back to an
// older replica created_at before this pass writes ended_at. created with ended_at does only
// while the spec is still running or the STS region cannot be read. abandoned and closed do not.
func blocksReplica(state string, ended *time.Time, running bool) bool {
	switch state {
	case recPending:
		return true
	case recCreated:
		if ended == nil {
			return true
		}
		return running
	default:
		return false
	}
}

// sessionRunning is true when this pass cannot show that the spec is terminal (E21-D062).
// A missing region, a down region, or a missing view blocks deletion. A missing spec does not.
// A missing or unreadable status counts as non-terminal.
func (s *Server) sessionRunning(views map[string]state.View, down map[string]struct{}, sts, sessionID string) bool {
	region, err := s.cfg.Topology.RegionOf(sts)
	if err != nil {
		return true
	}
	if _, unavail := down[region]; unavail {
		return true
	}
	view, ok := views[region]
	if !ok {
		return true
	}
	if _, present := view.Entries[state.SessionSpecKey(sts, sessionID)]; !present {
		return false
	}
	return !terminalStatus(view, sts, sessionID)
}

// liveEtcdReplicas is the (td_instance, api_id) pairs named by a non-terminal spec
// in an available region (E21-D062). An unreadable spec is skipped. A missing or
// unreadable status counts as non-terminal.
func liveEtcdReplicas(views map[string]state.View) map[replicaKey]struct{} {
	out := map[replicaKey]struct{}{}
	for _, view := range views {
		for key, e := range view.Entries {
			inst, id, ok := specOf(key)
			if !ok {
				continue
			}
			var spec sessionv1.SessionSpec
			if proto.Unmarshal(e.Value, &spec) != nil {
				continue
			}
			if terminalStatus(view, inst, id) {
				continue
			}
			for _, a := range spec.GetAccounts() {
				if a.GetApiId() <= 0 || a.GetTdInstance() == "" {
					continue
				}
				out[replicaKey{inst: a.GetTdInstance(), id: a.GetApiId()}] = struct{}{}
			}
		}
	}
	return out
}

// removeReplica locks the account row, then the replica row, in one transaction
// (E21-D063). A missing replica row returns. A live account whose updated_at is
// still inside td_replica_grace is left alone, so a session that reused the replica
// after this pass read the in-use set is not deleted. Otherwise the etcd account
// is deleted and then the row (E21-D057). The test hook "replica-after-etcd-delete"
// runs between those two. A failure leaves the row so the next reconcile retries.
// There is no compensating write. Exclusion is the database row locks.
func (s *Server) removeReplica(ctx context.Context, inst string, id int64) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return storageErr(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var deleted *time.Time
	err = tx.QueryRow(ctx, `
SELECT deleted_at FROM accounts WHERE api_id = $1 FOR UPDATE`, id).Scan(&deleted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return storageErr(err)
	}
	var touched time.Time
	err = tx.QueryRow(ctx, `
SELECT updated_at FROM account_replicas
WHERE td_instance = $1 AND api_id = $2
FOR UPDATE`, inst, id).Scan(&touched)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return storageErr(err)
	}
	// Exactly the grace is still inside it, matching replicaDue (E21-D062, E21-D063).
	if deleted == nil && !touched.UTC().Before(s.now().UTC().Add(-s.cfg.replicaGrace())) {
		return nil
	}
	if err := s.deleteProjection(ctx, inst, id); err != nil {
		return err
	}
	if err := s.fireHook("replica-after-etcd-delete"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM account_replicas
WHERE td_instance = $1 AND api_id = $2`, inst, id); err != nil {
		return storageErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return storageErr(err)
	}
	return nil
}

// expireWillDelete reports whether this pass will try to remove a replica.
// The before-expire test hook runs only then, after the in-use read.
func (s *Server) expireWillDelete(rows []replicaView, uses map[replicaKey]replicaUse, usesErr error) bool {
	for i := range rows {
		row := rows[i]
		if row.Deleted == nil && (usesErr != nil || !s.replicaDue(row, uses)) {
			continue
		}
		region, err := s.cfg.Topology.RegionOf(row.Inst)
		if err != nil || !s.regionAvailable(region) {
			continue
		}
		return true
	}
	return false
}

// tryRemoveReplica deletes one replica when its TD region can be written.
// A down or unknown region leaves the row for a later pass and does not log.
func (s *Server) tryRemoveReplica(ctx context.Context, inst string, id int64) {
	region, err := s.cfg.Topology.RegionOf(inst)
	if err != nil || !s.regionAvailable(region) {
		return
	}
	if err := s.removeReplica(ctx, inst, id); err != nil && !s.quiet(err) {
		s.log.Error().Err(err).Int64("api_id", id).Str("td_instance", inst).Msg("delete account replica")
	}
}

// expireReplicas drops replicas that are no longer referenced and are past grace,
// and retries a soft-deleted account without waiting for grace (E21-D056, E21-D060).
// It runs after this pass has written session_records.ended_at. A failure to read
// sessions deletes nothing that still depends on grace. Soft-deleted rows are still removed.
func (s *Server) expireReplicas(ctx context.Context) {
	if err := ctx.Err(); err != nil || s.stopped() {
		return
	}
	rows, err := s.loadReplicaViews(ctx)
	if err != nil {
		if !s.quiet(err) {
			s.log.Error().Err(err).Msg("load replicas")
		}
		return
	}
	uses, usesErr := s.replicaUses(ctx)
	if usesErr != nil && !s.quiet(usesErr) {
		s.log.Error().Err(usesErr).Msg("load replica sessions")
	}
	// After the in-use read and before any delete, so a test can create a session
	// in that window (E21-D063). A pass that will not delete does not call the hook.
	if s.expireWillDelete(rows, uses, usesErr) {
		if err := s.fireHook("replica-before-expire"); err != nil {
			if !s.quiet(err) {
				s.log.Error().Err(err).Msg("replica expire hook")
			}
			return
		}
	}
	for i := range rows {
		row := rows[i]
		if row.Deleted != nil {
			s.tryRemoveReplica(ctx, row.Inst, row.ID)
			continue
		}
		if usesErr != nil {
			continue
		}
		if s.replicaDue(row, uses) {
			s.tryRemoveReplica(ctx, row.Inst, row.ID)
		}
	}
}
