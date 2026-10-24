package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Session history (E10-D031, E10-D068–E10-D073). The audit pool is the one pool (E10-D048): session queries are
// single statements, and readiness still does not look at Postgres.
const (
	recPending   = "pending"
	recCreated   = "created"
	recAbandoned = "abandoned"
	recClosed    = "closed"

	desRunning = "running"
	desEnded   = "ended"

	// endReasonSpec is what the API records when it sets desired_state to ended (E13-D003, N3).
	endReasonSpec = "ended_by_spec"

	// pendingTimeout is how long a pending row waits for its spec before it is abandoned (E10-D069).
	pendingTimeout = 2 * time.Minute
	// idempotencyTTL is how long an Idempotency-Key is retained (E10-D068). The partial unique index
	// cannot express "newer than 24 hours", so reconcile nulls the column when the row is older.
	idempotencyTTL = 24 * time.Hour

	// reconcileEveryDefault is the full compare interval (E10-D070). Mirror changes also run a pass.
	// Config.ReconcileEvery overrides it in tests; it is not a deployment setting.
	reconcileEveryDefault = 30 * time.Second
)

const (
	idxOpenSession = "session_records_open_session_id"
	idxIdempotency = "session_records_idempotency_key"
)

const recordCols = `
id::text, session_id, sts_instance, create_revision, spec_bytes, spec_sha256,
state, desired_state, end_reason, created_by, idempotency_key,
created_at, ended_at, closed_at, state_out_digest`

const insertRecordSQL = `
INSERT INTO session_records (
    id, session_id, sts_instance, create_revision, spec_bytes, spec_sha256,
    state, desired_state, created_by, idempotency_key, created_at
) VALUES (
    $1::uuid, $2, $3, NULL, $4, $5,
    'pending', 'running', $6, $7, $8
)`

// sessionRecord is one session_records row (E10-D073).
type sessionRecord struct {
	ID             string
	SessionID      string
	STS            string
	CreateRevision *int64
	Spec           []byte
	SHA            []byte
	State          string
	Desired        string
	EndReason      *string
	CreatedBy      *string
	IdempotencyKey *string
	CreatedAt      time.Time
	EndedAt        *time.Time
	ClosedAt       *time.Time
	StateOut       *string
}

func digestSpec(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func newRecordID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("api: session record id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func idempotencyExpired(created, now time.Time) bool {
	return !created.After(now.Add(-idempotencyTTL))
}

func (s *Server) db() (*pgxpool.Pool, error) {
	if s == nil || s.audit == nil || s.audit.pool == nil {
		return nil, errStorage
	}
	return s.audit.pool, nil
}

func storageErr(err error) error {
	if err == nil || errors.Is(err, errStorage) {
		return err
	}
	return fmt.Errorf("%w: %w", errStorage, err)
}

func pgConstraint(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return pgErr.ConstraintName, true
	}
	return "", false
}

func scanRecord(row pgx.Row) (sessionRecord, error) {
	var r sessionRecord
	err := row.Scan(
		&r.ID, &r.SessionID, &r.STS, &r.CreateRevision, &r.Spec, &r.SHA,
		&r.State, &r.Desired, &r.EndReason, &r.CreatedBy, &r.IdempotencyKey,
		&r.CreatedAt, &r.EndedAt, &r.ClosedAt, &r.StateOut,
	)
	return r, err
}

func (s *Server) recordByIdempotency(ctx context.Context, key string) (*sessionRecord, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	row := pool.QueryRow(ctx, `SELECT `+recordCols+` FROM session_records WHERE idempotency_key = $1`, key)
	rec, err := scanRecord(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, storageErr(err)
	}
	return &rec, nil
}

// idempotencyRow returns the live row for key. A key older than 24 hours is cleared and reported
// as absent, so the next insert may use it (E10-D068).
func (s *Server) idempotencyRow(ctx context.Context, key string) (*sessionRecord, error) {
	rec, err := s.recordByIdempotency(ctx, key)
	if err != nil || rec == nil {
		return rec, err
	}
	if !idempotencyExpired(rec.CreatedAt, s.now()) {
		return rec, nil
	}
	if err := s.clearIdempotency(ctx, rec.ID, key); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Server) clearIdempotency(ctx context.Context, id, key string) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	cutoff := s.now().UTC().Add(-idempotencyTTL)
	_, err = pool.Exec(ctx, `
UPDATE session_records SET idempotency_key = NULL
WHERE id = $1::uuid AND idempotency_key = $2 AND created_at <= $3`, id, key, cutoff)
	return storageErr(err)
}

func (s *Server) expireIdempotency(ctx context.Context, cutoff time.Time) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
UPDATE session_records SET idempotency_key = NULL
WHERE idempotency_key IS NOT NULL AND created_at <= $1`, cutoff)
	return storageErr(err)
}

// insertPending writes the row that must exist before the etcd create (E10-D031). A session_id that
// is already open is errConflict so the caller draws another. The same idempotency key is either
// errIdempotentReplay (same spec) or errIdempotencyConflict (different spec).
func (s *Server) insertPending(ctx context.Context, sessionID, sts string, spec, sha []byte, actor, key string) (sessionRecord, error) {
	var zero sessionRecord
	pool, err := s.db()
	if err != nil {
		return zero, err
	}
	recID, err := newRecordID()
	if err != nil {
		return zero, err
	}
	now := s.now().UTC()
	var actorPtr, keyPtr *string
	if actor != "" {
		actorPtr = &actor
	}
	if key != "" {
		keyPtr = &key
	}
	row := sessionRecord{
		ID: recID, SessionID: sessionID, STS: sts,
		Spec: append([]byte(nil), spec...), SHA: append([]byte(nil), sha...),
		State: recPending, Desired: desRunning,
		CreatedBy: actorPtr, IdempotencyKey: keyPtr, CreatedAt: now,
	}
	for range 2 {
		_, err = pool.Exec(ctx, insertRecordSQL, recID, sessionID, sts, spec, sha, actorPtr, keyPtr, now)
		if err == nil {
			return row, nil
		}
		name, unique := pgConstraint(err)
		if !unique {
			return zero, storageErr(err)
		}
		if name == idxOpenSession {
			return zero, errConflict
		}
		if name != idxIdempotency {
			return zero, storageErr(err)
		}
		existing, lerr := s.recordByIdempotency(ctx, key)
		if lerr != nil {
			return zero, lerr
		}
		if existing == nil {
			return zero, storageErr(err)
		}
		if !idempotencyExpired(existing.CreatedAt, s.now()) {
			if specContentEqual(existing.Spec, spec) {
				return *existing, errIdempotentReplay
			}
			return *existing, errIdempotencyConflict
		}
		if err := s.clearIdempotency(ctx, existing.ID, key); err != nil {
			return zero, err
		}
		recID, err = newRecordID()
		if err != nil {
			return zero, err
		}
		row.ID = recID
	}
	return zero, storageErr(errors.New("idempotency key stayed taken"))
}

func (s *Server) openSessionIDs(ctx context.Context) (map[string]struct{}, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT session_id FROM session_records WHERE state IN ('pending', 'created')`)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storageErr(err)
		}
		out[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

func (s *Server) recordsForSession(ctx context.Context, sessionID string) ([]sessionRecord, error) {
	return s.recordsWhere(ctx, `session_id = $1 AND state IN ('pending', 'created', 'abandoned')`, sessionID)
}

// recordsForSessionAll is every generation of sessionID, closed rows included (E10-D007, E10-D084).
func (s *Server) recordsForSessionAll(ctx context.Context, sessionID string) ([]sessionRecord, error) {
	return s.recordsWhere(ctx, `session_id = $1`, sessionID)
}

// recordsBySessionIDs loads the rows retention may close or re-delete. An empty id list
// does not touch Postgres.
func (s *Server) recordsBySessionIDs(ctx context.Context, ids []string) ([]sessionRecord, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return s.recordsWhere(ctx, `session_id = ANY($1)`, ids)
}

// recordByID is one session_records row. A missing row is errNotFound.
func (s *Server) recordByID(ctx context.Context, id string) (*sessionRecord, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	row := pool.QueryRow(ctx, `SELECT `+recordCols+` FROM session_records WHERE id = $1::uuid`, id)
	rec, err := scanRecord(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, storageErr(err)
	}
	return &rec, nil
}

// recordPage is the /session-records query. keep drops rows the SQL cannot
// express (the strategy id); the cursor is the last row the caller returns,
// so skipped rows do not end the page early (E10-D097).
type recordPage struct {
	limit                int
	sessionID, createdBy *string
	after, before        *time.Time
	cursorAt             *time.Time
	cursorID             *string
	keep                 func(sessionRecord) bool
}

func (s *Server) pageSessionRecords(ctx context.Context, q recordPage) ([]sessionRecord, bool, error) {
	pool, err := s.db()
	if err != nil {
		return nil, false, err
	}
	rows, err := pool.Query(ctx, `
SELECT `+recordCols+`
FROM session_records
WHERE ($1::text IS NULL OR session_id = $1)
  AND ($2::text IS NULL OR created_by = $2)
  AND ($3::timestamptz IS NULL OR created_at > $3)
  AND ($4::timestamptz IS NULL OR created_at < $4)
  AND (
    $5::timestamptz IS NULL
    OR created_at < $5
    OR (created_at = $5 AND id < $6::uuid)
  )
ORDER BY created_at DESC, id DESC`,
		q.sessionID, q.createdBy, q.after, q.before, q.cursorAt, q.cursorID)
	if err != nil {
		return nil, false, storageErr(err)
	}
	defer rows.Close()
	var out []sessionRecord
	more := false
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, false, storageErr(err)
		}
		if q.keep != nil && !q.keep(rec) {
			continue
		}
		if q.limit > 0 && len(out) == q.limit {
			more = true
			break
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, false, storageErr(err)
	}
	if out == nil {
		out = []sessionRecord{}
	}
	return out, more, nil
}

func (s *Server) recordsWhere(ctx context.Context, where string, arg any) ([]sessionRecord, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT `+recordCols+` FROM session_records WHERE `+where, arg)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var out []sessionRecord
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, storageErr(err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

func (s *Server) loadLiveRecords(ctx context.Context) ([]sessionRecord, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT `+recordCols+`
FROM session_records
WHERE state IN ('pending', 'created', 'abandoned')`)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var out []sessionRecord
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, storageErr(err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

func (s *Server) deletePending(ctx context.Context, id string) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
DELETE FROM session_records
WHERE id = $1::uuid AND state = 'pending' AND create_revision IS NULL`, id)
	return storageErr(err)
}

func (s *Server) abandonPending(ctx context.Context, id string) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
UPDATE session_records SET state = 'abandoned'
WHERE id = $1::uuid AND state = 'pending' AND create_revision IS NULL`, id)
	return storageErr(err)
}

// markCreated stores the spec's create_revision and, when the bytes changed only because
// desired_state did, the etcd bytes (E10-D031, E10-D071). A unique violation is returned as-is so the
// caller can leave the row and not open another one (E10-D069).
func (s *Server) markCreated(ctx context.Context, row *sessionRecord, createRev int64, spec []byte) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	sha := digestSpec(spec)
	desired := desiredOfSpec(spec)
	var ended *time.Time
	reason := ""
	if desired == desEnded {
		reason = endReasonSpec
		if row.EndReason != nil && *row.EndReason != "" {
			reason = *row.EndReason
		}
		if row.EndedAt != nil {
			ended = row.EndedAt
		} else {
			t := s.now().UTC()
			ended = &t
		}
	}
	tag, err := pool.Exec(ctx, `
UPDATE session_records SET
    state = 'created',
    create_revision = $2,
    spec_bytes = $3,
    spec_sha256 = $4,
    desired_state = $5,
    end_reason = COALESCE(end_reason, NULLIF($6, '')),
    ended_at = COALESCE(ended_at, $7)
WHERE id = $1::uuid
  AND state IN ('pending', 'created', 'abandoned')
  AND (create_revision IS NULL OR create_revision = $2)`,
		row.ID, createRev, spec, sha, desired, reason, ended)
	if err != nil {
		if _, ok := pgConstraint(err); ok {
			return err
		}
		return storageErr(err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("session record was not updated")
	}
	row.State = recCreated
	row.CreateRevision = &createRev
	row.Spec = append([]byte(nil), spec...)
	row.SHA = sha
	row.Desired = desired
	if desired == desEnded {
		if row.EndReason == nil {
			r := reason
			row.EndReason = &r
		}
		row.EndedAt = ended
	}
	return nil
}

// syncDesired updates desired_state and the terminal columns without replacing spec bytes (E10-D071).
func (s *Server) syncDesired(ctx context.Context, row sessionRecord, desired, reason string, ended *time.Time) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
UPDATE session_records SET
    desired_state = $2,
    end_reason = COALESCE(end_reason, NULLIF($3, '')),
    ended_at = COALESCE(ended_at, $4)
WHERE id = $1::uuid
  AND state IN ('pending', 'created', 'abandoned')`,
		row.ID, desired, reason, ended)
	return storageErr(err)
}

// closeRecord writes the last status and closes the row (E10-D031, E10-D082). Spec bytes stay as they
// are. A row that is already closed is updated in place when a later status carries state_out.
// createRev is the spec's create_revision, or nil when the spec is already gone.
func (s *Server) closeRecord(ctx context.Context, row *sessionRecord, reason, digest string, ended time.Time, createRev *int64) error {
	pool, err := s.db()
	if err != nil {
		return err
	}
	closed := s.now().UTC()
	tag, err := pool.Exec(ctx, `
UPDATE session_records SET
    state = 'closed',
    closed_at = COALESCE(closed_at, $2),
    ended_at = COALESCE(ended_at, $3),
    end_reason = COALESCE(end_reason, NULLIF($4, '')),
    state_out_digest = COALESCE(NULLIF($5, ''), state_out_digest),
    create_revision = COALESCE(create_revision, $6)
WHERE id = $1::uuid
  AND state IN ('pending', 'created', 'abandoned', 'closed')
  AND ($6::bigint IS NULL OR create_revision IS NULL OR create_revision = $6)`,
		row.ID, closed, ended.UTC(), reason, digest, createRev)
	if err != nil {
		return storageErr(err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("session record was not closed")
	}
	row.State = recClosed
	if row.ClosedAt == nil {
		row.ClosedAt = &closed
	}
	if row.EndedAt == nil {
		t := ended.UTC()
		row.EndedAt = &t
	}
	if row.EndReason == nil && reason != "" {
		r := reason
		row.EndReason = &r
	}
	if digest != "" {
		d := digest
		if row.StateOut == nil {
			row.StateOut = &d
		}
	}
	if createRev != nil && row.CreateRevision == nil {
		v := *createRev
		row.CreateRevision = &v
	}
	return nil
}
