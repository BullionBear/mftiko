package api

import (
	"context"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/accountkey"
)

// Re-encryption limits (E21-D032). A batch that comes back short ends the pass.
// A full batch means more rows may be waiting, including rows this pass did not lock.
// The pass stops on the number of rows selected, not the number rewritten: rows that
// will not open must not stall a later good row that sorts after them.
const (
	reencryptBatch      = 16
	reencryptMaxBatches = 8
	reencryptIdle       = 5 * time.Second
	reencryptBusy       = 200 * time.Millisecond
)

// errNoCurrentKey means this process has no encryption key, so it must not report zero old rows.
var errNoCurrentKey = errors.New("re-encrypt account credentials: no current key")

const reencryptSelectSQL = `
SELECT api_id, kind, credentials, key_version
FROM accounts
WHERE key_version <> $1
  AND key_version = ANY($2::bigint[])
  AND NOT (api_id = ANY($3::bigint[]))
ORDER BY api_id
LIMIT $4
FOR UPDATE SKIP LOCKED`

const reencryptUpdateSQL = `
UPDATE accounts
SET credentials = $1, key_version = $2
WHERE api_id = $3 AND key_version = $4`

const reencryptCountSQL = `
SELECT key_version, count(*)::bigint
FROM accounts
WHERE key_version <> $1
GROUP BY key_version
ORDER BY key_version`

// credentialRewriter rewrites credentials onto keys.Current. skip is the api_ids this
// process will not lock again. One rewriter is one replica; skip is not safe to share.
type credentialRewriter struct {
	pool *pgxpool.Pool
	keys accountkey.Box
	page int
	log  zerolog.Logger
	skip map[int64]struct{}
}

type reencryptTick struct {
	rewritten int
	remaining int64
	more      bool
	old       string
}

type reencryptPoison struct {
	id     int64
	ver    int64
	reason string
}

type reencryptRow struct {
	id   int64
	kind string
	ct   []byte
	ver  int64
}

func newCredentialRewriter(pool *pgxpool.Pool, keys accountkey.Box, log zerolog.Logger) *credentialRewriter {
	return &credentialRewriter{
		pool: pool,
		keys: keys,
		page: reencryptBatch,
		log:  log,
		skip: map[int64]struct{}{},
	}
}

func (w *credentialRewriter) limit() int {
	if w.page < 1 {
		return reencryptBatch
	}
	return w.page
}

// knownOldVersions is every configured version except the one Seal uses.
// The empty result is a non-nil slice: a nil slice is SQL NULL, and ANY(NULL) matches nothing
// in a way that also rejects NOT (api_id = ANY(NULL)).
func knownOldVersions(keys accountkey.Box) []int64 {
	cur := keys.Current()
	out := make([]int64, 0, len(keys.Versions()))
	for _, v := range keys.Versions() {
		if v != cur {
			out = append(out, int64(v))
		}
	}
	return out
}

func (w *credentialRewriter) skipped() []int64 {
	out := make([]int64, 0, len(w.skip))
	for id := range w.skip {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// tick rewrites up to reencryptMaxBatches batches, then counts rows still off this process's
// current key. A failed batch rolls back and is not reported as rewritten. An earlier batch in
// this tick may already be committed.
func (w *credentialRewriter) tick(ctx context.Context) (reencryptTick, error) {
	current := w.keys.Current()
	if current == 0 {
		return reencryptTick{}, errNoCurrentKey
	}
	known := knownOldVersions(w.keys)
	var out reencryptTick
	for i := 0; i < reencryptMaxBatches; i++ {
		selected, ids, err := w.batch(ctx, current, known)
		if err != nil {
			return reencryptTick{}, err
		}
		out.rewritten += len(ids)
		if selected < w.limit() {
			out.more = false
			break
		}
		out.more = true
	}
	rem, old, err := accountKeyOldDetail(ctx, w.pool, current)
	if err != nil {
		return reencryptTick{}, err
	}
	out.remaining = rem
	out.old = old
	return out, nil
}

// batch locks one page, re-seals what it can, and commits the rewrites together with the
// decision to skip rows that will not open. ids is empty unless the commit succeeds.
func (w *credentialRewriter) batch(ctx context.Context, current uint32, known []int64) (selected int, ids []int64, err error) {
	if known == nil {
		known = []int64{}
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	rows, err := tx.Query(ctx, reencryptSelectSQL, int64(current), known, w.skipped(), w.limit())
	if err != nil {
		return 0, nil, err
	}
	got, err := scanReencryptRows(rows)
	rows.Close()
	if err != nil {
		return 0, nil, err
	}

	type sealed struct {
		id  int64
		old int64
		ct  []byte
	}
	var updates []sealed
	var poisons []reencryptPoison
	for _, row := range got {
		plain, oerr := w.keys.Open(row.id, row.kind, row.ct)
		if oerr != nil {
			if reason, ok := poisonReason(oerr); ok {
				poisons = append(poisons, reencryptPoison{id: row.id, ver: row.ver, reason: reason})
				continue
			}
			return 0, nil, oerr
		}
		next, serr := w.keys.Seal(row.id, row.kind, plain)
		clear(plain)
		if serr != nil {
			return 0, nil, serr
		}
		updates = append(updates, sealed{id: row.id, old: row.ver, ct: next})
	}

	rewritten := make([]int64, 0, len(updates))
	for _, u := range updates {
		tag, err := tx.Exec(ctx, reencryptUpdateSQL, u.ct, int64(current), u.id, u.old)
		if err != nil {
			return 0, nil, err
		}
		if tag.RowsAffected() > 0 {
			rewritten = append(rewritten, u.id)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, err
	}
	for _, p := range poisons {
		w.notePoison(p)
	}
	return len(got), rewritten, nil
}

func scanReencryptRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) ([]reencryptRow, error) {
	var out []reencryptRow
	for rows.Next() {
		var row reencryptRow
		var ct []byte
		if err := rows.Scan(&row.id, &row.kind, &ct, &row.ver); err != nil {
			return nil, err
		}
		// pgx may reuse the scan buffer on the next row.
		row.ct = append([]byte(nil), ct...)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *credentialRewriter) notePoison(p reencryptPoison) {
	if _, ok := w.skip[p.id]; ok {
		return
	}
	w.skip[p.id] = struct{}{}
	w.log.Warn().Int64("api_id", p.id).Int64("key_version", p.ver).Str("reason", p.reason).
		Msg("account credentials were not re-encrypted")
}

func poisonReason(err error) (string, bool) {
	switch {
	case errors.Is(err, accountkey.ErrUnknownVersion):
		return "unknown key version", true
	case errors.Is(err, accountkey.ErrUnauthentic):
		return "ciphertext does not open", true
	case errors.Is(err, accountkey.ErrMalformed):
		return "ciphertext is unreadable", true
	default:
		return "", false
	}
}

// accountKeyOldDetail counts rows whose key_version is not current, including soft-deleted rows
// and rows this process cannot open. The string is ver=count ordered by key_version.
func accountKeyOldDetail(ctx context.Context, pool *pgxpool.Pool, current uint32) (int64, string, error) {
	if current == 0 {
		return 0, "", errNoCurrentKey
	}
	if pool == nil {
		return 0, "", errStorage
	}
	rows, err := pool.Query(ctx, reencryptCountSQL, int64(current))
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	var b strings.Builder
	var total int64
	first := true
	for rows.Next() {
		var ver, n int64
		if err := rows.Scan(&ver, &n); err != nil {
			return 0, "", err
		}
		if n < 0 || total > math.MaxInt64-n {
			return 0, "", errors.New("re-encrypt account credentials: count overflow")
		}
		total += n
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(strconv.FormatInt(ver, 10))
		b.WriteByte('=')
		b.WriteString(strconv.FormatInt(n, 10))
	}
	if err := rows.Err(); err != nil {
		return 0, "", err
	}
	return total, b.String(), nil
}

// accountKeyOldCount is the gauge value. ok is false when the count must not be reported,
// including a missing current key. A missing sample is not zero (E21-D034).
func accountKeyOldCount(ctx context.Context, pool *pgxpool.Pool, current uint32) (int64, bool) {
	n, _, err := accountKeyOldDetail(ctx, pool, current)
	if err != nil || n < 0 || n > int64(math.MaxInt) {
		return 0, false
	}
	return n, true
}

// accountKeyOldSample is the scrape value. Negative means the sample is omitted.
func accountKeyOldSample(ctx context.Context, s *Server) int {
	pool, err := s.db()
	if err != nil {
		return -1
	}
	n, ok := accountKeyOldCount(ctx, pool, s.cfg.AccountKeys.Current())
	if !ok {
		return -1
	}
	return int(n)
}

// runReencrypt rewrites credentials that are not on the current account key.
// Every replica runs it. There is no leader (E21-D026, E21-D032).
// stop cancels s.ctx and then closes the pool before waiting, so a closed pool is a quiet exit.
func (s *Server) runReencrypt() {
	defer s.wg.Done()
	pool, err := s.db()
	if err != nil {
		s.log.Error().Err(err).Msg("re-encrypt account credentials")
		return
	}
	if s.cfg.AccountKeys.Current() == 0 {
		s.log.Error().Msg(errNoCurrentKey.Error())
		return
	}
	w := newCredentialRewriter(pool, s.cfg.AccountKeys, s.log)
	var last, lastErr string
	for {
		if s.ctx.Err() != nil {
			return
		}
		tick, err := w.tick(s.ctx)
		if err != nil {
			if s.quiet(err) {
				return
			}
			msg := err.Error()
			if msg != lastErr {
				s.log.Error().Err(err).Msg("re-encrypt account credentials")
				lastErr = msg
			}
			if !s.sleep(reencryptIdle) {
				return
			}
			continue
		}
		lastErr = ""
		sig := strings.Join([]string{
			strconv.FormatInt(tick.remaining, 10),
			strconv.Itoa(tick.rewritten),
			tick.old,
		}, "|")
		if sig != last {
			ev := s.log.Info().Uint32("current", s.cfg.AccountKeys.Current()).
				Int64("remaining", tick.remaining).Int("rewritten", tick.rewritten)
			if tick.old != "" {
				ev = ev.Str("old", tick.old)
			}
			ev.Msg("account key re-encrypt")
			last = sig
		}
		wait := reencryptIdle
		if tick.more {
			wait = reencryptBusy
		}
		if !s.sleep(wait) {
			return
		}
	}
}
