// Applying migrations (E12-D027–E12-D030). mftiko-migrate is the only caller. Agents do not migrate.
//
// Up reads the embedded *.up.sql files, in numeric order, and skips ones already recorded in
// schema_migrations. Each file and its row commit together. A session advisory lock keeps two
// hosts from applying at once. Down files are not applied; tests execute them themselves.
package migrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// LockName is the advisory-lock key. Up takes pg_advisory_lock(hashtext(LockName), 1)
// for the whole run and unlocks when it returns. A second Up waits (E12-D029).
const LockName = "mftiko-migrate"

const (
	LockStatement   = "SELECT pg_advisory_lock(hashtext('" + LockName + "'), 1)"
	UnlockStatement = "SELECT pg_advisory_unlock(hashtext('" + LockName + "'), 1)"
)

// Schema is the record table. Up creates it. It is not itself a numbered migration.
const Schema = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
)`

// State is how one migration stands relative to schema_migrations.
type State string

const (
	StateApplied State = "applied"
	StatePending State = "pending"
	StateChanged State = "changed"
	StateMissing State = "missing"
)

// Row is one migration, embedded or only recorded.
type Row struct {
	Version      int
	Name         string
	RecordedName string
	State        State
	AppliedAt    time.Time
	RecordedSHA  string
	FileSHA      string
}

// String is one status line: "0001 history applied <time> <sha>", "0002 symbol pending",
// "0001 history changed recorded <sha> file <sha>", or "0009 oldname missing".
func (s Row) String() string {
	label := fmt.Sprintf("%04d %s", s.Version, s.Name)
	switch s.State {
	case StateApplied:
		return fmt.Sprintf("%s applied %s %s", label, s.AppliedAt.UTC().Format(time.RFC3339Nano), s.RecordedSHA)
	case StatePending:
		return label + " pending"
	case StateChanged:
		if s.RecordedName != "" && s.RecordedName != s.Name {
			return fmt.Sprintf("%s changed recorded-name %s recorded %s file %s", label, s.RecordedName, s.RecordedSHA, s.FileSHA)
		}
		return fmt.Sprintf("%s changed recorded %s file %s", label, s.RecordedSHA, s.FileSHA)
	case StateMissing:
		return label + " missing"
	default:
		return label + " " + string(s.State)
	}
}

// Applied is one migration Up committed on this call.
type Applied struct {
	Version int
	Name    string
}

// String is the file stem, "0001_history".
func (a Applied) String() string {
	return fmt.Sprintf("%04d_%s", a.Version, a.Name)
}

// Result is what Up applied. Applied is empty when the database was already current.
type Result struct {
	Applied []Applied
}

// DriftError means a recorded migration does not match the embedded file, or the file is gone.
// Up stops before applying anything new (E12-D030).
type DriftError struct {
	Version      int
	Name         string
	RecordedName string
	State        State
	RecordedSHA  string
	FileSHA      string
}

func (e *DriftError) Error() string {
	switch e.State {
	case StateMissing:
		return fmt.Sprintf("schema_migrations version %d (%s) has no migration file", e.Version, e.Name)
	default:
		if e.RecordedName != "" && e.RecordedName != e.Name {
			return fmt.Sprintf("migration %04d_%s does not match schema_migrations (recorded name %s, recorded sha256 %s, file sha256 %s)", e.Version, e.Name, e.RecordedName, e.RecordedSHA, e.FileSHA)
		}
		return fmt.Sprintf("migration %04d_%s changed: recorded sha256 %s, file sha256 %s", e.Version, e.Name, e.RecordedSHA, e.FileSHA)
	}
}

// file is one *.up.sql.
type file struct {
	Version int
	Name    string
	Body    []byte
	SHA256  string
}

func (f file) stem() string {
	return fmt.Sprintf("%04d_%s", f.Version, f.Name)
}

var (
	fileRE = regexp.MustCompile(`^([0-9]+)_([a-z0-9_]+)\.(up|down)\.sql$`)
	nameRE = regexp.MustCompile(`^[a-z0-9_]+$`)
	shaRE  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Up applies every embedded up migration that schema_migrations does not yet record.
func Up(ctx context.Context, dsn string) (Result, error) {
	files, err := load()
	if err != nil {
		return Result{}, err
	}
	return applyFiles(ctx, dsn, files)
}

// Status reports every embedded up migration and any recorded version that has no file.
// It does not take the advisory lock. A non-nil DriftError means the lines are complete
// and at least one of them is changed or missing.
func Status(ctx context.Context, dsn string) ([]Row, error) {
	files, err := load()
	if err != nil {
		return nil, err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	recorded, err := readRecorded(ctx, conn)
	if err != nil {
		return nil, err
	}
	lines, drift := compare(files, recorded)
	if drift != nil {
		return lines, drift
	}
	return lines, nil
}

func applyFiles(ctx context.Context, dsn string, files []file) (Result, error) {
	if err := validate(files); err != nil {
		return Result{}, err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, LockStatement); err != nil {
		return Result{}, fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), UnlockStatement)
	}()

	if _, err := conn.Exec(ctx, Schema); err != nil {
		return Result{}, fmt.Errorf("schema_migrations: %w", err)
	}
	recorded, err := readRecorded(ctx, conn)
	if err != nil {
		return Result{}, err
	}
	if _, drift := compare(files, recorded); drift != nil {
		return Result{}, drift
	}
	have := map[int]struct{}{}
	for _, rec := range recorded {
		have[rec.version] = struct{}{}
	}

	var applied []Applied
	for _, f := range files {
		if _, ok := have[f.Version]; ok {
			continue
		}
		if err := applyOne(ctx, conn, f); err != nil {
			return Result{Applied: applied}, err
		}
		applied = append(applied, Applied{Version: f.Version, Name: f.Name})
	}
	return Result{Applied: applied}, nil
}

// applyOne runs the file and inserts its row in one transaction. On failure the transaction
// is rolled back and the connection is left usable so the advisory lock can be released.
func applyOne(ctx context.Context, conn *pgx.Conn, f file) error {
	var b strings.Builder
	b.WriteString("BEGIN;\n")
	b.Write(f.Body)
	if len(f.Body) == 0 || f.Body[len(f.Body)-1] != '\n' {
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "INSERT INTO schema_migrations (version, name, sha256, applied_at) VALUES (%d, '%s', '%s', now());\nCOMMIT;\n",
		f.Version, f.Name, f.SHA256)
	// Simple query protocol: a migration file is more than one statement, which the extended
	// protocol refuses. The whole script is one transaction, so a later statement's failure
	// rolls the file back with its row (E12-D029).
	if err := conn.PgConn().Exec(ctx, b.String()).Close(); err != nil {
		_ = conn.PgConn().Exec(context.Background(), "ROLLBACK").Close()
		return fmt.Errorf("migration %s: %w", f.stem(), err)
	}
	return nil
}

type record struct {
	version   int
	name      string
	sha       string
	appliedAt time.Time
}

func readRecorded(ctx context.Context, conn *pgx.Conn) ([]record, error) {
	var rel *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('schema_migrations')::text`).Scan(&rel); err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, nil
	}
	rows, err := conn.Query(ctx, `SELECT version, name, sha256, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []record
	for rows.Next() {
		var rec record
		if err := rows.Scan(&rec.version, &rec.name, &rec.sha, &rec.appliedAt); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// compare checks every recorded row against the files before any new file is applied.
// A recorded version with no file, a different name, or a different sha256 is drift.
// sha256 is compared case-insensitively so a manual insert may use either hex case.
func compare(files []file, recorded []record) ([]Row, error) {
	byVersion := make(map[int]file, len(files))
	for _, f := range files {
		byVersion[f.Version] = f
	}
	recBy := make(map[int]record, len(recorded))
	for _, rec := range recorded {
		recBy[rec.version] = rec
	}
	versions := make([]int, 0, len(files)+len(recorded))
	seen := map[int]struct{}{}
	for _, f := range files {
		if _, ok := seen[f.Version]; ok {
			continue
		}
		seen[f.Version] = struct{}{}
		versions = append(versions, f.Version)
	}
	for _, rec := range recorded {
		if _, ok := seen[rec.version]; ok {
			continue
		}
		seen[rec.version] = struct{}{}
		versions = append(versions, rec.version)
	}
	sort.Ints(versions)

	var lines []Row
	var drift error
	for _, v := range versions {
		f, hasFile := byVersion[v]
		rec, hasRec := recBy[v]
		switch {
		case hasFile && hasRec && f.Name == rec.name && strings.EqualFold(f.SHA256, rec.sha):
			lines = append(lines, Row{
				Version: v, Name: f.Name, RecordedName: rec.name, State: StateApplied,
				AppliedAt: rec.appliedAt, RecordedSHA: rec.sha, FileSHA: f.SHA256,
			})
		case hasFile && hasRec:
			lines = append(lines, Row{
				Version: v, Name: f.Name, RecordedName: rec.name, State: StateChanged,
				AppliedAt: rec.appliedAt, RecordedSHA: rec.sha, FileSHA: f.SHA256,
			})
			if drift == nil {
				drift = &DriftError{
					Version: v, Name: f.Name, RecordedName: rec.name, State: StateChanged,
					RecordedSHA: rec.sha, FileSHA: f.SHA256,
				}
			}
		case hasFile:
			lines = append(lines, Row{Version: v, Name: f.Name, State: StatePending, FileSHA: f.SHA256})
		default:
			lines = append(lines, Row{
				Version: v, Name: rec.name, RecordedName: rec.name, State: StateMissing,
				AppliedAt: rec.appliedAt, RecordedSHA: rec.sha,
			})
			if drift == nil {
				drift = &DriftError{Version: v, Name: rec.name, RecordedName: rec.name, State: StateMissing, RecordedSHA: rec.sha}
			}
		}
	}
	return lines, drift
}

func load() ([]file, error) {
	ents, err := fs.ReadDir(FS, ".")
	if err != nil {
		return nil, err
	}
	var files []file
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		ver, name, kind, ok := parseFile(e.Name())
		if !ok {
			return nil, fmt.Errorf("migrations: unexpected file %s", e.Name())
		}
		if kind == "down" {
			continue
		}
		body, err := FS.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		f, err := newFile(ver, name, body)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Version < files[j].Version })
	if err := validate(files); err != nil {
		return nil, err
	}
	return files, nil
}

func parseFile(name string) (version int, stem, kind string, ok bool) {
	m := fileRE.FindStringSubmatch(name)
	if m == nil {
		return 0, "", "", false
	}
	v, err := strconv.Atoi(m[1])
	if err != nil || v < 1 {
		return 0, "", "", false
	}
	return v, m[2], m[3], true
}

func newFile(version int, name string, body []byte) (file, error) {
	if version < 1 || !nameRE.MatchString(name) {
		return file{}, fmt.Errorf("migrations: bad migration %d %q", version, name)
	}
	sum := sha256.Sum256(body)
	return file{Version: version, Name: name, Body: body, SHA256: hex.EncodeToString(sum[:])}, nil
}

func validate(files []file) error {
	seen := map[int]string{}
	for _, f := range files {
		if prev, ok := seen[f.Version]; ok {
			return fmt.Errorf("migrations: duplicate version %d (%s and %s)", f.Version, prev, f.stem())
		}
		seen[f.Version] = f.stem()
		if !nameRE.MatchString(f.Name) || !shaRE.MatchString(f.SHA256) {
			return fmt.Errorf("migrations: bad migration %s", f.stem())
		}
	}
	return nil
}
