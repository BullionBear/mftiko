package migrations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

func TestMain(m *testing.M) {
	if os.Getenv("MFTIKO_MIGRATE_CHILD") == "1" {
		os.Exit(child())
	}
	os.Exit(m.Run())
}

func child() int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	files, err := filesFromDir(os.Getenv("MFTIKO_MIGRATE_DIR"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if _, err := applyFiles(ctx, os.Getenv("DATABASE_URL"), files); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func filesFromDir(dir string) ([]file, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []file
	for _, e := range ents {
		ver, name, kind, ok := parseFile(e.Name())
		if !ok || kind != "up" {
			return nil, fmt.Errorf("migrations: unexpected file %s", e.Name())
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
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
	return files, validate(files)
}

func TestLoadEmbedded(t *testing.T) {
	files, err := load()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"0001_history",
		"0002_symbol",
		"0003_audit",
		"0004_session_records",
		"0005_session_records_final",
		"0006_retired_instances",
	}
	if len(files) != len(want) {
		t.Fatalf("loaded %d files, want %d", len(files), len(want))
	}
	for i, f := range files {
		if f.stem() != want[i] || f.Version != i+1 {
			t.Fatalf("file %d = %s version %d", i, f.stem(), f.Version)
		}
		if len(f.SHA256) != 64 {
			t.Fatalf("%s sha256 %q", f.stem(), f.SHA256)
		}
	}
}

func TestParseFileRejects(t *testing.T) {
	ver, name, kind, ok := parseFile("0001_history.down.sql")
	if !ok || ver != 1 || name != "history" || kind != "down" {
		t.Fatalf("down file parsed as %d %s %s %v", ver, name, kind, ok)
	}
	for _, name := range []string{"README.sql", "0001_Bad.up.sql", "0000_x.up.sql", "notes.up.sql", "0001_.up.sql", "0001_a-b.up.sql"} {
		if _, _, _, ok := parseFile(name); ok {
			t.Errorf("accepted %s", name)
		}
	}
}

func TestDuplicateVersionRejected(t *testing.T) {
	a := mustFile(t, 1, "a", "SELECT 1;\n")
	b := mustFile(t, 1, "b", "SELECT 1;\n")
	_, err := applyFiles(context.Background(), "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable", []file{a, b})
	if err == nil || !strings.Contains(err.Error(), "duplicate version 1") {
		t.Fatal(err)
	}
}

func TestUpEmptyAndRerun(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := testCtx(t)
	res, err := Up(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"0001_history",
		"0002_symbol",
		"0003_audit",
		"0004_session_records",
		"0005_session_records_final",
		"0006_retired_instances",
	}
	if len(res.Applied) != len(want) {
		t.Fatalf("applied %v", res.Applied)
	}
	for i, a := range res.Applied {
		if a.String() != want[i] {
			t.Fatalf("applied[%d] = %s, want %s", i, a, want[i])
		}
	}
	conn := connect(t, srv.DSN())
	for _, table := range []string{"orders", "fills", "symbol_active", "audit", "session_records", "retired_instances", "schema_migrations"} {
		if !regclass(t, ctx, conn, table) {
			t.Fatalf("up did not create %s", table)
		}
	}
	var col string
	if err := conn.QueryRow(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'session_records' AND column_name = 'final_status'`).Scan(&col); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("schema_migrations rows = %d", n)
	}
	before := appliedAt(t, ctx, conn)

	res, err = Up(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("rerun applied %v", res.Applied)
	}
	if after := appliedAt(t, ctx, conn); !sameTimes(before, after) {
		t.Fatalf("rerun changed applied_at\n before %v\n after  %v", before, after)
	}
	lines, err := Status(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 6 {
		t.Fatalf("status %d lines", len(lines))
	}
	for _, line := range lines {
		if line.State != StateApplied {
			t.Fatalf("status %s", line)
		}
	}
}

func TestUpRollsBackFailedFile(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := testCtx(t)
	one := mustFile(t, 1, "one", "CREATE TABLE one (id integer);\n")
	bad := mustFile(t, 2, "two", "CREATE TABLE two (id integer);\nINSERT INTO missing_table (id) VALUES (1);\n")
	if _, err := applyFiles(ctx, srv.DSN(), []file{one}); err != nil {
		t.Fatal(err)
	}
	res, err := applyFiles(ctx, srv.DSN(), []file{one, bad})
	if err == nil {
		t.Fatal("bad migration succeeded")
	}
	if len(res.Applied) != 0 {
		t.Fatalf("failed run reported %v", res.Applied)
	}
	conn := connect(t, srv.DSN())
	if !regclass(t, ctx, conn, "one") {
		t.Fatal("rolled back the earlier migration")
	}
	if regclass(t, ctx, conn, "two") {
		t.Fatal("failed file left its table")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("schema_migrations rows = %d", n)
	}
	good := mustFile(t, 2, "two", "CREATE TABLE two (id integer);\n")
	res, err = applyFiles(ctx, srv.DSN(), []file{one, good})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != 1 || res.Applied[0].String() != "0002_two" {
		t.Fatalf("retry applied %v", res.Applied)
	}
	if !regclass(t, ctx, conn, "two") {
		t.Fatal("retry did not create two")
	}
}

func TestUpStopsWhenFileChanged(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := testCtx(t)
	orig := mustFile(t, 1, "one", "CREATE TABLE one (id integer);\n")
	if _, err := applyFiles(ctx, srv.DSN(), []file{orig}); err != nil {
		t.Fatal(err)
	}
	changed := mustFile(t, 1, "one", "CREATE TABLE one (id integer);\nCREATE TABLE extra (id integer);\n")
	next := mustFile(t, 2, "two", "CREATE TABLE two (id integer);\n")
	_, err := applyFiles(ctx, srv.DSN(), []file{changed, next})
	var drift *DriftError
	if !errors.As(err, &drift) || drift.State != StateChanged || drift.Version != 1 {
		t.Fatalf("err %#v %v", drift, err)
	}
	if !strings.Contains(err.Error(), "changed") {
		t.Fatal(err)
	}
	conn := connect(t, srv.DSN())
	if !regclass(t, ctx, conn, "one") || regclass(t, ctx, conn, "extra") || regclass(t, ctx, conn, "two") {
		t.Fatal("changed file was applied or the original table is gone")
	}
	lines, driftErr := compare([]file{changed, next}, mustRecords(t, ctx, conn))
	if driftErr == nil {
		t.Fatal("status did not report drift")
	}
	if len(lines) < 1 || lines[0].State != StateChanged || !strings.Contains(lines[0].String(), "changed") {
		t.Fatalf("lines %v err %v", lines, driftErr)
	}
}

func TestUpAcceptsUppercaseSHA(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := testCtx(t)
	one := mustFile(t, 1, "one", "CREATE TABLE one (id integer);\n")
	if _, err := applyFiles(ctx, srv.DSN(), []file{one}); err != nil {
		t.Fatal(err)
	}
	conn := connect(t, srv.DSN())
	if _, err := conn.Exec(ctx, `UPDATE schema_migrations SET sha256 = upper(sha256)`); err != nil {
		t.Fatal(err)
	}
	res, err := applyFiles(ctx, srv.DSN(), []file{one})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("uppercase sha was applied again: %v", res.Applied)
	}
}

func TestUpStopsWhenRecordHasNoFile(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := testCtx(t)
	conn := connect(t, srv.DSN())
	if _, err := conn.Exec(ctx, Schema); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO schema_migrations (version, name, sha256, applied_at)
		VALUES (9, 'gone', $1, now())`, strings.Repeat("ab", 32)); err != nil {
		t.Fatal(err)
	}
	one := mustFile(t, 1, "one", "CREATE TABLE one (id integer);\n")
	_, err := applyFiles(ctx, srv.DSN(), []file{one})
	var drift *DriftError
	if !errors.As(err, &drift) || drift.State != StateMissing || drift.Version != 9 {
		t.Fatalf("err %v", err)
	}
	if regclass(t, ctx, conn, "one") {
		t.Fatal("applied a new file after a missing record")
	}
}

func TestUpOneAtATime(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := testCtx(t)
	dir := t.TempDir()
	body := "SELECT pg_advisory_lock(hashtext('mftiko-migrate-gate'), 1);\n" +
		"CREATE TABLE migrate_gate (id integer);\n" +
		"INSERT INTO migrate_gate (id) VALUES (1);\n"
	if err := os.WriteFile(filepath.Join(dir, "0001_gate.up.sql"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	conn := connect(t, srv.DSN())
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('mftiko-migrate-gate'), 1)`); err != nil {
		t.Fatal(err)
	}

	start := func() (*exec.Cmd, *bytes.Buffer) {
		t.Helper()
		buf := &bytes.Buffer{}
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(),
			"MFTIKO_MIGRATE_CHILD=1",
			"MFTIKO_MIGRATE_DIR="+dir,
			"DATABASE_URL="+srv.DSN(),
		)
		cmd.Stdout = buf
		cmd.Stderr = buf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, buf
	}
	a, aOut := start()
	b, bOut := start()
	finished := false
	t.Cleanup(func() {
		if finished {
			return
		}
		if a.Process != nil {
			_ = a.Process.Kill()
			_, _ = a.Process.Wait()
		}
		if b.Process != nil {
			_ = b.Process.Kill()
			_, _ = b.Process.Wait()
		}
	})

	deadline := time.Now().Add(30 * time.Second)
	var last string
	for {
		var mg, mw, gg, gw int
		err := conn.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE classid = hashtext($1)::oid AND granted),
				count(*) FILTER (WHERE classid = hashtext($1)::oid AND NOT granted),
				count(*) FILTER (WHERE classid = hashtext($2)::oid AND granted),
				count(*) FILTER (WHERE classid = hashtext($2)::oid AND NOT granted)
			FROM pg_locks
			WHERE locktype = 'advisory' AND objid = 1 AND objsubid = 2`,
			LockName, "mftiko-migrate-gate").Scan(&mg, &mw, &gg, &gw)
		if err != nil {
			t.Fatal(err)
		}
		last = fmt.Sprintf("migrate granted=%d wait=%d gate granted=%d wait=%d", mg, mw, gg, gw)
		if mg == 1 && mw == 1 && gg >= 1 && gw == 1 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("locks never overlapped (%s)\nA: %s\nB: %s", last, aOut, bOut)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtext('mftiko-migrate-gate'), 1)`); err != nil {
		t.Fatal(err)
	}
	waitChild(t, a, aOut)
	waitChild(t, b, bOut)
	finished = true

	var n, g int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM migrate_gate`).Scan(&g); err != nil {
		t.Fatal(err)
	}
	if n != 1 || g != 1 {
		t.Fatalf("schema_migrations=%d migrate_gate=%d (both processes applied)", n, g)
	}
}

func waitChild(t *testing.T, cmd *exec.Cmd, out *bytes.Buffer) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("migrate process: %v\n%s", err, out)
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("migrate process did not exit\n%s", out)
	}
}

func mustFile(t *testing.T, version int, name, sql string) file {
	t.Helper()
	f, err := newFile(version, name, []byte(sql))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func appliedAt(t *testing.T, ctx context.Context, conn *pgx.Conn) []time.Time {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []time.Time
	for rows.Next() {
		var at time.Time
		if err := rows.Scan(&at); err != nil {
			t.Fatal(err)
		}
		out = append(out, at)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sameTimes(a, b []time.Time) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func mustRecords(t *testing.T, ctx context.Context, conn *pgx.Conn) []record {
	t.Helper()
	recs, err := readRecorded(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}
