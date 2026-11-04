package migrations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

// TestAccountsMigration is E21-03a: 0008 applies and rolls back, mftiko-migrate status
// reports no drift, a live name is unique, and a soft-deleted name can be used again.
func TestAccountsMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if _, err := Up(ctx, srv.DSN()); err != nil {
		t.Fatal(err)
	}
	assertNoDrift(t, ctx, srv.DSN())

	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if !regclass(t, ctx, conn, "accounts") || !regclass(t, ctx, conn, "accounts_name_live") {
		t.Fatal("up did not create accounts or accounts_name_live")
	}

	const cred = `decode(repeat('ab', 44), 'hex')`
	const sealed = `decode(repeat('cd', 57), 'hex')`
	insert := func(id, name, uid string) string {
		return `INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (` + id + `, 'BinanceUM', '` + name + `', ` + cred + `, 1, '{}', true, '` + uid + `', now(), now(), 1)`
	}
	mustExec(t, ctx, conn, insert("1", "desk", "uid-1"))
	mustFailConstraint(t, ctx, conn, "23505", "accounts_name_live", insert("2", "desk", "uid-2"))
	mustExec(t, ctx, conn, `UPDATE accounts SET deleted_at = now() WHERE api_id = 1`)
	mustExec(t, ctx, conn, insert("2", "desk", "uid-2"))
	mustExec(t, ctx, conn, `UPDATE accounts SET deleted_at = now() WHERE api_id = 2`)
	mustExec(t, ctx, conn, insert("3", "desk", "uid-3"))
	mustFailConstraint(t, ctx, conn, "23505", "accounts_pkey", insert("3", "other", "uid-9"))

	mustFailConstraint(t, ctx, conn, "23514", "accounts_api_id_chk", insert("0", "zero", "uid-z"))
	mustFailConstraint(t, ctx, conn, "23514", "accounts_api_id_chk", insert("9007199254740992", "wide", "uid-w"))
	mustExec(t, ctx, conn, insert("9007199254740991", "edge", "uid-e"))
	mustFailConstraint(t, ctx, conn, "23514", "accounts_name_chk", insert("4", "", "uid-n"))
	longName := strings.Repeat("n", 129)
	mustFailConstraint(t, ctx, conn, "23514", "accounts_name_chk", insert("5", longName, "uid-l"))
	mustFailConstraint(t, ctx, conn, "23514", "accounts_uid_chk", insert("6", "pad", " uid"))
	mustFailConstraint(t, ctx, conn, "23514", "accounts_uid_chk", insert("7", "longuid", strings.Repeat("u", 65)))
	mustExec(t, ctx, conn, insert("8", "okuid", strings.Repeat("u", 64)))
	mustFailConstraint(t, ctx, conn, "23514", "accounts_kind_chk", `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (9, '', 'kind', `+cred+`, 1, '{}', true, 'uid-k', now(), now(), 1)`)
	mustFailConstraint(t, ctx, conn, "23514", "accounts_credentials_len", `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (10, 'BinanceUM', 'short', decode('ab', 'hex'), 1, '{}', true, 'uid-s', now(), now(), 1)`)
	mustFailConstraint(t, ctx, conn, "23514", "accounts_key_version_chk", `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (11, 'BinanceUM', 'ver', `+cred+`, 0, '{}', true, 'uid-v', now(), now(), 1)`)
	mustFailConstraint(t, ctx, conn, "23514", "accounts_settings_obj", `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (12, 'BinanceUM', 'set', `+cred+`, 1, '[]', true, 'uid-j', now(), now(), 1)`)
	mustExec(t, ctx, conn, insert("13", "inst", "uid-i"))
	mustFailConstraint(t, ctx, conn, "23514", "account_replicas_instance_chk", `
		INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
		VALUES ('NOPE', 13, `+sealed+`, now(), now())`)
	mustFailConstraint(t, ctx, conn, "23514", "accounts_revision_chk", `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (14, 'BinanceUM', 'rev', `+cred+`, 1, '{}', true, 'uid-r', now(), now(), 0)`)
	mustExec(t, ctx, conn, insert("15", "seal", "uid-b"))
	mustFailConstraint(t, ctx, conn, "23514", "account_replicas_sealed_len", `
		INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
		VALUES ('td-tw-1', 15, decode('ab', 'hex'), now(), now())`)

	apply(t, ctx, conn, "0011_account_replicas.down.sql")
	apply(t, ctx, conn, "0009_account_projection.down.sql")
	apply(t, ctx, conn, "0008_accounts.down.sql")
	if regclass(t, ctx, conn, "accounts") || regclass(t, ctx, conn, "accounts_name_live") {
		t.Fatal("down left accounts")
	}
	assertNoDrift(t, ctx, srv.DSN())

	apply(t, ctx, conn, "0008_accounts.up.sql")
	apply(t, ctx, conn, "0009_account_projection.up.sql")
	if !regclass(t, ctx, conn, "accounts") {
		t.Fatal("second up did not create accounts")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second up left %d rows", n)
	}
	assertNoDrift(t, ctx, srv.DSN())
}

func assertNoDrift(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	lines, err := Status(ctx, dsn)
	if err != nil {
		t.Fatalf("mftiko-migrate status drift: %v", err)
	}
	var saw8, saw9 bool
	for _, line := range lines {
		if line.State != StateApplied {
			t.Fatalf("status %s", line)
		}
		if line.Version == 8 && line.Name == "accounts" {
			saw8 = true
		}
		if line.Version == 9 && line.Name == "account_projection" {
			saw9 = true
		}
	}
	if !saw8 || !saw9 {
		t.Fatalf("status has no applied 0008 accounts and 0009 account_projection:\n%v", lines)
	}
}

// TestAccountProjectionUpgrade is the non-empty path of 0009: a 0008 row receives revision 1
// and a 57-byte sealbox placeholder, then the defaults are dropped.
func TestAccountProjectionUpgrade(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, name := range []string{
		"0001_history.up.sql",
		"0002_symbol.up.sql",
		"0003_audit.up.sql",
		"0004_session_records.up.sql",
		"0005_session_records_final.up.sql",
		"0006_retired_instances.up.sql",
		"0007_drop_history.up.sql",
		"0008_accounts.up.sql",
	} {
		apply(t, ctx, conn, name)
	}
	mustExec(t, ctx, conn, `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, td_instance, created_at, updated_at
		) VALUES (1, 'BinanceUM', 'desk', decode(repeat('ab', 44), 'hex'), 1, '{}', true, 'uid-1', 'td-tw-1', now(), now())`)
	apply(t, ctx, conn, "0009_account_projection.up.sql")
	var rev, n int
	if err := conn.QueryRow(ctx, `SELECT revision, octet_length(sealed_credentials) FROM accounts WHERE api_id = 1`).Scan(&rev, &n); err != nil {
		t.Fatal(err)
	}
	if rev != 1 || n != 57 {
		t.Fatalf("backfill revision %d sealed %d", rev, n)
	}
	// NOT NULL is not a named constraint. Postgres 16 reports the column and leaves ConstraintName empty.
	_, err = conn.Exec(ctx, `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, td_instance, created_at, updated_at
		) VALUES (2, 'BinanceUM', 'next', decode(repeat('ab', 44), 'hex'), 1, '{}', true, 'uid-2', 'td-tw-1', now(), now())`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23502" || pgErr.ColumnName != "revision" || pgErr.TableName != "accounts" {
		t.Fatalf("insert without revision: %v", err)
	}
}

// TestAccountReplicasMigration is E21-D048 and E21-D049: 0011 moves the old
// columns into account_replicas, and down restores the earliest replica.
func TestAccountReplicasMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, name := range []string{
		"0008_accounts.up.sql",
		"0009_account_projection.up.sql",
	} {
		apply(t, ctx, conn, name)
	}
	mustExec(t, ctx, conn, `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, td_instance, created_at, updated_at, revision, sealed_credentials
		) VALUES (
		    1, 'BinanceUM', 'desk', decode(repeat('ab', 44), 'hex'), 1, '{}', true,
		    'uid-1', 'td-tw-1', '2020-01-02T03:04:05Z', '2020-01-02T03:04:05Z', 1,
		    decode(repeat('cd', 57), 'hex'))`)
	apply(t, ctx, conn, "0010_td_pubkeys.up.sql")
	apply(t, ctx, conn, "0011_account_replicas.up.sql")
	var cols int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'accounts'
		  AND column_name IN ('td_instance', 'sealed_credentials')`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	if cols != 0 {
		t.Fatalf("0011 left %d account columns", cols)
	}
	var inst string
	var n int
	if err := conn.QueryRow(ctx, `
		SELECT td_instance, octet_length(sealed_credentials)
		FROM account_replicas WHERE api_id = 1`).Scan(&inst, &n); err != nil {
		t.Fatal(err)
	}
	if inst != "td-tw-1" || n != 57 {
		t.Fatalf("moved replica %s sealed %d", inst, n)
	}
	mustExec(t, ctx, conn, `
		INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
		VALUES ('td-jp-1', 1, decode(repeat('ef', 57), 'hex'), '2021-01-02T03:04:05Z', '2021-01-02T03:04:05Z')`)
	mustExec(t, ctx, conn, `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (
		    2, 'BinanceUM', 'later', decode(repeat('ab', 44), 'hex'), 1, '{}', true,
		    'uid-2', now(), now(), 1)`)
	mustExec(t, ctx, conn, `
		INSERT INTO accounts (
		    api_id, kind, name, credentials, key_version, settings, enabled,
		    exch_account_uid, created_at, updated_at, revision
		) VALUES (
		    3, 'BinanceUM', 'tie', decode(repeat('ab', 44), 'hex'), 1, '{}', true,
		    'uid-3', now(), now(), 1)`)
	mustExec(t, ctx, conn, `
		INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
		VALUES
		    ('td-b', 3, decode(repeat('11', 57), 'hex'), '2022-01-02T03:04:05Z', '2022-01-02T03:04:05Z'),
		    ('td-a', 3, decode(repeat('22', 57), 'hex'), '2022-01-02T03:04:05Z', '2022-01-02T03:04:05Z')`)
	apply(t, ctx, conn, "0011_account_replicas.down.sql")
	if regclass(t, ctx, conn, "account_replicas") {
		t.Fatal("down left account_replicas")
	}
	var got string
	if err := conn.QueryRow(ctx, `SELECT td_instance FROM accounts WHERE api_id = 1`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "td-tw-1" {
		t.Fatalf("earliest replica %s", got)
	}
	if err := conn.QueryRow(ctx, `SELECT td_instance FROM accounts WHERE api_id = 3`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "td-a" {
		t.Fatalf("tie break %s", got)
	}
	var left int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE api_id = 2`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatal("down kept an account that had no replica")
	}
}
