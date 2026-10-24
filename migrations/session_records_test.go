package migrations

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

// TestSessionRecordsMigration is the E10-11 table (E10-D073, E10-D031, E10-D068): up, the open-session
// and idempotency indexes, reuse after abandon, then down and up again.
func TestSessionRecordsMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	apply(t, ctx, conn, "0004_session_records.up.sql")
	if !regclass(t, ctx, conn, "session_records") {
		t.Fatal("up did not create session_records")
	}
	for _, name := range []string{"session_records_open_session_id", "session_records_idempotency_key"} {
		if !regclass(t, ctx, conn, name) {
			t.Fatalf("index %s missing", name)
		}
	}

	const sha = `decode(repeat('ab', 32), 'hex')`
	mustExec(t, ctx, conn, `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES (
		    '11111111-1111-4111-8111-111111111111', 'aaaaaa', 'sts-tw-1', '\x01', `+sha+`, 'pending', 'running', now())`)
	mustFailConstraint(t, ctx, conn, "23505", "session_records_open_session_id", `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES (
		    '22222222-2222-4222-8222-222222222222', 'aaaaaa', 'sts-tw-1', '\x01', `+sha+`, 'pending', 'running', now())`)
	mustExec(t, ctx, conn, `UPDATE session_records SET state = 'abandoned' WHERE id = '11111111-1111-4111-8111-111111111111'`)
	mustExec(t, ctx, conn, `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES (
		    '33333333-3333-4333-8333-333333333333', 'aaaaaa', 'sts-tw-1', '\x02', `+sha+`, 'pending', 'running', now())`)

	mustExec(t, ctx, conn, `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, idempotency_key, created_at
		) VALUES (
		    '44444444-4444-4444-8444-444444444444', 'bbbbbb', 'sts-tw-1', '\x01', `+sha+`, 'created', 'running', 'key-1', now())`)
	mustFailConstraint(t, ctx, conn, "23505", "session_records_idempotency_key", `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, idempotency_key, created_at
		) VALUES (
		    '55555555-5555-4555-8555-555555555555', 'cccccc', 'sts-tw-1', '\x01', `+sha+`, 'created', 'running', 'key-1', now())`)
	mustExec(t, ctx, conn, `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES
		    ('66666666-6666-4666-8666-666666666666', 'dddddd', 'sts-tw-1', '\x01', `+sha+`, 'created', 'running', now()),
		    ('77777777-7777-4777-8777-777777777777', 'eeeeee', 'sts-tw-1', '\x01', `+sha+`, 'created', 'running', now())`)

	mustFailConstraint(t, ctx, conn, "23514", "session_records_state_chk", `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES (
		    '88888888-8888-4888-8888-888888888888', 'ffffff', 'sts-tw-1', '\x01', `+sha+`, 'nope', 'running', now())`)
	mustFailConstraint(t, ctx, conn, "23514", "session_records_sha_len", `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES (
		    '99999999-9999-4999-8999-999999999999', 'aabbcc', 'sts-tw-1', '\x01', decode('ab', 'hex'), 'pending', 'running', now())`)

	apply(t, ctx, conn, "0004_session_records.down.sql")
	if regclass(t, ctx, conn, "session_records") {
		t.Fatal("down left session_records")
	}
	apply(t, ctx, conn, "0004_session_records.up.sql")
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM session_records`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second up left %d rows", n)
	}
}
