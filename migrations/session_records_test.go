package migrations

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
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

// TestSessionRecordsFinalMigration is E10-D117: 0005 adds final_status on top of 0004, a row closed
// before it reads null, and down drops only the column.
func TestSessionRecordsFinalMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	apply(t, ctx, conn, "0004_session_records.up.sql")
	mustExec(t, ctx, conn, `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at, closed_at
		) VALUES (
		    '11111111-1111-4111-8111-111111111111', 'aaaaaa', 'sts-tw-1', '\x01', decode(repeat('ab', 32), 'hex'),
		    'closed', 'ended', now(), now())`)
	apply(t, ctx, conn, "0005_session_records_final.up.sql")
	var final []byte
	if err := conn.QueryRow(ctx, `SELECT final_status FROM session_records`).Scan(&final); err != nil {
		t.Fatal(err)
	}
	if final != nil {
		t.Fatalf("an old row has final_status %x", final)
	}
	mustExec(t, ctx, conn, `UPDATE session_records SET final_status = '\x0801'`)
	if err := conn.QueryRow(ctx, `SELECT final_status FROM session_records`).Scan(&final); err != nil {
		t.Fatal(err)
	}
	if string(final) != "\x08\x01" {
		t.Fatalf("final_status %x", final)
	}

	apply(t, ctx, conn, "0005_session_records_final.down.sql")
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'session_records' AND column_name = 'final_status'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 || !regclass(t, ctx, conn, "session_records") {
		t.Fatalf("down: final_status columns %d, table present %v", n, regclass(t, ctx, conn, "session_records"))
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM session_records`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("down lost rows: %d %v", n, err)
	}
	apply(t, ctx, conn, "0005_session_records_final.up.sql")
}

// TestSessionRecordAPIIDMigration is E21-D069–E21-D071: 0012 backfills api_ids from spec_bytes,
// including rows written before the column existed, and a spec that does not decode becomes empty.
func TestSessionRecordAPIIDMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	apply(t, ctx, conn, "0004_session_records.up.sql")
	rows := []struct {
		id   string
		sid  string
		raw  []byte
		want []int64
	}{
		{
			id: "11111111-1111-4111-8111-111111111111", sid: "aaaaaa",
			raw: mustSpec(t, &sessionv1.SessionSpec{
				Name:   "rich",
				Labels: map[string]string{"team": "desk"},
				Params: []byte(`{"z":1}`),
				Feeds:  []*sessionv1.FeedDecl{{Feed: "book.X", MdInstance: "md-tw-1"}},
				Accounts: []*sessionv1.AccountDecl{
					{Name: "b", ApiId: 43, TdInstance: "td-tw-1", AlgoIds: []uint32{1, 2}},
					{Name: "a", ApiId: 42, TdInstance: "td-tw-1", AlgoIds: []uint32{3}},
					{Name: "dup", ApiId: 42, TdInstance: "td-tw-1", AlgoIds: []uint32{4}},
					{Name: "zero", ApiId: 0, TdInstance: "td-tw-1"},
				},
				EventLog: &sessionv1.EventLog{Enabled: true, KeepMb: 1},
			}),
			want: []int64{42, 43},
		},
		{
			id: "22222222-2222-4222-8222-222222222222", sid: "bbbbbb",
			raw: mustSpec(t, &sessionv1.SessionSpec{Accounts: []*sessionv1.AccountDecl{
				{ApiId: 9}, {ApiId: 5}, {ApiId: 5},
			}}),
			want: []int64{5, 9},
		},
		{
			id: "33333333-3333-4333-8333-333333333333", sid: "cccccc",
			raw: mustSpec(t, &sessionv1.SessionSpec{Accounts: []*sessionv1.AccountDecl{
				{Name: "only-zero", ApiId: 0},
			}}),
			want: []int64{},
		},
		{
			id: "44444444-4444-4444-8444-444444444444", sid: "dddddd",
			raw: mustSpec(t, &sessionv1.SessionSpec{Accounts: []*sessionv1.AccountDecl{
				{ApiId: -1}, {ApiId: 1 << 62},
			}}),
			want: []int64{-1, 1 << 62},
		},
		{
			id: "55555555-5555-4555-8555-555555555555", sid: "eeeeee",
			raw:  []byte{0x01},
			want: []int64{},
		},
		{
			id: "66666666-6666-4666-8666-666666666666", sid: "ffffff",
			raw:  mustSpec(t, &sessionv1.SessionSpec{}),
			want: []int64{},
		},
	}
	for _, row := range rows {
		if _, err := conn.Exec(ctx, `
			INSERT INTO session_records (
			    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
			) VALUES (
			    $1::uuid, $2, 'sts-tw-1', $3, decode(repeat('ab', 32), 'hex'), 'closed', 'ended', now())`,
			row.id, row.sid, row.raw); err != nil {
			t.Fatal(err)
		}
	}

	apply(t, ctx, conn, "0012_session_record_api_ids.up.sql")
	if !regprocedure(t, ctx, conn, "session_spec_api_ids(bytea)") {
		t.Fatal("function missing")
	}
	var am string
	if err := conn.QueryRow(ctx, `
		SELECT am.amname FROM pg_class i
		JOIN pg_am am ON am.oid = i.relam
		WHERE i.relname = 'session_records_api_ids'`).Scan(&am); err != nil {
		t.Fatal(err)
	}
	if am != "gin" {
		t.Fatalf("index access method %s", am)
	}
	for _, row := range rows {
		var got []int64
		if err := conn.QueryRow(ctx, `SELECT api_ids FROM session_records WHERE id = $1::uuid`, row.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(normIDs(got), row.want) {
			t.Fatalf("%s api_ids %v, want %v", row.sid, got, row.want)
		}
	}
	var matched []string
	qrows, err := conn.Query(ctx, `
		SELECT session_id FROM session_records
		WHERE api_ids @> ARRAY[42]::bigint[]
		ORDER BY session_id`)
	if err != nil {
		t.Fatal(err)
	}
	for qrows.Next() {
		var sid string
		if err := qrows.Scan(&sid); err != nil {
			t.Fatal(err)
		}
		matched = append(matched, sid)
	}
	if err := qrows.Err(); err != nil {
		t.Fatal(err)
	}
	qrows.Close()
	if !slices.Equal(matched, []string{"aaaaaa"}) {
		t.Fatalf("api 42 matched %v", matched)
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO session_records (
		    id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at
		) VALUES (
		    '77777777-7777-4777-8777-777777777777', 'aabbcc', 'sts-tw-1', '\x',
		    decode(repeat('ab', 32), 'hex'), 'pending', 'running', now())`); err != nil {
		t.Fatal(err)
	}
	var fresh []int64
	if err := conn.QueryRow(ctx, `SELECT api_ids FROM session_records WHERE session_id = 'aabbcc'`).Scan(&fresh); err != nil {
		t.Fatal(err)
	}
	if len(fresh) != 0 {
		t.Fatalf("default %v", fresh)
	}
	if _, err := conn.Exec(ctx, `UPDATE session_records SET api_ids = NULL WHERE session_id = 'aabbcc'`); err == nil {
		t.Fatal("null api_ids was accepted")
	}

	apply(t, ctx, conn, "0012_session_record_api_ids.down.sql")
	if regprocedure(t, ctx, conn, "session_spec_api_ids(bytea)") || regclass(t, ctx, conn, "session_records_api_ids") {
		t.Fatal("down left the function or the index")
	}
	var cols int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'session_records' AND column_name = 'api_ids'`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	if cols != 0 {
		t.Fatal("down left api_ids")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM session_records`).Scan(&n); err != nil || n != len(rows)+1 {
		t.Fatalf("down lost rows: %d %v", n, err)
	}

	apply(t, ctx, conn, "0012_session_record_api_ids.up.sql")
	var again []int64
	if err := conn.QueryRow(ctx, `SELECT api_ids FROM session_records WHERE session_id = 'aaaaaa'`).Scan(&again); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(normIDs(again), []int64{42, 43}) {
		t.Fatalf("second up %v", again)
	}
}

func mustSpec(t *testing.T, spec *sessionv1.SessionSpec) []byte {
	t.Helper()
	b, err := proto.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func normIDs(ids []int64) []int64 {
	if ids == nil {
		return []int64{}
	}
	return ids
}

func regprocedure(t *testing.T, ctx context.Context, conn *pgx.Conn, name string) bool {
	t.Helper()
	var rel *string
	if err := conn.QueryRow(ctx, `SELECT to_regprocedure($1)::text`, name).Scan(&rel); err != nil {
		t.Fatal(err)
	}
	return rel != nil
}
