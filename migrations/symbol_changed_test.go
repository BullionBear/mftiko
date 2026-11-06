package migrations

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

// TestSymbolChangedMigration is E22-D009: 0013 notifies sym_changed on every committed revision of
// symbol_revision, not on a rollback or an update that keeps the revision, and down removes it.
func TestSymbolChangedMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := Up(ctx, srv.DSN()); err != nil {
		t.Fatal(err)
	}
	assertNoDrift(t, ctx, srv.DSN())

	listener, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(context.Background()) })
	mustExec(t, ctx, listener, "LISTEN sym_changed")
	writer, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })

	mustExec(t, ctx, writer, `INSERT INTO symbol_revision (venue, category, revision, refreshed_at_ns) VALUES ('Bybit', 'Spot', 1, 1)`)
	expectNotify(t, ctx, listener, "Bybit/Spot/1")

	mustExec(t, ctx, writer, `UPDATE symbol_revision SET refreshed_at_ns = 2 WHERE venue = 'Bybit'`)
	tx, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE symbol_revision SET revision = revision + 1 WHERE venue = 'Bybit'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Two bumps in one transaction arrive at commit, in order.
	tx, err = writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := tx.Exec(ctx, `UPDATE symbol_revision SET revision = revision + 1 WHERE venue = 'Bybit'`); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// The unchanged update and the rollback sent nothing, so the next two are the commit's.
	expectNotify(t, ctx, listener, "Bybit/Spot/2")
	expectNotify(t, ctx, listener, "Bybit/Spot/3")

	apply(t, ctx, writer, "0013_symbol_changed.down.sql")
	var n int
	if err := writer.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM pg_trigger WHERE tgname = 'symbol_revision_notify') +
		       (SELECT count(*) FROM pg_proc WHERE proname = 'symbol_revision_notify')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("down left %d trigger or function", n)
	}
	mustExec(t, ctx, writer, `UPDATE symbol_revision SET revision = revision + 1 WHERE venue = 'Bybit'`)
	quiet, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	if note, err := listener.WaitForNotification(quiet); err == nil {
		t.Fatalf("down still notifies: %s", note.Payload)
	} else if !errors.Is(err, context.DeadlineExceeded) && !pgconn.Timeout(err) {
		t.Fatal(err)
	}
}

func expectNotify(t *testing.T, ctx context.Context, conn *pgx.Conn, payload string) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	note, err := conn.WaitForNotification(wait)
	if err != nil {
		t.Fatalf("waiting for %s: %v", payload, err)
	}
	if note.Channel != "sym_changed" || note.Payload != payload {
		t.Fatalf("got %s %s, want sym_changed %s", note.Channel, note.Payload, payload)
	}
}
