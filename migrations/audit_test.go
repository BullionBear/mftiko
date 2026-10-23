package migrations

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

func TestAuditMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	apply(t, ctx, conn, "0003_audit.up.sql")
	if !regclass(t, ctx, conn, "audit") {
		t.Fatal("up did not create audit")
	}
	mustExec(t, ctx, conn, `
		INSERT INTO audit (at_ns, method, path, status, duration_ns, request_id, remote_addr)
		VALUES (1, 'GET', '/api/v1/auth/me', 401, 10, 'abc', '127.0.0.1:9')`)
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM audit`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("inserted %d rows", n)
	}

	apply(t, ctx, conn, "0003_audit.down.sql")
	if regclass(t, ctx, conn, "audit") {
		t.Fatal("down left audit")
	}
	apply(t, ctx, conn, "0003_audit.up.sql")
	if !regclass(t, ctx, conn, "audit") {
		t.Fatal("second up did not create audit")
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM audit`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second up left %d rows", n)
	}
}
