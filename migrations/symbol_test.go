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

// TestSymbolMigration is the E2-03 schema: up, down, up; exchange-ticker uniqueness
// inside one (venue, category) and not across categories; the terminal table keeping
// duplicate tickers; strike scale; and a uid rejected when the other table already has it.
func TestSymbolMigration(t *testing.T) {
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	apply(t, ctx, conn, "0001_history.up.sql")
	apply(t, ctx, conn, "0002_symbol.up.sql")

	if !regclass(t, ctx, conn, "orders") || !regclass(t, ctx, conn, "symbol_active") {
		t.Fatal("up did not create orders and symbol_active")
	}
	for _, gone := range []string{"symbol_ticker", "symbol_filter"} {
		if regclass(t, ctx, conn, gone) {
			t.Fatalf("table %s should not exist", gone)
		}
	}

	mustExec(t, ctx, conn, `
		INSERT INTO symbol_active (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, strike, status, created_at_ns, updated_at_ns
		) VALUES
		    (1, 'Bybit_Spot_BTCUSDT', 'BTCUSDT', 'BTC', 'USDT', 'BTC', 'USDT', NULL, 'TRADABLE', 1, 1),
		    (2, 'Bybit_Perp_BTCUSDT', 'BTCUSDT', 'BTC', 'USDT', 'BTC', 'USDT', 6400.0, 'TRADABLE', 1, 1)`)

	var venue, category, strike string
	if err := conn.QueryRow(ctx, `
		SELECT venue, category, coalesce(strike::text, '')
		FROM symbol_active WHERE uid = 2`).Scan(&venue, &category, &strike); err != nil {
		t.Fatal(err)
	}
	if venue != "Bybit" || category != "Perp" {
		t.Fatalf("generated columns = %s %s, want Bybit Perp", venue, category)
	}
	if strike != "6400" {
		t.Fatalf("strike stored as %q, want 6400 after trim_scale", strike)
	}
	mustExec(t, ctx, conn, `
		INSERT INTO symbol_active (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, strike, option_type, status, created_at_ns, updated_at_ns
		) VALUES (
		    8, 'Okx_Option_BTCUSDT-261225-6D4-C', 'BTC-USDT-261225-6.4-C', 'BTC', 'USDT', 'BTC', 'USDT', 6.40, 'C', 'TRADABLE', 1, 1)`)
	if err := conn.QueryRow(ctx, `SELECT strike::text FROM symbol_active WHERE uid = 8`).Scan(&strike); err != nil {
		t.Fatal(err)
	}
	if strike != "6.4" {
		t.Fatalf("fractional strike stored as %q, want 6.4", strike)
	}

	mustFailConstraint(t, ctx, conn, "23505", "symbol_active_exch", `
		INSERT INTO symbol_active (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, status, created_at_ns, updated_at_ns
		) VALUES (
		    3, 'Bybit_Spot_ETHUSDT', 'BTCUSDT', 'ETH', 'USDT', 'ETH', 'USDT', 'TRADABLE', 1, 1)`)

	mustFailConstraint(t, ctx, conn, "23514", "symbol_active_uid", `
		INSERT INTO symbol_active (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, status, created_at_ns, updated_at_ns
		) VALUES (
		    0, 'Bybit_Spot_ETHUSDT', 'ETHUSDT', 'ETH', 'USDT', 'ETH', 'USDT', 'TRADABLE', 1, 1)`)

	mustFailConstraint(t, ctx, conn, "23514", "symbol_active_status", `
		INSERT INTO symbol_active (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, status, created_at_ns, updated_at_ns
		) VALUES (
		    4, 'Bybit_Spot_ETHUSDT', 'ETHUSDT', 'ETH', 'USDT', 'ETH', 'USDT', 'DELISTED', 1, 1)`)

	mustExec(t, ctx, conn, `
		INSERT INTO symbol_terminal (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, status, created_at_ns, updated_at_ns
		) VALUES
		    (10, 'Bybit_Perp_OLD', 'OLD', 'BTC', 'USDT', 'BTC', 'USDT', 'DELISTED', 1, 1),
		    (11, 'Bybit_Perp_OLD', 'OLD', 'BTC', 'USDT', 'BTC', 'USDT', 'EXPIRED', 1, 1)`)

	mustFailConstraint(t, ctx, conn, "23505", "symbol_uid_across_tables", `
		INSERT INTO symbol_active (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, status, created_at_ns, updated_at_ns
		) VALUES (
		    10, 'Bybit_Spot_ETHUSDT', 'ETHUSDT', 'ETH', 'USDT', 'ETH', 'USDT', 'TRADABLE', 1, 1)`)

	mustFailConstraint(t, ctx, conn, "23514", "symbol_terminal_status", `
		INSERT INTO symbol_terminal (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_value_ccy, price_ccy, status, created_at_ns, updated_at_ns
		) VALUES (
		    12, 'Bybit_Spot_ETHUSDT', 'ETHUSDT', 'ETH', 'USDT', 'ETH', 'USDT', 'TRADABLE', 1, 1)`)

	apply(t, ctx, conn, "0002_symbol.down.sql")
	if regclass(t, ctx, conn, "symbol_active") || regclass(t, ctx, conn, "symbol_revision") {
		t.Fatal("down left symbol tables behind")
	}
	if !regclass(t, ctx, conn, "orders") {
		t.Fatal("down of 0002 dropped history tables")
	}

	apply(t, ctx, conn, "0002_symbol.up.sql")
	if !regclass(t, ctx, conn, "symbol_active") || !regclass(t, ctx, conn, "symbol_filter_terminal") {
		t.Fatal("second up did not recreate the symbol tables")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM symbol_active`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second up left %d active rows", n)
	}
}

func apply(t *testing.T, ctx context.Context, conn *pgx.Conn, name string) {
	t.Helper()
	b, err := FS.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgtest.ExecSQL(ctx, conn, string(b)); err != nil {
		t.Fatalf("apply %s: %v", name, err)
	}
}

func regclass(t *testing.T, ctx context.Context, conn *pgx.Conn, name string) bool {
	t.Helper()
	var rel *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1)::text`, name).Scan(&rel); err != nil {
		t.Fatal(err)
	}
	return rel != nil
}

func mustExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
}

func mustFailConstraint(t *testing.T, ctx context.Context, conn *pgx.Conn, code, constraint, sql string) {
	t.Helper()
	_, err := conn.Exec(ctx, sql)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("want SQLSTATE %s on %s, got %v", code, constraint, err)
	}
	if pgErr.Code != code || pgErr.ConstraintName != constraint {
		t.Fatalf("got %s %s (%s), want %s %s", pgErr.Code, pgErr.ConstraintName, pgErr.Message, code, constraint)
	}
}
