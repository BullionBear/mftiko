package sym

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/migrations"
)

func TestStoreRoundTrip(t *testing.T) {
	srv := pgtest.Start(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, name := range []string{"0001_history.up.sql", "0002_symbol.up.sql"} {
		b, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := pgtest.ExecSQL(ctx, conn, string(b)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}

	t.Run("collision then retry", func(t *testing.T) {
		truncate(t, ctx, conn)
		src := &script{uids: []uint64{7, 7, 8}}
		st := openStore(t, srv.DSN(), src)
		first := spot("Bybit_Spot_BTCUSDT", "BTCUSDT")
		if uid, err := st.Insert(ctx, first); err != nil || uid != 7 {
			t.Fatalf("first insert = %d, %v", uid, err)
		}
		second := spot("Bybit_Perp_ETHUSDT", "ETHUSDT")
		uid, err := st.Insert(ctx, second)
		if err != nil || uid != 8 {
			t.Fatalf("retry insert = %d, %v", uid, err)
		}
		if src.n != 3 {
			t.Fatalf("source draws = %d, want 3", src.n)
		}
	})

	t.Run("move keeps uid and filters, relist gets a new uid", func(t *testing.T) {
		truncate(t, ctx, conn)
		src := &script{uids: []uint64{4, 4, 5}}
		st := openStore(t, srv.DSN(), src)
		tick := "0.5"
		row := spot("Bybit_Spot_BTCUSDT", "BTCUSDT")
		row.Filters = []Filter{{Name: "price_tick", Value: &tick}, {Name: "max_qty", Value: nil}}
		row.UpdatedAtNs = 100
		uid, err := st.Insert(ctx, row)
		if err != nil || uid != 4 {
			t.Fatalf("insert = %d, %v", uid, err)
		}
		if err := st.Move(ctx, uid, Delisted, 200); err != nil {
			t.Fatal(err)
		}
		assertGone(t, ctx, conn, "symbol_active", uid)
		assertGone(t, ctx, conn, "symbol_filter_active", uid)
		var gotTicker, gotStatus string
		var updated int64
		if err := conn.QueryRow(ctx, `
			SELECT universal_ticker, status, updated_at_ns
			FROM symbol_terminal WHERE uid = $1`, uid).Scan(&gotTicker, &gotStatus, &updated); err != nil {
			t.Fatal(err)
		}
		if gotTicker != row.UniversalTicker || gotStatus != string(Delisted) || updated != 200 {
			t.Fatalf("terminal row = %s %s %d", gotTicker, gotStatus, updated)
		}
		var value *string
		if err := conn.QueryRow(ctx, `
			SELECT value::text FROM symbol_filter_terminal WHERE uid = $1 AND name = 'price_tick'`, uid).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value == nil || *value != "0.5" {
			t.Fatalf("price_tick = %v", value)
		}
		var unbound *string
		if err := conn.QueryRow(ctx, `
			SELECT value::text FROM symbol_filter_terminal WHERE uid = $1 AND name = 'max_qty'`, uid).Scan(&unbound); err != nil {
			t.Fatal(err)
		}
		if unbound != nil {
			t.Fatalf("max_qty = %q, want NULL", *unbound)
		}

		again, err := st.Insert(ctx, spot("Bybit_Spot_BTCUSDT", "BTCUSDT"))
		if err != nil {
			t.Fatal(err)
		}
		if again == uid || again != 5 {
			t.Fatalf("relist uid = %d, want a new uid 5", again)
		}
		var terminalLeft int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM symbol_terminal WHERE uid = $1`, uid).Scan(&terminalLeft); err != nil {
			t.Fatal(err)
		}
		if terminalLeft != 1 {
			t.Fatal("terminal row disappeared on relist")
		}
		var shared int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM symbol_active a
			JOIN symbol_terminal t ON t.uid = a.uid`).Scan(&shared); err != nil {
			t.Fatal(err)
		}
		if shared != 0 {
			t.Fatalf("%d uids are on both tables", shared)
		}
		var revision int64
		var refreshed int64
		if err := conn.QueryRow(ctx, `
			SELECT revision, refreshed_at_ns FROM symbol_revision
			WHERE venue = 'Bybit' AND category = 'Spot'`).Scan(&revision, &refreshed); err != nil {
			t.Fatal(err)
		}
		if revision != 3 || refreshed != 10 {
			// insert (100), move (200), relist (10): three bumps, last refreshed_at is the relist's.
			t.Fatalf("revision = %d at %d, want 3 at 10", revision, refreshed)
		}
	})

	t.Run("zero and out of range are not inserted", func(t *testing.T) {
		truncate(t, ctx, conn)
		src := &script{uids: []uint64{0, 1 << 63, 9}}
		st := openStore(t, srv.DSN(), src)
		uid, err := st.Insert(ctx, spot("Okx_Perp_BTCUSDT", "BTC-USDT"))
		if err != nil || uid != 9 {
			t.Fatalf("insert = %d, %v", uid, err)
		}
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM symbol_active`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("active rows = %d, want only the accepted uid", n)
		}
	})

	t.Run("same spelling in another category is a different instrument", func(t *testing.T) {
		truncate(t, ctx, conn)
		src := &script{uids: []uint64{21, 22, 23}}
		st := openStore(t, srv.DSN(), src)
		if _, err := st.Insert(ctx, spot("Bybit_Spot_BTCUSDT", "BTCUSDT")); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Insert(ctx, spot("Bybit_Perp_BTCUSDT", "BTCUSDT")); err != nil {
			t.Fatal(err)
		}
		_, err := st.Insert(ctx, spot("Bybit_Spot_ETHUSDT", "BTCUSDT"))
		if err == nil || errors.Is(err, ErrUID) {
			t.Fatalf("duplicate exch spelling = %v, want a non-uid error", err)
		}
		if src.n != 3 {
			t.Fatalf("draws = %d, want 3 (no uid retry)", src.n)
		}
	})

	t.Run("exhausted collisions", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint64{3}})
		if _, err := st.Insert(ctx, spot("Bybit_Spot_BTCUSDT", "BTCUSDT")); err != nil {
			t.Fatal(err)
		}
		stuck := openStore(t, srv.DSN(), fixed(3))
		_, err := stuck.Insert(ctx, spot("Bybit_Perp_ETHUSDT", "ETHUSDT"))
		if !errors.Is(err, ErrUID) {
			t.Fatalf("err = %v, want ErrUID", err)
		}
	})

	t.Run("terminal status is not inserted on the active table", func(t *testing.T) {
		truncate(t, ctx, conn)
		src := &script{uids: []uint64{30}}
		st := openStore(t, srv.DSN(), src)
		row := spot("Bybit_Spot_BTCUSDT", "BTCUSDT")
		row.Status = Delisted
		if _, err := st.Insert(ctx, row); !errors.Is(err, ErrStatus) {
			t.Fatalf("err = %v, want ErrStatus", err)
		}
		if src.n != 0 {
			t.Fatalf("drew a uid for a rejected status (%d)", src.n)
		}
	})
}

type script struct {
	uids []uint64
	n    int
}

func (s *script) NextUID() (uint64, error) {
	if s.n >= len(s.uids) {
		return 0, errors.New("sym: scripted uid source exhausted")
	}
	v := s.uids[s.n]
	s.n++
	return v, nil
}

type fixed uint64

func (f fixed) NextUID() (uint64, error) { return uint64(f), nil }

func openStore(t *testing.T, dsn string, src Source) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := Open(ctx, dsn, src)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func spot(ticker, exch string) Listing {
	return Listing{
		UniversalTicker:  ticker,
		ExchTicker:       exch,
		Base:             "BTC",
		Quote:            "USDT",
		ContractValueCcy: "BTC",
		PriceCcy:         "USDT",
		Status:           Tradable,
		CreatedAtNs:      10,
		UpdatedAtNs:      10,
	}
}

func truncate(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(ctx, `TRUNCATE symbol_active, symbol_terminal, symbol_revision CASCADE`); err != nil {
		t.Fatal(err)
	}
}

func assertGone(t *testing.T, ctx context.Context, conn *pgx.Conn, table string, uid uint64) {
	t.Helper()
	var n int
	q := `SELECT count(*) FROM ` + table + ` WHERE uid = $1`
	switch table {
	case "symbol_active", "symbol_filter_active", "symbol_terminal", "symbol_filter_terminal":
	default:
		t.Fatalf("unexpected table %s", table)
	}
	if err := conn.QueryRow(ctx, q, uid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%s still has uid %d", table, uid)
	}
}
