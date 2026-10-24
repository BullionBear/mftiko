package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/migrations"
)

// TestRealVenueRefresh reads production listings and stores them.
// It does not run in CI.
//
//	MFTIKO_SYM_LIVE=1 go test ./cmd/mftiko-sym/ -run TestRealVenueRefresh -v -count=1 -timeout 15m
//
// MFTIKO_SYM_DSN, when set, is the database to refresh into. Otherwise the test
// starts a throwaway server with pgtest.
func TestRealVenueRefresh(t *testing.T) {
	if os.Getenv("MFTIKO_SYM_LIVE") != "1" {
		t.Skip("set MFTIKO_SYM_LIVE=1 to refresh production listings into Postgres")
	}
	dsn := os.Getenv("MFTIKO_SYM_DSN")
	if dsn == "" {
		dsn = pgtest.Start(t).DSN()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
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

	doer, err := venueDoer("", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	feeds, err := sym.SelectFeeds(all.Registry(), phaseOneFeeds, doer)
	if err != nil {
		t.Fatal(err)
	}
	st, err := sym.Open(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	now := time.Now().UnixNano()
	for _, f := range feeds {
		rep, err := st.Refresh(ctx, f, now, zerolog.Nop())
		if err != nil {
			t.Fatalf("%s %s: %v", f.Venue, f.Category, err)
		}
		t.Logf("%s/%s seen=%d rejected=%d inserted=%d updated=%d delisted=%d expired=%d revision=%d",
			f.Venue, f.Category, rep.Seen, rep.Rejected, rep.Inserted, rep.Updated, rep.Delisted, rep.Expired, rep.Revision)
		if rep.Seen == 0 || rep.Inserted == 0 || rep.Revision != 1 {
			t.Fatalf("%s %s: %+v", f.Venue, f.Category, rep)
		}
	}

	for _, ticker := range []string{
		"Binance_Spot_BTCUSDT",
		"BinanceUM_Perp_BTCUSDT",
		"Bybit_Spot_BTCUSDT",
		"Bybit_Perp_BTCUSDT",
		"Okx_Spot_BTCUSDT",
		"Okx_Perp_BTCUSDT",
	} {
		row := activeRow(t, ctx, conn, ticker)
		if row.status != string(symbol.Tradable) {
			t.Fatalf("%s status %s", ticker, row.status)
		}
		if row.uid < 1 || row.uid > 4294967295 {
			t.Fatalf("%s uid %d", ticker, row.uid)
		}
		if _, ok := symbol.AssetAliases()[row.base]; ok || !symbol.IsCanonicalAsset(row.base) || !symbol.IsCanonicalAsset(row.quote) {
			t.Fatalf("%s assets %s %s", ticker, row.base, row.quote)
		}
		if !symbol.IsCanonicalAsset(row.value) || !symbol.IsCanonicalAsset(row.price) {
			t.Fatalf("%s currencies %s %s", ticker, row.value, row.price)
		}
	}

	spot := activeRow(t, ctx, conn, "Binance_Spot_BTCUSDT")
	if spot.exch != "BTCUSDT" || spot.base != "BTC" || spot.quote != "USDT" ||
		spot.value != "BTC" || spot.price != "USDT" || spot.settle != "" || spot.size != "" {
		t.Fatalf("binance spot %+v", spot)
	}
	if n := filterValue(t, ctx, conn, spot.uid, symbol.PriceTick); n == "" {
		t.Fatal("binance spot has no price tick")
	}

	um := activeRow(t, ctx, conn, "BinanceUM_Perp_BTCUSDT")
	if um.exch != "BTCUSDT" || um.base != "BTC" || um.quote != "USDT" ||
		um.value != "BTC" || um.price != "USDT" || um.settle != "USDT" || um.size != "1" {
		t.Fatalf("binance um %+v", um)
	}

	bybit := activeRow(t, ctx, conn, "Bybit_Perp_BTCUSDT")
	if bybit.base != "BTC" || bybit.quote != "USDT" || bybit.value != "BTC" ||
		bybit.price != "USDT" || bybit.settle != "USDT" || bybit.size != "1" {
		t.Fatalf("bybit perp %+v", bybit)
	}

	okxSpot := activeRow(t, ctx, conn, "Okx_Spot_BTCUSDT")
	if okxSpot.exch != "BTC-USDT" || okxSpot.value != "BTC" || okxSpot.price != "USDT" || okxSpot.settle != "" || okxSpot.size != "" {
		t.Fatalf("okx spot %+v", okxSpot)
	}
	okx := activeRow(t, ctx, conn, "Okx_Perp_BTCUSDT")
	if okx.exch != "BTC-USDT-SWAP" || okx.value != "BTC" || okx.price != "USDT" || okx.settle != "USDT" || okx.size == "" || okx.size == "0" {
		t.Fatalf("okx perp %+v", okx)
	}

	for _, feed := range []struct{ venue, category string }{
		{"BinanceUM", "Future"},
		{"Bybit", "Future"},
		{"Okx", "Future"},
	} {
		var n int
		err := conn.QueryRow(ctx, `
			SELECT count(*) FROM symbol_active
			WHERE venue = $1 AND category = $2 AND expiry_ns IS NOT NULL AND expiry_ns > $3`,
			feed.venue, feed.category, now).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n < 1 {
			t.Fatalf("%s %s stored %d unexpired futures", feed.venue, feed.category, n)
		}
		t.Logf("%s/%s unexpired futures %d", feed.venue, feed.category, n)
	}

	var bad int
	err = conn.QueryRow(ctx, `
		SELECT count(*) FROM symbol_active
		WHERE category = 'Spot' AND (
			settle IS NOT NULL OR contract_size IS NOT NULL
			OR contract_value_ccy <> base OR price_ccy <> quote
		)`).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d spot rows disagree with E02-D003", bad)
	}
	err = conn.QueryRow(ctx, `
		SELECT count(*) FROM symbol_active
		WHERE category <> 'Spot' AND NOT (
			contract_value_ccy = base AND price_ccy = quote AND settle = quote
		)`).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d contract rows are not linear E02-D003", bad)
	}
	err = conn.QueryRow(ctx, `
		SELECT count(*) FROM symbol_active
		WHERE base = 'XBT' OR quote = 'XBT' OR contract_value_ccy = 'XBT' OR settle = 'XBT'`).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d rows kept the XBT alias", bad)
	}
}

type listingRow struct {
	uid    int64
	exch   string
	base   string
	quote  string
	size   string
	value  string
	price  string
	settle string
	status string
}

func activeRow(t *testing.T, ctx context.Context, conn *pgx.Conn, ticker string) listingRow {
	t.Helper()
	var row listingRow
	var size, settle *string
	err := conn.QueryRow(ctx, `
		SELECT uid, exch_ticker, base, quote, contract_size::text, contract_value_ccy, price_ccy, settle, status
		FROM symbol_active WHERE universal_ticker = $1`, ticker).Scan(
		&row.uid, &row.exch, &row.base, &row.quote, &size, &row.value, &row.price, &settle, &row.status)
	if err != nil {
		t.Fatalf("%s: %v", ticker, err)
	}
	if size != nil {
		row.size = *size
	}
	if settle != nil {
		row.settle = *settle
	}
	return row
}

func filterValue(t *testing.T, ctx context.Context, conn *pgx.Conn, uid int64, name string) string {
	t.Helper()
	var v *string
	err := conn.QueryRow(ctx, `
		SELECT value::text FROM symbol_filter_active WHERE uid = $1 AND name = $2`, uid, name).Scan(&v)
	if err != nil {
		t.Fatalf("filter %s uid %d: %v", name, uid, err)
	}
	if v == nil {
		return ""
	}
	return *v
}
