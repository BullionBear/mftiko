package sym

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/migrations"
)

func TestRefresh(t *testing.T) {
	srv, conn := startSym(t)
	ctx := context.Background()

	t.Run("unchanged does not advance revision", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{4, 5}})
		row := spotL("BTC", "USDT", "BTCUSDT")
		rep := mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{row}, zerolog.Nop())
		if !rep.Changed || rep.Revision != 1 || rep.Inserted != 1 {
			t.Fatalf("first = %+v", rep)
		}
		rep = mustRefresh(t, st, symbol.Spot, 200, []symbol.Listing{row}, zerolog.Nop())
		if rep.Changed || rep.Revision != 1 || rep.Updated != 0 || rep.Inserted != 0 {
			t.Fatalf("second = %+v", rep)
		}
		rev, at := mustRevision(t, ctx, conn, "Bybit", "Spot")
		if rev != 1 || at != 100 {
			t.Fatalf("revision = %d at %d", rev, at)
		}
		uid, updated, status := mustActive(t, ctx, conn, "Bybit_Spot_BTCUSDT")
		if uid != 4 || updated != 100 || status != string(Tradable) {
			t.Fatalf("row = %d %s %d", uid, status, updated)
		}
	})

	t.Run("restart keeps the uid", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{9}})
		row := spotL("BTC", "USDT", "BTCUSDT")
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{row}, zerolog.Nop())
		again := openStore(t, srv.DSN(), denyUID{})
		rep := mustRefresh(t, again, symbol.Spot, 300, []symbol.Listing{row}, zerolog.Nop())
		if rep.Changed || rep.Inserted != 0 {
			t.Fatalf("restart refresh = %+v", rep)
		}
		uid, _, _ := mustActive(t, ctx, conn, "Bybit_Spot_BTCUSDT")
		if uid != 9 {
			t.Fatalf("uid = %d, want 9", uid)
		}
	})

	t.Run("delist stays inside the category and relist gets a new uid", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{4, 6, 5}})
		tick := "0.5"
		spot := spotL("BTC", "USDT", "BTCUSDT")
		spot.Filters = map[string]string{"price_tick": tick, "max_qty": ""}
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{spot}, zerolog.Nop())
		perp := perpL("BTC", "USDT", "BTCUSDT")
		mustRefresh(t, st, symbol.Perp, 100, []symbol.Listing{perp}, zerolog.Nop())

		rep := mustRefresh(t, st, symbol.Spot, 200, nil, zerolog.Nop())
		if rep.Delisted != 1 || rep.Revision != 2 || !rep.Changed {
			t.Fatalf("delist = %+v", rep)
		}
		if _, _, _, err := activeStatus(ctx, conn, "Bybit_Spot_BTCUSDT"); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("spot row still active")
		}
		status, updated := mustTerminal(t, ctx, conn, 4)
		if status != string(Delisted) || updated != 200 {
			t.Fatalf("terminal = %s %d", status, updated)
		}
		var value *string
		if err := conn.QueryRow(ctx, `
			SELECT value::text FROM symbol_filter_terminal
			WHERE uid = 4 AND name = 'price_tick'`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value == nil || *value != "0.5" {
			t.Fatalf("price_tick = %v", value)
		}
		var unbound *string
		if err := conn.QueryRow(ctx, `
			SELECT value::text FROM symbol_filter_terminal
			WHERE uid = 4 AND name = 'max_qty'`).Scan(&unbound); err != nil {
			t.Fatal(err)
		}
		if unbound != nil {
			t.Fatalf("max_qty = %v, want NULL", unbound)
		}
		perpUID, _, perpStatus := mustActive(t, ctx, conn, "Bybit_Perp_BTCUSDT")
		if perpUID != 6 || perpStatus != string(Tradable) {
			t.Fatalf("perp = %d %s", perpUID, perpStatus)
		}
		perpRev, perpAt := mustRevision(t, ctx, conn, "Bybit", "Perp")
		if perpRev != 1 || perpAt != 100 {
			t.Fatalf("perp revision = %d at %d", perpRev, perpAt)
		}

		rep = mustRefresh(t, st, symbol.Spot, 300, []symbol.Listing{spot}, zerolog.Nop())
		if rep.Inserted != 1 || rep.Revision != 3 {
			t.Fatalf("relist = %+v", rep)
		}
		newUID, _, _ := mustActive(t, ctx, conn, "Bybit_Spot_BTCUSDT")
		if newUID != 5 {
			t.Fatalf("relist uid = %d, want 5", newUID)
		}
		if left := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_terminal WHERE uid = 4`); left != 1 {
			t.Fatal("terminal row disappeared on relist")
		}
		if left := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_uid WHERE uid IN (4, 5)`); left != 2 {
			t.Fatalf("registry rows = %d", left)
		}
		if shared := countWhere(t, ctx, conn, `
			SELECT count(*) FROM symbol_active a JOIN symbol_terminal t ON t.uid = a.uid`); shared != 0 {
			t.Fatalf("%d uids are on both tables", shared)
		}
	})

	t.Run("failed read writes nothing", func(t *testing.T) {
		truncate(t, ctx, conn)
		src, err := exchangetest.New("Bybit")
		if err != nil {
			t.Fatal(err)
		}
		if err := src.Add(spotL("BTC", "USDT", "BTCUSDT")); err != nil {
			t.Fatal(err)
		}
		if err := src.Add(spotL("ETH", "USDT", "ETHUSDT")); err != nil {
			t.Fatal(err)
		}
		if err := src.SetPageSize(1); err != nil {
			t.Fatal(err)
		}
		st := openStore(t, srv.DSN(), &script{uids: []uint32{1, 2, 3}})
		feed := Feed{Venue: "Bybit", Category: symbol.Spot, REST: src}
		rep, err := st.Refresh(ctx, feed, 100, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		if rep.Inserted != 2 || rep.Revision != 1 {
			t.Fatalf("first = %+v", rep)
		}
		if err := src.FailPage(1, errors.New("page down")); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Refresh(ctx, feed, 400, zerolog.Nop()); err == nil {
			t.Fatal("expected the failed page to fail the refresh")
		}
		if got := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active`); got != 2 {
			t.Fatalf("active rows = %d", got)
		}
		rev, at := mustRevision(t, ctx, conn, "Bybit", "Spot")
		if rev != 1 || at != 100 {
			t.Fatalf("revision = %d at %d", rev, at)
		}
		partial := listREST{rows: []symbol.Listing{spotL("BTC", "USDT", "BTCUSDT")}, err: errors.New("half")}
		if _, err := st.Refresh(ctx, Feed{Venue: "Bybit", Category: symbol.Spot, REST: partial}, 500, zerolog.Nop()); err == nil {
			t.Fatal("expected a partial read to fail")
		}
		if got := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active`); got != 2 {
			t.Fatalf("partial read delisted, active = %d", got)
		}
		rev, at = mustRevision(t, ctx, conn, "Bybit", "Spot")
		if rev != 1 || at != 100 {
			t.Fatalf("revision after partial = %d at %d", rev, at)
		}
	})

	t.Run("one revision for many changes", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{11, 12, 13, 14}})
		first := []symbol.Listing{
			spotL("BTC", "USDT", "BTCUSDT"),
			spotL("ETH", "USDT", "ETHUSDT"),
			spotL("SOL", "USDT", "SOLUSDT"),
		}
		rep := mustRefresh(t, st, symbol.Spot, 100, first, zerolog.Nop())
		if rep.Inserted != 3 || rep.Revision != 1 {
			t.Fatalf("insert = %+v", rep)
		}
		eth := spotL("ETH", "USDT", "ETHUSDT")
		eth.Filters = map[string]string{"price_tick": "0.01"}
		rep = mustRefresh(t, st, symbol.Spot, 200, []symbol.Listing{
			spotL("BTC", "USDT", "BTCUSDT"),
			eth,
			spotL("XRP", "USDT", "XRPUSDT"),
		}, zerolog.Nop())
		if rep.Inserted != 1 || rep.Updated != 1 || rep.Delisted != 1 || rep.Revision != 2 {
			t.Fatalf("mixed = %+v", rep)
		}
	})

	t.Run("scale and strike spelling are not a change", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{21}})
		opt := optionL("6400.0")
		opt.ContractSize = "1.10"
		opt.Filters = map[string]string{"price_tick": "0.50"}
		mustRefresh(t, st, symbol.Option, 100, []symbol.Listing{opt}, zerolog.Nop())
		opt.Strike = "6400"
		opt.ContractSize = "1.1"
		opt.Filters = map[string]string{"price_tick": "0.5"}
		rep := mustRefresh(t, st, symbol.Option, 200, []symbol.Listing{opt}, zerolog.Nop())
		if rep.Changed || rep.Revision != 1 {
			t.Fatalf("scale refresh = %+v", rep)
		}
	})

	t.Run("halt stays active and advances revision once", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{31}})
		row := spotL("BTC", "USDT", "BTCUSDT")
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{row}, zerolog.Nop())
		row.Active = false
		rep := mustRefresh(t, st, symbol.Spot, 200, []symbol.Listing{row}, zerolog.Nop())
		if !rep.Changed || rep.Updated != 1 || rep.Delisted != 0 || rep.Revision != 2 {
			t.Fatalf("halt = %+v", rep)
		}
		uid, updated, status := mustActive(t, ctx, conn, "Bybit_Spot_BTCUSDT")
		if uid != 31 || status != string(Halted) || updated != 200 {
			t.Fatalf("row = %d %s %d", uid, status, updated)
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_terminal`); n != 0 {
			t.Fatal("halt moved the row")
		}
	})

	t.Run("rejected payload is logged and does not wipe the row", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{41}})
		row := symbol.Listing{
			Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USD", ExchTicker: "BTCUSD",
			ContractSize: "1", ContractValueCcy: "BTC", PriceCcy: "USD", Settle: "USD", Active: true,
		}
		mustRefresh(t, st, symbol.Perp, 100, []symbol.Listing{row}, zerolog.Nop())
		var buf bytes.Buffer
		log := zerolog.New(&buf)
		row.Settle = "USDC"
		fresh := symbol.Listing{
			Venue: "Bybit", Category: symbol.Perp, Base: "ETH", Quote: "USD", ExchTicker: "ETHUSD",
			ContractSize: "1", ContractValueCcy: "ETH", PriceCcy: "USD", Settle: "USDC", Active: true,
		}
		rep := mustRefresh(t, st, symbol.Perp, 200, []symbol.Listing{row, fresh}, log)
		if rep.Rejected != 2 || rep.Changed || rep.Revision != 1 {
			t.Fatalf("reject = %+v", rep)
		}
		uid, _, _ := mustActive(t, ctx, conn, "Bybit_Perp_BTCUSD")
		if uid != 41 {
			t.Fatalf("uid = %d", uid)
		}
		var settle string
		if err := conn.QueryRow(ctx, `SELECT settle FROM symbol_active WHERE uid = 41`).Scan(&settle); err != nil {
			t.Fatal(err)
		}
		if settle != "USD" {
			t.Fatalf("settle = %s, want the stored USD", settle)
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active WHERE universal_ticker = 'Bybit_Perp_ETHUSD'`); n != 0 {
			t.Fatal("quanto eth was stored")
		}
		if !bytes.Contains(buf.Bytes(), []byte("BTCUSD")) || !bytes.Contains(buf.Bytes(), []byte("ETHUSD")) {
			t.Fatalf("log = %s", buf.String())
		}
	})

	t.Run("expired moves and is not listed again", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{51}})
		row := futureL(5_000)
		row.Filters = map[string]string{"price_tick": "0.5"}
		mustRefresh(t, st, symbol.Future, 1_000, []symbol.Listing{row}, zerolog.Nop())
		row.Filters = map[string]string{"price_tick": "0.9"}
		rep := mustRefresh(t, st, symbol.Future, 5_000, []symbol.Listing{row}, zerolog.Nop())
		if rep.Expired != 1 || rep.Inserted != 0 || rep.Revision != 2 {
			t.Fatalf("expire = %+v", rep)
		}
		if _, _, _, err := activeStatus(ctx, conn, "BinanceUM_Future_BTCUSDT-260925"); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("expired row still active")
		}
		status, _ := mustTerminal(t, ctx, conn, 51)
		if status != string(Expired) {
			t.Fatalf("status = %s", status)
		}
		var tick string
		if err := conn.QueryRow(ctx, `
			SELECT value::text FROM symbol_filter_terminal WHERE uid = 51 AND name = 'price_tick'`).Scan(&tick); err != nil {
			t.Fatal(err)
		}
		if tick != "0.5" {
			t.Fatalf("terminal tick = %s, want the pre-expiry value", tick)
		}
		row.Filters = map[string]string{"price_tick": "1"}
		rep = mustRefresh(t, st, symbol.Future, 6_000, []symbol.Listing{row}, zerolog.Nop())
		if rep.Changed || rep.Inserted != 0 || rep.Revision != 2 {
			t.Fatalf("still expired = %+v", rep)
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active`); n != 0 {
			t.Fatal("expired contract was relisted")
		}
	})

	t.Run("missing dated contract past expiry is expired", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{61, 62}})
		live := futureL(5_000)
		mustRefresh(t, st, symbol.Future, 1_000, []symbol.Listing{live}, zerolog.Nop())
		rep := mustRefresh(t, st, symbol.Future, 2_000, nil, zerolog.Nop())
		if rep.Delisted != 1 || rep.Expired != 0 {
			t.Fatalf("early absence = %+v", rep)
		}
		status, _ := mustTerminal(t, ctx, conn, 61)
		if status != string(Delisted) {
			t.Fatalf("status = %s, want DELISTED", status)
		}

		truncate(t, ctx, conn)
		st = openStore(t, srv.DSN(), &script{uids: []uint32{63}})
		mustRefresh(t, st, symbol.Future, 1_000, []symbol.Listing{futureL(5_000)}, zerolog.Nop())
		rep = mustRefresh(t, st, symbol.Future, 5_000, nil, zerolog.Nop())
		if rep.Expired != 1 || rep.Delisted != 0 {
			t.Fatalf("late absence = %+v", rep)
		}
		status, _ = mustTerminal(t, ctx, conn, 63)
		if status != string(Expired) {
			t.Fatalf("status = %s, want EXPIRED", status)
		}
	})

	t.Run("duplicate batch writes nothing", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{71, 72}})
		eth := spotL("ETH", "USDT", "ETHUSDT")
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{eth}, zerolog.Nop())
		a := spotL("BTC", "USDT", "BTCUSDT")
		b := spotL("BTC", "USDT", "BTC-USDT")
		_, err := st.Refresh(ctx, Feed{Venue: "Bybit", Category: symbol.Spot, REST: listREST{rows: []symbol.Listing{a, b}}}, 200, zerolog.Nop())
		if !errors.Is(err, errDuplicate) {
			t.Fatalf("err = %v", err)
		}
		uid, _, _ := mustActive(t, ctx, conn, "Bybit_Spot_ETHUSDT")
		if uid != 71 {
			t.Fatalf("eth uid = %d", uid)
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active WHERE universal_ticker = 'Bybit_Spot_BTCUSDT'`); n != 0 {
			t.Fatal("duplicate batch stored BTC")
		}
		rev, at := mustRevision(t, ctx, conn, "Bybit", "Spot")
		if rev != 1 || at != 100 {
			t.Fatalf("revision = %d at %d", rev, at)
		}
	})

	t.Run("write failure rolls back the batch", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{81}})
		_, err := st.Refresh(ctx, Feed{
			Venue: "Bybit", Category: symbol.Spot,
			REST: listREST{rows: []symbol.Listing{
				spotL("BTC", "USDT", "BTCUSDT"),
				spotL("ETH", "USDT", "ETHUSDT"),
			}},
		}, 100, zerolog.Nop())
		if err == nil {
			t.Fatal("expected the second insert to fail")
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active`); n != 0 {
			t.Fatalf("active rows = %d", n)
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_uid`); n != 0 {
			t.Fatalf("registry rows = %d", n)
		}
		if _, _, ok := revisionOf(ctx, conn, "Bybit", "Spot"); ok {
			t.Fatal("revision row was written")
		}
	})

	t.Run("uid collision retries inside the batch", func(t *testing.T) {
		truncate(t, ctx, conn)
		first := openStore(t, srv.DSN(), &script{uids: []uint32{7}})
		if _, err := first.Insert(ctx, spot("Bybit_Perp_ETHUSDT", "ETHUSDT")); err != nil {
			t.Fatal(err)
		}
		st := openStore(t, srv.DSN(), &script{uids: []uint32{7, 8}})
		rep := mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{spotL("BTC", "USDT", "BTCUSDT")}, zerolog.Nop())
		if rep.Inserted != 1 || rep.Revision != 1 {
			t.Fatalf("refresh = %+v", rep)
		}
		uid, _, _ := mustActive(t, ctx, conn, "Bybit_Spot_BTCUSDT")
		if uid != 8 {
			t.Fatalf("uid = %d, want 8", uid)
		}
	})

	t.Run("other category in the payload is not stored", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{91, 92}})
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{spotL("BTC", "USDT", "BTCUSDT")}, zerolog.Nop())
		mustRefresh(t, st, symbol.Perp, 100, []symbol.Listing{perpL("BTC", "USDT", "BTCUSDT")}, zerolog.Nop())
		var buf bytes.Buffer
		rep := mustRefresh(t, st, symbol.Spot, 200, []symbol.Listing{perpL("ETH", "USDT", "ETHUSDT")}, zerolog.New(&buf))
		if rep.Rejected != 1 || rep.Delisted != 1 || rep.Inserted != 0 {
			t.Fatalf("report = %+v", rep)
		}
		if _, _, _, err := activeStatus(ctx, conn, "Bybit_Spot_BTCUSDT"); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("spot was not delisted")
		}
		uid, _, _ := mustActive(t, ctx, conn, "Bybit_Perp_BTCUSDT")
		if uid != 92 {
			t.Fatalf("perp uid = %d", uid)
		}
		if n := countWhere(t, ctx, conn, `SELECT count(*) FROM symbol_active WHERE universal_ticker = 'Bybit_Perp_ETHUSDT'`); n != 0 {
			t.Fatal("perp listing was stored by the spot refresh")
		}
		perpRev, _ := mustRevision(t, ctx, conn, "Bybit", "Perp")
		if perpRev != 1 {
			t.Fatalf("perp revision = %d", perpRev)
		}
	})

	t.Run("empty spot currencies are stored as base and quote", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{101}})
		row := symbol.Listing{
			Venue: "Bybit", Category: symbol.Spot, Base: "BTC", Quote: "USDT",
			ExchTicker: "BTCUSDT", Active: true,
		}
		rep := mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{row}, zerolog.Nop())
		if !rep.Changed || rep.Revision != 1 {
			t.Fatalf("first = %+v", rep)
		}
		var value, price *string
		var settle *string
		if err := conn.QueryRow(ctx, `
			SELECT contract_value_ccy, price_ccy, settle FROM symbol_active WHERE uid = 101`).Scan(&value, &price, &settle); err != nil {
			t.Fatal(err)
		}
		if value == nil || *value != "BTC" || price == nil || *price != "USDT" || settle != nil {
			t.Fatalf("ccy = %v %v %v", value, price, settle)
		}
		rep = mustRefresh(t, st, symbol.Spot, 200, []symbol.Listing{row}, zerolog.Nop())
		if rep.Changed {
			t.Fatalf("second = %+v", rep)
		}
	})
}

func TestCanonicalDecimalMatchesPostgres(t *testing.T) {
	_, conn := startSym(t)
	ctx := context.Background()
	for _, in := range []string{"1.10", "1", "0.50", "1e-8", "-0.50", "0", "10.500", "7E4"} {
		want, err := canonicalDecimal(in)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := conn.QueryRow(ctx, `SELECT trim_scale($1::numeric)::text`, in).Scan(&got); err != nil {
			t.Fatalf("postgres %s: %v", in, err)
		}
		if got != want {
			t.Fatalf("canonical %s = %s, postgres = %s", in, want, got)
		}
	}
}

func startSym(t *testing.T) (*pgtest.Server, *pgx.Conn) {
	t.Helper()
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
	return srv, conn
}

func mustRefresh(t *testing.T, st *Store, c symbol.Category, atNs int64, rows []symbol.Listing, log zerolog.Logger) Report {
	t.Helper()
	venue := "Bybit"
	if c == symbol.Future || c == symbol.Option {
		venue = "BinanceUM"
		if c == symbol.Option {
			venue = "Okx"
		}
	}
	rep, err := st.Refresh(context.Background(), Feed{Venue: venue, Category: c, REST: listREST{rows: rows}}, atNs, log)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func spotL(base, quote, exch string) symbol.Listing {
	return symbol.Listing{
		Venue: "Bybit", Category: symbol.Spot, Base: base, Quote: quote, ExchTicker: exch, Active: true,
		Filters: map[string]string{"price_tick": "0.5"},
	}
}

func perpL(base, quote, exch string) symbol.Listing {
	return symbol.Listing{
		Venue: "Bybit", Category: symbol.Perp, Base: base, Quote: quote, ExchTicker: exch,
		ContractSize: "1", ContractValueCcy: base, PriceCcy: quote, Settle: quote, Active: true,
	}
}

func futureL(expiry int64) symbol.Listing {
	return symbol.Listing{
		Venue: "BinanceUM", Category: symbol.Future, Base: "BTC", Quote: "USDT",
		ExchTicker: "BTCUSDT_260925", ExpiryCode: "260925", ExpiryNs: expiry,
		ContractSize: "1", ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT", Active: true,
	}
}

func optionL(strike string) symbol.Listing {
	return symbol.Listing{
		Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD",
		ExchTicker: "BTC-USD-261225-" + strike + "-C", ExpiryCode: "261225", ExpiryNs: 9_000_000_000,
		Strike: strike, OptionType: "C",
		ContractSize: "1", ContractValueCcy: "BTC", PriceCcy: "BTC", Settle: "BTC", Active: true,
	}
}

type listREST struct {
	rows []symbol.Listing
	err  error
}

func (s listREST) Listing(context.Context, symbol.Category) ([]symbol.Listing, error) {
	return s.rows, s.err
}
func (listREST) Depth(context.Context, *symbol.Info, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
func (listREST) Klines(context.Context, *symbol.Info, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}
func (listREST) KlinePage() int { return 0 }
func (listREST) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}

type denyUID struct{}

func (denyUID) NextUID() (uint32, error) { return 0, errors.New("drew a uid") }

func mustActive(t *testing.T, ctx context.Context, conn *pgx.Conn, ticker string) (uint32, int64, string) {
	t.Helper()
	uid, updated, status, err := activeStatus(ctx, conn, ticker)
	if err != nil {
		t.Fatal(err)
	}
	return uid, updated, status
}

func activeStatus(ctx context.Context, conn *pgx.Conn, ticker string) (uint32, int64, string, error) {
	var uid64, updated int64
	var status string
	err := conn.QueryRow(ctx, `
		SELECT uid, updated_at_ns, status FROM symbol_active WHERE universal_ticker = $1`, ticker).
		Scan(&uid64, &updated, &status)
	return uint32(uid64), updated, status, err
}

func mustTerminal(t *testing.T, ctx context.Context, conn *pgx.Conn, uid uint32) (string, int64) {
	t.Helper()
	var status string
	var updated int64
	if err := conn.QueryRow(ctx, `
		SELECT status, updated_at_ns FROM symbol_terminal WHERE uid = $1`, uid).Scan(&status, &updated); err != nil {
		t.Fatal(err)
	}
	return status, updated
}

func mustRevision(t *testing.T, ctx context.Context, conn *pgx.Conn, venue, category string) (int64, int64) {
	t.Helper()
	rev, at, ok := revisionOf(ctx, conn, venue, category)
	if !ok {
		t.Fatalf("no revision for %s %s", venue, category)
	}
	return rev, at
}

func revisionOf(ctx context.Context, conn *pgx.Conn, venue, category string) (int64, int64, bool) {
	var rev, at int64
	err := conn.QueryRow(ctx, `
		SELECT revision, refreshed_at_ns FROM symbol_revision
		WHERE venue = $1 AND category = $2`, venue, category).Scan(&rev, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false
	}
	if err != nil {
		return 0, 0, false
	}
	return rev, at, true
}

func countWhere(t *testing.T, ctx context.Context, conn *pgx.Conn, q string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
