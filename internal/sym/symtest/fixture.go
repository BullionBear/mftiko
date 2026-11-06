package symtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/migrations"
)

// The fixture's tables. Every uid and updated_at_ns is fixed, so a case can name them.
//
// Bybit/Spot, one refresh per change:
//
//	BTC   uid 10 listed @1000, delisted @6000; listed again as uid 15 @7000 (active)
//	ETH   uid 11 listed @2000 (active)
//	SOL   uid 12 listed @3000, delisted @8000; listed again as uid 5 @9000, delisted @10000
//	DOGE  uid 13 listed @4000, delisted, listed again as uid 3 and delisted, all @11000
//	XRP   uid 14 listed @5000 (active)
//
// SOL has two terminal rows: uid 5 is later and smaller, so updated_at_ns decides. DOGE has two
// terminal rows at the same time: uid 13 is greater, so uid decides.
//
// Bybit/Perp: BTC uid 20 @500 (active). Okx/Spot: bigRows rows of bigFilters filters each, so a
// page of all of them is over 1 MiB and a page of 10 is not.
//
// commit lists ADA on Bybit/Spot as uid 30 @12000: one more revision, for the watch cases. They
// run after every other case.
const (
	btcOld, btc, eth, solOld, sol, dogeOld, doge, xrp, perp, ada = 10, 15, 11, 12, 5, 13, 3, 14, 20, 30

	bigRows    = 40
	bigFilters = 300
)

// fixture is a Postgres store with the tables above, and the logic layer over it, with a Hub.
type fixture struct {
	api    sym.API
	commit func(t *testing.T)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	uids := []uint32{perp, btcOld, eth, solOld, dogeOld, xrp, btc, sol, doge}
	for i := range bigRows {
		uids = append(uids, uint32(1000+i))
	}
	uids = append(uids, ada)
	st, err := sym.Open(ctx, startPG(t), &script{uids: uids})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	bybit := listing{}
	spotFeed := sym.Feed{Venue: "Bybit", Category: symbol.Spot, REST: bybit}
	perpFeed := sym.Feed{Venue: "Bybit", Category: symbol.Perp, REST: bybit}
	refresh := func(f sym.Feed, at int64) {
		t.Helper()
		if _, err := st.Refresh(ctx, f, at, zerolog.Nop()); err != nil {
			t.Fatal(err)
		}
	}
	add := func(base string) { bybit.add(spotListing("Bybit", base, base+"USDT", 1)) }
	remove := func(base string) { delete(bybit[symbol.Spot], base+"USDT") }
	bybit.add(symbol.Listing{
		Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT",
		ContractSize: 1, ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT", Status: sym.Tradable,
		Filters: symbol.Filters{"price_tick": 0.1},
	})
	refresh(perpFeed, 500)
	for i, base := range []string{"BTC", "ETH", "SOL", "DOGE", "XRP"} {
		add(base)
		refresh(spotFeed, int64(i+1)*1000)
	}
	remove("BTC")
	refresh(spotFeed, 6000)
	add("BTC")
	refresh(spotFeed, 7000)
	remove("SOL")
	refresh(spotFeed, 8000)
	add("SOL")
	refresh(spotFeed, 9000)
	remove("SOL")
	refresh(spotFeed, 10000)
	remove("DOGE")
	refresh(spotFeed, 11000)
	add("DOGE")
	refresh(spotFeed, 11000)
	remove("DOGE")
	refresh(spotFeed, 11000)

	okx := listing{}
	pad := strings.Repeat("x", 80)
	for i := range bigRows {
		row := spotListing("Okx", fmt.Sprintf("C%02d", i), fmt.Sprintf("C%02d-USDT", i), bigFilters)
		for j := range bigFilters {
			row.Filters[fmt.Sprintf("%s_%03d", pad, j)] = float64(j)
		}
		okx.add(row)
	}
	refresh(sym.Feed{Venue: "Okx", Category: symbol.Spot, REST: okx}, 500)
	hub := sym.NewHub(st, sym.HubOptions{Log: zerolog.Nop()})
	hubCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = hub.Run(hubCtx)
	}()
	t.Cleanup(func() {
		stop()
		<-done
	})
	return &fixture{
		api: sym.NewService(st, nil, zerolog.Nop()).WithHub(hub),
		commit: func(t *testing.T) {
			t.Helper()
			bybit.add(spotListing("Bybit", "ADA", "ADAUSDT", 1))
			if _, err := st.Refresh(ctx, spotFeed, 12000, zerolog.Nop()); err != nil {
				t.Fatal(err)
			}
		},
	}
}

// listing is one venue's listing as the fixture sets it, by category and exch_ticker. Refresh
// reads it in exch_ticker order.
type listing map[symbol.Category]map[string]symbol.Listing

func (l listing) add(row symbol.Listing) {
	if l[row.Category] == nil {
		l[row.Category] = map[string]symbol.Listing{}
	}
	l[row.Category][row.ExchTicker] = row
}

func (l listing) Listing(_ context.Context, c symbol.Category) ([]symbol.Listing, error) {
	rows := slices.Collect(maps.Values(l[c]))
	slices.SortFunc(rows, func(a, b symbol.Listing) int { return strings.Compare(a.ExchTicker, b.ExchTicker) })
	return rows, nil
}

func spotListing(venue, base, exch string, filters int) symbol.Listing {
	f := make(symbol.Filters, filters)
	f["price_tick"] = 0.5
	return symbol.Listing{
		Venue: venue, Category: symbol.Spot, Base: base, Quote: "USDT", ExchTicker: exch,
		Status: sym.Tradable, Filters: f,
	}
}

// startPG is a fresh database with the symbol schema. It returns the DSN.
func startPG(t *testing.T) string {
	t.Helper()
	srv := pgtest.Start(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, name := range []string{"0001_history.up.sql", "0002_symbol.up.sql", "0013_symbol_changed.up.sql"} {
		b, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := pgtest.ExecSQL(ctx, conn, string(b)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	return srv.DSN()
}

// script draws the given uids in order. Each Bybit refresh inserts at most one row, so each
// row gets the uid the table above names. The Okx rows draw 1000 up in exch_ticker order.
type script struct {
	uids []uint32
	n    int
}

func (s *script) NextUID() (uint32, error) {
	if s.n >= len(s.uids) {
		return 0, errors.New("symtest: scripted uid source exhausted")
	}
	v := s.uids[s.n]
	s.n++
	return v, nil
}
