package sym

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

func TestResolverFakeSource(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Bybit")
	btc := bybitSpot("BTC", "BTCUSDT", symbol.Tradable)
	btc.Filters = map[string]string{symbol.PriceTick: "0.1", symbol.MaxQty: ""}
	eth := bybitSpot("ETH", "ETHUSDT", symbol.Halted)
	if err := src.Add(btc); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(eth); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(bybitPerp("BTC", "USDT", "BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	// Filtered: an asset code that is not an asset, spot settlement, quanto.
	if err := src.Add(symbol.Listing{
		Venue: "Bybit", Category: symbol.Spot, Base: "SO/L", Quote: "USDT", ExchTicker: "solusdt",
		Status: symbol.Tradable,
	}); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(symbol.Listing{
		Venue: "Bybit", Category: symbol.Spot, Base: "SOL", Quote: "USDT", ExchTicker: "SOLUSDT", Settle: "USDT",
		Status: symbol.Tradable,
	}); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(symbol.Listing{
		Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USD", ExchTicker: "BTCUSD",
		ContractSize: "1", ContractValueCcy: "BTC", PriceCcy: "USD", Settle: "USDC", Status: symbol.Tradable,
	}); err != nil {
		t.Fatal(err)
	}

	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot, symbol.Perp}, &script{uids: []uint32{10, 11, 12, 13}})

	spot, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Spot, ExchTicker: "BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	perp, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Perp, ExchTicker: "BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if spot.UID != 10 || perp.UID != 12 || spot.UID == perp.UID {
		t.Fatalf("spot uid %d, perp uid %d", spot.UID, perp.UID)
	}
	if spot.Ticker.String() != "Bybit_Spot_BTCUSDT" || perp.Ticker.String() != "Bybit_Perp_BTCUSDT" {
		t.Fatalf("tickers %s %s", spot.Ticker, perp.Ticker)
	}
	if spot.ExchTicker != "BTCUSDT" || perp.ExchTicker != "BTCUSDT" {
		t.Fatalf("exch %s %s", spot.ExchTicker, perp.ExchTicker)
	}
	if !spot.IsTradable() || !perp.IsTradable() || perp.ContractSize != "1" || perp.Settle != "USDT" {
		t.Fatalf("spot %+v perp %+v", spot, perp)
	}
	if spot.Filters[symbol.PriceTick] != "0.1" || spot.Filters[symbol.MaxQty] != "" {
		t.Fatalf("filters %#v", spot.Filters)
	}

	got, err := r.Get(ctx, spot.Ticker, false)
	if err != nil || got.UID != spot.UID {
		t.Fatalf("Get = %+v %v", got, err)
	}
	byUID, err := r.GetByUID(ctx, perp.UID, false)
	if err != nil || byUID.Ticker != perp.Ticker {
		t.Fatalf("GetByUID = %+v %v", byUID, err)
	}
	if _, err := r.GetByUID(ctx, 0, true); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("uid 0 err = %v", err)
	}
	if _, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Spot, ExchTicker: "BTCUSD"}); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("quanto reverse err = %v", err)
	}

	halted, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "ETHUSDT"}, true)
	if err != nil || halted.UID != 11 || halted.Status != symbol.Halted || halted.IsTradable() {
		t.Fatalf("halted = %+v %v", halted, err)
	}

	spotList, err := r.List(ctx, "Bybit", symbol.Spot)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedTickers(spotList); strings.Join(got, ",") != "Bybit_Spot_BTCUSDT,Bybit_Spot_ETHUSDT" {
		t.Fatalf("spot list %v", got)
	}
	perpList, err := r.List(ctx, "Bybit", symbol.Perp)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedTickers(perpList); strings.Join(got, ",") != "Bybit_Perp_BTCUSDT" {
		t.Fatalf("perp list %v", got)
	}
	empty, err := r.List(ctx, "Bybit", symbol.Option)
	if err != nil || len(empty) != 0 {
		t.Fatalf("unconfigured category %v %v", empty, err)
	}

	spot.Filters[symbol.PriceTick] = "mutated"
	spot.Base = "ETH"
	again, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "BTCUSDT"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if again.Base != "BTC" || again.Filters[symbol.PriceTick] != "0.1" {
		t.Fatalf("caller mutated the snapshot: %+v", again)
	}
}

func TestResolverOptionStrike(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Okx")
	if err := src.Add(symbol.Listing{
		Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD",
		ExpiryCode: "261225", Strike: "6400.0", OptionType: "c",
		ExchTicker:   "BTC-USD-261225-6400-C",
		ContractSize: "1", ContractValueCcy: "BTC", PriceCcy: "BTC", Settle: "BTC",
		Status: symbol.Tradable,
	}); err != nil {
		t.Fatal(err)
	}
	r := mustResolver(t, "Okx", src, []symbol.Category{symbol.Option}, &script{uids: []uint32{8}})
	info, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Okx", Category: symbol.Option, Symbol: "BTCUSD-261225-6400-C"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if info.UID != 8 || info.Strike != "6400" || info.OptionType != "C" {
		t.Fatalf("%+v", info)
	}
}

func TestResolverUIDStableAcrossReload(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Bybit")
	if err := src.Add(bybitSpot("BTC", "BTCUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(bybitPerp("BTC", "USDT", "BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot, symbol.Perp}, &script{uids: []uint32{1, 2, 3, 4}})
	btc := symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "BTCUSDT"}
	perp := symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Perp, Symbol: "BTCUSDT"}

	if err := src.Add(bybitSpot("ETH", "ETHUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	r.Invalidate("Bybit")
	got, err := r.Get(ctx, btc, false)
	if err != nil || got.UID != 1 {
		t.Fatalf("btc after add = %+v %v", got, err)
	}
	eth, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "ETHUSDT"}, false)
	if err != nil || eth.UID != 3 {
		t.Fatalf("eth = %+v %v", eth, err)
	}
	if p, err := r.Get(ctx, perp, false); err != nil || p.UID != 2 {
		t.Fatalf("perp = %+v %v", p, err)
	}

	if err := src.Remove(symbol.Spot, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	r.Invalidate("")
	if _, err := r.Get(ctx, btc, true); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("delisted Get err = %v", err)
	}
	if _, err := r.GetByUID(ctx, 1, true); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("retired uid err = %v", err)
	}
	if p, err := r.Get(ctx, perp, false); err != nil || p.UID != 2 {
		t.Fatalf("perp after spot delist = %+v %v", p, err)
	}

	if err := src.Add(bybitSpot("BTC", "BTCUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	r.Invalidate("Bybit")
	relisted, err := r.Get(ctx, btc, false)
	if err != nil {
		t.Fatal(err)
	}
	if relisted.UID == 1 || relisted.UID != 4 {
		t.Fatalf("relist uid %d, want 4", relisted.UID)
	}
}

func TestResolverReloadFailureKeepsTable(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Bybit")
	if err := src.Add(bybitSpot("BTC", "BTCUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot}, &script{uids: []uint32{1, 2}})
	btc := symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "BTCUSDT"}
	if err := src.Add(bybitSpot("ETH", "ETHUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	down := errors.New("down")
	if err := src.FailPage(0, down); err != nil {
		t.Fatal(err)
	}
	r.Invalidate("Bybit")
	if _, err := r.Get(ctx, btc, false); !errors.Is(err, down) {
		t.Fatalf("Get err = %v", err)
	}
	snap := r.snap.Load()
	if _, ok := snap.byTicker[btc]; !ok {
		t.Fatal("failed reload dropped the previous table")
	}
	eth := symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "ETHUSDT"}
	if _, ok := snap.byTicker[eth]; ok {
		t.Fatal("failed reload published the new listing")
	}
	if err := src.FailPage(0, nil); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, btc, false)
	if err != nil || got.UID != 1 {
		t.Fatalf("recovered btc = %+v %v", got, err)
	}
	if ethInfo, err := r.Get(ctx, eth, false); err != nil || ethInfo.UID != 2 {
		t.Fatalf("recovered eth = %+v %v", ethInfo, err)
	}
}

func TestResolverOtherVenueStillServes(t *testing.T) {
	ctx := context.Background()
	bybit := mustFake(t, "Bybit")
	okx := mustFake(t, "Okx")
	if err := bybit.Add(bybitSpot("BTC", "BTCUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	if err := okx.Add(symbol.Listing{
		Venue: "Okx", Category: symbol.Spot, Base: "BTC", Quote: "USDT", ExchTicker: "BTC-USDT", Status: symbol.Tradable,
	}); err != nil {
		t.Fatal(err)
	}
	r, err := NewResolver(ctx, []VenueListing{
		{Venue: "Bybit", REST: bybit, Categories: []symbol.Category{symbol.Spot}},
		{Venue: "Okx", REST: okx, Categories: []symbol.Category{symbol.Spot}},
	}, &script{uids: []uint32{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := bybit.Add(bybitSpot("ETH", "ETHUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	down := errors.New("okx down")
	if err := okx.FailPage(0, down); err != nil {
		t.Fatal(err)
	}
	r.Invalidate("")
	eth, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "ETHUSDT"}, false)
	if err != nil || eth.UID != 3 {
		t.Fatalf("bybit during okx failure = %+v %v", eth, err)
	}
	okxTicker := symbol.UniversalTicker{Venue: "Okx", Category: symbol.Spot, Symbol: "BTCUSDT"}
	if _, err := r.Get(ctx, okxTicker, false); !errors.Is(err, down) {
		t.Fatalf("okx err = %v", err)
	}
	if err := okx.FailPage(0, nil); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, okxTicker, false)
	if err != nil || got.UID != 2 {
		t.Fatalf("okx recovered = %+v %v", got, err)
	}
}

func TestResolverRejects(t *testing.T) {
	ctx := context.Background()
	_, err := NewResolver(ctx, []VenueListing{{
		Venue: "Bybit", REST: bareREST{}, Categories: []symbol.Category{symbol.Spot},
	}}, nil)
	if !errors.Is(err, ErrRealVenue) {
		t.Fatalf("real venue err = %v", err)
	}
	src := mustFake(t, "Bybit")
	_, err = NewResolver(ctx, []VenueListing{{Venue: "Bybit", REST: src}}, nil)
	if err == nil {
		t.Fatal("empty categories accepted")
	}
	_, err = NewResolver(ctx, []VenueListing{
		{Venue: "Bybit", REST: src, Categories: []symbol.Category{symbol.Spot}},
		{Venue: "Bybit", REST: src, Categories: []symbol.Category{symbol.Perp}},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("duplicate venue err = %v", err)
	}

	dup := &sliceREST{rows: map[symbol.Category][]symbol.Listing{
		symbol.Spot: {
			bybitSpot("BTC", "BTCUSDT", symbol.Tradable),
			bybitSpot("BTC", "XBTUSDT", symbol.Tradable),
		},
	}}
	if _, err := NewResolver(ctx, []VenueListing{{
		Venue: "Bybit", REST: dup, Categories: []symbol.Category{symbol.Spot},
	}}, &script{uids: []uint32{1}}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate ticker err = %v", err)
	}

	wrong := &sliceREST{rows: map[symbol.Category][]symbol.Listing{
		symbol.Spot: {{
			Venue: "Okx", Category: symbol.Spot, Base: "BTC", Quote: "USDT", ExchTicker: "BTC-USDT",
		}},
	}}
	if _, err := NewResolver(ctx, []VenueListing{{
		Venue: "Bybit", REST: wrong, Categories: []symbol.Category{symbol.Spot},
	}}, nil); err == nil || !strings.Contains(err.Error(), "Okx") {
		t.Fatalf("venue mismatch err = %v", err)
	}
}

// A row that passes Validate but fails Ticker is omitted. The rest of the venue
// still loads, the skip is logged, and it does not take a UID.
func TestResolverSkipsBadTicker(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	seq := &script{uids: []uint32{1, 2, 3}}
	src := &sliceREST{rows: map[symbol.Category][]symbol.Listing{
		symbol.Spot: {bybitSpot("BTC", "BTCUSDT", symbol.Tradable)},
		symbol.Future: {
			{
				Venue: "Bybit", Category: symbol.Future, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT-BAD",
				ContractSize: "1", ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT",
				Status: symbol.Tradable,
			},
			bybitFuture("BTC", "BTCUSDT-26DEC25", "261225"),
		},
	}}
	r, err := newResolver(ctx, []VenueListing{{
		Venue: "Bybit", REST: src, Categories: []symbol.Category{symbol.Spot, symbol.Future},
	}}, seq, zerolog.New(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if r.skipped != 1 {
		t.Fatalf("skipped = %d, want 1", r.skipped)
	}
	if !strings.Contains(buf.String(), "listing skipped") || !strings.Contains(buf.String(), "BTCUSDT-BAD") {
		t.Fatalf("log = %s", buf.String())
	}
	if seq.n != 2 {
		t.Fatalf("drew %d uids, want 2", seq.n)
	}
	spot, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "BTCUSDT"}, false)
	if err != nil || spot.UID != 1 {
		t.Fatalf("spot = %+v %v", spot, err)
	}
	fut, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Future, Symbol: "BTCUSDT-261225"}, false)
	if err != nil || fut.UID != 2 {
		t.Fatalf("future = %+v %v", fut, err)
	}
	if _, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Future, ExchTicker: "BTCUSDT-BAD"}); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("bad ticker err = %v", err)
	}
	if got := listedTickers(mustList(t, r, "Bybit", symbol.Future)); len(got) != 1 || got[0] != "Bybit_Future_BTCUSDT-261225" {
		t.Fatalf("futures = %v", got)
	}

	src.rows[symbol.Future] = []symbol.Listing{
		bybitFuture("BTC", "BTCUSDT-26DEC25", "261225"),
		{
			Venue: "Bybit", Category: symbol.Future, Base: "ETH", Quote: "USDT", ExchTicker: "ETHUSDT-BAD",
			ContractSize: "1", ContractValueCcy: "ETH", PriceCcy: "USDT", Settle: "USDT",
			Status: symbol.Tradable,
		},
	}
	r.Invalidate("Bybit")
	if _, err := r.List(ctx, "Bybit", symbol.Future); err != nil {
		t.Fatal(err)
	}
	if r.skipped != 2 {
		t.Fatalf("skipped after reload = %d, want 2", r.skipped)
	}
	if again, err := r.Get(ctx, fut.Ticker, false); err != nil || again.UID != 2 {
		t.Fatalf("future after reload = %+v %v", again, err)
	}
}

func TestResolverAppliesAlias(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Bybit")
	if err := src.Add(symbol.Listing{
		Venue: "Bybit", Category: symbol.Spot, Base: "xbt", Quote: "usdt", ExchTicker: "XBTUSDT",
		Status: symbol.Tradable,
	}); err != nil {
		t.Fatal(err)
	}
	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot}, &script{uids: []uint32{1}})
	info, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "BTCUSDT"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if info.UID != 1 || info.Base != "BTC" || info.Quote != "USDT" || info.ExchTicker != "XBTUSDT" || !info.IsTradable() {
		t.Fatalf("alias = %+v", info)
	}
}

func TestResolverCachesActiveStatuses(t *testing.T) {
	ctx := context.Background()
	seq := &script{uids: []uint32{1, 2, 3, 4, 5, 6}}
	src := &sliceREST{rows: map[symbol.Category][]symbol.Listing{
		symbol.Spot: {
			bybitSpot("BTC", "BTCUSDT", symbol.Tradable),
			bybitSpot("ETH", "ETHUSDT", symbol.Halted),
			bybitSpot("SOL", "SOLUSDT", symbol.Prelisting),
			bybitSpot("XRP", "XRPUSDT", symbol.Settling),
			bybitSpot("DOGE", "DOGEUSDT", symbol.Unknown),
			bybitSpot("LTC", "LTCUSDT", symbol.Expired),
			bybitSpot("BCH", "BCHUSDT", symbol.Delisted),
			bybitSpot("ADA", "ADAUSDT", symbol.Unspecified),
		},
	}}
	r, err := NewResolver(ctx, []VenueListing{{
		Venue: "Bybit", REST: src, Categories: []symbol.Category{symbol.Spot},
	}}, seq)
	if err != nil {
		t.Fatal(err)
	}
	if seq.n != 5 {
		t.Fatalf("drew %d uids, want 5", seq.n)
	}
	want := []struct {
		symbol string
		status symbol.Status
	}{
		{"BTCUSDT", symbol.Tradable},
		{"DOGEUSDT", symbol.Unknown},
		{"ETHUSDT", symbol.Halted},
		{"SOLUSDT", symbol.Prelisting},
		{"XRPUSDT", symbol.Settling},
	}
	list := mustList(t, r, "Bybit", symbol.Spot)
	if len(list) != len(want) {
		t.Fatalf("list = %v", listedTickers(list))
	}
	for i, w := range want {
		if list[i].Ticker.Symbol != w.symbol || list[i].Status != w.status {
			t.Fatalf("row %d = %+v, want %s %s", i, list[i], w.symbol, w.status)
		}
		tradable := w.status == symbol.Tradable
		if list[i].IsTradable() != tradable {
			t.Fatalf("%s tradable = %v", w.symbol, list[i].IsTradable())
		}
	}
	for _, symbolName := range []string{"LTCUSDT", "BCHUSDT", "ADAUSDT"} {
		_, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: symbolName}, true)
		if !errors.Is(err, symbol.ErrNotFound) {
			t.Fatalf("%s err = %v", symbolName, err)
		}
	}
}

func TestResolverSkipsInvalidUID(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Bybit")
	if err := src.Add(bybitSpot("BTC", "BTCUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(bybitSpot("ETH", "ETHUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot}, &script{uids: []uint32{0, 4, 4, 5}})
	btc, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "BTCUSDT"}, false)
	if err != nil || btc.UID != 4 {
		t.Fatalf("btc = %+v %v", btc, err)
	}
	eth, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Spot, Symbol: "ETHUSDT"}, false)
	if err != nil || eth.UID != 5 {
		t.Fatalf("eth = %+v %v", eth, err)
	}
}

func TestResolverDefaultDrawIsValid(t *testing.T) {
	ctx := context.Background()
	src := mustFake(t, "Bybit")
	if err := src.Add(bybitSpot("BTC", "BTCUSDT", symbol.Tradable)); err != nil {
		t.Fatal(err)
	}
	if err := src.Add(bybitPerp("BTC", "USDT", "BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot, symbol.Perp}, nil)
	a, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Spot, ExchTicker: "BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Perp, ExchTicker: "BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !symbol.ValidUID(a.UID) || !symbol.ValidUID(b.UID) || a.UID == b.UID {
		t.Fatalf("uids %d %d", a.UID, b.UID)
	}
}

func TestReloadDoesNotPublishHalfTable(t *testing.T) {
	ctx := context.Background()
	src := &genREST{}
	r := mustResolver(t, "Bybit", src, []symbol.Category{symbol.Spot, symbol.Perp}, nil)
	perp, err := r.Get(ctx, symbol.UniversalTicker{Venue: "Bybit", Category: symbol.Perp, Symbol: "BTCUSDT"}, false)
	if err != nil {
		t.Fatal(err)
	}
	perpUID := perp.UID

	var stop atomic.Bool
	var seen [2]atomic.Int32
	errCh := make(chan error, 1)
	fail := func(err error) {
		if err == nil {
			return
		}
		stop.Store(true)
		select {
		case errCh <- err:
		default:
		}
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				snap := r.snap.Load()
				gen, err := consistent(snap)
				if err != nil {
					fail(err)
					return
				}
				seen[gen%2].Add(1)
				list, err := r.List(ctx, "Bybit", symbol.Spot)
				if err != nil {
					fail(err)
					return
				}
				if _, ok := wholeSpot(list); !ok {
					fail(errors.New("torn spot list: " + strings.Join(listedTickers(list), ",")))
					return
				}
				got, err := r.GetByUID(ctx, perpUID, false)
				if err != nil || got.UID != perpUID || got.Ticker.Category != symbol.Perp || got.ExchTicker != "BTCUSDT" {
					fail(errors.New("perp uid changed during reload"))
					return
				}
				rev, err := r.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Spot, ExchTicker: "BTCUSDT"})
				if err != nil && !errors.Is(err, symbol.ErrNotFound) {
					fail(err)
					return
				}
				if err == nil && (rev.Ticker.String() != "Bybit_Spot_BTCUSDT" || rev.ExchTicker != "BTCUSDT") {
					fail(errors.New("reverse returned a mixed instrument"))
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 40 && !stop.Load(); i++ {
			src.set(i)
			r.Invalidate("Bybit")
			if _, err := r.List(ctx, "Bybit", symbol.Spot); err != nil {
				fail(err)
				return
			}
		}
		stop.Store(true)
	}()
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	if seen[0].Load() == 0 || seen[1].Load() == 0 {
		t.Fatalf("readers saw even=%d odd=%d generations", seen[0].Load(), seen[1].Load())
	}
}

func consistent(snap *snapshot) (int, error) {
	if snap == nil {
		return 0, errors.New("nil snapshot")
	}
	spot := snap.list("Bybit", symbol.Spot)
	perp := snap.list("Bybit", symbol.Perp)
	sg, sok := generation(spot)
	pg, pok := generation(perp)
	if !sok || !pok || sg != pg {
		return 0, errors.New("spot and perp generations differ")
	}
	want, ok := wholeSpot(spot)
	if !ok || want != sg%2 {
		return 0, errors.New("torn spot table in snapshot")
	}
	if len(perp) != 1 || perp[0].ExchTicker != "BTCUSDT" || perp[0].Ticker.String() != "Bybit_Perp_BTCUSDT" {
		return 0, errors.New("perp table is not the one BTCUSDT row")
	}
	for _, info := range append(spot, perp...) {
		uid, ok := snap.byTicker[info.Ticker]
		if !ok || uid != info.UID {
			return 0, errors.New("ticker index does not match info")
		}
		exch, ok := snap.byExch[info.Exch()]
		if !ok || exch != info.UID {
			return 0, errors.New("exch index does not match info")
		}
	}
	return sg, nil
}

func wholeSpot(list []symbol.Info) (int, bool) {
	got := map[string]bool{}
	gen, ok := generation(list)
	if !ok {
		return 0, false
	}
	for _, info := range list {
		got[info.ExchTicker] = true
	}
	even := got["BTCUSDT"] && got["ETHUSDT"] && len(got) == 2
	odd := got["SOLUSDT"] && got["XRPUSDT"] && len(got) == 2
	if gen%2 == 0 && even {
		return 0, true
	}
	if gen%2 == 1 && odd {
		return 1, true
	}
	return 0, false
}

func generation(list []symbol.Info) (int, bool) {
	if len(list) == 0 {
		return 0, false
	}
	raw := list[0].Filters["gen"]
	for _, info := range list[1:] {
		if info.Filters["gen"] != raw {
			return 0, false
		}
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return n, true
}

func mustFake(t *testing.T, venue string) *exchangetest.Source {
	t.Helper()
	src, err := exchangetest.New(venue)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func mustResolver(t *testing.T, venue string, rest exchange.PublicREST, cats []symbol.Category, uids Source) *Resolver {
	t.Helper()
	r, err := NewResolver(context.Background(), []VenueListing{{
		Venue: venue, REST: rest, Categories: cats,
	}}, uids)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func bybitSpot(base, exch string, status symbol.Status) symbol.Listing {
	return symbol.Listing{
		Venue: "Bybit", Category: symbol.Spot, Base: base, Quote: "USDT",
		ExchTicker: exch, Status: status,
	}
}

func bybitFuture(base, exch, expiry string) symbol.Listing {
	return symbol.Listing{
		Venue: "Bybit", Category: symbol.Future, Base: base, Quote: "USDT",
		ExchTicker: exch, ExpiryCode: expiry, Status: symbol.Tradable,
		ContractSize: "1", ContractValueCcy: base, PriceCcy: "USDT", Settle: "USDT",
	}
}

func bybitPerp(base, quote, exch string) symbol.Listing {
	return symbol.Listing{
		Venue: "Bybit", Category: symbol.Perp, Base: base, Quote: quote,
		ExchTicker: exch, Status: symbol.Tradable,
		ContractSize: "1", ContractValueCcy: base, PriceCcy: quote, Settle: quote,
	}
}

func mustList(t *testing.T, r *Resolver, venue string, c symbol.Category) []symbol.Info {
	t.Helper()
	list, err := r.List(context.Background(), venue, c)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func listedTickers(list []symbol.Info) []string {
	out := make([]string, len(list))
	for i, info := range list {
		out[i] = info.Ticker.String()
	}
	return out
}

type restStub struct{}

func (restStub) Depth(context.Context, *symbol.Info, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
func (restStub) Klines(context.Context, *symbol.Info, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}
func (restStub) KlinePage() int { return 0 }
func (restStub) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}

type bareREST struct{ restStub }

func (bareREST) Listing(context.Context, symbol.Category) ([]symbol.Listing, error) {
	return nil, nil
}

type sliceREST struct {
	restStub
	rows map[symbol.Category][]symbol.Listing
}

func (s *sliceREST) InProcessUIDs() {}

func (s *sliceREST) Listing(_ context.Context, c symbol.Category) ([]symbol.Listing, error) {
	return append([]symbol.Listing(nil), s.rows[c]...), nil
}

// genREST serves two complete spot generations plus one perp, stamped with the
// same generation. Listing is safe for concurrent use with set.
type genREST struct {
	restStub
	mu  sync.Mutex
	gen int
}

func (g *genREST) InProcessUIDs() {}

func (g *genREST) set(gen int) {
	g.mu.Lock()
	g.gen = gen
	g.mu.Unlock()
}

func (g *genREST) Listing(_ context.Context, c symbol.Category) ([]symbol.Listing, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch c {
	case symbol.Spot:
		return g.spot(), nil
	case symbol.Perp:
		p := bybitPerp("BTC", "USDT", "BTCUSDT")
		p.Filters = map[string]string{"gen": strconv.Itoa(g.gen)}
		return []symbol.Listing{p}, nil
	default:
		return nil, nil
	}
}

func (g *genREST) spot() []symbol.Listing {
	gen := strconv.Itoa(g.gen)
	var rows []symbol.Listing
	if g.gen%2 == 0 {
		rows = []symbol.Listing{bybitSpot("BTC", "BTCUSDT", symbol.Tradable), bybitSpot("ETH", "ETHUSDT", symbol.Tradable)}
	} else {
		rows = []symbol.Listing{bybitSpot("SOL", "SOLUSDT", symbol.Tradable), bybitSpot("XRP", "XRPUSDT", symbol.Tradable)}
	}
	for i := range rows {
		rows[i].Filters = map[string]string{"gen": gen}
	}
	return rows
}
