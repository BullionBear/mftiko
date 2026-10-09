package exchangetest_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

func newSource(t *testing.T) *exchangetest.Source {
	t.Helper()
	s, err := exchangetest.New("Bybit")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func inst(cat symbol.Category, base, ticker string) symbol.Listing {
	return symbol.Listing{
		Venue:        "Bybit",
		Category:     cat,
		Base:         base,
		Quote:        "USDT",
		ExchTicker:   ticker,
		ContractSize: 1,
		Settle:       "USDT",
		Status:       symbol.Tradable,
		Filters:      symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001},
	}
}

func mustAdd(t *testing.T, s *exchangetest.Source, ls ...symbol.Listing) {
	t.Helper()
	for _, l := range ls {
		if err := s.Add(l); err != nil {
			t.Fatalf("add %s %s: %v", l.Category, l.ExchTicker, err)
		}
	}
}

func listed(t *testing.T, s *exchangetest.Source, c symbol.Category) []symbol.Listing {
	t.Helper()
	got, err := s.Listing(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func equalList(t *testing.T, got, want []symbol.Listing) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
}

func TestNew(t *testing.T) {
	for _, tc := range []struct {
		name  string
		venue string
		ok    bool
	}{
		{name: "empty", venue: ""},
		{name: "bybit", venue: "Bybit", ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := exchangetest.New(tc.venue)
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v, ok = %v", err, tc.ok)
			}
			if tc.ok && s == nil {
				t.Fatal("nil source")
			}
		})
	}
}

func TestAddRemoveModify(t *testing.T) {
	btcPerp := inst(symbol.Perp, "BTC", "BTCUSDT")
	ethPerp := inst(symbol.Perp, "ETH", "ETHUSDT")
	btcSpot := inst(symbol.Spot, "BTC", "BTCUSDT")

	for _, tc := range []struct {
		name   string
		add    []symbol.Listing
		remove []struct {
			c      symbol.Category
			ticker string
		}
		modify  []symbol.Listing
		wantCat symbol.Category
		want    []symbol.Listing
		alsoCat symbol.Category
		also    []symbol.Listing
	}{
		{
			name:    "add lists one category",
			add:     []symbol.Listing{btcPerp},
			wantCat: symbol.Perp,
			want:    []symbol.Listing{btcPerp},
			alsoCat: symbol.Spot,
			also:    []symbol.Listing{},
		},
		{
			name:    "same ticker in two categories",
			add:     []symbol.Listing{btcPerp, btcSpot},
			wantCat: symbol.Spot,
			want:    []symbol.Listing{btcSpot},
			alsoCat: symbol.Perp,
			also:    []symbol.Listing{btcPerp},
		},
		{
			name: "remove drops only that category",
			add:  []symbol.Listing{btcPerp, btcSpot, ethPerp},
			remove: []struct {
				c      symbol.Category
				ticker string
			}{{symbol.Perp, "BTCUSDT"}},
			wantCat: symbol.Perp,
			want:    []symbol.Listing{ethPerp},
			alsoCat: symbol.Spot,
			also:    []symbol.Listing{btcSpot},
		},
		{
			name: "modify replaces the instrument and clears a zero field",
			add:  []symbol.Listing{btcPerp, ethPerp},
			modify: []symbol.Listing{{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
				ExchTicker: "BTCUSDT", ContractSize: 0.01, Settle: "USDC", Status: symbol.Halted,
				Filters: symbol.Filters{symbol.MinQty: 1},
			}},
			wantCat: symbol.Perp,
			want: []symbol.Listing{
				{
					Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
					ExchTicker: "BTCUSDT", ContractSize: 0.01, Settle: "USDC", Status: symbol.Halted,
					Filters: symbol.Filters{symbol.MinQty: 1},
				},
				ethPerp,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSource(t)
			mustAdd(t, s, tc.add...)
			for _, r := range tc.remove {
				if err := s.Remove(r.c, r.ticker); err != nil {
					t.Fatal(err)
				}
			}
			for _, l := range tc.modify {
				if err := s.Modify(l); err != nil {
					t.Fatal(err)
				}
			}
			equalList(t, listed(t, s, tc.wantCat), tc.want)
			if tc.alsoCat != "" {
				equalList(t, listed(t, s, tc.alsoCat), tc.also)
			}
		})
	}
}

func TestAddRemoveModifyErrors(t *testing.T) {
	btc := inst(symbol.Perp, "BTC", "BTCUSDT")
	for _, tc := range []struct {
		name   string
		have   bool
		op     string
		l      symbol.Listing
		cat    symbol.Category
		ticker string
		want   error
		substr string
	}{
		{name: "duplicate add", have: true, op: "add", l: btc, want: exchangetest.ErrDuplicate},
		{name: "wrong venue", op: "add", l: func() symbol.Listing { l := btc; l.Venue = "Okx"; return l }(), want: exchangetest.ErrVenueMismatch},
		{name: "empty category", op: "add", l: symbol.Listing{ExchTicker: "BTCUSDT"}, substr: "category is empty"},
		{name: "empty ticker", op: "add", l: symbol.Listing{Category: symbol.Perp}, substr: "exch ticker is empty"},
		{name: "remove missing", op: "remove", cat: symbol.Perp, ticker: "BTCUSDT", want: exchangetest.ErrNotFound},
		{name: "remove empty category", op: "remove", ticker: "BTCUSDT", substr: "category is empty"},
		{name: "remove empty ticker", op: "remove", cat: symbol.Perp, substr: "exch ticker is empty"},
		{name: "modify missing", op: "modify", l: btc, want: exchangetest.ErrNotFound},
		{name: "modify other category is a different instrument", have: true, op: "modify", l: inst(symbol.Spot, "BTC", "BTCUSDT"), want: exchangetest.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSource(t)
			if tc.have {
				mustAdd(t, s, btc)
			}
			var err error
			switch tc.op {
			case "add":
				err = s.Add(tc.l)
			case "remove":
				err = s.Remove(tc.cat, tc.ticker)
			case "modify":
				err = s.Modify(tc.l)
			default:
				t.Fatalf("op %q", tc.op)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if tc.substr != "" && (err == nil || !strings.Contains(err.Error(), tc.substr)) {
				t.Fatalf("got %v, want substring %q", err, tc.substr)
			}
			if tc.want == nil && tc.substr == "" {
				t.Fatal("case expects no error")
			}
			if tc.have {
				equalList(t, listed(t, s, symbol.Perp), []symbol.Listing{btc})
			} else if got := listed(t, s, symbol.Perp); len(got) != 0 {
				t.Fatalf("book changed: %#v", got)
			}
		})
	}
}

func TestAddFillsVenueAndCopies(t *testing.T) {
	s := newSource(t)
	l := inst(symbol.Perp, "BTC", "BTCUSDT")
	l.Venue = ""
	if err := s.Add(l); err != nil {
		t.Fatal(err)
	}
	l.Venue = "Okx"
	l.Status = symbol.Halted
	l.Filters[symbol.PriceTick] = 99
	l.Filters["extra"] = 1

	got := listed(t, s, symbol.Perp)
	want := inst(symbol.Perp, "BTC", "BTCUSDT")
	equalList(t, got, []symbol.Listing{want})

	got[0].Status = symbol.Halted
	got[0].Filters[symbol.PriceTick] = 98
	equalList(t, listed(t, s, symbol.Perp), []symbol.Listing{want})

	// Modify stores a copy too: mutating the argument afterwards does not stick.
	changed := want
	changed.Status = symbol.Halted
	changed.Filters = symbol.Filters{symbol.MinQty: 1}
	if err := s.Modify(changed); err != nil {
		t.Fatal(err)
	}
	changed.Status = symbol.Tradable
	changed.Filters[symbol.MinQty] = 97
	equalList(t, listed(t, s, symbol.Perp), []symbol.Listing{{
		Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
		ExchTicker: "BTCUSDT", ContractSize: 1, Settle: "USDT", Status: symbol.Halted,
		Filters: symbol.Filters{symbol.MinQty: 1},
	}})
}

func TestSourcesAreIndependent(t *testing.T) {
	a := newSource(t)
	b, err := exchangetest.New("Okx")
	if err != nil {
		t.Fatal(err)
	}
	mustAdd(t, a, inst(symbol.Perp, "BTC", "BTCUSDT"))
	okx := inst(symbol.Perp, "BTC", "BTCUSDT")
	okx.Venue = "Okx"
	mustAdd(t, b, okx)
	if got := listed(t, a, symbol.Perp); len(got) != 1 || got[0].Venue != "Bybit" {
		t.Fatalf("bybit = %#v", got)
	}
	if got := listed(t, b, symbol.Perp); len(got) != 1 || got[0].Venue != "Okx" {
		t.Fatalf("okx = %#v", got)
	}
}

func TestPageSize(t *testing.T) {
	// Inserted out of order. Every successful page size returns the same sorted list.
	added := []symbol.Listing{
		inst(symbol.Perp, "E", "EEE"),
		inst(symbol.Perp, "C", "CCC"),
		inst(symbol.Perp, "A", "AAA"),
		inst(symbol.Perp, "D", "DDD"),
		inst(symbol.Perp, "B", "BBB"),
	}
	want := []symbol.Listing{
		inst(symbol.Perp, "A", "AAA"),
		inst(symbol.Perp, "B", "BBB"),
		inst(symbol.Perp, "C", "CCC"),
		inst(symbol.Perp, "D", "DDD"),
		inst(symbol.Perp, "E", "EEE"),
	}
	for _, tc := range []struct {
		name string
		size int
		bad  bool
	}{
		{name: "default one page", size: 0},
		{name: "one each", size: 1},
		{name: "pairs", size: 2},
		{name: "exact", size: 5},
		{name: "larger than the list", size: 6},
		{name: "max int is still one page", size: math.MaxInt},
		{name: "negative is rejected", size: -1, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSource(t)
			mustAdd(t, s, added...)
			// A previous size stays in place when the new one is rejected. Page 1 of a one-per-page
			// read is reached; page 1 of a single page is not. The failure below distinguishes them.
			if err := s.SetPageSize(1); err != nil {
				t.Fatal(err)
			}
			err := s.SetPageSize(tc.size)
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "page size is negative") {
					t.Fatalf("got %v", err)
				}
				boom := errors.New("page 1")
				if err := s.FailPage(1, boom); err != nil {
					t.Fatal(err)
				}
				_, err := s.Listing(context.Background(), symbol.Perp)
				if !errors.Is(err, boom) {
					t.Fatalf("rejected size took effect: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			equalList(t, listed(t, s, symbol.Perp), want)
			// A size that covers the list is one page, so page 1 is never read. This is also what
			// catches a page width that overflowed int and walked off the slice.
			if tc.size <= 0 || tc.size >= len(want) {
				if err := s.FailPage(1, errors.New("not read")); err != nil {
					t.Fatal(err)
				}
				equalList(t, listed(t, s, symbol.Perp), want)
			}
			// Spot was not paged into the perp result, and paging perp did not create spot rows.
			if got := listed(t, s, symbol.Spot); len(got) != 0 || got == nil {
				t.Fatalf("spot = %#v", got)
			}
		})
	}
}

func TestFailPage(t *testing.T) {
	boom := errors.New("page down")
	other := errors.New("other")
	items := []symbol.Listing{
		inst(symbol.Perp, "A", "AAA"),
		inst(symbol.Perp, "B", "BBB"),
		inst(symbol.Perp, "C", "CCC"),
		inst(symbol.Perp, "D", "DDD"),
		inst(symbol.Perp, "E", "EEE"),
	}
	for _, tc := range []struct {
		name     string
		n        int
		size     int
		page     int
		set      bool
		err      error
		prev     error
		clear    bool
		badPage  bool
		wantErr  error
		emptyCat bool
	}{
		{name: "middle page of three", n: 5, size: 2, page: 1, set: true, err: boom, wantErr: boom},
		{name: "last partial page", n: 5, size: 2, page: 2, set: true, err: boom, wantErr: boom},
		{name: "page past the end is not read", n: 5, size: 2, page: 3, set: true, err: boom},
		{name: "single page ignores a later index", n: 5, size: 0, page: 1, set: true, err: boom},
		{name: "page size one reaches the last instrument", n: 5, size: 1, page: 4, set: true, err: boom, wantErr: boom},
		{name: "empty category is page zero", n: 0, size: 2, page: 0, set: true, err: boom, wantErr: boom, emptyCat: true},
		{name: "empty category does not read page one", n: 0, size: 2, page: 1, set: true, err: boom, emptyCat: true},
		{name: "cleared page succeeds", n: 5, size: 2, page: 1, set: true, err: boom, clear: true},
		{name: "later error replaces the earlier one", n: 1, size: 0, page: 0, set: true, prev: boom, err: other, wantErr: other},
		{name: "negative page is rejected", n: 1, size: 0, page: -1, badPage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSource(t)
			mustAdd(t, s, items[:tc.n]...)
			if err := s.SetPageSize(tc.size); err != nil {
				t.Fatal(err)
			}
			if tc.prev != nil {
				if err := s.FailPage(tc.page, tc.prev); err != nil {
					t.Fatal(err)
				}
			}
			err := s.FailPage(tc.page, tc.err)
			if tc.badPage {
				if err == nil || !strings.Contains(err.Error(), "page is negative") {
					t.Fatalf("FailPage: %v", err)
				}
			} else if !tc.set {
				t.Fatal("case does not set a page")
			} else if err != nil {
				t.Fatal(err)
			}
			if tc.clear {
				if err := s.FailPage(tc.page, nil); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.Listing(context.Background(), symbol.Perp)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || got != nil {
					t.Fatalf("got %v rows %v, want %v and no rows", err, got, tc.wantErr)
				}
				if tc.wantErr != boom && errors.Is(err, boom) {
					t.Fatalf("old page error still returned: %v", err)
				}
				// The failed read did not drop stored instruments.
				if err := s.FailPage(tc.page, nil); err != nil {
					t.Fatal(err)
				}
				equalList(t, listed(t, s, symbol.Perp), items[:tc.n])
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.emptyCat {
				if got == nil || len(got) != 0 {
					t.Fatalf("empty category = %#v", got)
				}
				return
			}
			equalList(t, got, items[:tc.n])
		})
	}
}

func TestFailPageIsPerRead(t *testing.T) {
	// One hook applies to whichever category is being read. Spot and perp do not share rows, so a
	// failed spot read leaves the perp book returnable once the page is cleared.
	s := newSource(t)
	mustAdd(t, s, inst(symbol.Perp, "BTC", "BTCUSDT"), inst(symbol.Spot, "BTC", "BTCUSDT"))
	boom := errors.New("spot down")
	if err := s.FailPage(0, boom); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Listing(context.Background(), symbol.Spot); !errors.Is(err, boom) {
		t.Fatalf("spot: %v", err)
	}
	if _, err := s.Listing(context.Background(), symbol.Perp); !errors.Is(err, boom) {
		t.Fatalf("perp: %v", err)
	}
	if err := s.FailPage(0, nil); err != nil {
		t.Fatal(err)
	}
	if got := listed(t, s, symbol.Perp); len(got) != 1 || got[0].Category != symbol.Perp {
		t.Fatalf("perp after clear = %#v", got)
	}
	if got := listed(t, s, symbol.Spot); len(got) != 1 || got[0].Category != symbol.Spot {
		t.Fatalf("spot after clear = %#v", got)
	}
}

func TestListingContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  func() context.Context
		want error
	}{
		{
			name: "canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			want: context.Canceled,
		},
		{
			name: "deadline already passed",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
				t.Cleanup(cancel)
				return ctx
			},
			want: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSource(t)
			mustAdd(t, s, inst(symbol.Perp, "BTC", "BTCUSDT"))
			got, err := s.Listing(tc.ctx(), symbol.Perp)
			if !errors.Is(err, tc.want) || got != nil {
				t.Fatalf("got %v rows %#v", err, got)
			}
			equalList(t, listed(t, s, symbol.Perp), []symbol.Listing{inst(symbol.Perp, "BTC", "BTCUSDT")})
		})
	}
}

func TestUnimplementedReads(t *testing.T) {
	s := newSource(t)
	var rest exchange.PublicREST = s
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{name: "depth", call: func() error { _, err := rest.Depth(ctx, nil, 10); return err }},
		{name: "klines", call: func() error { _, err := rest.Klines(ctx, nil, "1m", 10); return err }},
		{name: "open interest", call: func() error { _, err := rest.OpenInterest(ctx, nil); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, exchange.ErrNotImplemented) {
				t.Fatalf("got %v", err)
			}
		})
	}
	if rest.KlinePage() != 0 {
		t.Fatalf("KlinePage = %d", rest.KlinePage())
	}
}

func TestConcurrentUse(t *testing.T) {
	s := newSource(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ticker := string(rune('A' + i))
			l := inst(symbol.Perp, ticker, ticker)
			if err := s.Add(l); err != nil {
				t.Error(err)
				return
			}
			if _, err := s.Listing(context.Background(), symbol.Perp); err != nil {
				t.Error(err)
			}
			l.Status = symbol.Halted
			if err := s.Modify(l); err != nil {
				t.Error(err)
				return
			}
			if err := s.SetPageSize(i % 3); err != nil {
				t.Error(err)
			}
			// Page 100 is past anything eight instruments can reach, so this does not fail Listing.
			if err := s.FailPage(100, errors.New("x")); err != nil {
				t.Error(err)
			}
			if err := s.FailPage(100, nil); err != nil {
				t.Error(err)
			}
			if err := s.Remove(symbol.Perp, ticker); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := s.SetPageSize(0); err != nil {
		t.Fatal(err)
	}
	// A page failure installed by a goroutine is cleared above. The book is empty.
	got := listed(t, s, symbol.Perp)
	if len(got) != 0 || got == nil {
		t.Fatalf("left %#v", got)
	}
}
