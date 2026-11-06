package symtest

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/internal/sym"
)

// Serve starts one protocol's server for api and returns a client of it. It registers its own
// cleanup.
type Serve func(t *testing.T, api sym.API) Client

// Run builds the fixture, serves it with serve, and runs every case through the client. Each
// answer must be what the case expects, and the same, message for message or text for text, as
// the logic layer's own answer (Direct).
func Run(t *testing.T, serve Serve) {
	t.Helper()
	fx := newFixture(t)
	ref := Direct(fx.api)
	client := serve(t, fx.api)
	for _, c := range cases() {
		t.Run(c.name, func(t *testing.T) {
			got, gotErr := c.call(t.Context(), client)
			want, wantErr := c.call(t.Context(), ref)
			c.check(t, got, gotErr)
			same(t, got, gotErr, want, wantErr)
		})
	}
	w, ok := client.(Watcher)
	if !ok {
		return
	}
	refW := ref.(Watcher)
	for _, c := range watchErrors() {
		t.Run(c.name, func(t *testing.T) {
			_, gotErr := open(t, w, c.req)
			_, wantErr := open(t, refW, c.req)
			c.check(t, nil, gotErr)
			same(t, nil, gotErr, nil, wantErr)
		})
	}
	t.Run("watch snapshot then changed", func(t *testing.T) {
		req := &symv1.WatchChangesRequest{Venue: "Bybit"}
		got, err := open(t, w, req)
		if err != nil {
			t.Fatal(err)
		}
		want, err := open(t, refW, req)
		if err != nil {
			t.Fatal(err)
		}
		var tables []string
		var spot int64
		for _, r := range got.snapshot.GetTables() {
			tables = append(tables, r.GetVenue()+"/"+r.GetCategory())
			if r.GetCategory() == "Spot" {
				spot = r.GetRevision()
			}
		}
		if !slices.Equal(tables, []string{"Bybit/Perp", "Bybit/Spot"}) || !proto.Equal(got.snapshot, want.snapshot) {
			t.Fatalf("snapshot %v, the logic layer's %v", got.snapshot, want.snapshot)
		}
		fx.commit(t)
		for _, s := range []*opened{got, want} {
			resp, err := s.stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			ch := resp.GetChanged()
			if ch.GetVenue() != "Bybit" || ch.GetCategory() != "Spot" || ch.GetRevision() != spot+1 {
				t.Fatalf("changed %v, want Bybit/Spot/%d", resp, spot+1)
			}
		}
	})
}

type opened struct {
	stream   Stream
	snapshot *symv1.RevisionList
}

// open starts a watch and reads its snapshot. A failure from either step is the watch's.
func open(t *testing.T, w Watcher, req *symv1.WatchChangesRequest) (*opened, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := w.Watch(ctx, req)
	if err != nil {
		return nil, err
	}
	t.Cleanup(s.Close)
	resp, err := s.Recv()
	if err != nil {
		return nil, err
	}
	if resp.GetSnapshot() == nil {
		t.Fatalf("first message %v, want a snapshot", resp)
	}
	return &opened{stream: s, snapshot: resp.GetSnapshot()}, nil
}

type watchCase struct {
	tcase
	req *symv1.WatchChangesRequest
}

// watchErrors are the filters WatchChanges refuses (PROTOCOL §7.3).
func watchErrors() []watchCase {
	return []watchCase{
		{tcase{name: "watch category without venue", err: `invalid: category "Spot" needs a venue`},
			&symv1.WatchChangesRequest{Category: "Spot"}},
		{tcase{name: "watch venue case", err: `invalid: venue "bybit" is not canonical, use "Bybit"`},
			&symv1.WatchChangesRequest{Venue: "bybit"}},
		{tcase{name: "watch category case", err: `invalid: category "spot" is not canonical, use "Spot"`},
			&symv1.WatchChangesRequest{Venue: "Bybit", Category: "spot"}},
	}
}

type tcase struct {
	name string
	call func(context.Context, Client) (proto.Message, error)
	// ok checks a success. Exactly one of ok, err and errLike is set.
	ok func(*testing.T, proto.Message)
	// err is the whole failure text; its code is the text before ": ".
	err string
	// errLike matches a failure whose text holds a measured size.
	errLike *regexp.Regexp
}

func (c tcase) check(t *testing.T, got proto.Message, err error) {
	t.Helper()
	if c.ok != nil {
		if err != nil {
			t.Fatalf("got %v, want a reply", err)
		}
		c.ok(t, got)
		return
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("got %v (%T), want a business error", err, err)
	}
	code, _, _ := strings.Cut(e.Message, ": ")
	if e.Code != code {
		t.Fatalf("code %q, text %q: the protocol's code does not match the text", e.Code, e.Message)
	}
	if c.err != "" && e.Message != c.err {
		t.Fatalf("got %q, want %q", e.Message, c.err)
	}
	if c.errLike != nil && !c.errLike.MatchString(e.Message) {
		t.Fatalf("got %q, want %s", e.Message, c.errLike)
	}
}

func same(t *testing.T, got proto.Message, gotErr error, want proto.Message, wantErr error) {
	t.Helper()
	if wantErr != nil {
		var g, w *Error
		if !errors.As(gotErr, &g) || !errors.As(wantErr, &w) || *g != *w {
			t.Fatalf("got %v, the logic layer answers %v", gotErr, wantErr)
		}
		return
	}
	if gotErr != nil || !proto.Equal(got, want) {
		t.Fatalf("got %v %v, the logic layer answers %v", got, gotErr, want)
	}
}

func get(ticker string, inactive bool) func(context.Context, Client) (proto.Message, error) {
	return func(ctx context.Context, c Client) (proto.Message, error) {
		return c.Get(ctx, &symv1.GetRequest{UniversalTicker: ticker, IncludeInactive: inactive})
	}
}

func byUID(uid uint32, inactive bool) func(context.Context, Client) (proto.Message, error) {
	return func(ctx context.Context, c Client) (proto.Message, error) {
		return c.GetByUID(ctx, &symv1.GetByUidRequest{Uid: uid, IncludeInactive: inactive})
	}
}

func reverse(venue, category, exch string, inactive bool) func(context.Context, Client) (proto.Message, error) {
	return func(ctx context.Context, c Client) (proto.Message, error) {
		return c.Reverse(ctx, &symv1.ReverseRequest{Venue: venue, Category: category, ExchTicker: exch, IncludeInactive: inactive})
	}
}

func list(venue, category string, limit, offset uint32, inactive bool) func(context.Context, Client) (proto.Message, error) {
	return func(ctx context.Context, c Client) (proto.Message, error) {
		return c.List(ctx, &symv1.ListRequest{Venue: venue, Category: category, Limit: limit, Offset: offset, IncludeInactive: inactive})
	}
}

func revisions(ctx context.Context, c Client) (proto.Message, error) {
	return c.Revisions(ctx, &symv1.RevisionsRequest{})
}

const (
	tradable = symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE
	delisted = symv1.SymbolStatus_SYMBOL_STATUS_DELISTED
)

// row checks one SymbolInfo: which row it is, its state, and when it last changed.
func row(uid uint32, ticker string, status symv1.SymbolStatus, updatedAtNs int64) func(*testing.T, proto.Message) {
	return func(t *testing.T, m proto.Message) {
		t.Helper()
		info, ok := m.(*symv1.SymbolInfo)
		if !ok || info == nil {
			t.Fatalf("got %T, want SymbolInfo", m)
		}
		if info.GetUid() != uid || info.GetUniversalTicker() != ticker || info.GetStatus() != status || info.GetUpdatedAtNs() != updatedAtNs {
			t.Fatalf("got uid %d %s %v @%d, want uid %d %s %v @%d", info.GetUid(), info.GetUniversalTicker(), info.GetStatus(),
				info.GetUpdatedAtNs(), uid, ticker, status, updatedAtNs)
		}
	}
}

// page checks a SymbolList: the total and the uids of the page, in order.
func page(total uint32, uids ...uint32) func(*testing.T, proto.Message) {
	return func(t *testing.T, m proto.Message) {
		t.Helper()
		l, ok := m.(*symv1.SymbolList)
		if !ok || l == nil {
			t.Fatalf("got %T, want SymbolList", m)
		}
		got := make([]uint32, 0, len(l.GetSymbols()))
		for _, s := range l.GetSymbols() {
			got = append(got, s.GetUid())
		}
		if l.GetTotal() != total || !slices.Equal(got, uids) {
			t.Fatalf("got total %d uids %v, want %d %v", l.GetTotal(), got, total, uids)
		}
	}
}

// cases follow PROTOCOL §7.3 "查詢" and the table in fixture.go.
func cases() []tcase {
	const (
		btcT, ethT, xrpT, solT, dogeT = "Bybit_Spot_BTCUSDT", "Bybit_Spot_ETHUSDT", "Bybit_Spot_XRPUSDT", "Bybit_Spot_SOLUSDT", "Bybit_Spot_DOGEUSDT"
	)
	return []tcase{
		// One instrument by ticker. include_inactive reads the terminal table only when the
		// active one has no row.
		{name: "get active", call: get(ethT, false), ok: row(eth, ethT, tradable, 2000)},
		{name: "get relisted", call: get(btcT, false), ok: row(btc, btcT, tradable, 7000)},
		{name: "get active wins over terminal", call: get(btcT, true), ok: row(btc, btcT, tradable, 7000)},
		{name: "get terminal without include_inactive", call: get(solT, false),
			err: "not_found: ticker Bybit_Spot_SOLUSDT not found in active table (include_inactive=false)"},
		{name: "get latest terminal row", call: get(solT, true), ok: row(sol, solT, delisted, 10000)},
		{name: "get terminal tie goes to the greater uid", call: get(dogeT, true), ok: row(dogeOld, dogeT, delisted, 11000)},
		{name: "get absent", call: get("Bybit_Spot_NOSUCH", true),
			err: "not_found: ticker Bybit_Spot_NOSUCH not found in active or terminal table (include_inactive=true)"},
		{name: "get empty ticker", call: get("", false), err: `invalid: ticker "" is not a universal ticker`},
		{name: "get bad ticker", call: get("bybit_spot_BTCUSDT", false), err: `invalid: ticker "bybit_spot_BTCUSDT" is not a universal ticker`},

		// By uid: the row that uid names, never another row of the same ticker.
		{name: "uid 0", call: byUID(0, true), err: "not_found: uid 0 not found; 0 is not a valid uid"},
		{name: "uid active", call: byUID(btc, false), ok: row(btc, btcT, tradable, 7000)},
		{name: "uid terminal without include_inactive", call: byUID(solOld, false),
			err: "not_found: uid 12 not found in active table (include_inactive=false)"},
		{name: "uid terminal", call: byUID(solOld, true), ok: row(solOld, solT, delisted, 8000)},
		{name: "uid absent", call: byUID(9_000_000, true),
			err: "not_found: uid 9000000 not found in active or terminal table (include_inactive=true)"},

		// From the venue's spelling, by the same rules as Get.
		{name: "reverse active wins over terminal", call: reverse("Bybit", "Spot", "BTCUSDT", true), ok: row(btc, btcT, tradable, 7000)},
		{name: "reverse latest terminal row", call: reverse("Bybit", "Spot", "SOLUSDT", true), ok: row(sol, solT, delisted, 10000)},
		{name: "reverse terminal tie", call: reverse("Bybit", "Spot", "DOGEUSDT", true), ok: row(dogeOld, dogeT, delisted, 11000)},
		{name: "reverse terminal without include_inactive", call: reverse("Bybit", "Spot", "SOLUSDT", false),
			err: `not_found: exch_ticker "SOLUSDT" for Bybit/Spot not found in active table (include_inactive=false)`},
		{name: "reverse other category", call: reverse("Bybit", "Perp", "BTCUSDT", false), ok: row(perp, "Bybit_Perp_BTCUSDT", tradable, 500)},
		{name: "reverse empty exch_ticker", call: reverse("Bybit", "Spot", "", false), err: "invalid: exch_ticker is empty"},
		{name: "reverse venue case", call: reverse("bybit", "Spot", "BTCUSDT", false), err: `invalid: venue "bybit" is not canonical, use "Bybit"`},
		{name: "reverse category case", call: reverse("Bybit", "spot", "BTCUSDT", false), err: `invalid: category "spot" is not canonical, use "Spot"`},
		{name: "reverse unknown category", call: reverse("Bybit", "Swap", "BTCUSDT", false),
			err: `invalid: category "Swap" is not one of Spot, Perp, Inverse, Future, Option`},
		{name: "reverse empty category", call: reverse("Bybit", "", "BTCUSDT", false), err: `invalid: category "" is empty`},

		// Pages, by ticker then uid. limit 0 is 500; above 500 is invalid, not clamped.
		{name: "list limit 0", call: list("Bybit", "Spot", 0, 0, false), ok: page(3, btc, eth, xrp)},
		{name: "list limit 500", call: list("Bybit", "Spot", 500, 0, false), ok: page(3, btc, eth, xrp)},
		{name: "list limit 600", call: list("Bybit", "Spot", 600, 0, false), err: "invalid: limit 600 exceeds max 500"},
		{name: "list offset", call: list("Bybit", "Spot", 2, 2, false), ok: page(3, xrp)},
		{name: "list past the end", call: list("Bybit", "Spot", 2, 3, false), ok: page(3)},
		{name: "list include_inactive", call: list("Bybit", "Spot", 0, 0, true),
			ok: page(8, btcOld, btc, doge, dogeOld, eth, sol, solOld, xrp)},
		{name: "list venue with no rows", call: list("Nope", "Spot", 0, 0, false), ok: page(0)},
		{name: "list venue case", call: list("okx", "Spot", 0, 0, false), err: `invalid: venue "okx" is not canonical, use "Okx"`},
		{name: "list empty venue", call: list("", "Spot", 0, 0, false), err: `invalid: venue "" is empty`},
		{name: "list category case", call: list("Bybit", "PERP", 0, 0, false), err: `invalid: category "PERP" is not canonical, use "Perp"`},
		// A page that reaches 1 MiB is unavailable, not cut short, on every protocol (E22-D010).
		{name: "list page over 1 MiB", call: list("Okx", "Spot", 0, 0, false),
			errLike: regexp.MustCompile(`^unavailable: page of 40 rows is \d{7} bytes, exceeds 1 MiB limit; lower limit$`)},
		{name: "list smaller page of the same table", call: list("Okx", "Spot", 10, 0, false), ok: page(bigRows, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009)},

		{name: "revisions", call: revisions, ok: func(t *testing.T, m proto.Message) {
			t.Helper()
			var got []string
			for _, r := range m.(*symv1.RevisionList).GetTables() {
				got = append(got, r.GetVenue()+"/"+r.GetCategory())
				if r.GetRevision() < 1 {
					t.Fatalf("revision %v", r)
				}
			}
			if !slices.Equal(got, []string{"Bybit/Perp", "Bybit/Spot", "Okx/Spot"}) {
				t.Fatalf("tables %v", got)
			}
		}},
	}
}
