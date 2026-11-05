package sym

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
)

// fakeReader answers every read with info, rows or revs, or err when set.
type fakeReader struct {
	info  symbol.Info
	rows  []symbol.Info
	total uint32
	revs  []TableRevision
	err   error

	calls  int
	offset int64
	limit  int
}

func (f *fakeReader) GetByTicker(context.Context, string, bool) (symbol.Info, error) {
	f.calls++
	return f.info, f.err
}

func (f *fakeReader) GetByUID(context.Context, uint32, bool) (symbol.Info, error) {
	f.calls++
	return f.info, f.err
}

func (f *fakeReader) List(_ context.Context, _ string, _ symbol.Category, offset int64, limit int, _ bool) ([]symbol.Info, uint32, error) {
	f.calls++
	f.offset, f.limit = offset, limit
	return f.rows, f.total, f.err
}

func (f *fakeReader) Reverse(context.Context, symbol.ExchKey, bool) (symbol.Info, error) {
	f.calls++
	return f.info, f.err
}

func (f *fakeReader) Revisions(context.Context) ([]TableRevision, error) {
	f.calls++
	return f.revs, f.err
}

type fakeTrigger struct {
	reports []Report
	err     error
	calls   int
}

func (f *fakeTrigger) Trigger(context.Context, string) ([]Report, error) {
	f.calls++
	return f.reports, f.err
}

func info(t *testing.T, ticker string, uid uint32) symbol.Info {
	t.Helper()
	tk, err := symbol.Parse(ticker)
	if err != nil {
		t.Fatal(err)
	}
	return symbol.Info{UID: uid, Ticker: tk, ExchTicker: tk.Symbol, Status: Tradable}
}

// wantErr checks the business code and the full text every protocol sends.
func wantErr(t *testing.T, err error, code, text string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("got %v (%T), want *Error", err, err)
	}
	if e.Code != code || err.Error() != text {
		t.Fatalf("got %s %q, want %s %q", e.Code, err.Error(), code, text)
	}
}

func TestServiceValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		call func(API) error
		code string
		text string
	}{
		{"get nil", func(a API) error { _, err := a.Get(ctx, nil); return err },
			CodeInvalid, "invalid: request is empty"},
		{"get empty ticker", func(a API) error { _, err := a.Get(ctx, &symv1.GetRequest{}); return err },
			CodeInvalid, `invalid: ticker "" is not a universal ticker`},
		{"get bad ticker", func(a API) error {
			_, err := a.Get(ctx, &symv1.GetRequest{UniversalTicker: "bybit_spot_BTCUSDT"})
			return err
		}, CodeInvalid, `invalid: ticker "bybit_spot_BTCUSDT" is not a universal ticker`},
		{"uid nil", func(a API) error { _, err := a.GetByUID(ctx, nil); return err },
			CodeInvalid, "invalid: request is empty"},
		{"uid zero", func(a API) error { _, err := a.GetByUID(ctx, &symv1.GetByUidRequest{}); return err },
			CodeNotFound, "not_found: uid 0 not found; 0 is not a valid uid"},
		{"list nil", func(a API) error { _, err := a.List(ctx, nil); return err },
			CodeInvalid, "invalid: request is empty"},
		{"list venue empty", func(a API) error {
			_, err := a.List(ctx, &symv1.ListRequest{Category: "Spot"})
			return err
		}, CodeInvalid, `invalid: venue "" is empty`},
		{"list venue case", func(a API) error {
			_, err := a.List(ctx, &symv1.ListRequest{Venue: "binanceum", Category: "Spot"})
			return err
		}, CodeInvalid, `invalid: venue "binanceum" is not canonical, use "Binanceum"`},
		{"list venue grammar", func(a API) error {
			_, err := a.List(ctx, &symv1.ListRequest{Venue: "by_bit", Category: "Spot"})
			return err
		}, CodeInvalid, `invalid: venue "by_bit" is not canonical`},
		{"list category case", func(a API) error {
			_, err := a.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "PERP"})
			return err
		}, CodeInvalid, `invalid: category "PERP" is not canonical, use "Perp"`},
		{"list category unknown", func(a API) error {
			_, err := a.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Swap"})
			return err
		}, CodeInvalid, `invalid: category "Swap" is not one of Spot, Perp, Inverse, Future, Option`},
		{"list limit", func(a API) error {
			_, err := a.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot", Limit: 501})
			return err
		}, CodeInvalid, "invalid: limit 501 exceeds max 500"},
		{"reverse nil", func(a API) error { _, err := a.Reverse(ctx, nil); return err },
			CodeInvalid, "invalid: request is empty"},
		{"reverse venue", func(a API) error {
			_, err := a.Reverse(ctx, &symv1.ReverseRequest{Venue: "okx", Category: "Spot", ExchTicker: "BTC-USDT"})
			return err
		}, CodeInvalid, `invalid: venue "okx" is not canonical, use "Okx"`},
		{"reverse category", func(a API) error {
			_, err := a.Reverse(ctx, &symv1.ReverseRequest{Venue: "Bybit", ExchTicker: "BTCUSDT"})
			return err
		}, CodeInvalid, `invalid: category "" is empty`},
		{"reverse exch ticker", func(a API) error {
			_, err := a.Reverse(ctx, &symv1.ReverseRequest{Venue: "Bybit", Category: "Spot"})
			return err
		}, CodeInvalid, "invalid: exch_ticker is empty"},
		{"revisions nil", func(a API) error { _, err := a.Revisions(ctx, nil); return err },
			CodeInvalid, "invalid: request is empty"},
		{"refresh nil", func(a API) error { _, err := a.Refresh(ctx, nil); return err },
			CodeInvalid, "invalid: request is empty"},
		{"refresh venue", func(a API) error {
			_, err := a.Refresh(ctx, &symv1.RefreshRequest{Venue: "bybit"})
			return err
		}, CodeInvalid, `invalid: venue "bybit" is not canonical, use "Bybit"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			read := &fakeReader{}
			run := &fakeTrigger{}
			wantErr(t, tc.call(NewService(read, run, zerolog.Nop())), tc.code, tc.text)
			if read.calls != 0 || run.calls != 0 {
				t.Fatalf("a rejected request reached the store: %d reads, %d triggers", read.calls, run.calls)
			}
		})
	}
}

func TestServiceNotFound(t *testing.T) {
	ctx := context.Background()
	for _, absent := range []error{ErrNotFound, symbol.ErrNotFound, fmt.Errorf("wrapped: %w", ErrNotFound)} {
		sv := NewService(&fakeReader{err: absent}, nil, zerolog.Nop())
		_, err := sv.Get(ctx, &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT"})
		wantErr(t, err, CodeNotFound, "not_found: ticker Bybit_Spot_BTCUSDT not found in active table (include_inactive=false)")
		_, err = sv.Get(ctx, &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT", IncludeInactive: true})
		wantErr(t, err, CodeNotFound, "not_found: ticker Bybit_Spot_BTCUSDT not found in active or terminal table (include_inactive=true)")
		_, err = sv.GetByUID(ctx, &symv1.GetByUidRequest{Uid: 7})
		wantErr(t, err, CodeNotFound, "not_found: uid 7 not found in active table (include_inactive=false)")
		_, err = sv.Reverse(ctx, &symv1.ReverseRequest{Venue: "Bybit", Category: "Spot", ExchTicker: "BTCUSDT", IncludeInactive: true})
		wantErr(t, err, CodeNotFound, `not_found: exch_ticker "BTCUSDT" for Bybit/Spot not found in active or terminal table (include_inactive=true)`)
		_, err = sv.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"})
		wantErr(t, err, CodeNotFound, "not_found: list Bybit/Spot not found")
		_, err = sv.Revisions(ctx, &symv1.RevisionsRequest{})
		wantErr(t, err, CodeNotFound, "not_found: revisions not found")
	}
}

func TestServiceUnavailable(t *testing.T) {
	ctx := context.Background()
	t.Run("context", func(t *testing.T) {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			sv := NewService(&fakeReader{err: fmt.Errorf("read: %w", cause)}, nil, zerolog.Nop())
			_, err := sv.Get(ctx, &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT"})
			wantErr(t, err, CodeUnavailable, "unavailable: get ticker Bybit_Spot_BTCUSDT: read: "+cause.Error())
			if !errors.Is(err, cause) {
				t.Fatalf("%v does not wrap %v; an adapter could not map it", err, cause)
			}
		}
	})
	t.Run("sql stays in the cause", func(t *testing.T) {
		query := errors.New("ERROR: SELECT uid FROM symbol_active WHERE ticker = $1")
		sv := NewService(&fakeReader{err: query}, nil, zerolog.Nop())
		_, err := sv.GetByUID(ctx, &symv1.GetByUidRequest{Uid: 9})
		wantErr(t, err, CodeUnavailable, "unavailable: get uid 9: database read failed")
		if !errors.Is(err, query) {
			t.Fatal("cause dropped")
		}
		_, err = sv.Reverse(ctx, &symv1.ReverseRequest{Venue: "Bybit", Category: "Perp", ExchTicker: "BTCUSDT"})
		wantErr(t, err, CodeUnavailable, "unavailable: reverse Bybit/Perp BTCUSDT: database read failed")
		long := NewService(&fakeReader{err: errors.New(strings.Repeat("x", 301))}, nil, zerolog.Nop())
		_, err = long.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"})
		wantErr(t, err, CodeUnavailable, "unavailable: list Bybit/Spot: database read failed")
	})
	t.Run("status", func(t *testing.T) {
		bad := info(t, "Bybit_Spot_BTCUSDT", 3)
		bad.Status = symbol.Unspecified
		sv := NewService(&fakeReader{info: bad, rows: []symbol.Info{bad}}, nil, zerolog.Nop())
		_, err := sv.Get(ctx, &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT"})
		wantErr(t, err, CodeUnavailable, `unavailable: status "" cannot be encoded`)
		_, err = sv.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"})
		wantErr(t, err, CodeUnavailable, `unavailable: status "" cannot be encoded`)
	})
	t.Run("revisions row", func(t *testing.T) {
		sv := NewService(&fakeReader{revs: []TableRevision{{Venue: "bybit", Category: symbol.Spot, Revision: 1}}}, nil, zerolog.Nop())
		_, err := sv.Revisions(ctx, &symv1.RevisionsRequest{})
		wantErr(t, err, CodeUnavailable, `unavailable: revisions: venue "bybit" is not canonical`)
		sv = NewService(&fakeReader{revs: []TableRevision{{Venue: "Bybit", Category: "spot", Revision: 1}}}, nil, zerolog.Nop())
		_, err = sv.Revisions(ctx, &symv1.RevisionsRequest{})
		wantErr(t, err, CodeUnavailable, `unavailable: revisions: category "spot" is not a category`)
	})
}

func TestServiceLogsUnavailableOnce(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	cause := errors.New("connection refused")
	sv := NewService(&fakeReader{err: cause}, nil, zerolog.New(&buf))
	if _, err := sv.Get(ctx, &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT"}); err == nil {
		t.Fatal("no error")
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"message":"sym unavailable"`) ||
		!strings.Contains(lines[0], `"reason":"get ticker Bybit_Spot_BTCUSDT: connection refused"`) ||
		!strings.Contains(lines[0], `"error":"connection refused"`) {
		t.Fatalf("log %q", buf.String())
	}
	buf.Reset()
	if _, err := sv.Get(ctx, &symv1.GetRequest{UniversalTicker: "nope"}); err == nil {
		t.Fatal("no error")
	}
	if _, err := NewService(&fakeReader{err: ErrNotFound}, nil, zerolog.New(&buf)).GetByUID(ctx, &symv1.GetByUidRequest{Uid: 1}); err == nil {
		t.Fatal("no error")
	}
	if buf.Len() != 0 {
		t.Fatalf("invalid and not_found are logged: %q", buf.String())
	}
}

func TestServiceList(t *testing.T) {
	ctx := context.Background()
	read := &fakeReader{rows: []symbol.Info{info(t, "Bybit_Spot_BTCUSDT", 1), info(t, "Bybit_Spot_ETHUSDT", 2)}, total: 9}
	sv := NewService(read, nil, zerolog.Nop())
	page, err := sv.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot", Offset: 4})
	if err != nil {
		t.Fatal(err)
	}
	if read.limit != symbol.ListPage || read.offset != 4 {
		t.Fatalf("store read offset %d limit %d, want 4 and %d", read.offset, read.limit, symbol.ListPage)
	}
	if page.GetTotal() != 9 || len(page.GetSymbols()) != 2 || page.GetSymbols()[1].GetUid() != 2 {
		t.Fatalf("page %v", page)
	}
}

// TestServiceListPageCap measures SymbolList, the same message on every protocol (E22-D010).
func TestServiceListPageCap(t *testing.T) {
	ctx := context.Background()
	row := func(uid uint32, exch int) symbol.Info {
		in := info(t, "Bybit_Spot_BTCUSDT", uid)
		in.ExchTicker = strings.Repeat("x", exch)
		return in
	}
	pageSize := func(rows []symbol.Info) int {
		list := &symv1.SymbolList{Total: uint32(len(rows))}
		for _, r := range rows {
			msg, err := infoMessage(r)
			if err != nil {
				t.Fatal(err)
			}
			list.Symbols = append(list.Symbols, msg)
		}
		return proto.Size(list)
	}
	over := []symbol.Info{row(1, 600_000), row(2, 600_000)}
	n := pageSize(over)
	sv := NewService(&fakeReader{rows: over, total: 2}, nil, zerolog.Nop())
	_, err := sv.List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"})
	wantErr(t, err, CodeUnavailable, fmt.Sprintf("unavailable: page of 2 rows is %d bytes, exceeds 1 MiB limit; lower limit", n))

	// Grow one row until the page is exactly one byte under 1 MiB: that page is sent.
	exch := 1<<20 - 1 - pageSize([]symbol.Info{row(1, 0)})
	for pageSize([]symbol.Info{row(1, exch)}) > 1<<20-1 {
		exch--
	}
	under := []symbol.Info{row(1, exch)}
	if got := pageSize(under); got != 1<<20-1 {
		t.Fatalf("fixture page is %d bytes", got)
	}
	if _, err := NewService(&fakeReader{rows: under, total: 1}, nil, zerolog.Nop()).List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"}); err != nil {
		t.Fatalf("page under 1 MiB: %v", err)
	}
	at := []symbol.Info{row(1, exch+1)}
	_, err = NewService(&fakeReader{rows: at, total: 1}, nil, zerolog.Nop()).List(ctx, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"})
	wantErr(t, err, CodeUnavailable, fmt.Sprintf("unavailable: page of 1 rows is %d bytes, exceeds 1 MiB limit; lower limit", 1<<20))
}

func TestServiceRefresh(t *testing.T) {
	ctx := context.Background()
	_, err := NewService(&fakeReader{}, nil, zerolog.Nop()).Refresh(ctx, &symv1.RefreshRequest{})
	wantErr(t, err, CodeUnavailable, "unavailable: refresh is not served by this process")

	unknown := &fakeTrigger{err: fmt.Errorf("%w: Nope", ErrUnknownVenue)}
	_, err = NewService(&fakeReader{}, unknown, zerolog.Nop()).Refresh(ctx, &symv1.RefreshRequest{Venue: "Nope"})
	wantErr(t, err, CodeInvalid, "invalid: venue Nope is not refreshed by this process")

	boom := errors.New("boom")
	_, err = NewService(&fakeReader{}, &fakeTrigger{err: boom}, zerolog.Nop()).Refresh(ctx, &symv1.RefreshRequest{Venue: "Bybit"})
	wantErr(t, err, CodeUnavailable, `unavailable: refresh venue "Bybit": boom`)
	if !errors.Is(err, boom) {
		t.Fatal("cause dropped")
	}
	_, err = NewService(&fakeReader{}, &fakeTrigger{err: boom}, zerolog.Nop()).Refresh(ctx, &symv1.RefreshRequest{})
	wantErr(t, err, CodeUnavailable, "unavailable: refresh: boom")

	run := &fakeTrigger{reports: []Report{
		{Venue: "Bybit", Category: symbol.Spot, Seen: 4, Delisted: 1, Expired: 2},
		{Venue: "Bybit", Category: symbol.Perp, Err: errors.New("venue down")},
		{Venue: "Bybit", Category: symbol.Inverse, Seen: math.MaxUint32 + 1},
	}}
	res, err := NewService(&fakeReader{}, run, zerolog.Nop()).Refresh(ctx, &symv1.RefreshRequest{Venue: "Bybit"})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRefreshed()["Bybit/Spot"] != 4 || res.GetDeactivated()["Bybit/Spot"] != 3 {
		t.Fatalf("spot %v", res)
	}
	if res.GetFailed()["Bybit/Perp"] != "venue down" || len(res.GetFailed()) != 1 {
		t.Fatalf("failed %v", res.GetFailed())
	}
	if _, ok := res.GetRefreshed()["Bybit/Perp"]; ok {
		t.Fatal("a failed feed was counted as refreshed")
	}
	if res.GetRefreshed()["Bybit/Inverse"] != math.MaxUint32 {
		t.Fatalf("count did not saturate: %d", res.GetRefreshed()["Bybit/Inverse"])
	}
}

func TestErrorText(t *testing.T) {
	cases := []struct {
		err  *Error
		want string
	}{
		{nil, "unavailable"},
		{&Error{}, "unavailable"},
		{&Error{Code: CodeNotFound}, "not_found"},
		{&Error{Code: CodeInvalid, Reason: "limit 600 exceeds max 500", Cause: errors.New("hidden")}, "invalid: limit 600 exceeds max 500"},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Fatalf("got %q, want %q", got, tc.want)
		}
	}
}
