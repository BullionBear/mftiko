package symnats

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
	"github.com/BullionBear/mftiko/internal/sym"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/nats_golden.txt from the running service")

const goldenFile = "testdata/nats_golden.txt"

// TestNATSGolden pins the raw bytes of every reply the sym subject sends. An error reply is
// compared byte for byte. A success is compared after a deterministic re-encode, because
// proto.Marshal does not fix the order of map entries (filters, RefreshResult).
func TestNATSGolden(t *testing.T) {
	fx := newGoldenFixture(t)
	type step struct {
		name string
		req  *symv1.Request
		raw  []byte
		do   func(t *testing.T)
	}
	get := func(ticker string, inactive bool) *symv1.Request {
		return &symv1.Request{Req: &symv1.Request_Get{Get: &symv1.GetRequest{UniversalTicker: ticker, IncludeInactive: inactive}}}
	}
	uid := func(uid uint32, inactive bool) *symv1.Request {
		return &symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: &symv1.GetByUidRequest{Uid: uid, IncludeInactive: inactive}}}
	}
	rev := func(venue, category, exch string, inactive bool) *symv1.Request {
		return &symv1.Request{Req: &symv1.Request_Reverse{Reverse: &symv1.ReverseRequest{
			Venue: venue, Category: category, ExchTicker: exch, IncludeInactive: inactive,
		}}}
	}
	list := func(venue, category string, limit, offset uint32, inactive bool) *symv1.Request {
		return &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
			Venue: venue, Category: category, Limit: limit, Offset: offset, IncludeInactive: inactive,
		}}}
	}
	refresh := func(venue string) *symv1.Request {
		return &symv1.Request{Req: &symv1.Request_Refresh{Refresh: &symv1.RefreshRequest{Venue: venue}}}
	}
	revisions := &symv1.Request{Req: &symv1.Request_Revisions{Revisions: &symv1.RevisionsRequest{}}}
	steps := []step{
		{name: "revisions", req: revisions},
		{name: "get_active", req: get("Bybit_Spot_ETHUSDT", false)},
		{name: "get_terminal_missing", req: get("Bybit_Spot_BTCUSDT", false)},
		{name: "get_terminal", req: get("Bybit_Spot_BTCUSDT", true)},
		{name: "get_perp", req: get("Bybit_Perp_BTCUSDT", false)},
		{name: "get_nosuch", req: get("Bybit_Spot_NOSUCH", false)},
		{name: "get_nosuch_inactive", req: get("Bybit_Spot_NOSUCH", true)},
		{name: "get_empty_ticker", req: get("", false)},
		{name: "get_bad_ticker", req: get("not a ticker", false)},
		{name: "get_nil", req: &symv1.Request{Req: &symv1.Request_Get{}}},
		{name: "uid_zero", req: uid(0, false)},
		{name: "uid_terminal_missing", req: uid(1, false)},
		{name: "uid_terminal", req: uid(1, true)},
		{name: "uid_active", req: uid(2, false)},
		{name: "uid_absent", req: uid(9_000_000, false)},
		{name: "uid_absent_inactive", req: uid(9_000_000, true)},
		{name: "reverse_terminal_missing", req: rev("Bybit", "Spot", "BTCUSDT", false)},
		{name: "reverse_terminal", req: rev("Bybit", "Spot", "BTCUSDT", true)},
		{name: "reverse_perp", req: rev("Bybit", "Perp", "BTCUSDT", false)},
		{name: "reverse_nope", req: rev("Bybit", "Spot", "NOPE", true)},
		{name: "reverse_empty_exch", req: rev("Bybit", "Spot", "", false)},
		{name: "reverse_bad_venue", req: rev("bybit", "Spot", "BTCUSDT", false)},
		{name: "reverse_bad_category", req: rev("Bybit", "perp", "BTCUSDT", false)},
		{name: "list_page0", req: list("Bybit", "Spot", 2, 0, false)},
		{name: "list_page1", req: list("Bybit", "Spot", 2, 2, false)},
		{name: "list_past_end", req: list("Bybit", "Spot", 2, 4, false)},
		{name: "list_default_limit", req: list("Bybit", "Spot", 0, 0, false)},
		{name: "list_inactive", req: list("Bybit", "Spot", 0, 0, true)},
		{name: "list_perp", req: list("Bybit", "Perp", 10, 0, false)},
		{name: "list_unknown_venue", req: list("Nope", "Spot", 1, 0, false)},
		{name: "list_category_case", req: list("Bybit", "spot", 1, 0, false)},
		{name: "list_category_unknown", req: list("Bybit", "nope", 1, 0, false)},
		{name: "list_category_empty", req: list("Bybit", "", 1, 0, false)},
		{name: "list_venue_case", req: list("bybit", "Spot", 1, 0, false)},
		{name: "list_venue_bad", req: list("by-bit", "Spot", 1, 0, false)},
		{name: "list_venue_empty", req: list("", "Spot", 1, 0, false)},
		{name: "list_limit", req: list("Bybit", "Spot", 600, 0, false)},
		{name: "empty_request", req: &symv1.Request{}},
		{name: "bad_payload", raw: []byte{0xff, 0x00}},
		{name: "refresh_unknown_venue", req: refresh("Nope")},
		{name: "refresh_venue_case", req: refresh("bybit")},
		{name: "refresh_venue", req: refresh("Bybit")},
		{name: "refresh_all", req: refresh("")},
		{name: "fail_page", do: func(t *testing.T) {
			if err := fx.src.FailPage(0, errors.New("venue down")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "refresh_failed", req: refresh("Bybit")},
		{name: "close_store", do: func(*testing.T) { fx.store.Close() }},
		{name: "unavailable_get", req: get("Bybit_Spot_ETHUSDT", false)},
		{name: "unavailable_uid", req: uid(2, false)},
		{name: "unavailable_reverse", req: rev("Bybit", "Spot", "ETHUSDT", false)},
		{name: "unavailable_list", req: list("Bybit", "Spot", 1, 0, false)},
		{name: "unavailable_revisions", req: revisions},
	}
	var got []string
	for _, s := range steps {
		if s.do != nil {
			s.do(t)
			continue
		}
		data := s.raw
		if s.req != nil {
			b, err := proto.Marshal(s.req)
			if err != nil {
				t.Fatal(err)
			}
			data = b
		}
		msg, err := fx.raw.Request(sym.Subject, data, 5*time.Second)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		got = append(got, s.name+" "+hex.EncodeToString(canonicalReply(t, s.name, msg.Data)))
	}
	text := strings.Join(got, "\n") + "\n"
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenFile, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	b, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(want) != len(got) {
		t.Fatalf("%d replies, golden has %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("reply differs\n got  %s\n want %s", got[i], want[i])
		}
	}
}

// canonicalReply keeps an error reply as sent. natsx frames it with a leading zero byte, which no
// protobuf payload starts with. The one exception is U+00A0: protobuf-go writes it instead of a
// space in its own error text depending on the binary, so a -race build differs from a plain one.
func canonicalReply(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	if len(data) >= 6 && data[0] == 0x00 {
		msg := strings.ReplaceAll(string(data[6:]), "\u00a0", " ")
		out := append([]byte(nil), data[:6]...)
		binary.BigEndian.PutUint16(out[4:6], uint16(len(msg)))
		return append(out, msg...)
	}
	var rep symv1.Reply
	if err := proto.Unmarshal(data, &rep); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(&rep)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return b
}

type goldenFixture struct {
	raw   *nats.Conn
	src   *exchangetest.Source
	store *sym.Store
}

// newGoldenFixture fills the store one feed at a time, so uids and timestamps are the same on
// every run. Runner.Trigger refreshes feeds concurrently; it is only used by the Refresh cases,
// which assign no uid.
func newGoldenFixture(t *testing.T) *goldenFixture {
	t.Helper()
	ctx := context.Background()
	dsn := startPG(t)
	src, err := exchangetest.New("Bybit")
	if err != nil {
		t.Fatal(err)
	}
	btc := goldenSpot("BTC", "USDT", "BTCUSDT")
	btc.Filters = symbol.Filters{"price_tick": 0.5, "max_qty": math.NaN(), "min_notional": 5}
	st, err := sym.Open(ctx, dsn, &seqUIDs{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	spotFeed := sym.Feed{Venue: "Bybit", Category: symbol.Spot, REST: src}
	perpFeed := sym.Feed{Venue: "Bybit", Category: symbol.Perp, REST: src}
	// One listing per refresh, so the uid each row draws does not depend on listing order.
	at := int64(1_000)
	for _, row := range []symbol.Listing{
		btc,
		goldenSpot("ETH", "USDT", "ETHUSDT"),
		goldenSpot("SOL", "USDT", "SOLUSDT"),
		goldenSpot("XRP", "USDT", "XRPUSDT"),
		goldenSpot("DOGE", "USDT", "DOGEUSDT"),
	} {
		if err := src.Add(row); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Refresh(ctx, spotFeed, at, zerolog.Nop()); err != nil {
			t.Fatal(err)
		}
		at += 1_000
	}
	if err := src.Add(symbol.Listing{
		Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT",
		ContractSize: 1, ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT", Status: sym.Tradable,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Refresh(ctx, perpFeed, at, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	if err := src.Remove(symbol.Spot, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Refresh(ctx, spotFeed, 1_000_000_000, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	run, err := sym.NewRunner(st, []sym.Feed{spotFeed, perpFeed}, sym.Options{
		NoJitter: true,
		Now:      func() time.Time { return time.Unix(2, 0) },
		Log:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ns := natsxtest.Start(t)
	server, err := natsx.Connect(ns.URL(), natsx.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	if _, err := Serve(t.Context(), server, sym.NewService(st, run, zerolog.Nop()), zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	raw, err := nats.Connect(ns.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	return &goldenFixture{raw: raw, src: src, store: st}
}

func goldenSpot(base, quote, exch string) symbol.Listing {
	return symbol.Listing{
		Venue: "Bybit", Category: symbol.Spot, Base: base, Quote: quote, ExchTicker: exch, Status: sym.Tradable,
		Filters: symbol.Filters{"price_tick": 0.5},
	}
}

// seqUIDs draws 1, 2, 3, ...
type seqUIDs struct{ n uint32 }

func (s *seqUIDs) NextUID() (uint32, error) {
	s.n++
	return s.n, nil
}
