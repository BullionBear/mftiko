package sym

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
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
)

func TestSymService(t *testing.T) {
	fx := newSymFixture(t)

	t.Run("active get", func(t *testing.T) {
		info := getTicker(t, fx.client, "Bybit_Spot_ETHUSDT", false)
		if info.GetUid() == 0 || info.GetUid() == fx.btc || info.GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE {
			t.Fatalf("eth %+v", info)
		}
		if info.GetContractValueCcy() != "ETH" || info.GetPriceCcy() != "USDT" || info.Settle != nil {
			t.Fatalf("currencies %+v", info)
		}
		if info.GetBase() != "ETH" || info.GetQuote() != "USDT" || info.GetExchTicker() != "ETHUSDT" {
			t.Fatalf("identity %+v", info)
		}
	})

	t.Run("include inactive", func(t *testing.T) {
		callSymErr(t, fx.client, getReq("Bybit_Spot_BTCUSDT", false), CodeNotFound)
		info := getTicker(t, fx.client, "Bybit_Spot_BTCUSDT", true)
		if info.GetUid() != fx.btc || info.GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_DELISTED {
			t.Fatalf("terminal %+v, want uid %d DELISTED", info, fx.btc)
		}
		if info.GetFilters()["price_tick"].GetValue() != "0.5" {
			t.Fatalf("price_tick %+v", info.GetFilters()["price_tick"])
		}
		if bound, ok := info.GetFilters()["max_qty"]; !ok || bound.GetValue() != "" {
			t.Fatalf("max_qty present=%v value %q", ok, bound.GetValue())
		}
	})

	t.Run("get by uid", func(t *testing.T) {
		callSymErr(t, fx.client, &symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: &symv1.GetByUidRequest{}}}, CodeNotFound)
		callSymErr(t, fx.client, uidReq(fx.btc, false), CodeNotFound)
		info := getByUID(t, fx.client, fx.btc, true)
		if info.GetUid() != fx.btc || info.GetUniversalTicker() != "Bybit_Spot_BTCUSDT" {
			t.Fatalf("uid lookup %+v", info)
		}
		eth := getTicker(t, fx.client, "Bybit_Spot_ETHUSDT", false)
		again := getByUID(t, fx.client, eth.GetUid(), false)
		if again.GetUniversalTicker() != "Bybit_Spot_ETHUSDT" || again.GetUid() != eth.GetUid() {
			t.Fatalf("active uid %+v", again)
		}
		callSymErr(t, fx.client, uidReq(9_000_000, false), CodeNotFound)
	})

	t.Run("reverse keeps the original uid", func(t *testing.T) {
		info := reverse(t, fx.client, "Bybit", "Spot", "BTCUSDT")
		if info.GetUid() != fx.btc || info.GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_DELISTED {
			t.Fatalf("reverse terminal %+v, want uid %d", info, fx.btc)
		}
		perp := reverse(t, fx.client, "Bybit", "Perp", "BTCUSDT")
		if perp.GetUid() == fx.btc || perp.GetUniversalTicker() != "Bybit_Perp_BTCUSDT" {
			t.Fatalf("reverse perp %+v", perp)
		}
		if perp.GetContractSize() != "1" || perp.GetSettle() != "USDT" || perp.GetContractValueCcy() != "BTC" || perp.GetPriceCcy() != "USDT" {
			t.Fatalf("perp currencies %+v", perp)
		}
		if perp.GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE {
			t.Fatalf("perp status %v", perp.GetStatus())
		}
		callSymErr(t, fx.client, revReq("Bybit", "Spot", "NOPE"), CodeNotFound)
	})

	t.Run("list pages", func(t *testing.T) {
		active := []string{
			"Bybit_Spot_DOGEUSDT",
			"Bybit_Spot_ETHUSDT",
			"Bybit_Spot_SOLUSDT",
			"Bybit_Spot_XRPUSDT",
		}
		page := listPage(t, fx.client, "Bybit", "Spot", 2, 0, false)
		if page.GetTotal() != 4 || joinTickers(page.GetSymbols()) != join(active[:2]) {
			t.Fatalf("page 0 %+v", tickersOf(page))
		}
		page = listPage(t, fx.client, "Bybit", "Spot", 2, 2, false)
		if page.GetTotal() != 4 || joinTickers(page.GetSymbols()) != join(active[2:]) {
			t.Fatalf("page 1 %+v", tickersOf(page))
		}
		page = listPage(t, fx.client, "Bybit", "Spot", 2, 4, false)
		if page.GetTotal() != 4 || len(page.GetSymbols()) != 0 {
			t.Fatalf("past the end %+v", tickersOf(page))
		}
		page = listPage(t, fx.client, "Bybit", "Spot", 0, 0, false)
		if page.GetTotal() != 4 || len(page.GetSymbols()) != 4 {
			t.Fatalf("default limit %+v total %d", tickersOf(page), page.GetTotal())
		}
		for _, row := range page.GetSymbols() {
			if strings.Contains(row.GetUniversalTicker(), "_Perp_") {
				t.Fatalf("spot page contains %s", row.GetUniversalTicker())
			}
		}

		with := []string{
			"Bybit_Spot_BTCUSDT",
			"Bybit_Spot_DOGEUSDT",
			"Bybit_Spot_ETHUSDT",
			"Bybit_Spot_SOLUSDT",
			"Bybit_Spot_XRPUSDT",
		}
		page = listPage(t, fx.client, "Bybit", "Spot", 2, 0, true)
		if page.GetTotal() != 5 || joinTickers(page.GetSymbols()) != join(with[:2]) {
			t.Fatalf("inactive page 0 %+v total %d", tickersOf(page), page.GetTotal())
		}
		if page.GetSymbols()[0].GetUid() != fx.btc || page.GetSymbols()[0].GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_DELISTED {
			t.Fatalf("first inactive row %+v", page.GetSymbols()[0])
		}
		page = listPage(t, fx.client, "Bybit", "Spot", 2, 2, true)
		if joinTickers(page.GetSymbols()) != join(with[2:4]) || page.GetTotal() != 5 {
			t.Fatalf("inactive page 1 %+v", tickersOf(page))
		}
		page = listPage(t, fx.client, "Bybit", "Spot", 2, 4, true)
		if joinTickers(page.GetSymbols()) != with[4] || page.GetTotal() != 5 {
			t.Fatalf("inactive page 2 %+v", tickersOf(page))
		}

		perp := listPage(t, fx.client, "Bybit", "Perp", 10, 0, false)
		if perp.GetTotal() != 1 || perp.GetSymbols()[0].GetUniversalTicker() != "Bybit_Perp_BTCUSDT" {
			t.Fatalf("perp %+v", tickersOf(perp))
		}
	})

	t.Run("errors", func(t *testing.T) {
		callSymErr(t, fx.client, &symv1.Request{}, CodeInvalid)
		callSymErr(t, fx.client, getReq("", false), CodeInvalid)
		callSymErr(t, fx.client, getReq("not a ticker", false), CodeInvalid)
		callSymErr(t, fx.client, getReq("Bybit_Spot_NOSUCH", false), CodeNotFound)
		callSymErr(t, fx.client, &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
			Venue: "Bybit", Category: "spot", Limit: 1,
		}}}, CodeInvalid)
		callSymErr(t, fx.client, &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
			Venue: "bybit", Category: "Spot", Limit: 1,
		}}}, CodeInvalid)
		callSymErr(t, fx.client, &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
			Venue: "Bybit", Category: "Spot", Limit: uint32(symbol.ListPage) + 1,
		}}}, CodeInvalid)
		callSymErr(t, fx.client, revReq("Bybit", "Spot", ""), CodeInvalid)
		callSymErr(t, fx.client, &symv1.Request{Req: &symv1.Request_Refresh{Refresh: &symv1.RefreshRequest{Venue: "Nope"}}}, CodeInvalid)
	})

	t.Run("bad payload is not a business code", func(t *testing.T) {
		raw, err := nats.Connect(fx.url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(raw.Close)
		msg, err := raw.Request(Subject, []byte{0xff, 0x00}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(msg.Data) < 6 || msg.Data[0] != 0x00 || msg.Data[1] != 1 {
			t.Fatalf("framing %x", msg.Data)
		}
		if code := binary.BigEndian.Uint16(msg.Data[2:4]); code != natsx.CodeBadRequest {
			t.Fatalf("code %d, want bad request", code)
		}
	})

	// The ticker is listed again. The active row is a new uid. The terminal row keeps the old one.
	if err := fx.src.Add(fx.btcRow); err != nil {
		t.Fatal(err)
	}
	refreshed := callSym(t, fx.client, &symv1.Request{Req: &symv1.Request_Refresh{Refresh: &symv1.RefreshRequest{Venue: "Bybit"}}})
	res := refreshed.GetRefresh()
	if res == nil || res.GetFailed()["Bybit/Spot"] != "" || res.GetRefreshed()["Bybit/Spot"] == 0 {
		t.Fatalf("refresh %+v", res)
	}
	again := getTicker(t, fx.client, "Bybit_Spot_BTCUSDT", false)
	if again.GetUid() == fx.btc || again.GetUid() == 0 {
		t.Fatalf("relist uid %d, old %d", again.GetUid(), fx.btc)
	}
	withInactive := getTicker(t, fx.client, "Bybit_Spot_BTCUSDT", true)
	if withInactive.GetUid() != again.GetUid() {
		t.Fatalf("include_inactive returned %d, active is %d", withInactive.GetUid(), again.GetUid())
	}
	old := getByUID(t, fx.client, fx.btc, true)
	if old.GetUid() != fx.btc || old.GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_DELISTED {
		t.Fatalf("old uid %+v", old)
	}
	callSymErr(t, fx.client, uidReq(fx.btc, false), CodeNotFound)
	rev := reverse(t, fx.client, "Bybit", "Spot", "BTCUSDT")
	if rev.GetUid() != again.GetUid() {
		t.Fatalf("reverse after relist %d, want %d", rev.GetUid(), again.GetUid())
	}

	// A second delist leaves two terminal rows. Reverse returns the later one.
	if err := fx.src.Remove(symbol.Spot, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	fx.clock.Add(1_000_000_000)
	if _, err := fx.run.Trigger(context.Background(), "Bybit"); err != nil {
		t.Fatal(err)
	}
	later := reverse(t, fx.client, "Bybit", "Spot", "BTCUSDT")
	if later.GetUid() == fx.btc || later.GetStatus() != symv1.SymbolStatus_SYMBOL_STATUS_DELISTED {
		t.Fatalf("later terminal %+v", later)
	}
	first := getByUID(t, fx.client, fx.btc, true)
	if first.GetUid() != fx.btc {
		t.Fatalf("first terminal uid %d", first.GetUid())
	}

	if err := fx.src.FailPage(0, errors.New("venue down")); err != nil {
		t.Fatal(err)
	}
	failed := callSym(t, fx.client, &symv1.Request{Req: &symv1.Request_Refresh{Refresh: &symv1.RefreshRequest{Venue: "Bybit"}}})
	if !strings.Contains(failed.GetRefresh().GetFailed()["Bybit/Spot"], "venue down") {
		t.Fatalf("failed refresh %+v", failed.GetRefresh())
	}
	if failed.GetRefresh().GetRefreshed()["Bybit/Spot"] != 0 {
		t.Fatalf("failed feed was counted as refreshed: %+v", failed.GetRefresh())
	}
	if getTicker(t, fx.client, "Bybit_Spot_ETHUSDT", false).GetExchTicker() != "ETHUSDT" {
		t.Fatal("failed refresh changed the stored row")
	}
}

func TestSymUnavailable(t *testing.T) {
	fx := newSymFixture(t)
	fx.store.Close()
	callSymErr(t, fx.client, getReq("Bybit_Spot_ETHUSDT", false), CodeUnavailable)
	callSymErr(t, fx.client, &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
		Venue: "Bybit", Category: "Spot", Limit: 1,
	}}}, CodeUnavailable)
}

func TestReplyFits(t *testing.T) {
	small := &symv1.Reply{Result: &symv1.Reply_Info{Info: &symv1.SymbolInfo{UniversalTicker: "Bybit_Spot_BTCUSDT"}}}
	if err := fits(small); err != nil {
		t.Fatal(err)
	}
	huge := &symv1.Reply{Result: &symv1.Reply_Info{Info: &symv1.SymbolInfo{
		UniversalTicker: strings.Repeat("x", maxReplyBytes),
	}}}
	if proto.Size(huge) <= maxReplyBytes {
		t.Fatal("fixture is not over the cap")
	}
	err := fits(huge)
	var ce *codeError
	if !errors.As(err, &ce) || ce.code != CodeUnavailable {
		t.Fatalf("got %v", err)
	}
}

func TestStatusProto(t *testing.T) {
	cases := []struct {
		in   Status
		want symv1.SymbolStatus
	}{
		{Prelisting, symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING},
		{Tradable, symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE},
		{Halted, symv1.SymbolStatus_SYMBOL_STATUS_HALTED},
		{Settling, symv1.SymbolStatus_SYMBOL_STATUS_SETTLING},
		{Expired, symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED},
		{Delisted, symv1.SymbolStatus_SYMBOL_STATUS_DELISTED},
		{Unknown, symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN},
	}
	for _, tc := range cases {
		got, err := statusProto(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("%s -> %v %v", tc.in, got, err)
		}
	}
	if _, err := statusProto(symbol.Unspecified); err == nil {
		t.Fatal("unspecified was accepted")
	}
}

type symFixture struct {
	client *natsx.Conn
	src    *exchangetest.Source
	store  *Store
	run    *Runner
	url    string
	btc    uint32
	btcRow symbol.Listing
	clock  *atomicClock
}

func newSymFixture(t *testing.T) *symFixture {
	t.Helper()
	pg, _ := startSym(t)
	src, err := exchangetest.New("Bybit")
	if err != nil {
		t.Fatal(err)
	}
	btc := spotL("BTC", "USDT", "BTCUSDT")
	btc.Filters = map[string]string{"price_tick": "0.5", "max_qty": ""}
	for _, row := range []symbol.Listing{
		btc,
		spotL("ETH", "USDT", "ETHUSDT"),
		spotL("SOL", "USDT", "SOLUSDT"),
		spotL("XRP", "USDT", "XRPUSDT"),
		spotL("DOGE", "USDT", "DOGEUSDT"),
		perpL("BTC", "USDT", "BTCUSDT"),
	} {
		if err := src.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	clock := &atomicClock{ns: 1_000}
	uids := &safeScript{script: script{uids: uidSeq(1, 32)}}
	st := openStore(t, pg.DSN(), uids)
	run, err := NewRunner(st, []Feed{
		{Venue: "Bybit", Category: symbol.Spot, REST: src},
		{Venue: "Bybit", Category: symbol.Perp, REST: src},
	}, Options{NoJitter: true, Now: clock.Now, Log: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Trigger(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	info, err := st.GetByTicker(context.Background(), "Bybit_Spot_BTCUSDT", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Remove(symbol.Spot, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	clock.Add(1_000_000_000)
	if _, err := run.Trigger(context.Background(), "Bybit"); err != nil {
		t.Fatal(err)
	}
	ns := natsxtest.Start(t)
	server := dialSym(t, ns.URL())
	if _, err := NewService(st, run, zerolog.Nop()).Serve(t.Context(), server); err != nil {
		t.Fatal(err)
	}
	return &symFixture{
		client: dialSym(t, ns.URL()),
		src:    src,
		store:  st,
		run:    run,
		url:    ns.URL(),
		btc:    info.UID,
		btcRow: btc,
		clock:  clock,
	}
}

type atomicClock struct {
	mu sync.Mutex
	ns int64
}

func (c *atomicClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ns += 1_000
	return time.Unix(0, c.ns)
}

func (c *atomicClock) Add(ns int64) {
	c.mu.Lock()
	c.ns += ns
	c.mu.Unlock()
}

type safeScript struct {
	mu sync.Mutex
	script
}

func (s *safeScript) NextUID() (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.script.NextUID()
}

func uidSeq(from, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(from + i)
	}
	return out
}

func dialSym(t *testing.T, url string) *natsx.Conn {
	t.Helper()
	c, err := natsx.Connect(url, natsx.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func callSym(t *testing.T, c *natsx.Conn, req *symv1.Request) *symv1.Reply {
	t.Helper()
	var resp symv1.Reply
	if err := c.Request(t.Context(), Subject, req, &resp); err != nil {
		t.Fatalf("request: %v", err)
	}
	if resp.GetError() != "" || resp.GetReason() != "" {
		t.Fatalf("error fields %q %q", resp.GetError(), resp.GetReason())
	}
	return &resp
}

func callSymErr(t *testing.T, c *natsx.Conn, req *symv1.Request, code string) {
	t.Helper()
	var resp symv1.Reply
	err := c.Request(t.Context(), Subject, req, &resp)
	var re *natsx.ReplyError
	if !errors.As(err, &re) || re.Code != natsx.CodeHandler {
		t.Fatalf("got %v, want %s", err, code)
	}
	got, _, _ := strings.Cut(re.Message, ": ")
	if got != code {
		t.Fatalf("message %q, want %s", re.Message, code)
	}
}

func getReq(ticker string, inactive bool) *symv1.Request {
	return &symv1.Request{Req: &symv1.Request_Get{Get: &symv1.GetRequest{
		UniversalTicker: ticker, IncludeInactive: inactive,
	}}}
}

func uidReq(uid uint32, inactive bool) *symv1.Request {
	return &symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: &symv1.GetByUidRequest{
		Uid: uid, IncludeInactive: inactive,
	}}}
}

func revReq(venue, category, exch string) *symv1.Request {
	return &symv1.Request{Req: &symv1.Request_Reverse{Reverse: &symv1.ReverseRequest{
		Venue: venue, Category: category, ExchTicker: exch,
	}}}
}

func getTicker(t *testing.T, c *natsx.Conn, ticker string, inactive bool) *symv1.SymbolInfo {
	t.Helper()
	info := callSym(t, c, getReq(ticker, inactive)).GetInfo()
	if info == nil {
		t.Fatal("no info")
	}
	return info
}

func getByUID(t *testing.T, c *natsx.Conn, uid uint32, inactive bool) *symv1.SymbolInfo {
	t.Helper()
	info := callSym(t, c, uidReq(uid, inactive)).GetInfo()
	if info == nil {
		t.Fatal("no info")
	}
	return info
}

func reverse(t *testing.T, c *natsx.Conn, venue, category, exch string) *symv1.SymbolInfo {
	t.Helper()
	info := callSym(t, c, revReq(venue, category, exch)).GetInfo()
	if info == nil {
		t.Fatal("no info")
	}
	return info
}

func listPage(t *testing.T, c *natsx.Conn, venue, category string, limit, offset uint32, inactive bool) *symv1.SymbolList {
	t.Helper()
	page := callSym(t, c, &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
		Venue: venue, Category: category, Limit: limit, Offset: offset, IncludeInactive: inactive,
	}}}).GetList()
	if page == nil {
		t.Fatal("no list")
	}
	return page
}

func tickersOf(page *symv1.SymbolList) []string { return splitTickers(joinTickers(page.GetSymbols())) }

func joinTickers(rows []*symv1.SymbolInfo) string {
	parts := make([]string, len(rows))
	for i, row := range rows {
		parts[i] = row.GetUniversalTicker()
	}
	return strings.Join(parts, ",")
}

func splitTickers(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func join(parts []string) string { return strings.Join(parts, ",") }
