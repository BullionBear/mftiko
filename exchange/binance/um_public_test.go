package binance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

func TestUMBookSync(t *testing.T) {
	bs := umBook{}
	if bs.Snapshot() != exchange.SnapshotREST || bs.Depth() != 1000 {
		t.Fatalf("snapshot %v depth %d", bs.Snapshot(), bs.Depth())
	}
	seq := exchange.BookSeq{Last: 100, Valid: true}
	up := func(first, last, prev uint64) *exchange.BookUpdate {
		return &exchange.BookUpdate{First: first, Last: last, Prev: prev}
	}
	if bs.Check(seq, up(90, 95, 80)) != exchange.BookSkip {
		t.Fatal("an update already inside the snapshot was not skipped")
	}
	if bs.Check(seq, up(96, 110, 95)) != exchange.BookApply {
		t.Fatal("the update that overlaps the snapshot was not applied")
	}
	seq.Last = 110
	if bs.Check(seq, up(111, 120, 110)) != exchange.BookApply {
		t.Fatal("pu chain was not applied")
	}
	if bs.Check(seq, up(121, 130, 120)) != exchange.BookResync {
		t.Fatal("a dropped update was not a resync")
	}
	if bs.Check(exchange.BookSeq{}, up(1, 2, 0)) != exchange.BookResync {
		t.Fatal("a check before the snapshot was applied")
	}
}

func TestUMDecode(t *testing.T) {
	s := &session{}
	var got capture
	agg := `{"e":"aggTrade","E":10,"T":9,"s":"BTCUSDT","p":"1.50","q":"2","a":7,"f":3,"l":5,"m":true,"st":1}`
	if err := s.Decode([]byte(agg), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.aggs) != 1 || got.aggs[0].ch != "btcusdt@aggTrade" || got.aggs[0].side != exchange.SideSell ||
		got.aggs[0].id != "7" || got.aggs[0].n != 3 || got.aggs[0].px != 1.5 {
		t.Fatalf("agg %+v", got.aggs)
	}
	cm := `{"e":"bookTicker","u":1,"s":"XRPUSD_PERP","b":"1","B":"1","a":"2","A":"1","T":1,"E":1,"st":2}`
	got = capture{}
	if err := s.Decode([]byte(cm), &got); err != nil || got.quotes != 0 {
		t.Fatalf("CM row: quotes %d err %v", got.quotes, err)
	}
	arr := `[{"e":"24hrMiniTicker","E":1,"s":"BTCUSDT","c":"3","o":"2","h":"4","l":"1","v":"5","q":"6","st":1},` +
		`{"e":"24hrMiniTicker","E":1,"s":"XRPUSD_PERP","c":"1","o":"1","h":"1","l":"1","v":"1","q":"1","st":2}]`
	got = capture{}
	if err := s.Decode([]byte(arr), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.tickers) != 1 || got.tickers[0].ch != "btcusdt@miniTicker" || decimal.Has(got.tickers[0].pct) {
		t.Fatalf("mini %+v", got.tickers)
	}
	wrapped := `{"stream":"btcusdt@depth@500ms","data":{"e":"depthUpdate","E":2,"T":1,"s":"BTCUSDT","U":10,"u":12,"pu":9,"b":[["1","2"]],"a":[],"st":1}}`
	got = capture{}
	if err := s.Decode([]byte(wrapped), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.books) != 1 || got.books[0].ch != "btcusdt@depth@500ms" || got.books[0].first != 10 || got.books[0].prev != 9 {
		t.Fatalf("depth %+v", got.books)
	}
	k := `{"e":"kline","E":5,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1M","o":"1","c":"2","h":"3","l":"1","v":"4","q":"8","x":true}}`
	got = capture{}
	if err := s.Decode([]byte(k), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.klines) != 1 || got.klines[0].iv != "1mo" || !got.klines[0].closed || got.klines[0].q != 8 {
		t.Fatalf("kline %+v", got.klines)
	}
	liq := `{"e":"forceOrder","E":3,"o":{"s":"BTCUSDT","S":"SELL","q":"1.5","p":"10","ap":"11","T":3}}`
	got = capture{}
	if err := s.Decode([]byte(liq), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.liqs) != 1 || got.liqs[0].side != exchange.SideBuy || got.liqs[0].px != 11 {
		t.Fatalf("liq %+v", got.liqs)
	}
	mark := `{"e":"markPriceUpdate","E":4,"s":"BTCUSDT","p":"10","i":"9","r":"-0.0001","T":8,"st":1}`
	got = capture{}
	if err := s.Decode([]byte(mark), &got); err != nil {
		t.Fatal(err)
	}
	if got.marks != 1 || got.funds != 1 || got.rate != -0.0001 {
		t.Fatalf("mark %d fund %d rate %v", got.marks, got.funds, got.rate)
	}
	got = capture{}
	if err := s.Decode([]byte(`{"result":null,"id":1}`), &got); err != nil || got.acks != 1 {
		t.Fatalf("ack %d %v", got.acks, err)
	}
	if err := s.Decode([]byte(`{"conformance":"no"}`), &got); err == nil {
		t.Fatal("an unknown frame was accepted")
	}
	if err := s.Decode([]byte(`["conformance",1]`), &capture{}); err == nil {
		t.Fatal("an unknown array was accepted")
	}
}

// umTradeEvent is one UM trade event: BTCUSDT 2 at 100.5, the buyer the maker, so the taker sold.
func umTradeEvent(id int, x string) string {
	return fmt.Sprintf(`{"e":"trade","E":2000,"T":1000,"s":"BTCUSDT","t":%d,"p":"100.5","q":"2","X":"%s","m":true,"st":1}`, id, x)
}

// TestUMTrade: only X MARKET is sent, and t is checked on every event before that filter
// (E03-D005). A dropped trade is therefore not a gap, and a missing t is one whatever X is.
func TestUMTrade(t *testing.T) {
	s := &session{}
	decode := func(frame string) ([]tradeGot, error) {
		var got capture
		err := s.Decode([]byte(frame), &got)
		return got.trades, err
	}
	got, err := decode(umTradeEvent(1001, "MARKET"))
	want := tradeGot{ch: "btcusdt@trade", id: "1001", side: exchange.SideSell, px: 100.5, qty: 2, ts: 1000 * int64(time.Millisecond)}
	if err != nil || len(got) != 1 || got[0] != want {
		t.Fatalf("1001: %+v %v", got, err)
	}
	if got, err := decode(umTradeEvent(1002, "INSURANCE_FUND")); err != nil || len(got) != 0 || s.dropped != 1 {
		t.Fatalf("1002 INSURANCE_FUND: %+v dropped %d %v", got, s.dropped, err)
	}
	if got, err := decode(umTradeEvent(1003, "MARKET")); err != nil || len(got) != 1 || got[0].id != "1003" {
		t.Fatalf("1003 after a dropped 1002: %+v %v", got, err)
	}
	// 1004 never comes. 1005 is still sent, and the gap is the error.
	got, err = decode(umTradeEvent(1005, "MARKET"))
	if len(got) != 1 || got[0].id != "1005" || err == nil || err.Error() != "binance: btcusdt@trade gap: t=1005 after t=1003" {
		t.Fatalf("1005: %+v %v", got, err)
	}
	if got, err := decode(umTradeEvent(1006, "MARKET")); err != nil || len(got) != 1 {
		t.Fatalf("1006 after the gap: %+v %v", got, err)
	}
	// The gap before a trade that is dropped is still a gap.
	got, err = decode(umTradeEvent(1008, "ADL"))
	if len(got) != 0 || s.dropped != 2 || err == nil || err.Error() != "binance: btcusdt@trade gap: t=1008 after t=1006" {
		t.Fatalf("1008 ADL: %+v dropped %d %v", got, s.dropped, err)
	}
	noX := `{"e":"trade","E":2000,"T":1000,"s":"BTCUSDT","t":1009,"p":"100.5","q":"2","m":true,"st":1}`
	if got, err := decode(noX); err != nil || len(got) != 0 || s.dropped != 3 {
		t.Fatalf("1009 without X: %+v dropped %d %v", got, s.dropped, err)
	}
	if got, err := decode(umTradeEvent(1010, "MARKET")); err != nil || len(got) != 1 {
		t.Fatalf("1010: %+v %v", got, err)
	}
}

// TestTradeCursor: t is followed per channel, and a wrapped frame is the same channel as a raw
// one. Every frame is overwritten once Decode returns, so a channel name kept from a wrapped frame
// must be a copy (E03-D002). A subscribe starts the channel's count again.
func TestTradeCursor(t *testing.T) {
	s := &session{}
	decode := func(frame string) ([]tradeGot, error) {
		var got capture
		buf := []byte(frame)
		err := s.Decode(buf, &got)
		for i := range buf {
			buf[i] = 0xA5
		}
		return got.trades, err
	}
	wrapped := func(id int) string {
		return `{"stream":"btcusdt@trade","data":` + umTradeEvent(id, "MARKET") + `}`
	}
	if got, err := decode(wrapped(1001)); err != nil || len(got) != 1 || got[0].ch != "btcusdt@trade" {
		t.Fatalf("wrapped 1001: %+v %v", got, err)
	}
	if _, err := decode(wrapped(1003)); err == nil {
		t.Fatal("1003 after 1001 is not a gap")
	}
	if _, err := decode(wrapped(1005)); err == nil {
		t.Fatal("1005 after 1003 is not a gap")
	}
	if _, err := decode(wrapped(1006)); err != nil {
		t.Fatalf("1006: %v", err)
	}
	if _, err := decode(umTradeEvent(1007, "MARKET")); err != nil {
		t.Fatalf("raw 1007 after wrapped 1006: %v", err)
	}
	eth := `{"e":"trade","E":2000,"T":1000,"s":"ETHUSDT","t":1,"p":"10","q":"1","X":"MARKET","m":false,"st":1}`
	if got, err := decode(eth); err != nil || len(got) != 1 || got[0].ch != "ethusdt@trade" {
		t.Fatalf("ETHUSDT 1: %+v %v", got, err)
	}
	if _, err := s.Subscribe([]string{"btcusdt@trade"}); err != nil {
		t.Fatal(err)
	}
	if _, err := decode(umTradeEvent(2001, "MARKET")); err != nil {
		t.Fatalf("2001 after a new subscribe: %v", err)
	}
	noID := `{"e":"trade","E":2000,"T":1000,"s":"BTCUSDT","p":"100.5","q":"2","X":"MARKET","m":true,"st":1}`
	if got, err := decode(noID); err == nil || len(got) != 0 {
		t.Fatalf("a trade without t: %+v %v", got, err)
	}
	if _, err := decode(umTradeEvent(2002, "MARKET")); err != nil {
		t.Fatalf("2002 after a refused frame: %v", err)
	}
}

func TestUMListingSample(t *testing.T) {
	body := []byte(`{"symbols":[
		{"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","deliveryDate":4133404800000,"filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.10","minPrice":"1","maxPrice":"100000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"1000"},
			{"filterType":"MIN_NOTIONAL","notional":"50"}]},
		{"symbol":"XAUUSDT","contractType":"TRADIFI_PERPETUAL","status":"PENDING_TRADING","baseAsset":"XAU","quoteAsset":"USDT","marginAsset":"USDT","deliveryDate":4133404800000,"filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.01","minPrice":"1","maxPrice":"100000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"10"}]},
		{"symbol":"ETHUSDT","contractType":"PERPETUAL","status":"BREAK","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.01","minPrice":"1","maxPrice":"100000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"10"}]},
		{"symbol":"BTCUSDT_261225","contractType":"CURRENT_QUARTER","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","deliveryDate":1798185600000,"filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.1","minPrice":"1","maxPrice":"100000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"10"}]},
		{"symbol":"NOPEUSDT","contractType":"SOMETHING","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.1","minPrice":"1","maxPrice":"1"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"1"}]},
		{"symbol":"QUANTO","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USD","marginAsset":"USDC","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.1","minPrice":"1","maxPrice":"1"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"1"}]}
	]}`)
	r := &rest{p: um}
	perp, err := r.Listing(symbol.Perp)
	if err != nil {
		t.Fatal(err)
	}
	page, err := perp.Parse(&exchange.HTTPResponse{Status: 200, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 3 {
		t.Fatalf("perps %d", len(page.Items))
	}
	btc := page.Items[0]
	if btc.ExchTicker != "BTCUSDT" || btc.Status != symbol.Tradable || btc.ExpiryNs != 0 || btc.ContractSize != 1 ||
		btc.ContractValueCcy != "BTC" || btc.PriceCcy != "USDT" || btc.Settle != "USDT" ||
		btc.Filters[symbol.PriceTick] != 0.1 || btc.Filters[symbol.MinNotional] != 50 {
		t.Fatalf("btc %+v", btc)
	}
	if page.Items[1].Status != symbol.Prelisting || page.Items[1].ExchTicker != "XAUUSDT" {
		t.Fatalf("tradfi %+v", page.Items[1])
	}
	if page.Items[2].Status != symbol.Unknown {
		t.Fatalf("status %s", page.Items[2].Status)
	}
	fut, err := r.Listing(symbol.Future)
	if err != nil {
		t.Fatal(err)
	}
	fpage, err := fut.Parse(&exchange.HTTPResponse{Status: 200, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if len(fpage.Items) != 1 || fpage.Items[0].ExpiryCode != "261225" || fpage.Items[0].ExpiryNs != 1798185600000*int64(time.Millisecond) {
		t.Fatalf("future %+v", fpage.Items)
	}
	if _, err := r.Listing(symbol.Spot); err == nil {
		t.Fatal("spot listing was accepted")
	}
}

func TestUMPublicREST(t *testing.T) {
	r := &rest{p: um}
	d, err := r.Depth(btcInst(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Req.Query) != "symbol=BTCUSDT&limit=1000" || d.Req.Cost[0].N != 20 {
		t.Fatalf("depth req %+v", d.Req)
	}
	book, err := d.Parse(&exchange.HTTPResponse{Status: 200, Body: []byte(
		`{"lastUpdateId":10,"E":1000,"T":900,"bids":[["1.5","2"]],"asks":[["1.6","0"]]}`)})
	if err != nil || !book.Snapshot || book.Last != 10 || book.Bids[0].Qty != 2 || book.Asks[0].Qty != 0 {
		t.Fatalf("book %+v %v", book, err)
	}
	if _, err := r.Depth(btcInst(), 7); err == nil {
		t.Fatal("depth limit 7 was accepted")
	}
	k, err := r.Klines(btcInst(), "1m", 0, 5)
	if err != nil || k.Req.Cost[0].N != 1 || string(k.Req.Query) != "symbol=BTCUSDT&interval=1m&limit=5" {
		t.Fatalf("kline req %+v %v", k.Req, err)
	}
	rows, err := k.Parse(&exchange.HTTPResponse{Status: 200, Body: []byte(
		`[[1000,"1","2","0.5","1.5","3",1999,"4",1,"0","0","0"],[2000,"1","2","0.5","1.5","3",2999,"8",1,"0","0","0"]]`)})
	if err != nil || len(rows) != 1 || !rows[0].Closed || rows[0].QuoteVolume != 4 || rows[0].Interval != "1m" {
		t.Fatalf("klines %+v %v", rows, err)
	}
	if _, err := r.Klines(btcInst(), "2mo", 0, 1); !errorsIs(err, exchange.ErrUnsupported) {
		t.Fatalf("interval: %v", err)
	}
	o, err := r.OpenInterest(btcInst())
	if err != nil {
		t.Fatal(err)
	}
	oi, err := o.Parse(&exchange.HTTPResponse{Status: 200, Body: []byte(`{"openInterest":"1.5","time":1000}`)})
	if err != nil || oi.OpenInterest != 1.5 || decimal.Has(oi.OpenInterestValue) {
		t.Fatalf("oi %+v %v", oi, err)
	}
	now := time.Date(2026, 10, 11, 1, 2, 3, 0, time.UTC)
	used := r.Usage(&exchange.HTTPResponse{Header: []exchange.Header{{Name: "X-MBX-USED-WEIGHT-1M", Value: "20"}}}, now)
	if len(used) != 1 || used[0].N != 20 || used[0].Bucket != "weight_1m" || !used[0].ResetAt.Equal(time.Date(2026, 10, 11, 1, 3, 0, 0, time.UTC)) {
		t.Fatalf("usage %+v", used)
	}
}

// TestUsageResetAt uses the headers a Binance response carries. ResetAt is the next
// window boundary after the response time, aligned to the Unix epoch. A response that
// lands on a boundary belongs to the window that just opened.
func TestUsageResetAt(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 7, 0, time.UTC)
	resp := &exchange.HTTPResponse{Header: []exchange.Header{
		{Name: "X-MBX-USED-WEIGHT-1M", Value: "20"},
		{Name: "X-MBX-ORDER-COUNT-10S", Value: "1"},
		{Name: "X-MBX-ORDER-COUNT-1M", Value: "2"},
		{Name: "X-MBX-ORDER-COUNT-1D", Value: "3"},
	}}
	got := usedFrom(resp, now)
	want := map[string]time.Time{
		weight1m:  time.Date(2026, 10, 11, 12, 1, 0, 0, time.UTC),
		orders10s: time.Date(2026, 10, 11, 12, 0, 10, 0, time.UTC),
		orders1m:  time.Date(2026, 10, 11, 12, 1, 0, 0, time.UTC),
		orders1d:  time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("%+v", got)
	}
	for _, u := range got {
		if !u.ResetAt.Equal(want[u.Bucket]) {
			t.Fatalf("%s reset %s, want %s", u.Bucket, u.ResetAt, want[u.Bucket])
		}
	}
	on := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	boundary := usedFrom(&exchange.HTTPResponse{Header: []exchange.Header{
		{Name: "X-MBX-USED-WEIGHT-1M", Value: "1"},
	}}, on)
	if !boundary[0].ResetAt.Equal(on.Add(time.Minute)) {
		t.Fatalf("on the boundary %+v", boundary)
	}
	ban := &exchange.HTTPResponse{
		Status: 418,
		Header: []exchange.Header{{Name: "Retry-After", Value: "120"}},
		Body:   []byte(`{"code":-1003,"msg":"Way too many requests; IP banned."}`),
	}
	err := refuse(ban)
	var rl *exchange.RateLimited
	if !errors.As(err, &rl) || rl.RetryAfterMs != 120000 {
		t.Fatalf("418 %v", err)
	}
	limited := &exchange.HTTPResponse{
		Status: http.StatusTooManyRequests,
		Header: []exchange.Header{{Name: "Retry-After", Value: "12"}},
		Body:   []byte(`{"code":-1003,"msg":"Too many requests"}`),
	}
	err = refuse(limited)
	rl = nil
	if !errors.As(err, &rl) || rl.RetryAfterMs != 12000 {
		t.Fatalf("429 %v", err)
	}
}

func TestUMEndpoint(t *testing.T) {
	m := &market{p: um}
	ep, err := m.Endpoint("public", "")
	if err != nil || ep != "wss://fstream.binance.com/public/ws" {
		t.Fatalf("public %s %v", ep, err)
	}
	over, err := m.Endpoint("market", "wss://override.example")
	if err != nil || over != "wss://override.example/market/ws" {
		t.Fatalf("override %s %v", over, err)
	}
	s, err := m.NewSession("public")
	if err != nil {
		t.Fatal(err)
	}
	reqs, err := s.Subscribe([]string{"btcusdt@bookTicker", "btcusdt@depth@100ms"})
	if err != nil || len(reqs) != 1 || reqs[0].ID != 1 {
		t.Fatalf("sub %+v %v", reqs, err)
	}
	if !bytes.Contains(reqs[0].Frame, []byte(`"method":"SUBSCRIBE"`)) || !bytes.Contains(reqs[0].Frame, []byte(`btcusdt@depth@100ms`)) {
		t.Fatalf("frame %s", reqs[0].Frame)
	}
	tr, err := m.Atom(exchange.TopicTrade, btcInst())
	if err != nil || tr.Route != "public" || tr.Channel != "btcusdt@trade" {
		t.Fatalf("trade atom %+v %v", tr, err)
	}
	agg, err := m.Atom(exchange.TopicAggTrade, btcInst())
	if err != nil || agg.Route != "market" || agg.Channel != "btcusdt@aggTrade" {
		t.Fatalf("aggTrade atom %+v %v", agg, err)
	}
	a, err := m.Atom(exchange.TopicOrderBookDelta, btcInst())
	b, err2 := m.Atom(exchange.TopicOrderBookDelta, btcInst())
	if err != nil || err2 != nil || a != b || a.Route != "public" || a.Channel != "btcusdt@depth@100ms" {
		t.Fatalf("book atom %+v %+v %v %v", a, b, err, err2)
	}
	if _, ok := m.Book("btcusdt@depth20@100ms"); ok {
		t.Fatal("a partial book was treated as a diff")
	}
}

func errorsIs(err, target error) bool { return errors.Is(err, target) }

type capture struct {
	acks, quotes, marks, funds int
	ackErr                     error
	rate                       float64
	trades                     []tradeGot
	aggs                       []aggGot
	tickers                    []tickGot
	books                      []bookGot
	klines                     []klineGot
	liqs                       []liqGot
}

type tradeGot struct {
	ch, id  string
	side    exchange.Side
	px, qty float64
	ts      int64
}
type aggGot struct {
	ch, id string
	side   exchange.Side
	n      uint32
	px     float64
}
type tickGot struct {
	ch  string
	pct float64
}
type bookGot struct {
	ch          string
	first, prev uint64
}
type klineGot struct {
	iv     string
	q      float64
	closed bool
}
type liqGot struct {
	side exchange.Side
	px   float64
}

func (c *capture) Ack(_ uint64, err error) {
	c.acks++
	if err != nil && c.ackErr == nil {
		c.ackErr = err
	}
}
func (c *capture) BestQuote(string, *exchange.BestQuote) { c.quotes++ }

// Trade keeps copies: TestTradeCursor overwrites the frame once Decode returns.
func (c *capture) Trade(ch string, e *exchange.Trade) {
	c.trades = append(c.trades, tradeGot{strings.Clone(ch), strings.Clone(e.TradeID), e.Side, e.Price, e.Qty, e.ExchTsNs})
}
func (c *capture) AggTrade(ch string, e *exchange.AggTrade) {
	c.aggs = append(c.aggs, aggGot{ch, e.TradeID, e.Side, e.MatchCount, e.Price})
}
func (c *capture) Book(ch string, e *exchange.BookUpdate) {
	c.books = append(c.books, bookGot{ch, e.First, e.Prev})
}
func (c *capture) Kline(_ string, e *exchange.Kline) {
	c.klines = append(c.klines, klineGot{e.Interval, e.QuoteVolume, e.Closed})
}
func (c *capture) Ticker(ch string, e *exchange.Ticker) {
	c.tickers = append(c.tickers, tickGot{ch, e.ChangePct24h})
}
func (c *capture) Liquidation(_ string, e *exchange.Liquidation) {
	c.liqs = append(c.liqs, liqGot{e.Side, e.Price})
}
func (c *capture) FundingRate(_ string, e *exchange.FundingRate) {
	c.funds++
	c.rate = e.Rate
}
func (c *capture) OpenInterest(string, *exchange.OpenInterest) {}
func (c *capture) Greeks(string, *exchange.Greeks)             {}
func (c *capture) MarkPrice(string, *exchange.MarkPrice) { c.marks++ }

func TestRecordBinanceUMPublic(t *testing.T) {
	if os.Getenv("MFTIKO_BINANCE_UM_RECORD") != "1" {
		t.Skip("set MFTIKO_BINANCE_UM_RECORD=1 to record production public fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dir := "testdata/conformance/BinanceUM"
	if err := os.MkdirAll(filepath.Join(dir, "http"), 0o755); err != nil {
		t.Fatal(err)
	}
	recordREST(t, dir)
	recordPublic(t, ctx, dir)
	recordMarket(t, ctx, dir)
}

// TestRecordBinanceUMPublicTrade records @trade on the public route (E03-D004, E03-D008).
func TestRecordBinanceUMPublicTrade(t *testing.T) {
	if os.Getenv("MFTIKO_BINANCE_UM_RECORD") != "1" {
		t.Skip("set MFTIKO_BINANCE_UM_RECORD=1 to record production public fixtures")
	}
	recordTrade(t, um, "public", "testdata/conformance/BinanceUM", "public_trade",
		"USD-M BTCUSDT @trade on the public route (E03-D004): the ack and the first trades, t without a gap.")
}

func recordREST(t *testing.T, dir string) {
	t.Helper()
	r := &rest{p: um}
	save := func(name string, c exchange.HTTPRequest, parse func(*exchange.HTTPResponse) error) {
		t.Helper()
		resp := httpDo(t, &c)
		if err := parse(resp); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		f, err := os.Create(filepath.Join(dir, "http", name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := conformance.SaveExchange(f, &c, resp); err != nil {
			t.Fatal(err)
		}
	}
	perp, err := r.Listing(symbol.Perp)
	if err != nil {
		t.Fatal(err)
	}
	fut, err := r.Listing(symbol.Future)
	if err != nil {
		t.Fatal(err)
	}
	body := httpDo(t, &perp.Req)
	page, err := perp.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	fpage, err := fut.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("listing perp %d future %d", len(page.Items), len(fpage.Items))
	if len(page.Items) < 100 || len(fpage.Items) < 1 {
		t.Fatalf("listing looks short: perp %d future %d", len(page.Items), len(fpage.Items))
	}
	for _, name := range []string{"listing_perp", "listing_future"} {
		f, err := os.Create(filepath.Join(dir, "http", name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		req := perp.Req
		if name == "listing_future" {
			req = fut.Req
		}
		if err := conformance.SaveExchange(f, &req, body); err != nil {
			f.Close()
			t.Fatal(err)
		}
		f.Close()
	}
	depth, err := r.Depth(btcInst(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	save("depth", depth.Req, func(resp *exchange.HTTPResponse) error {
		_, err := depth.Parse(resp)
		return err
	})
	kl, err := r.Klines(btcInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	save("klines", kl.Req, func(resp *exchange.HTTPResponse) error {
		_, err := kl.Parse(resp)
		return err
	})
	oi, err := r.OpenInterest(btcInst())
	if err != nil {
		t.Fatal(err)
	}
	save("open_interest", oi.Req, func(resp *exchange.HTTPResponse) error {
		_, err := oi.Parse(resp)
		return err
	})
}

func recordPublic(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	tp, conn, sess, stop := dialRoute(t, ctx, "public")
	defer stop()
	sub(t, ctx, conn, sess, "btcusdt@bookTicker")
	waitN(t, tp, `"e":"bookTicker"`, 2, 10*time.Second)
	unsub(t, ctx, conn, sess, "btcusdt@bookTicker")
	sub(t, ctx, conn, sess, "btcusdt@depth@100ms")
	waitN(t, tp, `"e":"depthUpdate"`, 2, 10*time.Second)
	call, err := (&rest{p: um}).Depth(btcInst(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	resp := httpDo(t, &call.Req)
	if _, err := call.Parse(resp); err != nil {
		t.Fatal(err)
	}
	tp.add(piece{resp: resp, req: &call.Req, ch: "btcusdt@depth@100ms"})
	waitAfter(t, tp, `"e":"depthUpdate"`, 8, 15*time.Second)
	stop()
	if err := replayTape(tp.snapshot()); err != nil {
		t.Fatal(err)
	}
	writeSession(t, dir, "public", VenueUM, "public", "USD-M BTCUSDT bookTicker and diff depth, with a REST snapshot.", tp.snapshot())
}

func recordMarket(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	tp, conn, sess, stop := dialRoute(t, ctx, "market")
	defer stop()
	for _, ch := range []struct{ name, needle string }{
		{"btcusdt@aggTrade", `"e":"aggTrade"`},
		{"btcusdt@kline_1m", `"e":"kline"`},
		{"btcusdt@ticker", `"e":"24hrTicker"`},
		{"btcusdt@markPrice@1s", `"e":"markPriceUpdate"`},
		{"!miniTicker@arr", `"e":"24hrMiniTicker"`},
	} {
		sub(t, ctx, conn, sess, ch.name)
		waitN(t, tp, ch.needle, 1, 15*time.Second)
		unsub(t, ctx, conn, sess, ch.name)
	}
	stop()
	writeSession(t, dir, "market", VenueUM, "market", "USD-M BTCUSDT aggTrade, kline, ticker, mark price, and one all-market miniTicker array.", tp.snapshot())
}

// recordTrade records the ack and the first trades of btcusdt@trade. writeSession refuses a gap in t.
func recordTrade(t *testing.T, p product, route, dir, name, note string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tp, conn, sess, stop := dialProduct(t, ctx, p, route)
	defer stop()
	sub(t, ctx, conn, sess, "btcusdt@trade")
	waitN(t, tp, `"e":"trade"`, 12, 30*time.Second)
	stop()
	writeSession(t, dir, name, p.venue.Name, route, note, tp.snapshot())
}

type piece struct {
	frame []byte
	resp  *exchange.HTTPResponse
	req   *exchange.HTTPRequest
	ch    string
}

type liveTape struct {
	mu     sync.Mutex
	pieces []piece
	closed bool
}

func (t *liveTape) add(p piece) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.pieces = append(t.pieces, p)
	}
}

func (t *liveTape) snapshot() []piece {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	out := make([]piece, len(t.pieces))
	copy(out, t.pieces)
	return out
}

func (t *liveTape) count(needle string, afterSnap bool) int {
	b := []byte(needle)
	seen := !afterSnap
	n := 0
	for _, p := range t.snapshotPieces() {
		if p.resp != nil {
			seen = true
			continue
		}
		if seen && bytes.Contains(p.frame, b) {
			n++
		}
	}
	return n
}

func (t *liveTape) snapshotPieces() []piece {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]piece, len(t.pieces))
	copy(out, t.pieces)
	return out
}

func dialRoute(t *testing.T, ctx context.Context, route string) (*liveTape, *ws.Conn, exchange.Session, func()) {
	t.Helper()
	return dialProduct(t, ctx, um, route)
}

func dialProduct(t *testing.T, ctx context.Context, p product, route string) (*liveTape, *ws.Conn, exchange.Session, func()) {
	t.Helper()
	m := &market{p: p}
	ep, err := m.Endpoint(route, "")
	if err != nil {
		t.Fatal(err)
	}
	sess, err := m.NewSession(route)
	if err != nil {
		t.Fatal(err)
	}
	lim := sess.Limits()
	connCtx, cancel := context.WithCancel(ctx)
	conn, err := ws.Dial(connCtx, ep, ws.Options{ReadLimit: lim.ReadLimit, Keepalive: lim.Keepalive, MaxLifetime: lim.MaxLifetime})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	tp := &liveTape{}
	done := make(chan struct{})
	go func() {
		_ = conn.Run(connCtx, func(frame []byte) error {
			tp.add(piece{frame: append([]byte(nil), frame...)})
			return nil
		})
		close(done)
	}()
	stop := func() {
		cancel()
		_ = conn.Close()
		<-done
	}
	return tp, conn, sess, stop
}

func sub(t *testing.T, ctx context.Context, conn *ws.Conn, sess exchange.Session, ch string) {
	t.Helper()
	reqs, err := sess.Subscribe([]string{ch})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range reqs {
		if err := conn.Write(ctx, r.Frame); err != nil {
			t.Fatalf("subscribe %s: %v", ch, err)
		}
	}
}

func unsub(t *testing.T, ctx context.Context, conn *ws.Conn, sess exchange.Session, ch string) {
	t.Helper()
	reqs, err := sess.Unsubscribe([]string{ch})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range reqs {
		if err := conn.Write(ctx, r.Frame); err != nil {
			t.Fatalf("unsubscribe %s: %v", ch, err)
		}
	}
}

func waitN(t *testing.T, tp *liveTape, needle string, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if tp.count(needle, false) >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("saw %d %s, want %d", tp.count(needle, false), needle, n)
}

func waitAfter(t *testing.T, tp *liveTape, needle string, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if tp.count(needle, true) >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("saw %d %s after the snapshot, want %d", tp.count(needle, true), needle, n)
}

func httpDo(t *testing.T, req *exchange.HTTPRequest) *exchange.HTTPResponse {
	t.Helper()
	u := req.Base + req.Path
	if len(req.Query) > 0 {
		u += "?" + string(req.Query)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hr, err := http.NewRequestWithContext(ctx, req.Method, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: status %d body %s", req.Method, u, resp.StatusCode, clip(body))
	}
	out := &exchange.HTTPResponse{Status: resp.StatusCode, Body: body}
	if w := resp.Header.Get("X-MBX-USED-WEIGHT-1M"); w != "" {
		out.Header = []exchange.Header{{Name: "X-MBX-USED-WEIGHT-1M", Value: w}}
	}
	return out
}

func clip(b []byte) string {
	if len(b) > 240 {
		b = b[:240]
	}
	return string(b)
}

type seqSink struct {
	books  []exchange.BookUpdate
	ackErr error
}

func (s *seqSink) Ack(_ uint64, err error) {
	if err != nil && s.ackErr == nil {
		s.ackErr = err
	}
}
func (s *seqSink) BestQuote(string, *exchange.BestQuote) {}
func (s *seqSink) Trade(string, *exchange.Trade)         {}
func (s *seqSink) AggTrade(string, *exchange.AggTrade)   {}
func (s *seqSink) Book(_ string, e *exchange.BookUpdate) {
	s.books = append(s.books, exchange.BookUpdate{First: e.First, Last: e.Last, Prev: e.Prev, Snapshot: e.Snapshot})
}
func (s *seqSink) Kline(string, *exchange.Kline)               {}
func (s *seqSink) Ticker(string, *exchange.Ticker)             {}
func (s *seqSink) Liquidation(string, *exchange.Liquidation)   {}
func (s *seqSink) FundingRate(string, *exchange.FundingRate)   {}
func (s *seqSink) OpenInterest(string, *exchange.OpenInterest) {}
func (s *seqSink) Greeks(string, *exchange.Greeks)             {}
func (s *seqSink) MarkPrice(string, *exchange.MarkPrice) {}

func replayTape(pieces []piece) error {
	sess := &session{per: um.budget}
	var items []struct {
		rest bool
		u    exchange.BookUpdate
	}
	for i, p := range pieces {
		if p.resp != nil {
			call, err := (&rest{p: um}).Depth(btcInst(), 1000)
			if err != nil {
				return err
			}
			u, err := call.Parse(p.resp)
			if err != nil {
				return fmt.Errorf("snapshot: %w", err)
			}
			items = append(items, struct {
				rest bool
				u    exchange.BookUpdate
			}{true, *u})
			continue
		}
		sink := &seqSink{}
		if err := sess.Decode(p.frame, sink); err != nil {
			return fmt.Errorf("frame %d: %w (%s)", i, err, clip(p.frame))
		}
		if sink.ackErr != nil {
			return fmt.Errorf("frame %d: %w", i, sink.ackErr)
		}
		for _, u := range sink.books {
			items = append(items, struct {
				rest bool
				u    exchange.BookUpdate
			}{false, u})
		}
	}
	bs := umBook{}
	var seq exchange.BookSeq
	var pending []int
	applied := 0
	var resync []string
	step := func(i int) {
		it := items[i]
		switch bs.Check(seq, &it.u) {
		case exchange.BookApply:
			seq.Last = it.u.Last
			applied++
		case exchange.BookSkip:
		case exchange.BookResync:
			resync = append(resync, fmt.Sprintf("U=%d u=%d pu=%d last=%d", it.u.First, it.u.Last, it.u.Prev, seq.Last))
			seq = exchange.BookSeq{}
		}
	}
	for i := range items {
		it := items[i]
		switch {
		case it.rest || it.u.Snapshot:
			seq = exchange.BookSeq{Last: it.u.Last, Valid: true}
			for _, j := range pending {
				step(j)
			}
			pending = nil
		case !seq.Valid:
			pending = append(pending, i)
		default:
			step(i)
		}
	}
	if len(resync) > 0 || applied < 3 {
		var snap uint64
		for _, it := range items {
			if it.rest {
				snap = it.u.Last
			}
		}
		return fmt.Errorf("book replay applied %d snapshot %d resync %v", applied, snap, resync)
	}
	return nil
}

func writeSession(t *testing.T, dir, name, venue, route, note string, pieces []piece) {
	t.Helper()
	prod := um
	if venue == VenueSpot {
		prod = spot
	}
	sess, err := (&market{p: prod}).NewSession(route)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range pieces {
		if p.frame == nil {
			continue
		}
		got := &capture{}
		if err := sess.Decode(p.frame, got); err != nil {
			t.Fatalf("%s frame %d: %v (%s)", name, i, err, clip(p.frame))
		}
		if got.ackErr != nil {
			t.Fatalf("%s frame %d: subscribe rejected: %v", name, i, got.ackErr)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := conformance.NewWriter(&buf, conformance.Record{
		Venue: venue, Side: conformance.SideMkt, Route: route, Note: note,
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pieces {
		if p.resp != nil {
			if err := w.HTTP(p.ch, false, p.req, p.resp); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := w.Frame(conformance.KindIn, p.frame); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "ws", name+".jsonl")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s (%d bytes, %d records)", path, buf.Len(), len(pieces))
}
