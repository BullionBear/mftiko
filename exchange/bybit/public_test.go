package bybit

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

func TestSameChannelTwoRoutes(t *testing.T) {
	m := &market{}
	if got := m.Routes(); len(got) != 2 || got[0] != "spot" || got[1] != "linear" {
		t.Fatalf("routes %v", got)
	}
	spot, err := m.Atom(exchange.TopicTrade, spotInst())
	if err != nil {
		t.Fatal(err)
	}
	lin, err := m.Atom(exchange.TopicTrade, btcInst())
	if err != nil {
		t.Fatal(err)
	}
	if spot.Channel != "publicTrade.BTCUSDT" || lin.Channel != spot.Channel {
		t.Fatalf("channel spot %q linear %q", spot.Channel, lin.Channel)
	}
	if spot.Route != "spot" || lin.Route != "linear" {
		t.Fatalf("routes %s %s", spot.Route, lin.Route)
	}
	if atomID(spot) == atomID(lin) {
		t.Fatalf("atom id %s is one atom for two routes", atomID(spot))
	}
	sb, err := m.Atom(exchange.TopicOrderBook, spotInst())
	if err != nil {
		t.Fatal(err)
	}
	sd, err := m.Atom(exchange.TopicOrderBookDelta, spotInst())
	if err != nil {
		t.Fatal(err)
	}
	if sb.Channel != "orderbook.50.BTCUSDT" || sd.Channel != sb.Channel || sb.Route != "spot" {
		t.Fatalf("book atoms %+v %+v", sb, sd)
	}
	if _, ok := m.Book(sb.Channel); !ok {
		t.Fatal("orderbook.50 is not a book")
	}
	if _, ok := m.Book("orderbook.1.BTCUSDT"); ok {
		t.Fatal("orderbook.1 is a best quote, not a book")
	}
	q, err := m.Atom(exchange.TopicBestQuote, spotInst())
	if err != nil || q.Channel != "orderbook.1.BTCUSDT" {
		t.Fatalf("best quote %+v %v", q, err)
	}
	tk, err := m.Atom(exchange.TopicTicker, btcInst())
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []exchange.Topic{exchange.TopicFundingRate, exchange.TopicOpenInterest, exchange.TopicMarkPrice} {
		a, err := m.Atom(topic, btcInst())
		if err != nil || a != tk {
			t.Fatalf("%s %+v %v, ticker %+v", topic, a, err, tk)
		}
		if _, err := m.Atom(topic, spotInst()); !errors.Is(err, exchange.ErrUnsupported) {
			t.Fatalf("spot %s: %v", topic, err)
		}
	}
	liq, err := m.Atom(exchange.TopicLiquidation, btcInst())
	if err != nil || liq.Channel != "allLiquidation.BTCUSDT" || liq.Route != "linear" {
		t.Fatalf("liquidation %+v %v", liq, err)
	}
	if _, err := m.Atom(exchange.TopicLiquidation, spotInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("spot liquidation: %v", err)
	}
	k, err := m.Atom(exchange.KlineTopic("1m"), spotInst())
	if err != nil || k.Channel != "kline.1.BTCUSDT" {
		t.Fatalf("kline %+v %v", k, err)
	}
	inv := &symbol.Info{Ticker: symbol.UniversalTicker{Venue: Name, Category: symbol.Inverse}, ExchTicker: "BTCUSD"}
	if _, err := m.Atom(exchange.TopicTrade, inv); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("inverse: %v", err)
	}
	if _, err := m.Atom(exchange.TopicAggTrade, btcInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("aggTrade: %v", err)
	}
	if _, err := m.Atom(exchange.KlineTopic("8h"), btcInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("8h: %v", err)
	}
	ep, err := m.Endpoint("linear", "wss://override.example")
	if err != nil || ep != "wss://override.example/v5/public/linear" {
		t.Fatalf("endpoint %q %v", ep, err)
	}
	if m.Budget("spot") != argBudget || m.Cost("publicTrade.BTCUSDT") != len("publicTrade.BTCUSDT") {
		t.Fatalf("budget %d cost %d", m.Budget("spot"), m.Cost("publicTrade.BTCUSDT"))
	}
}

func atomID(a exchange.AtomRef) string { return Name + ":" + a.Route + ":" + a.Channel }

func TestBookSync(t *testing.T) {
	var b bybitBook
	if b.Snapshot() != exchange.SnapshotStream || b.Depth() != 0 {
		t.Fatalf("snapshot %v depth %d", b.Snapshot(), b.Depth())
	}
	seq := exchange.BookSeq{Last: 10, Valid: true}
	apply := &exchange.BookUpdate{Last: 11}
	if b.Check(seq, apply) != exchange.BookApply {
		t.Fatal("u = last+1 should apply")
	}
	if b.Check(seq, &exchange.BookUpdate{Last: 10}) != exchange.BookSkip {
		t.Fatal("an older u should skip")
	}
	if b.Check(seq, &exchange.BookUpdate{Last: 13}) != exchange.BookResync {
		t.Fatal("a gap should resync")
	}
	if b.Check(exchange.BookSeq{}, apply) != exchange.BookResync {
		t.Fatal("no snapshot should resync")
	}
}

func TestSubscribeChunks(t *testing.T) {
	s, err := (&market{}).NewSession("spot")
	if err != nil {
		t.Fatal(err)
	}
	var chs []string
	for i := 0; i < 11; i++ {
		chs = append(chs, "c"+string(rune('a'+i)))
	}
	reqs, err := s.Subscribe(chs)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 2 || len(reqs[0].Channels) != 10 || len(reqs[1].Channels) != 1 {
		t.Fatalf("spot pages %+v", reqs)
	}
	if reqs[0].ID != 1 || reqs[1].ID != 2 {
		t.Fatalf("ids %d %d", reqs[0].ID, reqs[1].ID)
	}
	if string(reqs[0].Frame) != `{"req_id":"1","op":"subscribe","args":["ca","cb","cc","cd","ce","cf","cg","ch","ci","cj"]}` {
		t.Fatalf("frame %s", reqs[0].Frame)
	}
	more, err := s.Unsubscribe([]string{"ca"})
	if err != nil || len(more) != 1 || more[0].ID != 3 {
		t.Fatalf("unsubscribe %+v %v", more, err)
	}
	lin, err := (&market{}).NewSession("linear")
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("x", 10000)
	reqs, err = lin.Subscribe([]string{long + "1", long + "2", long + "3"})
	if err != nil || len(reqs) != 2 || len(reqs[0].Channels) != 2 || len(reqs[1].Channels) != 1 {
		t.Fatalf("linear pages %d %v", len(reqs), err)
	}
	if _, err := lin.Subscribe([]string{strings.Repeat("y", argBudget+1)}); err == nil {
		t.Fatal("a channel past the budget was accepted")
	}
	lim := s.Limits()
	if lim.PerRequest != spotArgs || lim.PerRequestBytes != argBudget || string(s.Ping()) != `{"op":"ping"}` {
		t.Fatalf("limits %+v ping %s", lim, s.Ping())
	}
	if lin.Limits().PerRequest != 0 {
		t.Fatal("linear has a per-request count")
	}
}

func TestPublicDecode(t *testing.T) {
	s, err := (&market{}).NewSession("linear")
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	frames := []string{
		`{"op":"ping"}`,
		`{"success":true,"ret_msg":"","op":"subscribe","req_id":"1"}`,
		`{"topic":"orderbook.1.BTCUSDT","ts":"1672304484978","type":"snapshot","data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16493.60","0.010"]],"u":1,"seq":1}}`,
		`{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1672304484978,"data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":10,"seq":1}}`,
		`{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1672304484979,"data":{"s":"BTCUSDT","b":[["16493.50","0"]],"a":[["16611.00","1"]],"u":11,"seq":2}}`,
		`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[{"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.50","i":"trade-1","BT":false}]}`,
		`{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1760325052630,"data":{"symbol":"BTCUSDT","lastPrice":"66666.60","prevPrice24h":"79206.20","highPrice24h":"79266.30","lowPrice24h":"65076.90","volume24h":"73191.387","turnover24h":"4936790807.65","price24hPcnt":"-0.158315","markPrice":"66666.60","indexPrice":"115418.19","openInterest":"492373.72","openInterestValue":"32824881841.75","fundingRate":"-0.005","nextFundingTime":"1760342400000","fundingIntervalHour":"8"}}`,
		`{"topic":"kline.1.BTCUSDT","data":[{"start":1672324800000,"end":1672324859999,"interval":"1","open":"16649.5","close":"16650","high":"16677","low":"16608","volume":"2.081","turnover":"34666.4005","confirm":false,"timestamp":1672324800000}],"ts":1672324800000,"type":"snapshot"}`,
		`{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"0.2","p":"64000"}]}`,
		`{"topic":"not.a.topic","data":{}}`,
	}
	for _, f := range frames {
		if err := s.Decode([]byte(f), sink); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	if len(sink.acks) != 1 || sink.acks[0].id != 1 || sink.acks[0].err != nil {
		t.Fatalf("acks %+v", sink.acks)
	}
	if len(sink.quotes) != 1 || sink.quotes[0].Bid != 16493.50 || sink.quotes[0].AskQty != 0.010 || sink.quotes[0].ExchTsNs != 1672304484978*int64(time.Millisecond) {
		t.Fatalf("quote %+v", sink.quotes)
	}
	if len(sink.books) != 2 || !sink.books[0].Snapshot || sink.books[0].Last != 10 || sink.books[1].Snapshot || sink.books[1].Bids[0].Qty != 0 {
		t.Fatalf("books %+v", sink.books)
	}
	if len(sink.trades) != 1 || sink.trades[0].TradeID != "trade-1" || sink.trades[0].Side != exchange.SideBuy || sink.trades[0].Price != 16578.50 {
		t.Fatalf("trade %+v", sink.trades)
	}
	if len(sink.tickers) != 1 || sink.tickers[0].ChangePct24h != -0.158315 || sink.tickers[0].Last != 66666.60 {
		t.Fatalf("ticker %+v", sink.tickers)
	}
	if len(sink.marks) != 1 || sink.marks[0].MarkPrice != 66666.60 || sink.marks[0].IndexPrice != 115418.19 {
		t.Fatalf("mark %+v", sink.marks)
	}
	if len(sink.ois) != 1 || sink.ois[0].OpenInterest != 492373.72 || sink.ois[0].OpenInterestValue != 32824881841.75 {
		t.Fatalf("oi %+v", sink.ois)
	}
	if len(sink.funds) != 1 || sink.funds[0].Rate != -0.005 || sink.funds[0].IntervalS != 8*3600 || sink.funds[0].NextFundingNs != 1760342400000*int64(time.Millisecond) {
		t.Fatalf("funding %+v", sink.funds)
	}
	if len(sink.klines) != 1 || sink.klines[0].Interval != "1m" || sink.klines[0].Closed || sink.klines[0].QuoteVolume != 34666.4005 {
		t.Fatalf("kline %+v", sink.klines)
	}
	if len(sink.liqs) != 1 || sink.liqs[0].Side != exchange.SideSell || sink.liqs[0].Price != 64000 || sink.liqs[0].Qty != 0.2 {
		t.Fatalf("liq %+v", sink.liqs)
	}
	for _, bad := range []string{`{"conformance":"a frame no venue sends"}`, `["conformance",1]`, "\x00\x01 not json"} {
		if err := s.Decode([]byte(bad), &memSink{}); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	refused := &memSink{}
	if err := s.Decode([]byte(`{"success":false,"ret_msg":"error:handler not found,topic:nope","op":"subscribe","req_id":2}`), refused); err != nil || len(refused.acks) != 1 || refused.acks[0].err == nil {
		t.Fatalf("refused ack %+v %v", refused.acks, err)
	}
}

func TestListingRows(t *testing.T) {
	r := &rest{}
	spot, err := r.Listing(symbol.Spot)
	if err != nil {
		t.Fatal(err)
	}
	if spot.Req.Method != "GET" || spot.Req.Path != "/v5/market/instruments-info" || string(spot.Req.Query) != "category=spot&limit=1000" {
		t.Fatalf("spot request %+v", spot.Req)
	}
	page, err := spot.Parse(okBody(`{"list":[
		{"symbol":"BTCUSDT","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"basePrecision":"0.000001","minOrderQty":"0.000001","maxOrderQty":"230","minOrderAmt":"5","maxOrderAmt":"8000000"}},
		{"symbol":"BADUSDT","status":"Trading","baseCoin":"BAD/COIN","quoteCoin":"USDT","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"basePrecision":"0.01","minOrderQty":"1"}},
		{"symbol":"NEWUSDT","status":"Trading","baseCoin":"NEW","quoteCoin":"USDT","isPreListing":true,"priceFilter":{"tickSize":"0.01"},"lotSizeFilter":{"basePrecision":"0.1","minOrderQty":"1"}}
	],"nextPageCursor":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if page.Next != nil || len(page.Items) != 2 {
		t.Fatalf("spot page %d next %v", len(page.Items), page.Next != nil)
	}
	btc := page.Items[0]
	if btc.Category != symbol.Spot || btc.Settle != "" || btc.ContractSize != 0 || btc.Status != symbol.Tradable {
		t.Fatalf("btc %+v", btc)
	}
	if btc.Filters[symbol.PriceTick] != 0.1 || btc.Filters[symbol.QtyStep] != 0.000001 || btc.Filters[symbol.MinNotional] != 5 || btc.Filters[symbol.MaxNotional] != 8000000 {
		t.Fatalf("filters %v", btc.Filters)
	}
	if page.Items[1].Status != symbol.Prelisting || page.Items[1].ExchTicker != "NEWUSDT" {
		t.Fatalf("prelisting %+v", page.Items[1])
	}
	lin, err := r.Listing(symbol.Perp)
	if err != nil {
		t.Fatal(err)
	}
	page, err = lin.Parse(okBody(`{"list":[
		{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","deliveryTime":"0","priceFilter":{"minPrice":"0.10","maxPrice":"1999999.80","tickSize":"0.10"},"lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001","maxOrderQty":"1500","minNotionalValue":"5"}},
		{"symbol":"BTCUSDT-16OCT26","contractType":"LinearFutures","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","deliveryTime":"1792137600000","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001"}},
		{"symbol":"ETHPERP","contractType":"LinearPerpetual","status":"Trading","baseCoin":"ETH","quoteCoin":"USDT","settleCoin":"USD","priceFilter":{"tickSize":"0.01"},"lotSizeFilter":{"qtyStep":"0.01","minOrderQty":"0.01"}},
		{"symbol":"BTCUSDT-NOTADATE","contractType":"LinearFutures","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001"}},
		{"symbol":"SOLUSDT","contractType":"LinearPerpetual","status":"PreLaunch","baseCoin":"SOL","quoteCoin":"USDT","settleCoin":"USDT","priceFilter":{"tickSize":"0.01"},"lotSizeFilter":{"qtyStep":"0.1","minOrderQty":"0.1"}},
		{"symbol":"XBTUSDT","contractType":"LinearPerpetual","status":"Closed","baseCoin":"XBT","quoteCoin":"USDT","settleCoin":"USDT","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001"}}
	],"nextPageCursor":"abc"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 3 || page.Next == nil {
		t.Fatalf("linear %d next %v", len(page.Items), page.Next != nil)
	}
	if page.Items[0].Category != symbol.Perp || page.Items[0].ContractSize != 1 || page.Items[0].Settle != "USDT" || page.Items[0].Filters[symbol.MinNotional] != 5 {
		t.Fatalf("perp %+v", page.Items[0])
	}
	if page.Items[1].Status != symbol.Prelisting || page.Items[1].ExchTicker != "SOLUSDT" {
		t.Fatalf("prelaunch %+v", page.Items[1])
	}
	xbt := page.Items[2]
	if xbt.Base != "BTC" || xbt.Status != symbol.Delisted {
		t.Fatalf("xbt %+v", xbt)
	}
	fut, err := r.Listing(symbol.Future)
	if err != nil {
		t.Fatal(err)
	}
	fpage, err := fut.Parse(okBody(`{"list":[
		{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001"}},
		{"symbol":"BTCUSDT-16OCT26","contractType":"LinearFutures","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","deliveryTime":"1792137600000","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001"}}
	],"nextPageCursor":""}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(fpage.Items) != 1 || fpage.Next != nil || fpage.Items[0].ExpiryCode != "261016" || fpage.Items[0].ExpiryNs != 1792137600000*int64(time.Millisecond) {
		t.Fatalf("future %+v", fpage.Items)
	}
	next, err := page.Next(time.Time{})
	if err != nil || !strings.Contains(string(next.Req.Query), "cursor=abc") {
		t.Fatalf("next %s %v", next.Req.Query, err)
	}
	if _, err := r.Listing(symbol.Inverse); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("inverse listing %v", err)
	}
	if _, err := r.OpenInterest(btcInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("oi %v", err)
	}
	if r.KlinePage() != 1000 {
		t.Fatal(r.KlinePage())
	}
}

func TestKlinesAndDepth(t *testing.T) {
	r := &rest{}
	call, err := r.Klines(btcInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(call.Req.Query) != "category=linear&symbol=BTCUSDT&interval=1&limit=5" {
		t.Fatalf("query %s", call.Req.Query)
	}
	rows, err := call.Parse(okBody(`{"list":[["2000","2","3","1","2.5","4","10"],["1000","1","2","0.5","1.5","3","8"]]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].OpenTimeNs != 1000*int64(time.Millisecond) || !rows[0].Closed || rows[0].QuoteVolume != 8 || rows[0].Interval != "1m" {
		t.Fatalf("latest %+v", rows)
	}
	ended, err := r.Klines(btcInst(), "1h", 3000*int64(time.Millisecond), 2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ended.Req.Query), "interval=60") || !strings.Contains(string(ended.Req.Query), "end=3000") {
		t.Fatalf("ended query %s", ended.Req.Query)
	}
	rows, err = ended.Parse(okBody(`{"list":[["4000","2","2","2","2","1","1"],["1000","1","1","1","1","1","1"]]}`))
	if err != nil || len(rows) != 1 || rows[0].Open != 1 {
		t.Fatalf("ended %+v %v", rows, err)
	}
	if _, err := r.Klines(spotInst(), "8h", 0, 1); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("8h %v", err)
	}
	depth, err := r.Depth(spotInst(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(depth.Req.Query) != "category=spot&symbol=BTCUSDT&limit=1" {
		t.Fatalf("depth %s", depth.Req.Query)
	}
	book, err := depth.Parse(okBody(`{"s":"BTCUSDT","b":[["82804.20","9.601"]],"a":[["82804.30","0.653"]],"ts":1791630124402,"u":34207253,"seq":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if !book.Snapshot || book.Last != 34207253 || book.Bids[0].Price != 82804.20 || book.Asks[0].Qty != 0.653 {
		t.Fatalf("book %+v", book)
	}
}

func okBody(result string) *exchange.HTTPResponse {
	return &exchange.HTTPResponse{Status: 200, Body: []byte(`{"retCode":0,"retMsg":"OK","result":` + result + `}`)}
}

type ack struct {
	id  uint64
	err error
}

type memSink struct {
	acks    []ack
	quotes  []exchange.BestQuote
	books   []exchange.BookUpdate
	trades  []exchange.Trade
	tickers []exchange.Ticker
	marks   []exchange.MarkPrice
	ois     []exchange.OpenInterest
	funds   []exchange.FundingRate
	klines  []exchange.Kline
	liqs    []exchange.Liquidation
}

func (s *memSink) Ack(id uint64, err error) { s.acks = append(s.acks, ack{id, err}) }
func (s *memSink) BestQuote(ch string, e *exchange.BestQuote) {
	s.quotes = append(s.quotes, *e)
}
func (s *memSink) Trade(ch string, e *exchange.Trade)       { s.trades = append(s.trades, *e) }
func (s *memSink) AggTrade(ch string, e *exchange.AggTrade) {}
func (s *memSink) Book(ch string, e *exchange.BookUpdate) {
	u := *e
	u.Bids, u.Asks = append([]exchange.Level(nil), e.Bids...), append([]exchange.Level(nil), e.Asks...)
	s.books = append(s.books, u)
}
func (s *memSink) Kline(ch string, e *exchange.Kline) { s.klines = append(s.klines, *e) }
func (s *memSink) Ticker(ch string, e *exchange.Ticker) {
	s.tickers = append(s.tickers, *e)
}
func (s *memSink) Liquidation(ch string, e *exchange.Liquidation) {
	s.liqs = append(s.liqs, *e)
}
func (s *memSink) FundingRate(ch string, e *exchange.FundingRate) {
	s.funds = append(s.funds, *e)
}
func (s *memSink) OpenInterest(ch string, e *exchange.OpenInterest) {
	s.ois = append(s.ois, *e)
}
func (s *memSink) Greeks(ch string, e *exchange.Greeks) {}
func (s *memSink) MarkPrice(ch string, e *exchange.MarkPrice) {
	s.marks = append(s.marks, *e)
}
func (s *memSink) FeedEnd(ch string, e *exchange.FeedEnd) {}

func tickerCovered(rows []exchange.Ticker) bool {
	var last, open, high, low, vol, quote, pct bool
	for _, row := range rows {
		last = last || decimal.Has(row.Last)
		open = open || decimal.Has(row.Open24h)
		high = high || decimal.Has(row.High24h)
		low = low || decimal.Has(row.Low24h)
		vol = vol || decimal.Has(row.Volume24h)
		quote = quote || decimal.Has(row.QuoteVolume24h)
		pct = pct || decimal.Has(row.ChangePct24h)
	}
	return last && open && high && low && vol && quote && pct
}
