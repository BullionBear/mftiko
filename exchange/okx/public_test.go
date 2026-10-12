package okx

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

func TestPublicAtom(t *testing.T) {
	m := &market{}
	spot, swap := spotInst(), swapInst()
	cases := []struct {
		topic   exchange.Topic
		inst    *symbol.Info
		route   string
		channel string
		bad     bool
	}{
		{exchange.TopicBestQuote, spot, "public", "bbo-tbt:BTC-USDT", false},
		{exchange.TopicOrderBookDelta, spot, "public", "books:BTC-USDT", false},
		{exchange.TopicOrderBookDelta, swap, "public", "books:BTC-USDT-SWAP", false},
		{exchange.TopicTrade, spot, "public", "trades:BTC-USDT", false},
		{exchange.TopicTicker, spot, "public", "tickers:BTC-USDT", false},
		{exchange.KlineTopic("1m"), spot, "business", "candle1m:BTC-USDT", false},
		{exchange.KlineTopic("1h"), swap, "business", "candle1H:BTC-USDT-SWAP", false},
		{exchange.KlineTopic("1mo"), spot, "business", "candle1M:BTC-USDT", false},
		{exchange.TopicFundingRate, swap, "public", "funding-rate:BTC-USDT-SWAP", false},
		{exchange.TopicOpenInterest, swap, "public", "open-interest:BTC-USDT-SWAP", false},
		{exchange.TopicMarkPrice, swap, "public", "mark-price:BTC-USDT-SWAP", false},
		{exchange.TopicFundingRate, spot, "", "", true},
		{exchange.TopicLiquidation, swap, "", "", true},
		{exchange.TopicAggTrade, spot, "", "", true},
		{exchange.KlineTopic("2mo"), spot, "", "", true},
	}
	for _, tc := range cases {
		a, err := m.Atom(tc.topic, tc.inst)
		if tc.bad {
			if !errors.Is(err, exchange.ErrUnsupported) {
				t.Errorf("%s: err %v", tc.topic, err)
			}
			continue
		}
		if err != nil || a.Route != tc.route || a.Channel != tc.channel {
			t.Errorf("%s: %+v %v", tc.topic, a, err)
		}
	}
	if _, err := m.Atom(exchange.TopicTrade, nil); err == nil {
		t.Fatal("nil instrument")
	}
	pub, err := m.Endpoint("public", "")
	if err != nil || pub != prodPublicWS {
		t.Fatalf("public %s %v", pub, err)
	}
	biz, err := m.Endpoint("business", "wss://override.example/ignored")
	if err != nil || biz != "wss://override.example/ws/v5/business" {
		t.Fatalf("business %s %v", biz, err)
	}
	bs, ok := m.Book("books:BTC-USDT")
	if !ok || bs.Snapshot() != exchange.SnapshotStream {
		t.Fatalf("book %v %v", ok, bs)
	}
	if _, ok := m.Book("bbo-tbt:BTC-USDT"); ok {
		t.Fatal("bbo is not a book")
	}
	if _, ok := bs.(exchange.BookChecksum); ok {
		t.Fatal("checksum is deprecated and always 0")
	}
}

func TestBookChain(t *testing.T) {
	var b okxBook
	seq := exchange.BookSeq{Last: 10, Valid: true}
	if b.Check(seq, &exchange.BookUpdate{Prev: 10, Last: 14}) != exchange.BookApply {
		t.Fatal("chain")
	}
	if b.Check(seq, &exchange.BookUpdate{Prev: 14, Last: 20}) != exchange.BookResync {
		t.Fatal("gap")
	}
	held := exchange.BookSeq{Last: 14, Valid: true}
	if b.Check(held, &exchange.BookUpdate{Prev: 10, Last: 14}) != exchange.BookSkip {
		t.Fatal("old")
	}
	if b.Check(exchange.BookSeq{}, &exchange.BookUpdate{Prev: 10, Last: 14}) != exchange.BookResync {
		t.Fatal("no snapshot")
	}
}

func TestPublicSubscribe(t *testing.T) {
	s := &pubSession{}
	if string(s.Ping()) != "ping" {
		t.Fatalf("ping %q", s.Ping())
	}
	lim := s.Limits()
	if lim.PerRequest != 0 || lim.PerRequestBytes != channelBytes || lim.ReadLimit <= 0 {
		t.Fatalf("limits %+v", lim)
	}
	if len(lim.Rate) != 1 || lim.Rate[0].N != subPerHour {
		t.Fatalf("rate %+v", lim.Rate)
	}
	reqs, err := s.Subscribe([]string{"books:BTC-USDT", "trades:BTC-USDT"})
	if err != nil || len(reqs) != 1 || reqs[0].ID != 1 {
		t.Fatalf("subscribe %+v %v", reqs, err)
	}
	if !strings.Contains(string(reqs[0].Frame), `"id":"1"`) || !strings.Contains(string(reqs[0].Frame), `"instId":"BTC-USDT"`) {
		t.Fatalf("frame %s", reqs[0].Frame)
	}
	wide := "books:" + strings.Repeat("A", channelBytes)
	if _, err := s.Subscribe([]string{wide}); err == nil {
		t.Fatal("channel longer than the budget")
	}
	one := "books:" + strings.Repeat("B", channelBytes-len("books:"))
	pages, err := (&pubSession{}).Subscribe([]string{one, one})
	if err != nil || len(pages) != 2 || pages[0].ID != 1 || pages[1].ID != 2 {
		t.Fatalf("pages %+v %v", pages, err)
	}
	more, err := s.Unsubscribe([]string{"trades:BTC-USDT"})
	if err != nil || len(more) != 1 || more[0].ID != 2 {
		t.Fatalf("unsubscribe id %v %v", more, err)
	}
}

func TestPublicDecode(t *testing.T) {
	s := &pubSession{}
	sink := &pubSink{}
	ack := []byte(`{"id":"7","event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"connId":"c"}`)
	if err := s.Decode(ack, sink); err != nil || len(sink.acks) != 1 || sink.acks[0] != 7 || sink.ackErr[0] != nil {
		t.Fatalf("ack %+v %v", sink.acks, err)
	}
	if err := s.Decode([]byte("pong"), sink); err != nil {
		t.Fatal(err)
	}
	book := []byte(`{"action":"snapshot","arg":{"channel":"books","instId":"BTC-USDT"},"data":[{"asks":[["2","1","0","1"]],"bids":[["1","3","0","1"]],"ts":"1000","checksum":0,"prevSeqId":-1,"seqId":10}]}`)
	if err := s.Decode(book, sink); err != nil || len(sink.books) != 1 || !sink.books[0].Snapshot || sink.books[0].Last != 10 {
		t.Fatalf("snapshot %+v %v", sink.books, err)
	}
	if sink.chs[0] != "books:BTC-USDT" {
		t.Fatalf("channel %s", sink.chs[0])
	}
	up := []byte(`{"action":"update","arg":{"channel":"books","instId":"BTC-USDT"},"data":[{"asks":[],"bids":[["1","0","0","0"]],"ts":"1001","checksum":0,"prevSeqId":10,"seqId":14}]}`)
	if err := s.Decode(up, sink); err != nil || sink.books[1].Snapshot || sink.books[1].Prev != 10 || sink.books[1].Bids[0].Qty != 0 {
		t.Fatalf("update %+v %v", sink.books, err)
	}
	tick := []byte(`{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instType":"SPOT","last":"9","open24h":"8","high24h":"10","low24h":"7","vol24h":"1.5","volCcy24h":"12","ts":"1000"}]}`)
	if err := s.Decode(tick, sink); err != nil || len(sink.tickers) != 1 {
		t.Fatal(err)
	}
	got := sink.tickers[0]
	if decimal.Has(got.ChangePct24h) || got.Volume24h != 1.5 || got.QuoteVolume24h != 12 {
		t.Fatalf("ticker %+v", got)
	}
	swapTick := []byte(`{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"instType":"SWAP","last":"9","open24h":"8","high24h":"10","low24h":"7","vol24h":"100","volCcy24h":"1","ts":"1000"}]}`)
	if err := s.Decode(swapTick, sink); err != nil {
		t.Fatal(err)
	}
	if decimal.Has(sink.tickers[1].QuoteVolume24h) || sink.tickers[1].Volume24h != 1 {
		t.Fatalf("swap ticker %+v", sink.tickers[1])
	}
	trade := []byte(`{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"tradeId":"42","px":"9","sz":"0.1","side":"buy","ts":"1000"}]}`)
	if err := s.Decode(trade, sink); err != nil || sink.trades[0].TradeID != "42" || sink.trades[0].Side != exchange.SideBuy {
		t.Fatalf("trade %+v %v", sink.trades, err)
	}
	fund := []byte(`{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"fundingRate":"0.0001","fundingTime":"3000","nextFundingTime":"3000","ts":"1000"}]}`)
	if err := s.Decode(fund, sink); err != nil || sink.funds[0].IntervalS != 0 || sink.funds[0].Rate != 0.0001 {
		t.Fatalf("funding %+v %v", sink.funds, err)
	}
	fund = []byte(`{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"fundingRate":"0.0001","fundingTime":"1000","nextFundingTime":"28801000","ts":"1000"}]}`)
	if err := s.Decode(fund, sink); err != nil || sink.funds[1].IntervalS != 28800 {
		t.Fatalf("interval %+v %v", sink.funds, err)
	}
	candle := []byte(`{"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1000","1","2","0.5","1.5","3","30","30","0"]]}`)
	if err := s.Decode(candle, sink); err != nil || sink.klines[0].Interval != "1m" || sink.klines[0].Closed || sink.klines[0].Volume != 3 {
		t.Fatalf("candle %+v %v", sink.klines, err)
	}
	if err := s.Decode([]byte(`{"event":"login"}`), sink); err == nil {
		t.Fatal("login on the public socket")
	}
	if err := s.Decode([]byte(`{"arg":{"channel":"books5","instId":"BTC-USDT"}}`), sink); err != nil {
		t.Fatal(err)
	}
}

func TestListingFilters(t *testing.T) {
	body := []byte(`{"code":"0","data":[
		{"instId":"BTC-USDT","instType":"SPOT","baseCcy":"BTC","quoteCcy":"USDT","lotSz":"0.00000001","minSz":"0.00001","tickSz":"0.1","maxLmtSz":"9999999999","maxLmtAmt":"20000000","state":"live"},
		{"instId":"NO-PAIR","instType":"SPOT","baseCcy":"BTC/X","quoteCcy":"USDT","lotSz":"0.1","minSz":"0.1","tickSz":"0.1","state":"live"},
		{"instId":"BTC-USDT-SWAP","instType":"SWAP","uly":"BTC-USDT","ctVal":"0.01","ctValCcy":"BTC","ctType":"linear","settleCcy":"USDT","lotSz":"0.01","minSz":"0.01","tickSz":"0.1","maxLmtSz":"100000000","state":"live"},
		{"instId":"BTC-USD-SWAP","instType":"SWAP","uly":"BTC-USD","ctVal":"100","ctValCcy":"USD","ctType":"inverse","settleCcy":"BTC","lotSz":"1","minSz":"1","tickSz":"0.1","maxLmtSz":"1000000","state":"live"},
		{"instId":"BTC-USD-QUANTO","instType":"SWAP","uly":"BTC-USD","ctVal":"0.01","ctValCcy":"BTC","ctType":"linear","settleCcy":"USDC","lotSz":"0.01","minSz":"0.01","tickSz":"0.1","state":"live"},
		{"instId":"BTC-USD_UM-261030","instType":"FUTURES","uly":"BTC-USD","ctVal":"0.01","ctValCcy":"BTC","ctType":"linear","settleCcy":"USD","lotSz":"0.01","minSz":"0.01","tickSz":"0.1","maxLmtSz":"1000","expTime":"1793347200000","state":"live"},
		{"instId":"BTC-USD-261030","instType":"FUTURES","uly":"BTC-USD","ctVal":"100","ctValCcy":"USD","ctType":"inverse","settleCcy":"BTC","lotSz":"1","minSz":"1","tickSz":"0.1","expTime":"1793347200000","state":"live"}
	]}`)
	resp := &exchange.HTTPResponse{Status: 200, Body: body}

	spot := mustPage(t, symbol.Spot, resp)
	if len(spot) != 1 || spot[0].ExchTicker != "BTC-USDT" {
		t.Fatalf("spot %+v", spot)
	}
	if spot[0].Settle != "" || spot[0].ContractSize != 0 {
		t.Fatalf("spot contract %+v", spot[0])
	}
	step := spot[0].Filters[symbol.QtyStep]
	maxQty := spot[0].Filters[symbol.MaxQty]
	if !decimal.Equal(step, 1e-8, 1e-8) || !(maxQty > 0) || maxQty >= 1e7 {
		t.Fatalf("spot step %v max %v", step, maxQty)
	}
	assertSteps(t, spot[0])
	if spot[0].Filters[symbol.MaxNotional] != 20_000_000 {
		t.Fatalf("notional %v", spot[0].Filters[symbol.MaxNotional])
	}

	perp := mustPage(t, symbol.Perp, resp)
	if len(perp) != 1 || perp[0].ExchTicker != "BTC-USDT-SWAP" {
		t.Fatalf("perp %+v", ids(perp))
	}
	if !decimal.Equal(perp[0].ContractSize, 0.01, 0.01) || !decimal.Equal(perp[0].Filters[symbol.QtyStep], 0.0001, 0.0001) {
		t.Fatalf("perp size %v step %v", perp[0].ContractSize, perp[0].Filters[symbol.QtyStep])
	}
	if perp[0].ContractValueCcy != "BTC" || perp[0].Settle != "USDT" || perp[0].PriceCcy != "USDT" {
		t.Fatalf("perp ccy %+v", perp[0])
	}
	if !decimal.Equal(perp[0].Filters[symbol.MaxQty], 1e6, 0.0001) {
		t.Fatalf("perp max %v", perp[0].Filters[symbol.MaxQty])
	}
	assertSteps(t, perp[0])

	inv := mustPage(t, symbol.Inverse, resp)
	if len(inv) != 1 || inv[0].ExchTicker != "BTC-USD-SWAP" || inv[0].ContractValueCcy != "USD" || inv[0].Settle != "BTC" {
		t.Fatalf("inverse %+v", inv)
	}
	if !decimal.Equal(inv[0].Filters[symbol.QtyStep], 100, 100) {
		t.Fatalf("inverse step %v", inv[0].Filters[symbol.QtyStep])
	}

	fut := mustPage(t, symbol.Future, resp)
	if len(fut) != 1 || fut[0].ExchTicker != "BTC-USD_UM-261030" || fut[0].ExpiryCode != "261030" {
		t.Fatalf("future %+v", fut)
	}
	if fut[0].ExpiryNs != 1793347200000*int64(1_000_000) || fut[0].Quote != "USD" {
		t.Fatalf("future expiry %d quote %s", fut[0].ExpiryNs, fut[0].Quote)
	}
	assertSteps(t, fut[0])

	if _, err := (&rest{}).OpenInterest(swapInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("oi %v", err)
	}
	if _, err := (&rest{}).Listing(symbol.Option); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("option %v", err)
	}
}

func TestKlinesClosed(t *testing.T) {
	body := []byte(`{"code":"0","data":[
		["2000","2","3","1","2.5","1","20","20","0"],
		["1000","1","2","0.5","1.5","4","40","40","1"]
	]}`)
	call, err := (&rest{}).Klines(spotInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if call.Req.Path != "/api/v5/market/candles" || !strings.Contains(string(call.Req.Query), "bar=1m") {
		t.Fatalf("req %+v", call.Req)
	}
	rows, err := call.Parse(&exchange.HTTPResponse{Status: 200, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Closed || rows[0].OpenTimeNs != 1000*int64(1_000_000) || rows[0].Volume != 4 {
		t.Fatalf("klines %+v", rows)
	}
}

func mustPage(t *testing.T, c symbol.Category, resp *exchange.HTTPResponse) []symbol.Listing {
	t.Helper()
	call, err := (&rest{}).Listing(c)
	if err != nil {
		t.Fatal(err)
	}
	page, err := call.Parse(resp)
	if err != nil {
		t.Fatal(err)
	}
	return page.Items
}

func ids(ls []symbol.Listing) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.ExchTicker
	}
	return out
}

func assertSteps(t *testing.T, l symbol.Listing) {
	t.Helper()
	if err := l.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{symbol.PriceTick, symbol.MaxPrice}, {symbol.QtyStep, symbol.MaxQty}} {
		step := l.Filters[pair[0]]
		top := l.Filters[pair[1]]
		if !(top > 0) {
			top = 1e7
		}
		digits := len(strconv.FormatFloat(math.Floor(top), 'f', 0, 64)) + decimal.Places(step)
		if digits > 15 {
			t.Fatalf("%s %s %v up to %v needs %d digits", l.ExchTicker, pair[0], step, top, digits)
		}
	}
}

type pubSink struct {
	acks    []uint64
	ackErr  []error
	chs     []string
	books   []*exchange.BookUpdate
	tickers []*exchange.Ticker
	trades  []*exchange.Trade
	funds   []*exchange.FundingRate
	klines  []*exchange.Kline
}

func (s *pubSink) Ack(id uint64, err error) {
	s.acks = append(s.acks, id)
	s.ackErr = append(s.ackErr, err)
}
func (s *pubSink) BestQuote(string, *exchange.BestQuote) {}
func (s *pubSink) Trade(_ string, e *exchange.Trade) {
	tr := *e
	s.trades = append(s.trades, &tr)
}
func (s *pubSink) AggTrade(string, *exchange.AggTrade) {}
func (s *pubSink) Book(ch string, e *exchange.BookUpdate) {
	s.chs = append(s.chs, ch)
	u := *e
	u.Bids, u.Asks = append([]exchange.Level(nil), e.Bids...), append([]exchange.Level(nil), e.Asks...)
	s.books = append(s.books, &u)
}
func (s *pubSink) Kline(_ string, e *exchange.Kline) { s.klines = append(s.klines, e) }
func (s *pubSink) Ticker(_ string, e *exchange.Ticker) {
	t := *e
	s.tickers = append(s.tickers, &t)
}
func (s *pubSink) Liquidation(string, *exchange.Liquidation) {}
func (s *pubSink) FundingRate(_ string, e *exchange.FundingRate) {
	f := *e
	s.funds = append(s.funds, &f)
}
func (s *pubSink) OpenInterest(string, *exchange.OpenInterest) {}
func (s *pubSink) Greeks(string, *exchange.Greeks)             {}
func (s *pubSink) MarkPrice(string, *exchange.MarkPrice) {}
