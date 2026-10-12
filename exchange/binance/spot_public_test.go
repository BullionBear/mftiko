package binance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unicode"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestSpotBookSync(t *testing.T) {
	bs := spotBookSync{}
	if bs.Snapshot() != exchange.SnapshotREST || bs.Depth() != 1000 {
		t.Fatalf("snapshot %v depth %d", bs.Snapshot(), bs.Depth())
	}
	seq := exchange.BookSeq{Last: 100, Valid: true}
	up := func(first, last uint64) *exchange.BookUpdate {
		return &exchange.BookUpdate{First: first, Last: last}
	}
	if bs.Check(seq, up(90, 100)) != exchange.BookSkip {
		t.Fatal("u equal to the snapshot was not skipped")
	}
	if bs.Check(seq, up(90, 95)) != exchange.BookSkip {
		t.Fatal("an update already inside the snapshot was not skipped")
	}
	if bs.Check(seq, up(96, 110)) != exchange.BookApply {
		t.Fatal("the update that overlaps the snapshot was not applied")
	}
	seq.Last = 110
	if bs.Check(seq, up(111, 120)) != exchange.BookApply {
		t.Fatal("the next U was not applied")
	}
	if bs.Check(seq, up(112, 130)) != exchange.BookResync {
		t.Fatal("a dropped update was not a resync")
	}
	if bs.Check(exchange.BookSeq{}, up(1, 2)) != exchange.BookResync {
		t.Fatal("a check before the snapshot was applied")
	}
	m := &market{p: spot}
	if _, ok := m.Book("btcusdt@depth@100ms"); !ok {
		t.Fatal("spot diff depth is not a book")
	}
	if _, ok := m.Book("btcusdt@depth5"); ok {
		t.Fatal("a partial book was treated as a diff")
	}
	if _, ok := m.Book("btcusdt@depth@250ms"); ok {
		t.Fatal("spot has no @250ms diff")
	}
}

func TestSpotPublicDecode(t *testing.T) {
	s := &session{}
	var got quoteSink
	book := `{"u":400900217,"s":"BTCUSDT","b":"25.35","B":"31.21","a":"25.36","A":"40.66"}`
	if err := s.Decode([]byte(book), &got); err != nil {
		t.Fatal(err)
	}
	if got.quotes != 1 || got.ch != "btcusdt@bookTicker" || got.ts != 0 {
		t.Fatalf("quote ch %q ts %d quotes %d", got.ch, got.ts, got.quotes)
	}
	wrapped := `{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}}`
	got = quoteSink{}
	if err := s.Decode([]byte(wrapped), &got); err != nil || got.quotes != 1 || got.ch != "btcusdt@bookTicker" {
		t.Fatalf("wrapped quotes %d ch %q err %v", got.quotes, got.ch, err)
	}
	depth := `{"e":"depthUpdate","E":1672515782136,"s":"BTCUSDT","U":157,"u":160,"b":[["1","2"]],"a":[["3","4"]]}`
	got = quoteSink{}
	if err := s.Decode([]byte(depth), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.books) != 1 || got.books[0].ch != "btcusdt@depth@100ms" || got.books[0].first != 157 || got.books[0].prev != 0 {
		t.Fatalf("depth %+v", got.books)
	}
	if err := s.Decode([]byte(`{"conformance":"a frame no venue sends"}`), &capture{}); err == nil {
		t.Fatal("an unknown frame was accepted")
	}
	if err := s.Decode([]byte(`["conformance",1]`), &capture{}); err == nil {
		t.Fatal("an unknown array was accepted")
	}
}

func TestSpotPlanning(t *testing.T) {
	m := &market{p: spot}
	topics := map[exchange.Topic]bool{}
	for _, c := range m.Topics() {
		topics[c.Topic] = true
	}
	for _, want := range []exchange.Topic{
		exchange.TopicBestQuote, exchange.TopicAggTrade,
		exchange.TopicOrderBookDelta, exchange.TopicTicker, exchange.KlineTopic("1m"),
	} {
		if !topics[want] {
			t.Fatalf("missing %s", want)
		}
	}
	for _, absent := range []exchange.Topic{
		exchange.TopicTrade, exchange.TopicLiquidation, exchange.TopicFundingRate,
		exchange.TopicMarkPrice, exchange.TopicOpenInterest, exchange.KlineTopic("1s"),
	} {
		if topics[absent] {
			t.Fatalf("%s is listed", absent)
		}
		if _, err := m.Atom(absent, spotInst()); !errors.Is(err, exchange.ErrUnsupported) {
			t.Fatalf("%s: %v", absent, err)
		}
	}
	a, err := m.Atom(exchange.TopicBestQuote, spotInst())
	if err != nil || a.Route != exchange.RouteDefault || a.Channel != "btcusdt@bookTicker" {
		t.Fatalf("book %+v %v", a, err)
	}
	a, err = m.Atom(exchange.TopicOrderBookDelta, spotInst())
	if err != nil || a.Channel != "btcusdt@depth@100ms" {
		t.Fatalf("depth %+v %v", a, err)
	}
	ep, err := m.Endpoint(exchange.RouteDefault, "")
	if err != nil || ep != "wss://stream.binance.com:9443/ws" {
		t.Fatalf("endpoint %q %v", ep, err)
	}
	ep, err = m.Endpoint(exchange.RouteDefault, "wss://override.example")
	if err != nil || ep != "wss://override.example/ws" {
		t.Fatalf("override %q %v", ep, err)
	}
	if _, err := m.Endpoint("public", ""); err == nil {
		t.Fatal("spot accepted the UM route")
	}
	if _, err := (&market{p: cm}).Endpoint(exchange.RouteDefault, ""); !errors.Is(err, exchange.ErrNotImplemented) {
		t.Fatalf("CM endpoint: %v", err)
	}
	ums, err := (&market{p: um}).NewSession("public")
	if err != nil {
		t.Fatal(err)
	}
	ul := ums.Limits()
	if ul.Rate[0].N != 10 || ul.Rate[0].Per != time.Second || ul.Keepalive.Timeout != 10*time.Minute {
		t.Fatalf("UM limits %+v", ul)
	}
	ss, err := m.NewSession(exchange.RouteDefault)
	if err != nil {
		t.Fatal(err)
	}
	sl := ss.Limits()
	if sl.Rate[0].N != 5 || sl.Rate[0].Per != time.Second || !sl.Keepalive.ServerPing ||
		sl.Keepalive.Timeout != time.Minute || sl.Keepalive.Interval != 0 || sl.MaxLifetime != 24*time.Hour {
		t.Fatalf("spot limits %+v", sl)
	}
	if ss.Ping() != nil {
		t.Fatal("spot sends a client ping")
	}
}

func TestSpotListingSample(t *testing.T) {
	body := []byte(`{"symbols":[
		{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"btc","quoteAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"1000000","tickSize":"0.01"},
			{"filterType":"LOT_SIZE","minQty":"0.00001","maxQty":"9000","stepSize":"0.00001"},
			{"filterType":"NOTIONAL","minNotional":"5.00000000"}
		]},
		{"symbol":"ETHUSDT","status":"PRE_TRADING","baseAsset":"ETH","quoteAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.01","maxPrice":"100000"},
			{"filterType":"LOT_SIZE","stepSize":"0.0001","maxQty":"1000"}
		]},
		{"symbol":"XRPUSDT","status":"HALT","baseAsset":"XRP","quoteAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.0001","maxPrice":"1000"},
			{"filterType":"LOT_SIZE","stepSize":"0.1","maxQty":"1000"}
		]},
		{"symbol":"BNBUSDT","status":"BREAK","baseAsset":"BNB","quoteAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.01","maxPrice":"1000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","maxQty":"1000"}
		]},
		{"symbol":"BADUSDT","status":"TRADING","baseAsset":"BAD/USD","quoteAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.01","maxPrice":"1000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","maxQty":"1000"}
		]},
		{"symbol":"NOTICK","status":"TRADING","baseAsset":"AAA","quoteAsset":"USDT","filters":[]},
		{"symbol":"WIDEUSDT","status":"TRADING","baseAsset":"BBB","quoteAsset":"USDT","filters":[
			{"filterType":"PRICE_FILTER","tickSize":"0.00000001","maxPrice":"1000000000000"},
			{"filterType":"LOT_SIZE","stepSize":"0.001","maxQty":"10"}
		]}
	]}`)
	page, err := parseSpotListing(&exchange.HTTPResponse{Status: 200, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 4 {
		t.Fatalf("kept %d, want the four storable symbols", len(page.Items))
	}
	btc := page.Items[0]
	if btc.Venue != VenueSpot || btc.Category != symbol.Spot || btc.Base != "BTC" || btc.Quote != "USDT" ||
		btc.ContractSize != 0 || btc.Settle != "" || btc.ContractValueCcy != "BTC" || btc.PriceCcy != "USDT" ||
		btc.Status != symbol.Tradable || btc.Filters[symbol.MinNotional] != 5 || btc.Filters[symbol.PriceTick] != 0.01 {
		t.Fatalf("btc %+v", btc)
	}
	if page.Items[1].Status != symbol.Prelisting || page.Items[2].Status != symbol.Halted || page.Items[3].Status != symbol.Unknown {
		t.Fatalf("statuses %s %s %s", page.Items[1].Status, page.Items[2].Status, page.Items[3].Status)
	}
	r := &rest{p: spot}
	if _, err := r.Listing(symbol.Perp); err == nil {
		t.Fatal("spot listed a perp")
	}
	call, err := r.Listing(symbol.Spot)
	if err != nil {
		t.Fatal(err)
	}
	if call.Req.Base != spotREST || call.Req.Path != "/api/v3/exchangeInfo" || string(call.Req.Query) != "permissions=SPOT&showPermissionSets=false" || call.Req.Cost[0].N != 20 {
		t.Fatalf("listing request %+v query %s", call.Req, call.Req.Query)
	}
}

func TestSpotPublicREST(t *testing.T) {
	r := &rest{p: spot}
	d, err := r.Depth(spotInst(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if d.Req.Base != spotREST || d.Req.Path != "/api/v3/depth" || string(d.Req.Query) != "symbol=BTCUSDT&limit=1000" || d.Req.Cost[0].N != 50 {
		t.Fatalf("depth %+v %s", d.Req, d.Req.Query)
	}
	if _, err := r.Depth(spotInst(), 5001); err == nil {
		t.Fatal("depth accepted a limit past 5000")
	}
	k, err := r.Klines(spotInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if k.Req.Path != "/api/v3/klines" || string(k.Req.Query) != "symbol=BTCUSDT&interval=1m&limit=5" || k.Req.Cost[0].N != 2 {
		t.Fatalf("klines %+v %s", k.Req, k.Req.Query)
	}
	if _, err := r.Klines(spotInst(), "1s", 0, 5); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("1s: %v", err)
	}
	if r.KlinePage() != 1000 {
		t.Fatalf("page %d", r.KlinePage())
	}
	if _, err := r.OpenInterest(spotInst()); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("open interest: %v", err)
	}
	if _, err := (&rest{p: cm}).OpenInterest(spotInst()); !errors.Is(err, exchange.ErrNotImplemented) {
		t.Fatalf("CM open interest: %v", err)
	}
	if (&rest{p: um}).KlinePage() != 1500 {
		t.Fatal("UM kline page changed")
	}
}

func TestRecordBinanceSpotPublic(t *testing.T) {
	if os.Getenv("MFTIKO_BINANCE_SPOT_RECORD") != "1" {
		t.Skip("set MFTIKO_BINANCE_SPOT_RECORD=1 to record production public fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dir := "testdata/conformance/Binance"
	if err := os.MkdirAll(filepath.Join(dir, "http"), 0o755); err != nil {
		t.Fatal(err)
	}
	recordSpotREST(t, dir)
	recordSpotMarket(t, ctx, dir)
}

func recordSpotREST(t *testing.T, dir string) {
	t.Helper()
	r := &rest{p: spot}
	list, err := r.Listing(symbol.Spot)
	if err != nil {
		t.Fatal(err)
	}
	body := httpDo(t, &list.Req)
	page, err := list.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	cjk := 0
	for _, item := range page.Items {
		if item.Validate() != nil {
			t.Fatalf("%s: %v", item.ExchTicker, item.Validate())
		}
		for _, r := range item.Base {
			if r > unicode.MaxASCII {
				cjk++
				break
			}
		}
	}
	t.Logf("listing spot %d, %d with a non-ascii base", len(page.Items), cjk)
	if len(page.Items) < 100 {
		t.Fatalf("listing looks short: %d", len(page.Items))
	}
	f, err := os.Create(filepath.Join(dir, "http", "listing_spot.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := conformance.SaveExchange(f, &list.Req, body); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
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
	depth, err := r.Depth(spotInst(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	save("depth", depth.Req, func(resp *exchange.HTTPResponse) error {
		_, err := depth.Parse(resp)
		return err
	})
	kl, err := r.Klines(spotInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	save("klines", kl.Req, func(resp *exchange.HTTPResponse) error {
		rows, err := kl.Parse(resp)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("no closed kline")
		}
		return nil
	})
}

func recordSpotMarket(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	tp, conn, sess, stop := dialProduct(t, ctx, spot, exchange.RouteDefault)
	defer stop()
	sub(t, ctx, conn, sess, "btcusdt@bookTicker")
	waitN(t, tp, `"s":"BTCUSDT"`, 2, 15*time.Second)
	unsub(t, ctx, conn, sess, "btcusdt@bookTicker")
	sub(t, ctx, conn, sess, "btcusdt@depth@100ms")
	waitN(t, tp, `"e":"depthUpdate"`, 2, 15*time.Second)
	call, err := (&rest{p: spot}).Depth(spotInst(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	resp := httpDo(t, &call.Req)
	if _, err := call.Parse(resp); err != nil {
		t.Fatal(err)
	}
	tp.add(piece{resp: resp, req: &call.Req, ch: "btcusdt@depth@100ms"})
	waitAfter(t, tp, `"e":"depthUpdate"`, 10, 20*time.Second)
	for _, ch := range []struct{ name, needle string }{
		{"btcusdt@aggTrade", `"e":"aggTrade"`},
		{"btcusdt@kline_1m", `"e":"kline"`},
		{"btcusdt@ticker", `"e":"24hrTicker"`},
	} {
		sub(t, ctx, conn, sess, ch.name)
		waitN(t, tp, ch.needle, 1, 20*time.Second)
		unsub(t, ctx, conn, sess, ch.name)
	}
	stop()
	if err := replaySpot(tp.snapshot()); err != nil {
		t.Fatal(err)
	}
	writeSession(t, dir, "default", VenueSpot, exchange.RouteDefault,
		"Spot BTCUSDT bookTicker, diff depth with a REST snapshot, aggTrade, kline and ticker.", tp.snapshot())
}

func replaySpot(pieces []piece) error {
	sess := &session{}
	var items []struct {
		rest bool
		u    exchange.BookUpdate
	}
	for i, p := range pieces {
		if p.resp != nil {
			call, err := (&rest{p: spot}).Depth(spotInst(), 1000)
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
	bs := spotBookSync{}
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
			resync = append(resync, fmt.Sprintf("U=%d u=%d last=%d", it.u.First, it.u.Last, seq.Last))
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

type quoteSink struct {
	capture
	ch string
	ts int64
}

func (q *quoteSink) BestQuote(ch string, e *exchange.BestQuote) {
	q.quotes++
	q.ch = ch
	q.ts = e.ExchTsNs
}
