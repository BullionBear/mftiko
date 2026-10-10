package okx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

func TestRecordOkxPublic(t *testing.T) {
	if os.Getenv("MFTIKO_OKX_PUBLIC_RECORD") != "1" {
		t.Skip("set MFTIKO_OKX_PUBLIC_RECORD=1 to record production public fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dir := "testdata/conformance/Okx"
	if err := os.MkdirAll(filepath.Join(dir, "http"), 0o755); err != nil {
		t.Fatal(err)
	}
	recordListings(t, dir)
	recordKlines(t, dir)
	recordPublic(t, ctx, dir)
	recordBusiness(t, ctx, dir)
}

func recordListings(t *testing.T, dir string) {
	t.Helper()
	r := &rest{}
	spot, err := r.Listing(symbol.Spot)
	if err != nil {
		t.Fatal(err)
	}
	perp, err := r.Listing(symbol.Perp)
	if err != nil {
		t.Fatal(err)
	}
	fut, err := r.Listing(symbol.Future)
	if err != nil {
		t.Fatal(err)
	}
	inv, err := r.Listing(symbol.Inverse)
	if err != nil {
		t.Fatal(err)
	}
	spotBody := httpDo(t, &spot.Req)
	swapBody := httpDo(t, &perp.Req)
	futBody := httpDo(t, &fut.Req)
	sp, err := spot.Parse(spotBody)
	if err != nil {
		t.Fatal(err)
	}
	pp, err := perp.Parse(swapBody)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fut.Parse(futBody)
	if err != nil {
		t.Fatal(err)
	}
	ip, err := inv.Parse(swapBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("listing spot %d perp %d future %d inverse %d", len(sp.Items), len(pp.Items), len(fp.Items), len(ip.Items))
	if len(sp.Items) < 100 || len(pp.Items) < 50 || len(fp.Items) < 1 || len(ip.Items) < 1 {
		t.Fatalf("listing looks short: spot %d perp %d future %d inverse %d", len(sp.Items), len(pp.Items), len(fp.Items), len(ip.Items))
	}
	if !hasInst(sp.Items, "BTC-USDT") || !hasInst(pp.Items, "BTC-USDT-SWAP") || !hasInst(ip.Items, "BTC-USD-SWAP") {
		t.Fatal("BTC-USDT, BTC-USDT-SWAP or BTC-USD-SWAP was dropped")
	}
	if sp.Next != nil || pp.Next != nil || fp.Next != nil || ip.Next != nil {
		t.Fatal("a listing page has a cursor; OKX does not page this call")
	}
	saveExchange(t, dir, "listing_spot", &spot.Req, spotBody)
	saveExchange(t, dir, "listing_perp", &perp.Req, swapBody)
	saveExchange(t, dir, "listing_future", &fut.Req, futBody)
	saveExchange(t, dir, "listing_inverse", &inv.Req, swapBody)
}

func hasInst(items []symbol.Listing, id string) bool {
	for _, item := range items {
		if item.ExchTicker == id {
			return true
		}
	}
	return false
}

func recordKlines(t *testing.T, dir string) {
	t.Helper()
	call, err := (&rest{}).Klines(spotInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	body := httpDo(t, &call.Req)
	rows, err := call.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 1 || !rows[0].Closed {
		t.Fatalf("klines %+v", rows)
	}
	t.Logf("klines %d oldest %d", len(rows), rows[0].OpenTimeNs)
	saveExchange(t, dir, "klines", &call.Req, body)
}

func saveExchange(t *testing.T, dir, name string, req *exchange.HTTPRequest, resp *exchange.HTTPResponse) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, "http", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := conformance.SaveExchange(f, req, resp); err != nil {
		t.Fatal(err)
	}
}

func recordPublic(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	tape := dialPublic(t, ctx, "public", []string{
		"bbo-tbt:BTC-USDT",
		"bbo-tbt:BTC-USDT-SWAP",
		"trades:BTC-USDT",
		"trades:BTC-USDT-SWAP",
		"books:BTC-USDT",
		"books:BTC-USDT-SWAP",
		"tickers:BTC-USDT",
		"tickers:BTC-USDT-SWAP",
		"funding-rate:BTC-USDT-SWAP",
		"open-interest:BTC-USDT-SWAP",
		"mark-price:BTC-USDT-SWAP",
	})
	if !tape.readyPublic() {
		t.Fatalf("public: short tape %s", tape.stats())
	}
	writeSession(t, dir, "public", "public", "mainnet public BTC-USDT and BTC-USDT-SWAP", tape.copied())
}

func recordBusiness(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	tape := dialPublic(t, ctx, "business", []string{
		"candle1m:BTC-USDT",
		"candle1m:BTC-USDT-SWAP",
	})
	if !tape.readyBusiness() {
		t.Fatalf("business: short tape %s", tape.stats())
	}
	writeSession(t, dir, "business", "business", "mainnet business BTC-USDT and BTC-USDT-SWAP 1m candles", tape.copied())
}

func dialPublic(t *testing.T, ctx context.Context, route string, channels []string) *pubTape {
	t.Helper()
	m := &market{}
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
	defer cancel()
	conn, err := ws.Dial(connCtx, ep, ws.Options{
		ReadLimit: lim.ReadLimit, Keepalive: lim.Keepalive, MaxLifetime: lim.MaxLifetime, Ping: sess.Ping(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tape := &pubTape{route: route, sess: sess, books: map[string]*bookStat{}}
	done := make(chan struct{})
	go func() {
		_ = conn.Run(connCtx, func(frame []byte) error {
			if err := tape.add(frame); err != nil {
				return err
			}
			if route == "public" && tape.readyPublic() || route == "business" && tape.readyBusiness() {
				return errRecorded
			}
			return nil
		})
		close(done)
	}()
	reqs, err := sess.Subscribe(channels)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range reqs {
		if err := conn.Write(ctx, q.Frame); err != nil {
			t.Fatalf("subscribe %s: %v", route, err)
		}
	}
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		cancel()
		<-done
	}
	if err := tape.err(); err != nil {
		t.Fatalf("%s: %v", route, err)
	}
	return tape
}

var errRecorded = errors.New("recorded")

type bookStat struct {
	snaps, applied int
	last           uint64
	valid          bool
}

type pubTape struct {
	mu         sync.Mutex
	route      string
	sess       exchange.Session
	raw        [][]byte
	books      map[string]*bookStat
	quotes     int
	trades     int
	quoteVol   bool
	interval   bool
	oiValue    bool
	marks      int
	klines     int
	klineQuote bool
	fail       error
}

func (p *pubTape) add(frame []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	buf := append([]byte(nil), frame...)
	got := &recSink{}
	if err := p.sess.Decode(buf, got); err != nil {
		p.fail = fmt.Errorf("%s", clip(buf))
		return fmt.Errorf("%w: %s", err, clip(buf))
	}
	p.raw = append(p.raw, buf)
	p.quotes += len(got.quotes)
	p.trades += len(got.trades)
	p.marks += len(got.marks)
	for _, tk := range got.tickers {
		p.quoteVol = p.quoteVol || decimal.Has(tk.QuoteVolume24h)
	}
	for _, f := range got.funds {
		p.interval = p.interval || f.IntervalS != 0
	}
	for _, o := range got.ois {
		p.oiValue = p.oiValue || decimal.Has(o.OpenInterestValue)
	}
	for _, k := range got.klines {
		p.klines++
		p.klineQuote = p.klineQuote || decimal.Has(k.QuoteVolume)
	}
	mkt := &market{}
	for i, u := range got.books {
		ch := got.bookCh[i]
		bs, ok := mkt.Book(ch)
		if !ok {
			p.fail = fmt.Errorf("%s is not a book", ch)
			return p.fail
		}
		st := p.books[ch]
		if st == nil {
			st = &bookStat{}
			p.books[ch] = st
		}
		if !st.valid {
			if !u.Snapshot {
				continue
			}
			st.snaps++
			st.last = u.Last
			st.valid = true
			continue
		}
		if u.Snapshot {
			st.snaps++
			st.last = u.Last
			continue
		}
		switch bs.Check(exchange.BookSeq{Last: st.last, Valid: true}, u) {
		case exchange.BookApply:
			st.applied++
			st.last = u.Last
		case exchange.BookSkip:
		case exchange.BookResync:
			p.fail = fmt.Errorf("%s resync last %d prev %d seq %d", ch, st.last, u.Prev, u.Last)
			return p.fail
		}
	}
	return nil
}

func (p *pubTape) readyPublic() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ch := range []string{"books:BTC-USDT", "books:BTC-USDT-SWAP"} {
		st := p.books[ch]
		if st == nil || st.snaps < 1 || st.applied < 3 {
			return false
		}
	}
	return p.quotes > 0 && p.trades > 0 && p.quoteVol && p.interval && p.oiValue && p.marks > 0
}

func (p *pubTape) readyBusiness() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.klines > 0 && p.klineQuote
}

func (p *pubTape) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fail
}

func (p *pubTape) copied() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]byte, len(p.raw))
	copy(out, p.raw)
	return out
}

func (p *pubTape) stats() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("frames=%d books=%v quotes=%d trades=%d quoteVol=%v interval=%v oi=%v marks=%d klines=%d klineQuote=%v",
		len(p.raw), p.books, p.quotes, p.trades, p.quoteVol, p.interval, p.oiValue, p.marks, p.klines, p.klineQuote)
}

func writeSession(t *testing.T, dir, name, route, note string, frames [][]byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := conformance.NewWriter(&buf, conformance.Record{
		Venue: Name, Side: conformance.SideMkt, Route: route, Note: note,
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		if err := w.Frame(conformance.KindIn, f); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "ws", name+".jsonl")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s (%d bytes, %d frames)", path, buf.Len(), len(frames))
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
	for _, h := range req.Header {
		hr.Header.Add(h.Name, h.Value)
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
		t.Fatalf("%s: http %d %s", u, resp.StatusCode, clip(body))
	}
	return &exchange.HTTPResponse{Status: resp.StatusCode, Body: body}
}

func clip(b []byte) string {
	if len(b) > 180 {
		b = b[:180]
	}
	return string(b)
}

type recSink struct {
	quotes  []*exchange.BestQuote
	trades  []*exchange.Trade
	tickers []*exchange.Ticker
	funds   []*exchange.FundingRate
	ois     []*exchange.OpenInterest
	marks   []*exchange.MarkPrice
	klines  []*exchange.Kline
	books   []*exchange.BookUpdate
	bookCh  []string
}

func (s *recSink) Ack(uint64, error)                         {}
func (s *recSink) BestQuote(_ string, e *exchange.BestQuote) { s.quotes = append(s.quotes, e) }
func (s *recSink) Trade(_ string, e *exchange.Trade)         { s.trades = append(s.trades, e) }
func (s *recSink) AggTrade(string, *exchange.AggTrade)       {}
func (s *recSink) Book(ch string, e *exchange.BookUpdate) {
	u := *e
	s.books = append(s.books, &u)
	s.bookCh = append(s.bookCh, ch)
}
func (s *recSink) Kline(_ string, e *exchange.Kline) { s.klines = append(s.klines, e) }
func (s *recSink) Ticker(_ string, e *exchange.Ticker) {
	tk := *e
	s.tickers = append(s.tickers, &tk)
}
func (s *recSink) Liquidation(string, *exchange.Liquidation) {}
func (s *recSink) FundingRate(_ string, e *exchange.FundingRate) {
	f := *e
	s.funds = append(s.funds, &f)
}
func (s *recSink) OpenInterest(_ string, e *exchange.OpenInterest) {
	o := *e
	s.ois = append(s.ois, &o)
}
func (s *recSink) Greeks(string, *exchange.Greeks) {}
func (s *recSink) MarkPrice(_ string, e *exchange.MarkPrice) {
	m := *e
	s.marks = append(s.marks, &m)
}
func (s *recSink) FeedEnd(string, *exchange.FeedEnd) {}
