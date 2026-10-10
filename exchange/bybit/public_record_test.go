package bybit

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

func TestRecordBybitPublic(t *testing.T) {
	if os.Getenv("MFTIKO_BYBIT_PUBLIC_RECORD") != "1" {
		t.Skip("set MFTIKO_BYBIT_PUBLIC_RECORD=1 to record production public fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dir := "testdata/conformance/Bybit"
	if err := os.MkdirAll(filepath.Join(dir, "http"), 0o755); err != nil {
		t.Fatal(err)
	}
	recordListings(t, dir)
	recordKlines(t, dir)
	recordRoute(t, ctx, dir, "spot")
	recordRoute(t, ctx, dir, "linear")
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
	spotBody := httpDo(t, &spot.Req)
	linBody := httpDo(t, &perp.Req)
	sp, err := spot.Parse(spotBody)
	if err != nil {
		t.Fatal(err)
	}
	pp, err := perp.Parse(linBody)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fut.Parse(linBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("listing spot %d perp %d future %d", len(sp.Items), len(pp.Items), len(fp.Items))
	if len(sp.Items) < 100 || len(pp.Items) < 100 || len(fp.Items) < 1 {
		t.Fatalf("listing looks short: spot %d perp %d future %d", len(sp.Items), len(pp.Items), len(fp.Items))
	}
	if sp.Next != nil || pp.Next != nil || fp.Next != nil {
		t.Fatal("a listing page has a cursor; record the next page before committing one page")
	}
	saveExchange(t, dir, "listing_spot", &spot.Req, spotBody)
	saveExchange(t, dir, "listing_perp", &perp.Req, linBody)
	saveExchange(t, dir, "listing_future", &fut.Req, linBody)
}

func recordKlines(t *testing.T, dir string) {
	t.Helper()
	call, err := (&rest{}).Klines(btcInst(), "1m", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	body := httpDo(t, &call.Req)
	rows, err := call.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 1 {
		t.Fatal("klines parsed empty")
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

func recordRoute(t *testing.T, ctx context.Context, dir, route string) {
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
	tape := &pubTape{route: route, sess: sess}
	done := make(chan struct{})
	go func() {
		_ = conn.Run(connCtx, func(frame []byte) error {
			if err := tape.add(frame); err != nil {
				return err
			}
			if tape.ready() {
				return errRecorded
			}
			return nil
		})
		close(done)
	}()
	chs := []string{
		"orderbook.1.BTCUSDT",
		"orderbook.50.BTCUSDT",
		"publicTrade.BTCUSDT",
		"tickers.BTCUSDT",
		"kline.1.BTCUSDT",
	}
	if route == "linear" {
		chs = append(chs, "allLiquidation.BTCUSDT")
	}
	reqs, err := sess.Subscribe(chs)
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
	if !tape.ready() {
		t.Fatalf("%s: short tape %+v", route, tape.stats())
	}
	writeSession(t, dir, route, route, "mainnet "+route+" BTCUSDT public book, trades, ticker and 1m kline", tape.copied())
}

var errRecorded = errors.New("recorded")

type pubTape struct {
	mu      sync.Mutex
	route   string
	sess    exchange.Session
	raw     [][]byte
	sink    memSink
	snaps   int
	applied int
	last    uint64
	valid   bool
	fail    error
}

func (p *pubTape) add(frame []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	buf := append([]byte(nil), frame...)
	got := &memSink{}
	if err := p.sess.Decode(buf, got); err != nil {
		p.fail = fmt.Errorf("%s", clip(buf))
		return fmt.Errorf("%w: %s", err, clip(buf))
	}
	p.raw = append(p.raw, buf)
	p.sink.quotes = append(p.sink.quotes, got.quotes...)
	p.sink.trades = append(p.sink.trades, got.trades...)
	p.sink.tickers = append(p.sink.tickers, got.tickers...)
	p.sink.marks = append(p.sink.marks, got.marks...)
	p.sink.ois = append(p.sink.ois, got.ois...)
	p.sink.funds = append(p.sink.funds, got.funds...)
	p.sink.klines = append(p.sink.klines, got.klines...)
	p.sink.liqs = append(p.sink.liqs, got.liqs...)
	for _, b := range got.books {
		switch {
		case b.Snapshot || !p.valid:
			p.snaps++
			p.last = b.Last
			p.valid = true
		default:
			switch (bybitBook{}).Check(exchange.BookSeq{Last: p.last, Valid: true}, &b) {
			case exchange.BookApply:
				p.applied++
				p.last = b.Last
			case exchange.BookSkip:
			default:
				p.fail = fmt.Errorf("book resync u=%d last=%d", b.Last, p.last)
				return p.fail
			}
		}
	}
	return nil
}

func (p *pubTape) ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snaps < 1 || p.applied < 5 || len(p.sink.quotes) < 1 || len(p.sink.trades) < 1 || len(p.sink.klines) < 1 || !tickerCovered(p.sink.tickers) {
		return false
	}
	if p.route != "linear" {
		return true
	}
	if len(p.sink.marks) < 1 || len(p.sink.ois) < 1 || len(p.sink.funds) < 1 {
		return false
	}
	var index, value bool
	var interval bool
	for _, m := range p.sink.marks {
		index = index || decimal.Has(m.IndexPrice)
	}
	for _, o := range p.sink.ois {
		value = value || decimal.Has(o.OpenInterestValue)
	}
	for _, f := range p.sink.funds {
		interval = interval || f.IntervalS != 0
	}
	return index && value && interval
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
	return fmt.Sprintf("frames=%d snaps=%d applied=%d quotes=%d trades=%d tickers=%d klines=%d marks=%d oi=%d funding=%d liq=%d",
		len(p.raw), p.snaps, p.applied, len(p.sink.quotes), len(p.sink.trades), len(p.sink.tickers), len(p.sink.klines),
		len(p.sink.marks), len(p.sink.ois), len(p.sink.funds), len(p.sink.liqs))
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
	return &exchange.HTTPResponse{Status: resp.StatusCode, Body: body}
}

func clip(b []byte) string {
	if len(b) > 180 {
		b = b[:180]
	}
	return string(b)
}
