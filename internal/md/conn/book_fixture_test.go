package conn

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/binance"
	"github.com/BullionBear/mftiko/exchange/bybit"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

// The BinanceUM fixture is the public UM testnet, not fapi.binance.com: that host answers 451
// from the network that recorded it. The symbol is BCHUSDT. The replay takes one depth snapshot
// and then only the recorded increments (D426).
func TestBookBinanceUMFixture(t *testing.T) {
	if prob := binanceFixtureProblem(t); prob != "" {
		t.Fatal(prob)
	}
}

// binanceFixtureProblem replays the committed session. An empty string means the local book
// matches the closing snapshot and depth was called once.
func binanceFixtureProblem(t *testing.T) string {
	t.Helper()
	const ch = "bchusdt@depth@100ms"
	recs := readFixture(t, "testdata/book/binanceum.jsonl")
	var syncResp, checkResp *exchange.HTTPResponse
	var pre, post [][]byte
	seen := false
	for _, r := range recs {
		switch r.Kind {
		case conformance.KindIn:
			b, err := r.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if seen {
				post = append(post, b)
			} else {
				pre = append(pre, b)
			}
		case conformance.KindHTTP:
			if r.Exchange == nil || r.Exchange.Response == nil {
				t.Fatal("depth record has no response")
			}
			resp := r.Exchange.Response.HTTP()
			if r.Check {
				checkResp = resp
				continue
			}
			if seen {
				t.Fatal("more than one sync snapshot")
			}
			syncResp = resp
			seen = true
		}
	}
	if syncResp == nil || checkResp == nil || len(pre) == 0 || len(post) == 0 {
		t.Fatalf("fixture sync=%v check=%v pre=%d post=%d", syncResp != nil, checkResp != nil, len(pre), len(post))
	}
	rest := umREST(t)
	checkBook := parseDepth(t, rest, checkResp)
	hold := make(chan struct{})
	e := startFixtureBook(t, binance.VenueUM, "public", ch, "BCHUSDT", umMarket(t), rest, 20, []doerStep{{
		resp: syncResp, hold: hold,
	}})
	e.doer.wait(t, 1)
	for _, frame := range pre {
		e.feed(t, frame)
	}
	close(hold)
	e.waitUntil(t, func() bool {
		_, _, ok := e.maps()
		return ok
	})
	for _, frame := range post {
		e.feed(t, frame)
	}
	if n := e.doer.calls(); n != 1 {
		return "depth calls " + strconv.Itoa(n) + ", want 1"
	}
	gotB, gotA, ok := e.maps()
	if !ok {
		return "book was dropped"
	}
	if probs := compareFold(gotB, gotA, checkBook); len(probs) > 0 {
		return strconv.Itoa(len(probs)) + " book mismatches: " + stringsJoin(probs)
	}
	d := deltasOf(e.open(t))
	if len(d) == 0 || !d[0].ev.GetOrderBookDelta().GetReset_() {
		return "first snapshot was not a reset"
	}
	return ""
}

func TestBookBybitFixture(t *testing.T) {
	const ch = "orderbook.50.BTCUSDT"
	recs := readFixture(t, "testdata/book/bybit.jsonl")
	bs := bybitBookSync(t)
	reg, err := exchange.NewRegistry(bybit.Declare())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reg.Venue(bybit.Name)
	if !ok || v.Market == nil {
		t.Fatal("no bybit market")
	}
	e := startFixtureBook(t, bybit.Name, "linear", ch, "BTCUSDT", v.Market, nil, 20, nil)
	sess, err := v.Market.NewSession("linear")
	if err != nil {
		t.Fatal(err)
	}
	catch := &bookCatch{}
	n := 0
	for _, r := range recs {
		if r.Kind != conformance.KindIn {
			continue
		}
		b, err := r.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		e.feed(t, b)
		if err := sess.Decode(b, catch); err != nil {
			t.Fatalf("reference decode: %v", err)
		}
		n++
	}
	if n == 0 {
		t.Fatal("no frames")
	}
	if calls := e.doer.calls(); calls != 0 {
		t.Fatalf("stream called depth %d", calls)
	}
	wantB, wantA := foldStream(bs, catch.ups)
	gotB, gotA, ok := e.maps()
	if !ok {
		t.Fatal("stream book missing")
	}
	if probs := compareMaps(gotB, gotA, wantB, wantA); len(probs) > 0 {
		t.Fatalf("%d stream mismatches, first: %s", len(probs), probs[0])
	}
	d := deltasOf(e.open(t))
	if len(d) == 0 || !d[0].ev.GetOrderBookDelta().GetReset_() {
		t.Fatal("stream snapshot was not a reset")
	}
}

func readFixture(t *testing.T, path string) []conformance.Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := conformance.ReadRecords(f)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func parseDepth(t *testing.T, rest exchange.PublicREST, resp *exchange.HTTPResponse) *exchange.BookUpdate {
	t.Helper()
	call, err := rest.Depth(&symbol.Info{
		ExchTicker: "BCHUSDT",
		Ticker:     symbol.UniversalTicker{Venue: binance.VenueUM},
	}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	u, err := call.Parse(resp)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func startFixtureBook(t *testing.T, venue, route, ch, exch string, m exchange.Market, rest exchange.PublicREST, depth uint32, steps []doerStep) *bookEnv {
	t.Helper()
	doer := &seqDoer{hit: make(chan int, 8), steps: steps}
	rec := newRecorder()
	c := New(Config{
		Venue: venue, Route: route, Instance: "md", URL: "ws://127.0.0.1:9",
		Market: m, Publish: rec.publish, BookDepth: depth,
		REST: rest, Doer: doer, RetryAfter: -1,
	})
	c.SetDesired(&mdctlv1.Desired{
		Version: &commonv1.Version{Seq: 1},
		Atoms: []*mdctlv1.AtomAssignment{{
			AtomId: venue + ":" + route + ":" + ch,
			Feeds:  []string{"orderbook.T", "orderbook_delta.T"}, ExchTicker: exch,
		}},
	})
	if c.fatal != nil {
		t.Fatal(c.fatal)
	}
	sess, err := m.NewSession(route)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.epoch = 4
	c.live = newLink(sess)
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.syncBooks(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		c.fetchWG.Wait()
	})
	c.wakeBook()
	return &bookEnv{t: t, c: c, rec: rec, doer: doer, ch: ch}
}

func (e *bookEnv) feed(t *testing.T, frame []byte) {
	t.Helper()
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	if e.c.live == nil || e.c.live.session == nil {
		t.Fatal("no session")
	}
	if err := e.c.live.session.Decode(frame, e.c); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func (e *bookEnv) maps() (bids, asks map[float64]float64, ok bool) {
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	if e.c.live == nil {
		return nil, nil, false
	}
	sh := e.c.live.books[e.ch]
	if sh == nil || sh.book == nil {
		return nil, nil, false
	}
	return levelMap(sortedLevels(sh.book.bids, true)), levelMap(sortedLevels(sh.book.asks, false)), true
}

// compareFold is the conformance check: every closing-snapshot level is in the folded book, and
// the folded book has nothing inside that snapshot's range that the snapshot lacks.
func compareFold(bids, asks map[float64]float64, snap *exchange.BookUpdate) []string {
	return compareMaps(bids, asks, levelMap(snap.Bids), levelMap(snap.Asks))
}

func compareMaps(gotB, gotA, wantB, wantA map[float64]float64) []string {
	var out []string
	side := func(name string, got, want map[float64]float64, desc bool) {
		if len(want) == 0 {
			return
		}
		worst := 0.0
		first := true
		for p := range want {
			if first || (desc && p < worst) || (!desc && p > worst) {
				worst = p
				first = false
			}
		}
		for p, q := range want {
			if got[p] != q {
				out = append(out, name+" "+formatLevel(p, q, got[p]))
				if len(out) > 8 {
					return
				}
			}
		}
		for p := range got {
			inside := (desc && p >= worst) || (!desc && p <= worst)
			if _, ok := want[p]; inside && !ok {
				out = append(out, name+" has an extra inside level")
				return
			}
		}
	}
	side("bid", gotB, wantB, true)
	side("ask", gotA, wantA, false)
	return out
}

func formatLevel(p, want, got float64) string {
	return strconv.FormatFloat(p, 'f', -1, 64) + " snapshot " + strconv.FormatFloat(want, 'f', -1, 64) + " folded " + strconv.FormatFloat(got, 'f', -1, 64)
}

func stringsJoin(ss []string) string { return strings.Join(ss, "; ") }

func foldStream(bs exchange.BookSync, ups []exchange.BookUpdate) (bids, asks map[float64]float64) {
	bids, asks = map[float64]float64{}, map[float64]float64{}
	var seq exchange.BookSeq
	apply := func(u *exchange.BookUpdate) {
		for _, l := range u.Bids {
			if l.Qty == 0 {
				delete(bids, l.Price)
			} else {
				bids[l.Price] = l.Qty
			}
		}
		for _, l := range u.Asks {
			if l.Qty == 0 {
				delete(asks, l.Price)
			} else {
				asks[l.Price] = l.Qty
			}
		}
	}
	for i := range ups {
		u := &ups[i]
		if u.Snapshot {
			bids, asks = map[float64]float64{}, map[float64]float64{}
			apply(u)
			seq = exchange.BookSeq{Last: u.Last, Valid: true}
			continue
		}
		if !seq.Valid {
			continue
		}
		switch bs.Check(seq, u) {
		case exchange.BookApply:
			apply(u)
			seq.Last = u.Last
			seq.Valid = true
		case exchange.BookSkip:
		default:
			bids, asks = map[float64]float64{}, map[float64]float64{}
			seq = exchange.BookSeq{}
		}
	}
	return bids, asks
}

type bookCatch struct{ ups []exchange.BookUpdate }

func (s *bookCatch) Ack(uint64, error)                         {}
func (s *bookCatch) BestQuote(string, *exchange.BestQuote)     {}
func (s *bookCatch) Trade(string, *exchange.Trade)             {}
func (s *bookCatch) AggTrade(string, *exchange.AggTrade)       {}
func (s *bookCatch) Kline(string, *exchange.Kline)             {}
func (s *bookCatch) Ticker(string, *exchange.Ticker)           {}
func (s *bookCatch) Liquidation(string, *exchange.Liquidation) {}
func (s *bookCatch) FundingRate(string, *exchange.FundingRate) {}
func (s *bookCatch) OpenInterest(string, *exchange.OpenInterest) {
}
func (s *bookCatch) Greeks(string, *exchange.Greeks)       {}
func (s *bookCatch) MarkPrice(string, *exchange.MarkPrice) {}
func (s *bookCatch) FeedEnd(string, *exchange.FeedEnd)     {}
func (s *bookCatch) Book(_ string, u *exchange.BookUpdate) {
	if u != nil {
		s.ups = append(s.ups, cloneUpdate(u))
	}
}
