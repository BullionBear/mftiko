package conn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/binance"
)

func TestRunRequiresConfig(t *testing.T) {
	err := New(Config{}).Run(t.Context())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSessionError(t *testing.T) {
	m := &stubMarket{}
	c := New(Config{
		Venue: "V", Route: "r", Instance: "i", URL: "ws://127.0.0.1:1",
		Market: m, Publish: func(string, []byte) error { return nil },
	})
	err := c.Run(t.Context())
	if err == nil || c.Epoch() != 0 || m.dials.Load() != 1 {
		t.Fatalf("err=%v epoch=%d dials=%d", err, c.Epoch(), m.dials.Load())
	}
}

func TestNextDelay(t *testing.T) {
	// Run stores the returned wait. A connection that came up yields 0, so the next failure
	// starts again at the base instead of continuing the old doubling.
	if d := nextDelay(0, false, 0, 0); d != time.Second {
		t.Fatalf("first failure: %s", d)
	}
	if d := nextDelay(time.Second, false, 0, 0); d != 2*time.Second {
		t.Fatalf("second failure: %s", d)
	}
	if d := nextDelay(16*time.Second, false, 0, 0); d != 30*time.Second {
		t.Fatalf("cap: %s", d)
	}
	if d := nextDelay(30*time.Second, false, 0, 0); d != 30*time.Second {
		t.Fatalf("stay at cap: %s", d)
	}
	if d := nextDelay(5*time.Second, true, 0, 0); d != 0 {
		t.Fatalf("after a connection: %s", d)
	}
	if d := nextDelay(5*time.Second, false, -1, 0); d != 0 {
		t.Fatalf("disabled: %s", d)
	}
	if d := nextDelay(2*time.Second, false, 2*time.Second, 3*time.Second); d != 3*time.Second {
		t.Fatalf("custom cap: %s", d)
	}
}

func TestLimiter(t *testing.T) {
	now := time.Unix(100, 0)
	l := newLimiter([]exchange.RateLimit{{N: 2, Per: 100 * time.Millisecond}})
	if l.delay(now) != 0 {
		t.Fatal("empty window")
	}
	l.mark(now)
	if l.delay(now) != 0 {
		t.Fatal("one hit of two")
	}
	l.mark(now.Add(10 * time.Millisecond))
	if d := l.delay(now.Add(10 * time.Millisecond)); d != 90*time.Millisecond {
		t.Fatalf("full window: %s", d)
	}
	// The hit at now is exactly one window old and drops out. One hit remains, under N.
	if d := l.delay(now.Add(100 * time.Millisecond)); d != 0 {
		t.Fatalf("aged out: %s", d)
	}

	off := newLimiter([]exchange.RateLimit{{N: 0, Per: time.Second}, {N: 1, Per: 0}})
	off.mark(now)
	if off.delay(now) != 0 {
		t.Fatal("disabled limits")
	}

	both := newLimiter([]exchange.RateLimit{{N: 1, Per: 80 * time.Millisecond}, {N: 10, Per: time.Second}})
	for range 10 {
		both.mark(now)
	}
	if d := both.delay(now); d != time.Second {
		t.Fatalf("longer window wins: %s", d)
	}
}

func TestDesiredVersion(t *testing.T) {
	c := New(Config{Venue: "V", Route: "r", Instance: "i", Market: &stubMarket{}, Publish: func(string, []byte) error { return nil }})
	c.SetDesired(nil)
	if c.have {
		t.Fatal("nil desired applied")
	}
	c.SetDesired(&mdctlv1.Desired{Atoms: []*mdctlv1.AtomAssignment{
		{AtomId: "V:r:keep", Feeds: []string{"trade.T", "trade.T"}, Record: true},
		{AtomId: "V:r:colon:chan", Feeds: []string{"aggtrade.T", "nodot", "trade.T"}},
		{AtomId: "V:r:empty", Feeds: []string{"nodot"}},
		{AtomId: "Other:r:no", Feeds: []string{"trade.X"}},
		{AtomId: "V:other:no", Feeds: []string{"trade.Y"}},
		{AtomId: "bad", Feeds: []string{"trade.Z"}},
		nil,
	}})
	if !c.have {
		t.Fatal("first version not applied")
	}
	if got := feedTopics(c.wanted["keep"]); !slices.Equal(got, []string{"trade"}) {
		t.Fatalf("keep: %v", got)
	}
	if got := feedTopics(c.wanted["colon:chan"]); !slices.Equal(got, []string{"aggtrade", "trade"}) {
		t.Fatalf("colon channel: %v", got)
	}
	if _, ok := c.wanted["empty"]; ok {
		t.Fatal("atom with no feed was kept")
	}
	if _, ok := c.wanted["no"]; ok {
		t.Fatal("other venue or route was kept")
	}

	c.SetDesired(&mdctlv1.Desired{Atoms: []*mdctlv1.AtomAssignment{
		{AtomId: "V:r:new", Feeds: []string{"trade.N"}},
	}})
	if _, ok := c.wanted["new"]; ok {
		t.Fatal("same version applied")
	}

	c.SetDesired(&mdctlv1.Desired{
		Version: &commonv1.Version{Seq: 1},
		Atoms:   []*mdctlv1.AtomAssignment{{AtomId: "V:r:new", Feeds: []string{"trade.N"}}},
	})
	if _, ok := c.wanted["keep"]; ok {
		t.Fatal("previous channels kept")
	}
	if _, ok := c.wanted["new"]; !ok {
		t.Fatal("newer version dropped")
	}

	c.SetDesired(&mdctlv1.Desired{
		Version: &commonv1.Version{},
		Atoms:   []*mdctlv1.AtomAssignment{{AtomId: "V:r:old", Feeds: []string{"trade.O"}}},
	})
	if _, ok := c.wanted["old"]; ok {
		t.Fatal("older version applied")
	}
	c.SetDesired(&mdctlv1.Desired{
		Version: &commonv1.Version{Epoch: 1},
		Atoms:   []*mdctlv1.AtomAssignment{{AtomId: "V:r:ep", Feeds: []string{"trade.E"}}},
	})
	if _, ok := c.wanted["ep"]; !ok {
		t.Fatal("higher epoch dropped")
	}
}

func TestPlan(t *testing.T) {
	s := &memSession{per: 1}
	c := New(Config{Venue: "V", Route: "r", Instance: "i", Market: &stubMarket{s: s}, Publish: func(string, []byte) error { return nil }})
	c.live = newLink(s)
	c.wanted = map[string][]feed{
		"a": {{topic: "trade", ticker: "A"}},
		"b": {{topic: "trade", ticker: "B"}},
	}
	if frames := c.plan(); len(frames) != 2 {
		t.Fatalf("frames %d", len(frames))
	}
	if len(s.sub) != 1 || !slices.Equal(s.sub[0], []string{"a", "b"}) {
		t.Fatalf("subscribe %v", s.sub)
	}
	if c.plan() != nil {
		t.Fatal("in-flight channels were sent again")
	}

	c.Ack(1, nil)
	c.Ack(2, errors.New("no"))
	if _, ok := c.live.observed["a"]; !ok {
		t.Fatal("a not observed")
	}
	if _, ok := c.live.observed["b"]; ok {
		t.Fatal("failed ack observed b")
	}
	if _, ok := c.live.subPending["b"]; ok {
		t.Fatal("failed ack left b pending")
	}
	c.plan()
	if len(s.sub) != 2 || !slices.Equal(s.sub[1], []string{"b"}) {
		t.Fatalf("resubscribe %v", s.sub)
	}
	c.Ack(3, nil)

	delete(c.wanted, "a")
	c.plan()
	if len(s.unsub) != 1 || !slices.Equal(s.unsub[0], []string{"a"}) {
		t.Fatalf("unsubscribe %v", s.unsub)
	}
	c.Ack(4, nil)
	if _, ok := c.live.observed["a"]; ok {
		t.Fatal("a still observed")
	}
	c.Ack(1, nil)
	if _, ok := c.live.observed["a"]; ok {
		t.Fatal("late ack observed a")
	}

	// Accepted after desired had already dropped it: the venue has it, so the next diff undoes it.
	c.live.subInflight[9] = []string{"c"}
	c.live.subPending["c"] = struct{}{}
	c.Ack(9, nil)
	if _, ok := c.live.observed["c"]; !ok {
		t.Fatal("c not observed")
	}
	c.plan()
	if len(s.unsub) != 2 || !slices.Equal(s.unsub[1], []string{"c"}) {
		t.Fatalf("unsubscribe dropped channel %v", s.unsub)
	}
	c.Ack(99, nil)
	if _, ok := c.live.observed["b"]; !ok {
		t.Fatal("unknown ack changed b")
	}
}

func TestPublish(t *testing.T) {
	rec := newRecorder()
	stamp := time.Unix(0, 42)
	c := New(Config{
		Venue: "V", Route: "r", Instance: "md1", Market: &stubMarket{},
		Publish: rec.publish, Now: func() time.Time { return stamp },
	})
	c.epoch = 4
	c.live = newLink(nil)
	const ch = "ch"
	c.wanted = map[string][]feed{
		ch: {
			{topic: "trade", ticker: "T"},
			{topic: "aggtrade", ticker: "T"},
			{topic: "bestquote", ticker: "T"},
			{topic: "kline_1m", ticker: "T"},
			{topic: "ticker", ticker: "T"},
			{topic: "liquidation", ticker: "T"},
			{topic: "funding_rate", ticker: "T"},
			{topic: "open_interest", ticker: "T"},
			{topic: "greeks", ticker: "T"},
			{topic: "mark_price", ticker: "T"},
		},
	}

	c.Book(ch, &exchange.BookUpdate{Last: 9})
	c.Trade(ch, nil)
	c.Trade("other", &exchange.Trade{TradeID: "x"})
	if n := len(rec.snapshot()); n != 0 {
		t.Fatalf("dropped events published: %d", n)
	}

	c.Trade(ch, &exchange.Trade{ExchTsNs: 5, TradeID: "id", Price: 1, Qty: 2, Side: exchange.SideSell})
	p := rec.next(t)
	if p.subject != "md.f.md1.trade.T" {
		t.Fatal(p.subject)
	}
	mdTs, epoch, seq, ev := openPub(t, p)
	if mdTs != 42 || epoch != 4 || seq != 1 || ev.GetTrade().GetTradeId() != "id" || ev.GetTrade().GetSide() != commonv1.Side_SIDE_SELL {
		t.Fatalf("ts %d epoch %d seq %d trade %+v", mdTs, epoch, seq, ev.GetTrade())
	}

	rec.fail.Store(true)
	c.Trade(ch, &exchange.Trade{TradeID: "a"})
	c.Trade(ch, &exchange.Trade{TradeID: "b"})
	rec.fail.Store(false)
	c.Trade(ch, &exchange.Trade{TradeID: "c"})
	p = rec.next(t)
	_, _, seq, ev = openPub(t, p)
	if seq != 4 || ev.GetTrade().GetTradeId() != "c" {
		t.Fatalf("seq %d id %s", seq, ev.GetTrade().GetTradeId())
	}

	c.AggTrade(ch, &exchange.AggTrade{TradeID: "g", Side: exchange.SideBuy, FirstTradeID: "1", LastTradeID: "2", MatchCount: 2})
	p = rec.next(t)
	if p.subject != "md.f.md1.aggtrade.T" {
		t.Fatal(p.subject)
	}
	_, _, seq, ev = openPub(t, p)
	if seq != 1 || ev.GetAggTrade().GetSide() != commonv1.Side_SIDE_BUY || ev.GetAggTrade().GetMatchCount() != 2 {
		t.Fatalf("agg %+v seq %d", ev.GetAggTrade(), seq)
	}

	c.BestQuote(ch, &exchange.BestQuote{Bid: 1, Ask: 2})
	if rec.next(t).subject != "md.f.md1.bestquote.T" {
		t.Fatal("bestquote")
	}

	c.Kline(ch, &exchange.Kline{Interval: "5m"})
	c.Kline(ch, &exchange.Kline{Interval: "1m", QuoteVolume: decimal.Absent(), Open: 3})
	p = rec.next(t)
	if p.subject != "md.f.md1.kline_1m.T" {
		t.Fatal(p.subject)
	}
	_, _, _, ev = openPub(t, p)
	if ev.GetKline().GetInterval() != "1m" || ev.GetKline().QuoteVolume != nil {
		t.Fatalf("kline %+v", ev.GetKline())
	}

	c.Ticker(ch, &exchange.Ticker{Last: decimal.Absent(), Open24h: 3})
	_, _, _, ev = openPub(t, rec.next(t))
	if ev.GetTicker().Last != nil || ev.GetTicker().Open_24H == nil {
		t.Fatalf("ticker %+v", ev.GetTicker())
	}

	c.Liquidation(ch, &exchange.Liquidation{Side: exchange.SideBuy, Price: 1, Qty: 2})
	_, _, _, ev = openPub(t, rec.next(t))
	if ev.GetLiquidation().GetSide() != commonv1.Side_SIDE_BUY {
		t.Fatal(ev.GetLiquidation())
	}

	c.FundingRate(ch, &exchange.FundingRate{Rate: 0.1})
	_, _, _, ev = openPub(t, rec.next(t))
	if ev.GetFundingRate().IntervalS != nil || ev.GetFundingRate().GetRate() == 0 {
		t.Fatal(ev.GetFundingRate())
	}

	c.OpenInterest(ch, &exchange.OpenInterest{OpenInterest: 4, OpenInterestValue: decimal.Absent()})
	_, _, _, ev = openPub(t, rec.next(t))
	if ev.GetOpenInterest().OpenInterestValue != nil {
		t.Fatal(ev.GetOpenInterest())
	}

	c.Greeks(ch, &exchange.Greeks{Delta: decimal.Absent(), MarkIV: 1})
	_, _, _, ev = openPub(t, rec.next(t))
	if ev.GetGreeks().Delta != nil || ev.GetGreeks().MarkIv == nil {
		t.Fatal(ev.GetGreeks())
	}

	c.MarkPrice(ch, &exchange.MarkPrice{MarkPrice: 5, IndexPrice: decimal.Absent()})
	_, _, _, ev = openPub(t, rec.next(t))
	if ev.GetMarkPrice().GetMarkPrice() != 5 || ev.GetMarkPrice().IndexPrice != nil {
		t.Fatal(ev.GetMarkPrice())
	}

	before := len(rec.snapshot())
	c.FeedEnd(ch, &exchange.FeedEnd{Reason: "expired", AtNs: 9})
	got := rec.snapshot()
	if len(got)-before != len(c.wanted[ch]) {
		t.Fatalf("feed end %d, want %d", len(got)-before, len(c.wanted[ch]))
	}
	_, _, seq, ev = openPub(t, got[len(got)-1])
	if ev.GetFeedEnd().GetReason() != "expired" || seq != 2 {
		t.Fatalf("feed end seq %d %+v", seq, ev.GetFeedEnd())
	}
	// trade was already at 4, so its end is 5. It is the first feed of the channel.
	_, _, seq, _ = openPub(t, got[before])
	if seq != 5 {
		t.Fatalf("trade feed end seq %d", seq)
	}
}

func TestDialFailureKeepsEpoch(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	m := &stubMarket{s: &memSession{lim: exchange.SessionLimits{ReadLimit: 1 << 20}}}
	c := New(Config{
		Venue: "V", Route: "r", Instance: "i",
		URL:    "ws://" + addr,
		Market: m,
		Publish: func(string, []byte) error {
			t.Error("published")
			return nil
		},
		RetryAfter: time.Second,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	err = c.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run: %v", err)
	}
	if c.Epoch() != 0 {
		t.Fatalf("epoch %d", c.Epoch())
	}
	if n := m.dials.Load(); n != 1 {
		t.Fatalf("dials %d", n)
	}
}

func TestReportBeforeFailedDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	var got []uint64
	m := &stubMarket{s: &memSession{lim: exchange.SessionLimits{ReadLimit: 1 << 20}}}
	c := New(Config{
		Venue: "V", Route: "r", Instance: "i",
		URL:    "ws://" + addr,
		Market: m,
		Publish: func(string, []byte) error {
			t.Error("published")
			return nil
		},
		Epoch: 0,
		Report: func(epoch uint64) error {
			got = append(got, epoch)
			return nil
		},
		RetryAfter: time.Second,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	err = c.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run: %v", err)
	}
	if c.Epoch() != 0 || len(got) != 1 || got[0] != 1 {
		t.Fatalf("epoch %d reported %v", c.Epoch(), got)
	}
}

func TestReportErrorDoesNotDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		close(accepted)
		c.Close()
	}()
	c := New(Config{
		Venue: "V", Route: "r", Instance: "i",
		URL:    "ws://" + ln.Addr().String(),
		Market: &stubMarket{s: &memSession{lim: exchange.SessionLimits{ReadLimit: 1 << 20}}},
		Publish: func(string, []byte) error {
			t.Error("published")
			return nil
		},
		Report: func(uint64) error { return errors.New("no shim") },
	})
	err = c.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "no shim") {
		t.Fatalf("run: %v", err)
	}
	select {
	case <-accepted:
		t.Fatal("dialed after the report failed")
	default:
	}
}

func TestRateLimit(t *testing.T) {
	s := &memSession{
		per: 1,
		lim: exchange.SessionLimits{
			PerRequest: 1,
			Rate:       []exchange.RateLimit{{N: 1, Per: 80 * time.Millisecond}},
			ReadLimit:  1 << 20,
		},
	}
	var mu sync.Mutex
	var at []time.Time
	var payloads []string
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		for {
			_, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			at = append(at, time.Now())
			payloads = append(payloads, string(msg))
			mu.Unlock()
		}
	})
	c := New(Config{
		Venue: "V", Route: "r", Instance: "i", URL: url,
		Market: &stubMarket{s: s}, Publish: func(string, []byte) error { return nil },
		RetryAfter: -1,
	})
	c.SetDesired(assigns("a", "b", "c"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go c.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(at)
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(at) < 3 {
		t.Fatalf("writes %d: %v", len(at), payloads)
	}
	if at[2].Sub(at[0]) < 140*time.Millisecond {
		t.Fatalf("three writes in %s", at[2].Sub(at[0]))
	}
	if payloads[0] != "a" || payloads[1] != "b" || payloads[2] != "c" {
		t.Fatalf("payloads %v", payloads[:3])
	}
}

func TestAckErrorResends(t *testing.T) {
	mkt := umMarket(t)
	notes := make(chan subNote, 8)
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		readSubs(ctx, c, 1, notes, func(i int, id uint64) []byte {
			if i == 1 {
				return []byte(fmt.Sprintf(`{"id":%d,"error":{"code":1,"msg":"no"}}`, id))
			}
			return nil
		})
	})
	c := New(Config{
		Venue: binance.VenueUM, Route: "market", Instance: "md1", URL: url,
		Market: mkt, Publish: func(string, []byte) error { return nil },
		RetryAfter: -1,
	})
	c.SetDesired(desired(1, 1, atom("BinanceUM:market:btcusdt@aggTrade", "aggtrade.BinanceUM_Perp_BTCUSDT")))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go c.Run(ctx)

	a := waitNote(t, notes)
	b := waitNote(t, notes)
	if a.method != "SUBSCRIBE" || b.method != "SUBSCRIBE" {
		t.Fatalf("%s then %s", a.method, b.method)
	}
	if !slices.Equal(a.params, []string{"btcusdt@aggTrade"}) || !slices.Equal(b.params, a.params) {
		t.Fatalf("params %v then %v", a.params, b.params)
	}
	select {
	case extra := <-notes:
		t.Fatalf("extra subscribe %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDecodeErrorKeepsEpoch(t *testing.T) {
	_, agg := recordedMarket(t)
	var conns atomic.Int32
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		conns.Add(1)
		_, msg, err := c.Read(ctx)
		if err != nil {
			return
		}
		var f struct {
			ID uint64 `json:"id"`
		}
		if json.Unmarshal(msg, &f) != nil {
			return
		}
		_ = c.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf(`{"result":null,"id":%d}`, f.ID)))
		_ = c.Write(ctx, websocket.MessageText, []byte("{"))
		_ = c.Write(ctx, websocket.MessageText, agg)
		<-ctx.Done()
	})
	rec := newRecorder()
	c := New(Config{
		Venue: binance.VenueUM, Route: "market", Instance: "md1", URL: url,
		Market: umMarket(t), Publish: rec.publish, RetryAfter: -1,
	})
	c.SetDesired(desired(1, 1, atom("BinanceUM:market:btcusdt@aggTrade", "aggtrade.BinanceUM_Perp_BTCUSDT")))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go c.Run(ctx)

	got := rec.wait(t, 1)
	if len(got) != 1 {
		t.Fatalf("pubs %d", len(got))
	}
	_, epoch, seq, ev := openPub(t, got[0])
	if got[0].subject != "md.f.md1.aggtrade.BinanceUM_Perp_BTCUSDT" || epoch != 1 || seq != 1 || ev.GetAggTrade().GetTradeId() != "3481002985" {
		t.Fatalf("%s epoch %d seq %d id %s", got[0].subject, epoch, seq, ev.GetAggTrade().GetTradeId())
	}
	time.Sleep(200 * time.Millisecond)
	if conns.Load() != 1 {
		t.Fatalf("connections %d", conns.Load())
	}
	if c.Epoch() != 1 {
		t.Fatalf("epoch %d", c.Epoch())
	}
}

func TestTradeSession(t *testing.T) {
	events, agg := recordedMarket(t)
	notes := make(chan subNote, 8)
	var gen atomic.Int32
	play := make(chan struct{})
	release := make(chan struct{})
	again := make(chan struct{})
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		n := int(gen.Add(1))
		go readSubs(ctx, c, n, notes, nil)
		if n == 1 {
			select {
			case <-play:
			case <-ctx.Done():
				return
			}
			for _, frame := range events {
				if err := c.Write(ctx, websocket.MessageText, frame); err != nil {
					return
				}
			}
			// The recording has one aggTrade. A second copy is how seq 2 is observed.
			if err := c.Write(ctx, websocket.MessageText, agg); err != nil {
				return
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
		select {
		case <-again:
		case <-ctx.Done():
			return
		}
		if err := c.Write(ctx, websocket.MessageText, agg); err != nil {
			return
		}
		<-ctx.Done()
	})

	rec := newRecorder()
	stamp := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	c := New(Config{
		Venue: binance.VenueUM, Route: "market", Instance: "md1", URL: url,
		Market:     umMarket(t),
		Publish:    rec.publish,
		Now:        func() time.Time { return stamp },
		RetryAfter: -1,
	})
	aggAtom := atom("BinanceUM:market:btcusdt@aggTrade", "aggtrade.BinanceUM_Perp_BTCUSDT")
	klineAtom := atom("BinanceUM:market:btcusdt@kline_1m", "kline_1m.BinanceUM_Perp_BTCUSDT")
	klineAtom.Record = true
	c.SetDesired(desired(1, 1, aggAtom, &mdctlv1.AtomAssignment{AtomId: "not-an-atom", Feeds: []string{"trade.X"}}))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()

	first := waitNote(t, notes)
	if first.gen != 1 || first.method != "SUBSCRIBE" || !slices.Equal(first.params, []string{"btcusdt@aggTrade"}) {
		t.Fatalf("first subscribe %+v", first)
	}
	c.SetDesired(desired(1, 2, aggAtom, klineAtom, &mdctlv1.AtomAssignment{
		AtomId: "Binance:default:btcusdt@aggTrade", Feeds: []string{"aggtrade.Binance_Spot_BTCUSDT"},
	}))
	second := waitNote(t, notes)
	if second.method != "SUBSCRIBE" || !slices.Equal(second.params, []string{"btcusdt@kline_1m"}) {
		t.Fatalf("diff %+v", second)
	}
	c.SetDesired(desired(1, 1, aggAtom))
	select {
	case extra := <-notes:
		t.Fatalf("older desired wrote %+v", extra)
	case <-time.After(200 * time.Millisecond):
	}

	close(play)
	got := rec.wait(t, 3)
	if len(got) != 3 {
		t.Fatalf("pubs %d: %v", len(got), subjects(got))
	}
	wantStamp := stamp.UnixNano()
	checkAgg := func(p publication, seq, epoch uint64) {
		t.Helper()
		if p.subject != "md.f.md1.aggtrade.BinanceUM_Perp_BTCUSDT" {
			t.Fatal(p.subject)
		}
		mdTs, gotEpoch, gotSeq, ev := openPub(t, p)
		tr := ev.GetAggTrade()
		if mdTs != wantStamp || gotEpoch != epoch || gotSeq != seq || tr.GetTradeId() != "3481002985" || tr.GetSide() != commonv1.Side_SIDE_BUY {
			t.Fatalf("ts %d epoch %d seq %d trade %+v", mdTs, gotEpoch, gotSeq, tr)
		}
		if tr.GetMeta().GetExchTsNs() != 1791616205180*int64(time.Millisecond) {
			t.Fatalf("exch ts %d", tr.GetMeta().GetExchTsNs())
		}
	}
	checkAgg(got[0], 1, 1)
	if got[1].subject != "md.f.md1.kline_1m.BinanceUM_Perp_BTCUSDT" {
		t.Fatal(got[1].subject)
	}
	mdTs, epoch, seq, ev := openPub(t, got[1])
	kl := ev.GetKline()
	if mdTs != wantStamp || epoch != 1 || seq != 1 || kl.GetInterval() != "1m" || kl.GetClosed() || kl.QuoteVolume == nil {
		t.Fatalf("kline ts %d epoch %d seq %d %+v", mdTs, epoch, seq, kl)
	}
	checkAgg(got[2], 2, 1)

	close(release)
	third := waitNote(t, notes)
	if third.gen != 2 || third.method != "SUBSCRIBE" || !slices.Equal(third.params, []string{"btcusdt@aggTrade", "btcusdt@kline_1m"}) {
		t.Fatalf("resubscribe %+v", third)
	}
	select {
	case extra := <-notes:
		t.Fatalf("extra after reconnect %+v", extra)
	case <-time.After(80 * time.Millisecond):
	}
	close(again)
	got = rec.wait(t, 4)
	if len(got) != 4 {
		t.Fatalf("pubs %d: %v", len(got), subjects(got))
	}
	checkAgg(got[3], 1, 2)

	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return")
	}
	if c.Epoch() != 2 {
		t.Fatalf("epoch %d", c.Epoch())
	}
}

type publication struct {
	subject string
	msg     []byte
}

type recorder struct {
	mu   sync.Mutex
	pubs []publication
	i    int
	got  chan struct{}
	fail atomic.Bool
}

func newRecorder() *recorder { return &recorder{got: make(chan struct{}, 1)} }

func (r *recorder) publish(subject string, msg []byte) error {
	if r.fail.Load() {
		return errors.New("down")
	}
	r.mu.Lock()
	r.pubs = append(r.pubs, publication{subject, bytes.Clone(msg)})
	r.mu.Unlock()
	select {
	case r.got <- struct{}{}:
	default:
	}
	return nil
}

func (r *recorder) snapshot() []publication {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]publication(nil), r.pubs...)
}

func (r *recorder) next(t *testing.T) publication {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.i >= len(r.pubs) {
		t.Fatal("missing publish")
	}
	p := r.pubs[r.i]
	r.i++
	return p
}

func (r *recorder) wait(t *testing.T, n int) []publication {
	t.Helper()
	dead := time.NewTimer(5 * time.Second)
	defer dead.Stop()
	for {
		if got := r.snapshot(); len(got) >= n {
			return got
		}
		select {
		case <-r.got:
		case <-dead.C:
			t.Fatalf("published %d, want %d: %v", len(r.snapshot()), n, subjects(r.snapshot()))
		}
	}
}

func subjects(ps []publication) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.subject
	}
	return out
}

func openPub(t *testing.T, p publication) (mdTs int64, epoch, seq uint64, ev *mdv1.FeedEvent) {
	t.Helper()
	var payload []byte
	var err error
	mdTs, epoch, seq, _, payload, err = envelope.Parse(p.msg)
	if err != nil {
		t.Fatal(err)
	}
	ev = &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		t.Fatal(err)
	}
	return mdTs, epoch, seq, ev
}

type subNote struct {
	gen    int
	method string
	params []string
}

func waitNote(t *testing.T, notes <-chan subNote) subNote {
	t.Helper()
	select {
	case n := <-notes:
		return n
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subscribe")
	}
	return subNote{}
}

func readSubs(ctx context.Context, c *websocket.Conn, gen int, notes chan<- subNote, ack func(i int, id uint64) []byte) {
	for i := 1; ; i++ {
		_, msg, err := c.Read(ctx)
		if err != nil {
			return
		}
		var f struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
			ID     uint64   `json:"id"`
		}
		if json.Unmarshal(msg, &f) != nil || f.Method == "" {
			continue
		}
		body := []byte(fmt.Sprintf(`{"result":null,"id":%d}`, f.ID))
		if ack != nil {
			if b := ack(i, f.ID); b != nil {
				body = b
			}
		}
		if err := c.Write(ctx, websocket.MessageText, body); err != nil {
			return
		}
		select {
		case notes <- subNote{gen: gen, method: f.Method, params: append([]string(nil), f.Params...)}:
		case <-ctx.Done():
			return
		}
	}
}

func serve(t *testing.T, fn func(ctx context.Context, c *websocket.Conn)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		fn(r.Context(), c)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func umMarket(t *testing.T) exchange.Market {
	t.Helper()
	reg, err := exchange.NewRegistry(binance.Declare())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reg.Venue(binance.VenueUM)
	if !ok || v.Market == nil {
		t.Fatal("no BinanceUM market")
	}
	return v.Market
}

func recordedMarket(t *testing.T) (events [][]byte, agg []byte) {
	t.Helper()
	raw, err := os.ReadFile("../../../exchange/binance/testdata/conformance/BinanceUM/ws/market.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var row struct {
			Kind  string `json:"kind"`
			Frame string `json:"frame"`
		}
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if row.Kind != "ws.in" || row.Frame == "" || ackFrame([]byte(row.Frame)) {
			continue
		}
		frame := []byte(row.Frame)
		events = append(events, frame)
		if bytes.Contains(frame, []byte(`"e":"aggTrade"`)) {
			agg = frame
		}
	}
	if agg == nil || len(events) == 0 {
		t.Fatal("recording has no events")
	}
	return events, agg
}

func ackFrame(frame []byte) bool {
	if len(frame) == 0 || frame[0] != '{' {
		return false
	}
	var h struct {
		ID *int64 `json:"id"`
		E  string `json:"e"`
	}
	if json.Unmarshal(frame, &h) != nil {
		return false
	}
	return h.E == "" && h.ID != nil
}

func desired(epoch int64, seq uint64, atoms ...*mdctlv1.AtomAssignment) *mdctlv1.Desired {
	return &mdctlv1.Desired{Version: &commonv1.Version{Epoch: epoch, Seq: seq}, Atoms: atoms}
}

func atom(id, feed string) *mdctlv1.AtomAssignment {
	return &mdctlv1.AtomAssignment{AtomId: id, Feeds: []string{feed}}
}

func assigns(chs ...string) *mdctlv1.Desired {
	d := &mdctlv1.Desired{Version: &commonv1.Version{Epoch: 1, Seq: 1}}
	for _, ch := range chs {
		d.Atoms = append(d.Atoms, atom("V:r:"+ch, "trade."+ch))
	}
	return d
}

func feedTopics(fs []feed) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.topic
	}
	sort.Strings(out)
	return out
}

// stubMarket is the Market methods a conn test does not exercise.
type stubMarket struct {
	s     exchange.Session
	dials atomic.Int32
}

func (m *stubMarket) Topics() []exchange.TopicCaps { return nil }
func (m *stubMarket) Atom(exchange.Topic, *symbol.Info) (exchange.AtomRef, error) {
	return exchange.AtomRef{}, exchange.ErrNotImplemented
}
func (m *stubMarket) Routes() []string  { return nil }
func (m *stubMarket) Budget(string) int { return 0 }
func (m *stubMarket) Cost(string) int   { return 0 }
func (m *stubMarket) Endpoint(string, string) (string, error) {
	return "ws://127.0.0.1:9", nil
}
func (m *stubMarket) NewSession(string) (exchange.Session, error) {
	m.dials.Add(1)
	if m.s == nil {
		return nil, errors.New("no session")
	}
	return m.s, nil
}
func (m *stubMarket) Book(string) (exchange.BookSync, bool) { return nil, false }

// memSession records subscribe and unsubscribe and pages like a venue with a small per-request cap.
// It is used from one goroutine.
type memSession struct {
	per        int
	n          uint64
	lim        exchange.SessionLimits
	sub, unsub [][]string
}

func (s *memSession) Limits() exchange.SessionLimits { return s.lim }
func (s *memSession) Ping() []byte                   { return nil }
func (s *memSession) Decode([]byte, exchange.Sink) error {
	return errors.New("decode")
}
func (s *memSession) Subscribe(ch []string) ([]exchange.Request, error) {
	return s.reqs("SUBSCRIBE", ch, &s.sub)
}
func (s *memSession) Unsubscribe(ch []string) ([]exchange.Request, error) {
	return s.reqs("UNSUBSCRIBE", ch, &s.unsub)
}
func (s *memSession) reqs(method string, ch []string, log *[][]string) ([]exchange.Request, error) {
	if len(ch) == 0 {
		return nil, nil
	}
	*log = append(*log, append([]string(nil), ch...))
	n := s.per
	if n <= 0 || n > len(ch) {
		n = len(ch)
	}
	var out []exchange.Request
	for len(ch) > 0 {
		if len(ch) < n {
			n = len(ch)
		}
		page := append([]string(nil), ch[:n]...)
		ch = ch[n:]
		s.n++
		out = append(out, exchange.Request{
			ID: s.n, Channels: page, Frame: []byte(strings.Join(page, ",")),
		})
	}
	return out, nil
}
