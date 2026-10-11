package conn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/binance"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
)

func TestTapeHeadSeam(t *testing.T) {
	clk := newManualClock(time.Unix(0, 1_000_000))
	rec := newRecorder()
	c := newTapeConn(t, clk, rec.publish, Config{})
	c.mu.Lock()
	c.epoch = 4
	c.mu.Unlock()
	a := atom("V:r:ch", "trade.T")
	a.Record = true
	c.SetDesired(desired(1, 1, a))

	// No live event yet. The view returns, and head is the current epoch with seq 0 (D437).
	page, raw := askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	if len(page.GetEvents()) != 0 || page.GetMore() || page.GetNext() != nil {
		t.Fatalf("empty page %+v", page)
	}
	if page.GetHead().GetConnEpoch() != 4 || page.GetHead().GetSeq() != 0 {
		t.Fatalf("head %+v", page.GetHead())
	}
	if len(raw) == 0 || raw[0] == 0 {
		t.Fatalf("reply %x", raw)
	}

	for i := 1; i <= 5; i++ {
		clk.Add(time.Millisecond)
		pushTrade(c, "ch", strconv.Itoa(i))
	}
	page, raw = askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	pubs := rec.snapshot()
	if page.GetHead().GetConnEpoch() != 4 || page.GetHead().GetSeq() != 5 {
		t.Fatalf("head %+v", page.GetHead())
	}
	if len(page.GetEvents()) != len(pubs) {
		t.Fatalf("events %d pubs %d", len(page.GetEvents()), len(pubs))
	}
	payloads := tapePayloads(t, raw)
	for i, ev := range page.GetEvents() {
		pub := pubs[len(pubs)-1-i]
		_, epoch, seq, got := openPub(t, pub)
		_, _, _, _, payload, err := envelope.Parse(pub.msg)
		if err != nil {
			t.Fatal(err)
		}
		if ev.GetConnEpoch() != epoch || ev.GetSeq() != seq || !bytes.Equal(payloads[i], payload) {
			t.Fatalf("event %d cursor %d/%d payload mismatch", i, ev.GetConnEpoch(), ev.GetSeq())
		}
		if !proto.Equal(ev.GetEvent(), got) {
			t.Fatalf("event %d %+v", i, ev.GetEvent())
		}
		if cursorLess(cursor{page.GetHead().GetConnEpoch(), page.GetHead().GetSeq()}, cursor{ev.GetConnEpoch(), ev.GetSeq()}) {
			t.Fatalf("event %d/%d is newer than head", ev.GetConnEpoch(), ev.GetSeq())
		}
	}
	if merged := mergeSeam(t, page, pubs); !cursorsEqual(merged, pubCursors(t, pubs)) {
		t.Fatalf("seam %v pubs %v", merged, pubCursors(t, pubs))
	}

	// A live event published after the view is applied; everything at or under head is not.
	clk.Add(time.Millisecond)
	pushTrade(c, "ch", "6")
	pubs = rec.snapshot()
	if merged := mergeSeam(t, page, pubs); !cursorsEqual(merged, pubCursors(t, pubs)) {
		t.Fatalf("after live %v pubs %v", merged, pubCursors(t, pubs))
	}
}

func TestTapeHeadSeamConcurrent(t *testing.T) {
	clk := newManualClock(time.Unix(0, 2_000_000))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	a := atom("V:r:ch", "trade.T")
	a.Record = true
	c.SetDesired(desired(1, 1, a))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 100 {
			c.mu.Lock()
			c.Trade("ch", &exchange.Trade{TradeID: strconv.Itoa(i), Price: 1, Qty: 1, Side: exchange.SideBuy})
			c.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		req, _ := proto.Marshal(&mdv1.ViewRequest{Limit: 10})
		for range 100 {
			raw := c.handleView(viewSubject(c.inst, "trade", "T"), req)
			page := unmarshalTape(t, raw)
			head := cursor{page.GetHead().GetConnEpoch(), page.GetHead().GetSeq()}
			var prev cursor
			for i, ev := range page.GetEvents() {
				cur := cursor{ev.GetConnEpoch(), ev.GetSeq()}
				if cursorLess(head, cur) {
					t.Errorf("event newer than head")
					return
				}
				if i > 0 && !cursorLess(cur, prev) {
					t.Errorf("page not newest-first")
					return
				}
				prev = cur
			}
		}
	}()
	wg.Wait()
}

func TestTapeSinceAndBefore(t *testing.T) {
	clk := newManualClock(time.Unix(0, 0))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	a := atom("V:r:ch", "trade.T")
	a.Record = true
	c.SetDesired(desired(1, 1, a))
	for i, ts := range []int64{100, 200, 300, 400} {
		clk.Set(time.Unix(0, ts))
		pushTrade(c, "ch", strconv.Itoa(i+1))
	}
	since := int64(200)
	page, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{SinceNs: &since})
	if page.GetMore() || idsOf(page) != "4,3,2" {
		t.Fatalf("since %+v more %v", idsOf(page), page.GetMore())
	}
	// Older than since exist. The walk stops, so this is not a truncated page.
	if page.GetNext() != nil {
		t.Fatal("since set next")
	}

	page, _ = askTape(t, c, "trade", "T", &mdv1.ViewRequest{
		Before: &mdv1.Cursor{ConnEpoch: 1, Seq: 4},
	})
	if idsOf(page) != "3,2,1" || page.GetMore() {
		t.Fatalf("before %+v", idsOf(page))
	}
	// The cursor named by before is not itself on the page.
	for _, ev := range page.GetEvents() {
		if ev.GetSeq() == 4 {
			t.Fatal("before included")
		}
	}

	page, _ = askTape(t, c, "trade", "T", &mdv1.ViewRequest{
		Before:  &mdv1.Cursor{ConnEpoch: 1, Seq: 4},
		SinceNs: &since,
	})
	if idsOf(page) != "3,2" || page.GetMore() {
		t.Fatalf("combined %+v more %v", idsOf(page), page.GetMore())
	}

	var got []string
	var before *mdv1.Cursor
	for {
		page, _ = askTape(t, c, "trade", "T", &mdv1.ViewRequest{Limit: 2, Before: before})
		evs := page.GetEvents()
		for _, ev := range evs {
			got = append(got, ev.GetEvent().GetTrade().GetTradeId())
		}
		if !page.GetMore() {
			if page.GetNext() != nil {
				t.Fatal("next on the last page")
			}
			break
		}
		if page.GetNext() == nil || len(evs) != 2 {
			t.Fatalf("page %+v", page)
		}
		oldest := evs[len(evs)-1]
		if page.GetNext().GetConnEpoch() != oldest.GetConnEpoch() || page.GetNext().GetSeq() != oldest.GetSeq() {
			t.Fatalf("next %+v oldest %d", page.GetNext(), oldest.GetSeq())
		}
		before = page.GetNext()
	}
	if join(got) != "4,3,2,1" {
		t.Fatalf("pages %s", join(got))
	}
}

func TestTapeLimit(t *testing.T) {
	clk := newManualClock(time.Unix(0, 10))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{TapeEvents: 2000})
	a := atom("V:r:ch", "trade.T")
	a.Record = true
	c.SetDesired(desired(1, 1, a))
	for i := 1; i <= tapePageDefault+1; i++ {
		pushTrade(c, "ch", strconv.Itoa(i))
	}
	page, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	if len(page.GetEvents()) != tapePageDefault || !page.GetMore() || page.GetNext().GetSeq() == 0 {
		t.Fatalf("default page %d more %v", len(page.GetEvents()), page.GetMore())
	}
	rest, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{Before: page.GetNext()})
	if len(rest.GetEvents()) != 1 || rest.GetMore() || rest.GetEvents()[0].GetSeq() != 1 {
		t.Fatalf("rest %d more %v seq %d", len(rest.GetEvents()), rest.GetMore(), rest.GetEvents()[0].GetSeq())
	}

	raw := c.handleView(viewSubject("md1", "trade", "T"), mustReq(t, &mdv1.ViewRequest{Limit: tapePageLimit + 1}))
	reply := &mdv1.ViewReply{}
	if err := proto.Unmarshal(raw, reply); err != nil {
		t.Fatal(err)
	}
	if reply.GetError() != errInvalid || reply.GetTape() != nil {
		t.Fatalf("limit %+v", reply)
	}

	k := atom("V:r:k", "kline_1m.K")
	k.Record = true
	c.SetDesired(desired(1, 2, a, k))
	raw = c.handleView(viewSubject("md1", "kline_1m", "K"), mustReq(t, &mdv1.ViewRequest{Limit: 99999}))
	reply.Reset()
	if err := proto.Unmarshal(raw, reply); err != nil {
		t.Fatal(err)
	}
	if reply.GetError() != errUnsupported {
		t.Fatalf("kline limit %+v", reply)
	}
}

func TestTapeReplyBudget(t *testing.T) {
	clk := newManualClock(time.Unix(0, 20))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{TapeEvents: 100})
	a := atom("V:r:ch", "trade.T")
	a.Record = true
	c.SetDesired(desired(1, 1, a))
	const body = 100 * 1024
	for i := 1; i <= 12; i++ {
		clk.Add(time.Millisecond)
		pushTrade(c, "ch", fmt.Sprintf("%02d%s", i, bytes.Repeat([]byte("x"), body)))
	}
	var (
		got []string
		raw []byte
	)
	var before *mdv1.Cursor
	pages := 0
	for {
		pages++
		if pages > 20 {
			t.Fatal("pagination did not finish")
		}
		page, enc := askTape(t, c, "trade", "T", &mdv1.ViewRequest{Before: before})
		raw = enc
		if len(page.GetEvents()) != 1 && len(enc) > tapeReplyBudget {
			t.Fatalf("page %d is %d bytes with %d events", pages, len(enc), len(page.GetEvents()))
		}
		if size := sizeOfReply(t, c, "trade.T", before); size != len(enc) {
			t.Fatalf("size %d len %d", size, len(enc))
		}
		for _, ev := range page.GetEvents() {
			got = append(got, ev.GetEvent().GetTrade().GetTradeId()[:2])
		}
		if !page.GetMore() {
			break
		}
		before = page.GetNext()
	}
	if join(got) != "12,11,10,09,08,07,06,05,04,03,02,01" {
		t.Fatalf("budget pages %s (%d)", join(got), pages)
	}
	if pages < 2 {
		t.Fatal("one page held every event")
	}
	_ = raw

	// One event larger than the budget is returned alone so the cursor moves.
	huge := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	h := atom("V:r:ch", "trade.T")
	h.Record = true
	huge.SetDesired(desired(1, 1, h))
	huge.mu.Lock()
	huge.epoch = 1
	huge.live = newLink(&memSession{})
	huge.mu.Unlock()
	pushTrade(huge, "ch", "z")
	pushTrade(huge, "ch", string(bytes.Repeat([]byte("y"), tapeReplyBudget+1)))
	page, enc := askTape(t, huge, "trade", "T", &mdv1.ViewRequest{})
	if len(page.GetEvents()) != 1 || !page.GetMore() || len(enc) <= tapeReplyBudget {
		t.Fatalf("oversized events %d more %v len %d", len(page.GetEvents()), page.GetMore(), len(enc))
	}
	if len(page.GetEvents()[0].GetEvent().GetTrade().GetTradeId()) < 10 {
		t.Fatal(page.GetEvents()[0].GetEvent().GetTrade().GetTradeId())
	}
	next, _ := askTape(t, huge, "trade", "T", &mdv1.ViewRequest{Before: page.GetNext()})
	if len(next.GetEvents()) != 1 || next.GetMore() || next.GetEvents()[0].GetEvent().GetTrade().GetTradeId() != "z" {
		t.Fatalf("second page %+v", next.GetEvents())
	}
}

func TestTapeReconnectGap(t *testing.T) {
	clk := newManualClock(time.Unix(0, 1_000))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	a := &mdctlv1.AtomAssignment{
		AtomId: "V:r:ch", Feeds: []string{"trade.T", "aggtrade.T"}, Record: true,
	}
	c.SetDesired(desired(1, 1, a))
	clk.Set(time.Unix(0, 100))
	pushTrade(c, "ch", "old")
	c.mu.Lock()
	c.AggTrade("ch", &exchange.AggTrade{TradeID: "old-agg", Price: 1, Qty: 1, Side: exchange.SideBuy})
	c.mu.Unlock()

	// The first dial of the process does not invent a gap.
	fresh := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	fresh.mu.Lock()
	fresh.seenUp = false
	fresh.markDown()
	n := len(fresh.downFeeds)
	fresh.mu.Unlock()
	if n != 0 {
		t.Fatalf("first dial feeds %d", n)
	}

	clk.Set(time.Unix(0, 300))
	c.downUp(time.Unix(0, 400))
	page, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	if len(page.GetGaps()) != 1 || page.GetGaps()[0].GetFirst() != nil {
		t.Fatalf("open gap %+v", page.GetGaps())
	}
	g := page.GetGaps()[0]
	if g.GetStartNs() != 300 || g.GetEndNs() != 400 || page.GetContinuousSinceNs() != 400 {
		t.Fatalf("gap %+v continuous %d", g, page.GetContinuousSinceNs())
	}
	if g.GetLast().GetConnEpoch() != 1 || g.GetLast().GetSeq() != 1 {
		t.Fatalf("last %+v", g.GetLast())
	}
	// The agg feed has its own open gap. The trade page does not carry it.
	agg, _ := askTape(t, c, "aggtrade", "T", &mdv1.ViewRequest{})
	if len(agg.GetGaps()) != 1 || agg.GetGaps()[0].GetLast().GetSeq() != 1 {
		t.Fatalf("agg gap %+v", agg.GetGaps())
	}

	clk.Set(time.Unix(0, 500))
	pushTrade(c, "ch", "new")
	page, _ = askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	if idsOf(page) != "new,old" {
		t.Fatalf("kept %s", idsOf(page))
	}
	if page.GetEvents()[0].GetConnEpoch() != 2 || page.GetEvents()[0].GetSeq() != 1 {
		t.Fatalf("newest %+v", page.GetEvents()[0])
	}
	if page.GetEvents()[1].GetConnEpoch() != 1 || page.GetEvents()[1].GetSeq() != 1 {
		t.Fatalf("oldest %+v", page.GetEvents()[1])
	}
	g = page.GetGaps()[0]
	if g.GetFirst().GetConnEpoch() != 2 || g.GetFirst().GetSeq() != 1 {
		t.Fatalf("first %+v", g.GetFirst())
	}
	if page.GetContinuousSinceNs() != 400 {
		t.Fatalf("continuous %d", page.GetContinuousSinceNs())
	}
	// Agg still has no post-reconnect event, so its gap stays open and off the trade page.
	agg, _ = askTape(t, c, "aggtrade", "T", &mdv1.ViewRequest{})
	if agg.GetGaps()[0].GetFirst() != nil {
		t.Fatal("agg gap closed by a trade")
	}
}

func TestTapeGapBetweenPages(t *testing.T) {
	clk := newManualClock(time.Unix(0, 1))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	a := atom("V:r:ch", "trade.T")
	a.Record = true
	c.SetDesired(desired(1, 1, a))
	pushTrade(c, "ch", "a1")
	pushTrade(c, "ch", "a2")
	c.downUp(time.Unix(0, 50))
	pushTrade(c, "ch", "b1")
	pushTrade(c, "ch", "b2")
	pushTrade(c, "ch", "b3")
	// Drop the first event of the new epoch so the gap falls between two pages of limit 2.
	c.mu.Lock()
	r := c.rings["V:r:ch"]
	for i, e := range r.events {
		if e.epoch == c.epoch && e.seq == 1 {
			c.tapeBytes -= int64(len(e.raw))
			r.bytes -= int64(len(e.raw))
			r.events = append(r.events[:i], r.events[i+1:]...)
			break
		}
	}
	c.mu.Unlock()

	page, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{Limit: 2})
	if idsOf(page) != "b3,b2" || !page.GetMore() || len(page.GetGaps()) != 1 {
		t.Fatalf("boundary page %s gaps %d more %v", idsOf(page), len(page.GetGaps()), page.GetMore())
	}
	next, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{Limit: 2, Before: page.GetNext()})
	if idsOf(next) != "a2,a1" || next.GetMore() || len(next.GetGaps()) != 0 {
		t.Fatalf("older page %s gaps %d", idsOf(next), len(next.GetGaps()))
	}
}

func TestTapeReconnectSession(t *testing.T) {
	_, agg := recordedMarket(t)
	notes := make(chan subNote, 4)
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
		_ = c.Write(ctx, websocket.MessageText, agg)
		<-ctx.Done()
	})
	var tick atomic.Int64
	tick.Store(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC).UnixNano())
	rec := newRecorder()
	c := New(Config{
		Venue: binance.VenueUM, Route: "market", Instance: "md1", URL: url,
		Market: umMarket(t), Publish: rec.publish, RetryAfter: -1,
		Now: func() time.Time { return time.Unix(0, tick.Add(int64(time.Millisecond))) },
	})
	a := atom("BinanceUM:market:btcusdt@aggTrade", "aggtrade.BinanceUM_Perp_BTCUSDT")
	a.Record = true
	c.SetDesired(desired(1, 1, a))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go c.Run(ctx)

	if note := waitNote(t, notes); note.gen != 1 {
		t.Fatalf("subscribe %+v", note)
	}
	close(play)
	rec.wait(t, 1)
	before, _ := askTape(t, c, "aggtrade", "BinanceUM_Perp_BTCUSDT", &mdv1.ViewRequest{})
	if len(before.GetEvents()) != 1 || len(before.GetGaps()) != 0 || before.GetEvents()[0].GetSeq() != 1 {
		t.Fatalf("before disconnect %+v", before)
	}
	close(release)
	if note := waitNote(t, notes); note.gen != 2 {
		t.Fatalf("resubscribe %+v", note)
	}
	close(again)
	rec.wait(t, 2)
	page, raw := askTape(t, c, "aggtrade", "BinanceUM_Perp_BTCUSDT", &mdv1.ViewRequest{})
	if len(page.GetEvents()) != 2 || page.GetEvents()[0].GetConnEpoch() != 2 || page.GetEvents()[1].GetConnEpoch() != 1 {
		t.Fatalf("epochs %+v %+v", page.GetEvents()[0], page.GetEvents()[1])
	}
	if page.GetEvents()[0].GetSeq() != 1 || page.GetEvents()[1].GetSeq() != 1 {
		t.Fatalf("seqs %d %d", page.GetEvents()[0].GetSeq(), page.GetEvents()[1].GetSeq())
	}
	if len(page.GetGaps()) != 1 {
		t.Fatalf("gaps %+v", page.GetGaps())
	}
	g := page.GetGaps()[0]
	if g.GetLast().GetConnEpoch() != 1 || g.GetLast().GetSeq() != 1 ||
		g.GetFirst().GetConnEpoch() != 2 || g.GetFirst().GetSeq() != 1 ||
		g.GetStartNs() > g.GetEndNs() || page.GetContinuousSinceNs() != g.GetEndNs() {
		t.Fatalf("gap %+v continuous %d", g, page.GetContinuousSinceNs())
	}
	payloads := tapePayloads(t, raw)
	pubs := rec.snapshot()
	for i, ev := range page.GetEvents() {
		pub := pubs[len(pubs)-1-i]
		_, _, _, _, payload, err := envelope.Parse(pub.msg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payloads[i], payload) {
			t.Fatal("payload drifted")
		}
		if ev.GetMdTsNs() == 0 {
			t.Fatal("missing md_ts")
		}
	}
	if page.GetEvents()[1].GetMdTsNs() > g.GetStartNs() || page.GetEvents()[0].GetMdTsNs() < g.GetEndNs() {
		t.Fatalf("times event %d/%d gap %d/%d", page.GetEvents()[1].GetMdTsNs(), page.GetEvents()[0].GetMdTsNs(), g.GetStartNs(), g.GetEndNs())
	}
}

func TestTapeLimits(t *testing.T) {
	w, n, b := tapeFromInit(1500, 2, 1)
	if w != 1500*time.Millisecond || n != 2 || b != 1 {
		t.Fatalf("init %s %d %d", w, n, b)
	}
	if _, _, b := tapeFromInit(0, 0, ^uint64(0)); b != int64(^uint64(0)>>1) {
		t.Fatalf("clamp %d", b)
	}
	zero := New(Config{Venue: "V", Route: "r", Instance: "i", Market: &stubMarket{}, Publish: func(string, []byte) error { return nil }})
	if zero.tapeWindow != DefaultTapeWindow || zero.tapeMax != DefaultTapeEvents || zero.tapeMaxBytes != DefaultTapeBytes {
		t.Fatalf("defaults %s %d %d", zero.tapeWindow, zero.tapeMax, zero.tapeMaxBytes)
	}
	custom := New(Config{
		Venue: "V", Route: "r", Instance: "i", Market: &stubMarket{}, Publish: func(string, []byte) error { return nil },
		TapeWindow: w, TapeEvents: n, TapeBytes: b,
	})
	if custom.tapeWindow != w || custom.tapeMax != n || custom.tapeMaxBytes != b {
		t.Fatal("config not applied")
	}

	clk := newManualClock(time.Unix(0, 1_000))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{TapeEvents: 2})
	a := &mdctlv1.AtomAssignment{AtomId: "V:r:ch", Feeds: []string{"trade.T", "aggtrade.T"}, Record: true}
	c.SetDesired(desired(1, 1, a))
	pushTrade(c, "ch", "t1")
	c.mu.Lock()
	c.AggTrade("ch", &exchange.AggTrade{TradeID: "g1", Price: 1, Qty: 1, Side: exchange.SideBuy})
	c.mu.Unlock()
	pushTrade(c, "ch", "t2")
	trade, _ := askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	agg, _ := askTape(t, c, "aggtrade", "T", &mdv1.ViewRequest{})
	if idsOf(trade) != "t2" || idsOf(agg) != "g1" {
		t.Fatalf("shared cap trade %s agg %s", idsOf(trade), idsOf(agg))
	}
	if trade.GetHead().GetSeq() != 2 || trade.GetHead().GetConnEpoch() != 1 {
		t.Fatalf("head after trim %+v", trade.GetHead())
	}

	late := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{TapeWindow: time.Minute})
	one := atom("V:r:ch", "trade.T")
	one.Record = true
	late.SetDesired(desired(1, 1, one))
	clk.Set(time.Unix(0, 0))
	pushTrade(late, "ch", "old")
	clk.Set(time.Unix(0, int64(time.Minute)+1))
	page, _ := askTape(t, late, "trade", "T", &mdv1.ViewRequest{})
	if len(page.GetEvents()) != 0 || page.GetHead().GetSeq() != 1 || page.GetHead().GetConnEpoch() != 1 {
		t.Fatalf("expired %+v head %+v", page.GetEvents(), page.GetHead())
	}

	// Two atoms, cap equal to one stored message: the newer event drops the older atom's event.
	probe := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	pa := atom("V:r:a", "trade.A")
	pa.Record = true
	probe.SetDesired(desired(1, 1, pa))
	clk.Set(time.Unix(0, 10))
	pushTrade(probe, "a", "same")
	probe.mu.Lock()
	size := probe.tapeBytes
	probe.mu.Unlock()
	clk2 := newManualClock(time.Unix(0, 10))
	capped := newTapeConn(t, clk2, func(string, []byte) error { return nil }, Config{TapeBytes: size})
	left := atom("V:r:a", "trade.A")
	left.Record = true
	right := atom("V:r:b", "trade.B")
	right.Record = true
	capped.SetDesired(desired(1, 1, left, right))
	pushTrade(capped, "a", "same")
	clk2.Set(time.Unix(0, 20))
	pushTrade(capped, "b", "same")
	older, _ := askTape(t, capped, "trade", "A", &mdv1.ViewRequest{})
	newer, _ := askTape(t, capped, "trade", "B", &mdv1.ViewRequest{})
	if len(older.GetEvents()) != 0 || idsOf(newer) != "same" {
		t.Fatalf("worker cap older %d newer %s", len(older.GetEvents()), idsOf(newer))
	}
	if older.GetHead().GetSeq() != 1 {
		t.Fatalf("dropped atom head %+v", older.GetHead())
	}
}

func TestTapeUnsupportedAndFeedEnd(t *testing.T) {
	clk := newManualClock(time.Unix(0, 5))
	c := newTapeConn(t, clk, func(string, []byte) error { return nil }, Config{})
	off := atom("V:r:ch", "trade.T")
	k := atom("V:r:ch", "kline_1m.T")
	k.Record = true
	c.SetDesired(desired(1, 1, off, k))
	pushTrade(c, "ch", "nope")
	c.mu.Lock()
	c.Kline("ch", &exchange.Kline{Interval: "1m", Open: 1, Close: 1, Volume: 1})
	c.mu.Unlock()
	for _, subj := range []struct{ topic, ticker string }{{"trade", "T"}, {"kline_1m", "T"}} {
		raw := c.handleView(viewSubject("md1", subj.topic, subj.ticker), mustReq(t, &mdv1.ViewRequest{}))
		reply := &mdv1.ViewReply{}
		if err := proto.Unmarshal(raw, reply); err != nil {
			t.Fatal(err)
		}
		if reply.GetError() != errUnsupported || reply.GetTape() != nil {
			t.Fatalf("%s %+v", subj.topic, reply)
		}
	}
	c.mu.Lock()
	stored := 0
	for _, r := range c.rings {
		stored += len(r.events)
	}
	c.mu.Unlock()
	if stored != 0 {
		t.Fatalf("stored %d", stored)
	}

	on := atom("V:r:ch", "trade.T")
	on.Record = true
	c.SetDesired(desired(1, 2, on))
	pushTrade(c, "ch", "yes")
	c.mu.Lock()
	c.FeedEnd("ch", &exchange.FeedEnd{Reason: "expired", AtNs: 9})
	c.mu.Unlock()
	page, raw := askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	// The unrecorded trade still took seq 1, "yes" is seq 2, and FeedEnd takes seq 3 without being stored.
	if idsOf(page) != "yes" || page.GetEvents()[0].GetSeq() != 2 || page.GetHead().GetSeq() != 3 {
		t.Fatalf("feed end page %s head %+v", idsOf(page), page.GetHead())
	}
	if page.GetEvents()[0].GetEvent().GetFeedEnd() != nil {
		t.Fatal("feed end recorded")
	}
	if err := proto.Unmarshal(raw, &mdv1.ViewReply{}); err != nil {
		t.Fatal(err)
	}
	if c.handleView(viewSubject("md1", "trade", "T"), []byte{0xff, 0xff, 0xff, 0xff, 0x0f}) == nil {
		t.Fatal("bad protobuf produced no reply")
	}
	bad := &mdv1.ViewReply{}
	if err := proto.Unmarshal(c.handleView(viewSubject("md1", "trade", "T"), []byte{0xff, 0xff, 0xff, 0xff, 0x0f}), bad); err != nil || bad.GetError() != errInvalid {
		t.Fatalf("bad protobuf %+v %v", bad, err)
	}

	// Publish can fail and the seq is still tape, because the number was spent.
	c.pub = func(string, []byte) error { return errors.New("down") }
	pushTrade(c, "ch", "kept")
	page, _ = askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	if idsOf(page) != "kept,yes" || page.GetHead().GetSeq() != 4 {
		t.Fatalf("failed publish %s head %d", idsOf(page), page.GetHead().GetSeq())
	}

	c.mu.Lock()
	c.live = nil
	c.mu.Unlock()
	page, _ = askTape(t, c, "trade", "T", &mdv1.ViewRequest{})
	if idsOf(page) != "kept,yes" {
		t.Fatalf("down tape %s", idsOf(page))
	}
}

func TestViewLeavesNoResponder(t *testing.T) {
	ns := natsxtest.Start(t)
	worker, err := natsx.Connect(ns.URL(), natsx.Options{Name: "worker", InboxPrefix: "_INBOX_md"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := natsx.Connect(ns.URL(), natsx.Options{Name: "client"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		worker.Close()
		client.Close()
	})
	ctx := t.Context()
	c := New(Config{
		Venue: "V", Route: "r", Instance: "md1",
		Market: &stubMarket{}, Publish: func(string, []byte) error { return nil },
		ViewListen: func(subject string, handler func(req []byte) []byte) (func(), error) {
			sub, err := worker.Subscribe(ctx, subject, "", func(m natsx.Msg) {
				if m.Reply == "" {
					return
				}
				reply := handler(bytes.Clone(m.Data))
				if len(reply) == 0 {
					return
				}
				_ = worker.Publish(m.Reply, reply)
			})
			if err != nil {
				return nil, err
			}
			return func() {
				_ = sub.Unsubscribe()
				_ = worker.Flush()
			}, nil
		},
	})
	trade := atom("V:r:ch", "trade.T")
	trade.Record = true
	kline := atom("V:r:k", "kline_1m.K")
	kline.Record = true
	quiet := atom("V:r:q", "trade.Q")
	c.SetDesired(desired(1, 1, trade, kline, quiet))

	var reply mdv1.ViewReply
	if err := client.Request(ctx, "md.view.md1.trade.T", &mdv1.ViewRequest{}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.GetError() != "" || reply.GetTape() == nil || reply.GetTape().GetHead() == nil {
		t.Fatalf("tape %+v", &reply)
	}
	reply.Reset()
	if err := client.Request(ctx, "md.view.md1.kline_1m.K", &mdv1.ViewRequest{Limit: 99999}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.GetError() != errUnsupported {
		t.Fatalf("kline %+v", &reply)
	}
	reply.Reset()
	if err := client.Request(ctx, "md.view.md1.trade.Q", &mdv1.ViewRequest{Limit: 99999}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.GetError() != errUnsupported {
		t.Fatalf("record false %+v", &reply)
	}
	// Still held, including while an agent would be lingering: the worker has no linger timer.
	reply.Reset()
	if err := client.Request(ctx, "md.view.md1.trade.T", &mdv1.ViewRequest{}, &reply); err != nil || reply.GetError() != "" {
		t.Fatalf("linger %v %+v", err, &reply)
	}

	c.SetDesired(desired(1, 2))
	reply.Reset()
	err = client.Request(ctx, "md.view.md1.trade.T", &mdv1.ViewRequest{}, &reply)
	if !errors.Is(err, natsx.ErrNoResponders) {
		t.Fatalf("removed err %v reply %+v", err, &reply)
	}
	raw := c.handleView(viewSubject("md1", "trade", "T"), mustReq(t, &mdv1.ViewRequest{}))
	reply.Reset()
	if err := proto.Unmarshal(raw, &reply); err != nil || reply.GetError() != errNotHeld {
		t.Fatalf("in-flight %+v %v", &reply, err)
	}
}

func TestBookView(t *testing.T) {
	hold := make(chan struct{})
	e := newBookEnv(t, umBookSync(t), 1, nil, []doerStep{{
		resp: bookHTTP(100, 7, [][2]float64{{100, 1}, {90, 1}}, [][2]float64{{110, 1}, {200, 1}}),
		hold: hold,
	}})
	e.doer.wait(t, 1)
	if errStr, _ := askBook(t, e.c, "orderbook_delta", "T"); errStr != errNotSynced {
		t.Fatalf("while snapshot %s", errStr)
	}
	close(hold)
	e.waitPubs(t, 2)

	errStr, snap := askBook(t, e.c, "orderbook_delta", "T")
	if errStr != "" || snap == nil {
		t.Fatalf("delta view %s %+v", errStr, snap)
	}
	if !sameLevels(gotLevels(snap.GetBids()), []exchange.Level{{Price: 100, Qty: 1}, {Price: 90, Qty: 1}}) {
		t.Fatalf("full bids %v", gotLevels(snap.GetBids()))
	}
	if !sameLevels(gotLevels(snap.GetAsks()), []exchange.Level{{Price: 110, Qty: 1}, {Price: 200, Qty: 1}}) {
		t.Fatalf("full asks %v", gotLevels(snap.GetAsks()))
	}
	all := e.open(t)
	d := deltasOf(all)
	b := booksOf(all)
	if snap.GetConnEpoch() != d[0].epoch || snap.GetSeq() != d[0].seq {
		t.Fatalf("delta cursor %d/%d want %d/%d", snap.GetConnEpoch(), snap.GetSeq(), d[0].epoch, d[0].seq)
	}
	if snap.GetMeta().GetExchTsNs() != d[0].ev.GetOrderBookDelta().GetMeta().GetExchTsNs() {
		t.Fatalf("exch ts %d", snap.GetMeta().GetExchTsNs())
	}
	topErr, top := askBook(t, e.c, "orderbook", "T")
	if topErr != "" || top.GetSeq() != b[0].seq {
		t.Fatalf("orderbook view %s seq %d want %d", topErr, top.GetSeq(), b[0].seq)
	}
	if !sameLevels(gotLevels(top.GetBids()), gotLevels(snap.GetBids())) {
		t.Fatal("orderbook view is top-N")
	}
	// A limit that would be invalid for tape does not apply to a book.
	raw := e.c.handleView(viewSubject(e.c.inst, "orderbook", "T"), mustReq(t, &mdv1.ViewRequest{Limit: tapePageLimit + 1}))
	reply := &mdv1.ViewReply{}
	if err := proto.Unmarshal(raw, reply); err != nil || reply.GetError() != "" || reply.GetBook() == nil {
		t.Fatalf("book limit %+v %v", reply, err)
	}

	e.push(exchange.BookUpdate{First: 101, Last: 101, Prev: 100, ExchTsNs: 8, Bids: []exchange.Level{{Price: 90, Qty: 2}}})
	e.waitPubs(t, 3)
	all = e.open(t)
	d = deltasOf(all)
	b = booksOf(all)
	if len(b) != 1 {
		t.Fatal("deep change republished top-N")
	}
	errStr, snap = askBook(t, e.c, "orderbook_delta", "T")
	if errStr != "" || snap.GetSeq() != d[len(d)-1].seq {
		t.Fatalf("delta after %s seq %d", errStr, snap.GetSeq())
	}
	if !sameLevels(gotLevels(snap.GetBids()), []exchange.Level{{Price: 100, Qty: 1}, {Price: 90, Qty: 2}}) {
		t.Fatalf("window %v", gotLevels(snap.GetBids()))
	}
	if snap.GetMeta().GetExchTsNs() != 8 {
		t.Fatalf("apply ts %d", snap.GetMeta().GetExchTsNs())
	}
	_, top = askBook(t, e.c, "orderbook", "T")
	if top.GetSeq() != b[0].seq {
		t.Fatalf("orderbook seq moved to %d", top.GetSeq())
	}
	if !sameLevels(gotLevels(top.GetBids()), gotLevels(snap.GetBids())) {
		t.Fatal("orderbook view missed the deep level")
	}

	e.c.mu.Lock()
	e.c.live.books[e.ch].holdResub = true
	e.c.mu.Unlock()
	if errStr, _ = askBook(t, e.c, "orderbook_delta", "T"); errStr != errNotSynced {
		t.Fatalf("resub %s", errStr)
	}
	e.c.mu.Lock()
	e.c.live.books[e.ch].holdResub = false
	e.c.live = nil
	e.c.mu.Unlock()
	if errStr, _ = askBook(t, e.c, "orderbook", "T"); errStr != errNotSynced {
		t.Fatalf("down %s", errStr)
	}

	stalled := newBookEnv(t, umBookSync(t), 1, &bookTune{zeroTick: true}, nil)
	stalled.waitUntil(t, func() bool {
		stalled.c.mu.Lock()
		defer stalled.c.mu.Unlock()
		sh := stalled.c.live.books[stalled.ch]
		return sh != nil && sh.stalled
	})
	if errStr, _ = askBook(t, stalled.c, "orderbook_delta", "T"); errStr != errNotSynced {
		t.Fatalf("stalled %s", errStr)
	}
}

func newTapeConn(t *testing.T, clk *manualClock, pub func(string, []byte) error, cfg Config) *Conn {
	t.Helper()
	cfg.Venue = "V"
	cfg.Route = "r"
	cfg.Instance = "md1"
	cfg.Market = &stubMarket{}
	cfg.Publish = pub
	cfg.Now = clk.Now
	c := New(cfg)
	c.mu.Lock()
	c.epoch = 1
	c.live = newLink(&memSession{})
	c.seenUp = true
	c.mu.Unlock()
	return c
}

func pushTrade(c *Conn, ch, id string) {
	c.mu.Lock()
	c.Trade(ch, &exchange.Trade{TradeID: id, Price: 1, Qty: 1, Side: exchange.SideBuy})
	c.mu.Unlock()
}

func (c *Conn) downUp(end time.Time) {
	c.mu.Lock()
	c.markDown()
	c.live = nil
	feeds := append([]string(nil), c.downFeeds...)
	c.downFeeds = nil
	prev := c.epoch
	start := c.downNs
	c.epoch++
	c.live = newLink(&memSession{})
	if c.seenUp {
		c.openGaps(feeds, prev, start, end.UnixNano())
	}
	c.seenUp = true
	c.mu.Unlock()
}

func askTape(t *testing.T, c *Conn, topic, ticker string, req *mdv1.ViewRequest) (*mdv1.TapePage, []byte) {
	t.Helper()
	raw := c.handleView(viewSubject(c.inst, topic, ticker), mustReq(t, req))
	page := unmarshalTape(t, raw)
	return page, raw
}

func unmarshalTape(t *testing.T, raw []byte) *mdv1.TapePage {
	t.Helper()
	reply := &mdv1.ViewReply{}
	if err := proto.Unmarshal(raw, reply); err != nil {
		t.Fatal(err)
	}
	if reply.GetError() != "" || reply.GetTape() == nil {
		t.Fatalf("tape reply %+v", reply)
	}
	return reply.GetTape()
}

func askBook(t *testing.T, c *Conn, topic, ticker string) (string, *mdv1.BookSnapshot) {
	t.Helper()
	raw := c.handleView(viewSubject(c.inst, topic, ticker), mustReq(t, &mdv1.ViewRequest{}))
	reply := &mdv1.ViewReply{}
	if err := proto.Unmarshal(raw, reply); err != nil {
		t.Fatal(err)
	}
	return reply.GetError(), reply.GetBook()
}

func mustReq(t *testing.T, req *mdv1.ViewRequest) []byte {
	t.Helper()
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func idsOf(page *mdv1.TapePage) string {
	var ids []string
	for _, ev := range page.GetEvents() {
		id := ev.GetEvent().GetTrade().GetTradeId()
		if id == "" {
			id = ev.GetEvent().GetAggTrade().GetTradeId()
		}
		ids = append(ids, id)
	}
	return join(ids)
}

func join(s []string) string { return strings.Join(s, ",") }

func tapePayloads(t *testing.T, reply []byte) [][]byte {
	t.Helper()
	pages := takeBytes(t, reply, 10)
	if len(pages) != 1 {
		t.Fatalf("pages %d", len(pages))
	}
	events := takeBytes(t, pages[0], 1)
	out := make([][]byte, len(events))
	for i, ev := range events {
		fields := takeBytes(t, ev, 4)
		if len(fields) != 1 {
			t.Fatalf("event %d field 4 count %d", i, len(fields))
		}
		out[i] = fields[0]
	}
	return out
}

func takeBytes(t *testing.T, b []byte, want protowire.Number) [][]byte {
	t.Helper()
	var out [][]byte
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatalf("tag")
		}
		b = b[n:]
		switch typ {
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				t.Fatal("bytes")
			}
			b = b[m:]
			if num == want {
				out = append(out, append([]byte(nil), v...))
			}
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(b)
			if m < 0 {
				t.Fatal("varint")
			}
			b = b[m:]
		case protowire.Fixed64Type:
			if len(b) < 8 {
				t.Fatal("fixed64")
			}
			b = b[8:]
		default:
			t.Fatalf("wire type %d", typ)
		}
	}
	return out
}

func sizeOfReply(t *testing.T, c *Conn, feed string, before *mdv1.Cursor) int {
	t.Helper()
	var b *cursor
	if before != nil {
		b = &cursor{epoch: before.GetConnEpoch(), seq: before.GetSeq()}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ow := c.owners[feed]
	d := c.buildTape(c.rings[ow.atomID], feed, 0, b, nil)
	enc := encodeTapeReply(d)
	if sizeTapeReply(d) != len(enc) {
		t.Fatalf("encoder size %d len %d", sizeTapeReply(d), len(enc))
	}
	return len(enc)
}

func mergeSeam(t *testing.T, page *mdv1.TapePage, pubs []publication) []cursor {
	t.Helper()
	ev := page.GetEvents()
	out := make([]cursor, 0, len(pubs))
	for i := len(ev) - 1; i >= 0; i-- {
		out = append(out, cursor{epoch: ev[i].GetConnEpoch(), seq: ev[i].GetSeq()})
	}
	head := cursor{epoch: page.GetHead().GetConnEpoch(), seq: page.GetHead().GetSeq()}
	for _, cur := range pubCursors(t, pubs) {
		if cursorLess(head, cur) {
			out = append(out, cur)
		}
	}
	return out
}

func pubCursors(t *testing.T, pubs []publication) []cursor {
	t.Helper()
	out := make([]cursor, len(pubs))
	for i, p := range pubs {
		_, epoch, seq, _ := openPub(t, p)
		out[i] = cursor{epoch: epoch, seq: seq}
	}
	return out
}

func cursorsEqual(a, b []cursor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock(t time.Time) *manualClock { return &manualClock{t: t} }

func (m *manualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *manualClock) Set(t time.Time) {
	m.mu.Lock()
	m.t = t
	m.mu.Unlock()
}

func (m *manualClock) Add(d time.Duration) {
	m.mu.Lock()
	m.t = m.t.Add(d)
	m.mu.Unlock()
}
