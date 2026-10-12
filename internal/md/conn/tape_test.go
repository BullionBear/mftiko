package conn

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

const (
	tapeTicker = "Binance_Spot_BTCUSDT"
	tapeCh     = "btcusdt@depth"
)

func TestTapeDefaults(t *testing.T) {
	c := New(Config{Venue: "V", Route: "r", Instance: "i", Market: &stubMarket{}, Publish: func(string, []byte) error { return nil }})
	if c.tapeWindow != defaultTapeWindow || c.tapeMax != defaultTapeMaxEvents || c.tapeBudget != defaultTapeBudget {
		t.Fatalf("defaults window %d max %d budget %d", c.tapeWindow, c.tapeMax, c.tapeBudget)
	}
}

func TestTapeRecordsThroughSetFeeds(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade", "aggtrade", "liquidation", "best_quote")
	h.up(7)

	rep, ok := h.ask("trade", nil)
	if !ok || rep.GetError() != "" || rep.GetTape().GetHead().GetConnEpoch() != 7 || rep.GetTape().GetHead().GetSeq() != 0 || len(rep.GetTape().GetEvents()) != 0 {
		t.Fatalf("empty head %+v ok %v", rep, ok)
	}

	h.at(100)
	h.trade("1/1")
	h.at(200)
	h.trade("1/2")
	h.at(300)
	h.agg("a")
	h.liq("L")
	h.c.mu.Lock()
	h.c.BestQuote(tapeCh, &exchange.BestQuote{Bid: 1, Ask: 2})
	h.c.FeedEnd(tapeCh, &exchange.FeedEnd{Reason: "expired", AtNs: 1})
	h.c.mu.Unlock()

	rep, ok = h.ask("trade", nil)
	if !ok || rep.GetError() != "" {
		t.Fatalf("trade view %+v", rep)
	}
	ev := rep.GetTape().GetEvents()
	if len(ev) != 2 || ev[0].GetSeq() != 2 || ev[1].GetSeq() != 1 || ev[0].GetConnEpoch() != 7 {
		t.Fatalf("events %+v", ev)
	}
	if ev[0].GetEvent().GetTrade().GetTradeId() != "1/2" || ev[0].GetMdTsNs() != 200 {
		t.Fatalf("newest %+v", ev[0])
	}
	head := rep.GetTape().GetHead()
	if head.GetConnEpoch() != 7 || head.GetSeq() != 2 || cursorLess(head.GetConnEpoch(), head.GetSeq(), ev[0].GetConnEpoch(), ev[0].GetSeq()) {
		t.Fatalf("head %+v newest %d/%d", head, ev[0].GetConnEpoch(), ev[0].GetSeq())
	}
	if rep.GetTape().GetContinuousSinceNs() != 100 {
		t.Fatalf("continuous %d", rep.GetTape().GetContinuousSinceNs())
	}
	if _, ok := h.ask("best_quote", nil); !ok {
		t.Fatal("best_quote has no responder")
	}
	if other, _ := h.ask("best_quote", nil); other.GetError() != "unsupported" || other.GetTape() != nil {
		t.Fatalf("best_quote %+v", other)
	}
	if agg, _ := h.ask("aggtrade", nil); len(agg.GetTape().GetEvents()) != 1 || agg.GetTape().GetEvents()[0].GetEvent().GetAggTrade().GetTradeId() != "a" {
		t.Fatalf("agg %+v", agg)
	}
	if liq, _ := h.ask("liquidation", nil); len(liq.GetTape().GetEvents()) != 1 || liq.GetTape().GetEvents()[0].GetEvent().GetLiquidation() == nil {
		t.Fatalf("liq %+v", liq)
	}

	h.c.mu.Lock()
	ring := h.c.views["trade."+tapeTicker].ring
	rec, good := ring.load(ring.pub.Load().tail)
	h.c.mu.Unlock()
	if !good {
		t.Fatal("load")
	}
	fe := &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payloadOf(rec.raw), fe); err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(&mdv1.TapeEvent{ConnEpoch: rec.epoch, Seq: rec.seqNo, MdTsNs: rec.mdTs, Event: fe})
	if err != nil {
		t.Fatal(err)
	}
	if got := encodeTapeEvent(rec); string(got) != string(want) {
		t.Fatalf("protowire %d bytes, proto.Marshal %d", len(got), len(want))
	}
}

func TestTapePageBeforeSinceAndLimit(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.up(1)
	for i := 1; i <= 10; i++ {
		h.at(int64(i) * 1000)
		h.trade(fmt.Sprintf("1/%d", i))
	}
	rep, _ := h.ask("trade", &mdv1.ViewRequest{Limit: 3})
	got := tapeSeqs(t, rep)
	if strings.Join(got, ",") != "10,9,8" || !rep.GetTape().GetMore() || rep.GetTape().GetNext().GetSeq() != 8 {
		t.Fatalf("page1 %+v more %v next %+v", got, rep.GetTape().GetMore(), rep.GetTape().GetNext())
	}
	rep, _ = h.ask("trade", &mdv1.ViewRequest{Limit: 3, Before: rep.GetTape().GetNext()})
	got = tapeSeqs(t, rep)
	if strings.Join(got, ",") != "7,6,5" || rep.GetTape().GetNext().GetSeq() != 5 {
		t.Fatalf("page2 %+v", got)
	}
	rep, _ = h.ask("trade", &mdv1.ViewRequest{Limit: 3, Before: &mdv1.Cursor{ConnEpoch: 1, Seq: 4}})
	got = tapeSeqs(t, rep)
	if strings.Join(got, ",") != "3,2,1" || rep.GetTape().GetMore() {
		t.Fatalf("before %+v more %v", got, rep.GetTape().GetMore())
	}

	since := int64(5000)
	rep, _ = h.ask("trade", &mdv1.ViewRequest{SinceNs: &since, Before: &mdv1.Cursor{ConnEpoch: 1, Seq: 8}})
	got = tapeSeqs(t, rep)
	if strings.Join(got, ",") != "7,6,5" || rep.GetTape().GetMore() {
		t.Fatalf("since+before %+v more %v", got, rep.GetTape().GetMore())
	}
	if zero, _ := h.ask("trade", &mdv1.ViewRequest{}); len(zero.GetTape().GetEvents()) != 10 {
		t.Fatalf("default limit %d", len(zero.GetTape().GetEvents()))
	}
	if bad, _ := h.ask("trade", &mdv1.ViewRequest{Limit: 5001}); bad.GetError() != "invalid" || bad.GetTape() != nil {
		t.Fatalf("invalid %+v", bad)
	}
}

func TestTapePageStopsNear900KiB(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.up(1)
	const n = 80
	for i := 1; i <= n; i++ {
		h.at(int64(i))
		h.trade(strings.Repeat("x", 20000) + fmt.Sprintf("-%d", i))
	}
	rep, _ := h.ask("trade", &mdv1.ViewRequest{Limit: 5000})
	raw, err := proto.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.GetTape().GetMore() || len(rep.GetTape().GetEvents()) >= n || len(raw) > tapePageMax || len(raw) < 800*1024 {
		t.Fatalf("events %d bytes %d more %v", len(rep.GetTape().GetEvents()), len(raw), rep.GetTape().GetMore())
	}
	next, _ := h.ask("trade", &mdv1.ViewRequest{Limit: 5000, Before: rep.GetTape().GetNext()})
	if len(next.GetTape().GetEvents()) == 0 || next.GetTape().GetEvents()[0].GetSeq() >= rep.GetTape().GetEvents()[len(rep.GetTape().GetEvents())-1].GetSeq() {
		t.Fatalf("next page %+v", tapeSeqs(t, next))
	}
}

func TestTapeReconnectGap(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.at(1000)
	h.up(1)
	h.at(1100)
	h.trade("1/1")
	h.at(1200)
	h.trade("1/2")
	h.at(2000)
	h.down()
	h.at(3000)
	h.up(2)

	rep, _ := h.ask("trade", nil)
	if rep.GetTape().GetHead().GetConnEpoch() != 1 || rep.GetTape().GetHead().GetSeq() != 2 {
		t.Fatalf("head before the next trade %+v", rep.GetTape().GetHead())
	}
	gaps := rep.GetTape().GetGaps()
	if len(gaps) != 1 || gaps[0].GetStartNs() != 2000 || gaps[0].GetEndNs() != 3000 {
		t.Fatalf("gap %+v", gaps)
	}
	if gaps[0].GetLast().GetConnEpoch() != 1 || gaps[0].GetLast().GetSeq() != 2 || gaps[0].GetFirst() != nil {
		t.Fatalf("cursors %+v", gaps[0])
	}
	if rep.GetTape().GetContinuousSinceNs() != 3000 {
		t.Fatalf("continuous %d", rep.GetTape().GetContinuousSinceNs())
	}
	if strings.Join(tapeSeqs(t, rep), ",") != "2,1" {
		t.Fatalf("kept %+v", tapeSeqs(t, rep))
	}

	h.at(3100)
	h.trade("2/1")
	rep, _ = h.ask("trade", nil)
	got := tapeSeqs(t, rep)
	if strings.Join(got, ",") != "1,2,1" || rep.GetTape().GetEvents()[0].GetConnEpoch() != 2 {
		t.Fatalf("after %+v", rep.GetTape().GetEvents())
	}
	gaps = rep.GetTape().GetGaps()
	if len(gaps) != 1 || gaps[0].GetFirst().GetConnEpoch() != 2 || gaps[0].GetFirst().GetSeq() != 1 {
		t.Fatalf("first %+v", gaps)
	}
	if rep.GetTape().GetHead().GetConnEpoch() != 2 || rep.GetTape().GetHead().GetSeq() != 1 {
		t.Fatalf("head %+v", rep.GetTape().GetHead())
	}
	if cursorLess(rep.GetTape().GetHead().GetConnEpoch(), rep.GetTape().GetHead().GetSeq(), rep.GetTape().GetEvents()[0].GetConnEpoch(), rep.GetTape().GetEvents()[0].GetSeq()) {
		t.Fatal("head behind the newest event")
	}
}

func TestTapePendingKeepsTheRing(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.up(1)
	h.trade("1/1")

	h.c.symbols = stubSymbols{info: tapeInfo(symbol.Halted)}
	h.c.SetDesired(feedsDesired(2, "trade."+tapeTicker))
	rep, ok := h.ask("trade", nil)
	if !ok || len(rep.GetTape().GetEvents()) != 1 {
		t.Fatalf("pending dropped the ring ok %v %+v", ok, rep)
	}
	h.c.mu.Lock()
	n := len(h.c.wanted)
	h.c.mu.Unlock()
	if n != 0 {
		t.Fatal("halted feed stayed in wanted")
	}

	fresh := newTapeBox(t, tapeBoxOptions{})
	fresh.c.symbols = stubSymbols{info: tapeInfo(symbol.Halted)}
	fresh.c.SetDesired(feedsDesired(1, "trade."+tapeTicker))
	if _, ok := fresh.ask("trade", nil); ok {
		t.Fatal("a feed that was never tradable got a responder")
	}

	h.c.symbols = stubSymbols{miss: true}
	h.c.SetDesired(feedsDesired(3, "trade."+tapeTicker))
	if _, ok := h.ask("trade", nil); ok {
		t.Fatal("rejected feed still has a responder")
	}
}

func TestTapeRemovalDropsTheResponder(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.up(1)
	h.trade("1/1")
	h.c.SetDesired(feedsDesired(2))
	if _, ok := h.ask("trade", nil); ok {
		t.Fatal("removed feed still has a responder")
	}
}

func TestTapeApplyPathDoesNotRecord(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.c.SetDesired(&mdctlv1.Desired{
		Version: &commonv1.Version{Seq: 1},
		Atoms: []*mdctlv1.AtomAssignment{{
			AtomId: "Binance:public:" + tapeCh,
			Feeds:  []string{"trade." + tapeTicker},
		}},
	})
	h.up(1)
	h.trade("x")
	if _, ok := h.ask("trade", nil); ok {
		t.Fatal("apply path subscribed md.view")
	}
	h.c.mu.Lock()
	n := len(h.c.views)
	h.c.mu.Unlock()
	if n != 0 {
		t.Fatalf("views %d", n)
	}
}

func TestTapeTrimWindowCountAndBudget(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{window: time.Millisecond, max: 3, budget: 1 << 20})
	h.want("trade")
	h.up(1)
	h.at(int64(10 * time.Millisecond))
	h.trade("old")
	h.at(int64(12 * time.Millisecond))
	h.trade("new")
	rep, _ := h.ask("trade", nil)
	if len(rep.GetTape().GetEvents()) != 1 || rep.GetTape().GetEvents()[0].GetEvent().GetTrade().GetTradeId() != "new" {
		t.Fatalf("window %+v", rep.GetTape().GetEvents())
	}

	count := newTapeBox(t, tapeBoxOptions{max: 3})
	count.want("trade")
	count.up(1)
	for i := 1; i <= 5; i++ {
		count.at(int64(i))
		count.trade(fmt.Sprintf("%d", i))
	}
	rep, _ = count.ask("trade", nil)
	if strings.Join(seqsOf(rep), ",") != "5,4,3" {
		t.Fatalf("count %+v", seqsOf(rep))
	}

	budget := newTapeBox(t, tapeBoxOptions{max: 1000, budget: 400})
	budget.want("trade", "aggtrade")
	budget.up(1)
	for i := 0; i < 30; i++ {
		budget.at(int64(i + 1))
		if i%2 == 0 {
			budget.trade(fmt.Sprintf("t%d", i))
		} else {
			budget.agg(fmt.Sprintf("a%d", i))
		}
	}
	budget.c.mu.Lock()
	bytes, events := budget.c.tapeBytes, 0
	for _, v := range budget.c.views {
		if v.ring != nil {
			events += v.ring.events
		}
	}
	budget.c.mu.Unlock()
	if bytes > 400 || events >= 30 || events == 0 {
		t.Fatalf("budget bytes %d events %d", bytes, events)
	}

	// A ring with no events left reports the current epoch and seq 0 (D437).
	gone := newTapeBox(t, tapeBoxOptions{max: 10, budget: 1})
	gone.want("trade")
	gone.up(3)
	gone.trade("only")
	rep, _ = gone.ask("trade", nil)
	if len(rep.GetTape().GetEvents()) != 0 || rep.GetTape().GetHead().GetConnEpoch() != 3 || rep.GetTape().GetHead().GetSeq() != 0 {
		t.Fatalf("empty after budget %+v", rep.GetTape())
	}
}

func TestBookViewSnapshotAndNotSynced(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{depth: 20})
	h.want("orderbook", "orderbook_delta")
	h.up(4)
	if rep, ok := h.ask("orderbook_delta", nil); !ok || rep.GetError() != "not_synced" || rep.GetBook() != nil {
		t.Fatalf("before sync %+v", rep)
	}
	h.c.mu.Lock()
	h.c.Book(tapeCh, &exchange.BookUpdate{
		Snapshot: true, ExchTsNs: 77,
		Bids: []exchange.Level{{Price: 100, Qty: 1}},
		Asks: []exchange.Level{{Price: 101, Qty: 2}},
	})
	h.c.mu.Unlock()
	delta, _ := h.ask("orderbook_delta", nil)
	top, _ := h.ask("orderbook", nil)
	if delta.GetError() != "" || top.GetError() != "" {
		t.Fatalf("delta %s top %s", delta.GetError(), top.GetError())
	}
	b := delta.GetBook()
	if b.GetConnEpoch() != 4 || b.GetSeq() == 0 || b.GetMeta().GetExchTsNs() != 77 || len(b.GetBids()) != 1 || len(b.GetAsks()) != 1 {
		t.Fatalf("book %+v", b)
	}
	if top.GetBook().GetConnEpoch() != b.GetConnEpoch() || top.GetBook().GetSeq() != b.GetSeq() {
		t.Fatalf("subjects differ %d/%d vs %d/%d", top.GetBook().GetConnEpoch(), top.GetBook().GetSeq(), b.GetConnEpoch(), b.GetSeq())
	}
	if b.GetBids()[0].GetQty() != 1 || b.GetAsks()[0].GetQty() != 2 {
		t.Fatalf("levels %+v %+v", b.GetBids(), b.GetAsks())
	}

	h.c.mu.Lock()
	sh := h.c.live.books[tapeCh]
	h.c.beginResync(tapeCh, sh, nil)
	h.c.mu.Unlock()
	if rep, _ := h.ask("orderbook", nil); rep.GetError() != "not_synced" {
		t.Fatalf("resync %+v", rep)
	}

	h.c.mu.Lock()
	h.c.Book(tapeCh, &exchange.BookUpdate{
		Snapshot: true, ExchTsNs: 80,
		Bids: []exchange.Level{{Price: 100, Qty: 3}},
		Asks: []exchange.Level{{Price: 101, Qty: 4}},
	})
	h.c.mu.Unlock()
	if rep, _ := h.ask("orderbook_delta", nil); rep.GetError() != "" {
		t.Fatalf("resynced %+v", rep)
	}
	h.down()
	if rep, _ := h.ask("orderbook_delta", nil); rep.GetError() != "not_synced" {
		t.Fatalf("down %+v", rep)
	}
}

func TestTapePageTakesNoConnLock(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.up(1)
	for i := 0; i < 100; i++ {
		h.trade(fmt.Sprintf("1/%d", i+1))
	}
	h.c.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		rep, ok := h.ask("trade", &mdv1.ViewRequest{Limit: 5000})
		if !ok || rep.GetError() != "" || len(rep.GetTape().GetEvents()) != 100 {
			t.Errorf("page %+v ok %v", rep, ok)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("md.view blocked on the conn lock")
	}
	h.c.mu.Unlock()
}

func TestTapeReadRace(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{max: 200})
	h.want("trade")
	h.up(3)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rep, ok := h.ask("trade", &mdv1.ViewRequest{Limit: 40})
				if !ok {
					t.Error("lost responder")
					return
				}
				if err := consistent(rep); err != nil {
					t.Error(err)
					return
				}
				if rep.GetTape().GetMore() {
					older, ok := h.ask("trade", &mdv1.ViewRequest{Limit: 40, Before: rep.GetTape().GetNext()})
					if !ok {
						t.Error("lost responder")
						return
					}
					if err := consistent(older); err != nil {
						t.Error(err)
						return
					}
					if err := noOverlap(rep, older); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	for i := 1; i <= 3000; i++ {
		h.c.mu.Lock()
		next := h.c.live.seq["trade."+tapeTicker] + 1
		h.c.Trade(tapeCh, &exchange.Trade{TradeID: fmt.Sprintf("%d/%d", h.c.epoch, next), Price: 1, Qty: 1})
		h.c.mu.Unlock()
	}
	close(stop)
	wg.Wait()
}

func TestPublishNotStalledByTapeRead(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{max: 100_000, budget: 1 << 30, window: time.Hour})
	h.want("trade")
	h.up(1)
	h.c.mu.Lock()
	for i := 1; i <= 100_000; i++ {
		h.c.Trade(tapeCh, &exchange.Trade{TradeID: fmt.Sprintf("1/%d", i), Price: 1, Qty: 1})
	}
	ring := h.c.views["trade."+tapeTicker].ring
	h.c.mu.Unlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					ring.page(nil, 0, false, 5000)
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	var max time.Duration
	for i := 0; i < 100; i++ {
		start := time.Now()
		h.c.mu.Lock()
		h.c.Trade(tapeCh, &exchange.Trade{TradeID: "more", Price: 1, Qty: 1})
		h.c.mu.Unlock()
		if d := time.Since(start); d > max {
			max = d
		}
	}
	close(stop)
	wg.Wait()
	if max > 100*time.Millisecond {
		t.Fatalf("slowest publish %s while reading limit 5000 from 100k", max)
	}
	t.Logf("slowest publish %s", max)
}

type tapeBoxOptions struct {
	window time.Duration
	max    int
	budget int64
	depth  uint32
}

type tapeBox struct {
	t     *testing.T
	c     *Conn
	clock atomic.Int64
	mu    sync.Mutex
	subs  map[string]func(string, []byte)
	wait  map[string]chan []byte
	n     atomic.Int64
}

func newTapeBox(t *testing.T, opt tapeBoxOptions) *tapeBox {
	t.Helper()
	h := &tapeBox{t: t, subs: map[string]func(string, []byte){}, wait: map[string]chan []byte{}}
	h.clock.Store(1)
	h.c = New(Config{
		Venue: "Binance", Route: "public", Instance: "md",
		Market: &reconMarket{}, BookDepth: opt.depth,
		Symbols:       stubSymbols{info: tapeInfo(symbol.Tradable)},
		TapeWindow:    opt.window,
		TapeMaxEvents: opt.max,
		TapeBudget:    opt.budget,
		Now:           func() time.Time { return time.Unix(0, h.clock.Load()) },
		Publish:       h.publish,
		Subscribe:     h.subscribe,
	})
	return h
}

func tapeInfo(status symbol.Status) *symbol.Info {
	return &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: "Binance", Category: symbol.Spot, Symbol: "BTCUSDT"},
		ExchTicker: "BTCUSDT", Status: status,
		Filters: symbol.Filters{symbol.PriceTick: 0.1},
	}
}

func (h *tapeBox) want(topics ...string) {
	h.t.Helper()
	feeds := make([]string, len(topics))
	for i, topic := range topics {
		feeds[i] = topic + "." + tapeTicker
	}
	h.c.SetDesired(feedsDesired(1, feeds...))
}

func (h *tapeBox) up(epoch uint64) {
	h.c.mu.Lock()
	h.c.epoch = epoch
	h.c.live = newLink(nil)
	h.c.onUpLocked()
	h.c.mu.Unlock()
}

func (h *tapeBox) down() {
	h.c.mu.Lock()
	h.c.onDownLocked()
	h.c.live = nil
	h.c.mu.Unlock()
}

func (h *tapeBox) at(ns int64) { h.clock.Store(ns) }

func (h *tapeBox) trade(id string) {
	h.c.mu.Lock()
	h.c.Trade(tapeCh, &exchange.Trade{TradeID: id, Price: 1, Qty: 1})
	h.c.mu.Unlock()
}

func (h *tapeBox) agg(id string) {
	h.c.mu.Lock()
	h.c.AggTrade(tapeCh, &exchange.AggTrade{TradeID: id, Price: 1, Qty: 1})
	h.c.mu.Unlock()
}

func (h *tapeBox) liq(id string) {
	h.c.mu.Lock()
	h.c.Liquidation(tapeCh, &exchange.Liquidation{Price: 1, Qty: 1})
	h.c.mu.Unlock()
}

func (h *tapeBox) subscribe(subject string, handler func(string, []byte)) (func(), error) {
	h.mu.Lock()
	h.subs[subject] = handler
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		delete(h.subs, subject)
		h.mu.Unlock()
	}, nil
}

func (h *tapeBox) publish(subject string, msg []byte) error {
	if !strings.HasPrefix(subject, "_INBOX_") {
		return nil
	}
	h.mu.Lock()
	ch := h.wait[subject]
	delete(h.wait, subject)
	h.mu.Unlock()
	if ch != nil {
		ch <- append([]byte(nil), msg...)
	}
	return nil
}

func (h *tapeBox) ask(topic string, req *mdv1.ViewRequest) (*mdv1.ViewReply, bool) {
	h.t.Helper()
	if req == nil {
		req = &mdv1.ViewRequest{}
	}
	h.mu.Lock()
	fn := h.subs[viewSubject("md", topic, tapeTicker)]
	h.mu.Unlock()
	if fn == nil {
		return nil, false
	}
	body, err := proto.Marshal(req)
	if err != nil {
		h.t.Fatal(err)
	}
	inbox := fmt.Sprintf("_INBOX_%d", h.n.Add(1))
	ch := make(chan []byte, 1)
	h.mu.Lock()
	h.wait[inbox] = ch
	h.mu.Unlock()
	fn(inbox, body)
	select {
	case got := <-ch:
		rep := &mdv1.ViewReply{}
		if err := proto.Unmarshal(got, rep); err != nil {
			h.t.Fatal(err)
		}
		return rep, true
	case <-time.After(5 * time.Second):
		h.t.Fatal("md.view timed out")
		return nil, false
	}
}

func tapeSeqs(t *testing.T, rep *mdv1.ViewReply) []string {
	t.Helper()
	if rep.GetError() != "" {
		t.Fatalf("error %s", rep.GetError())
	}
	return seqsOf(rep)
}

func seqsOf(rep *mdv1.ViewReply) []string {
	var out []string
	for _, ev := range rep.GetTape().GetEvents() {
		out = append(out, fmt.Sprintf("%d", ev.GetSeq()))
	}
	return out
}

func consistent(rep *mdv1.ViewReply) error {
	if rep.GetError() != "" {
		return fmt.Errorf("error %s", rep.GetError())
	}
	tape := rep.GetTape()
	var prevEpoch, prevSeq uint64
	var have bool
	seen := map[[2]uint64]struct{}{}
	for _, ev := range tape.GetEvents() {
		id := fmt.Sprintf("%d/%d", ev.GetConnEpoch(), ev.GetSeq())
		if ev.GetEvent().GetTrade().GetTradeId() != id {
			return fmt.Errorf("trade %q at %s", ev.GetEvent().GetTrade().GetTradeId(), id)
		}
		key := [2]uint64{ev.GetConnEpoch(), ev.GetSeq()}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate %s", id)
		}
		seen[key] = struct{}{}
		if have && !cursorLess(ev.GetConnEpoch(), ev.GetSeq(), prevEpoch, prevSeq) {
			return fmt.Errorf("order %d/%d then %s", prevEpoch, prevSeq, id)
		}
		if cursorLess(tape.GetHead().GetConnEpoch(), tape.GetHead().GetSeq(), ev.GetConnEpoch(), ev.GetSeq()) {
			return fmt.Errorf("event %s after head %d/%d", id, tape.GetHead().GetConnEpoch(), tape.GetHead().GetSeq())
		}
		prevEpoch, prevSeq = ev.GetConnEpoch(), ev.GetSeq()
		have = true
	}
	return nil
}

func noOverlap(a, b *mdv1.ViewReply) error {
	if a.GetTape().GetNext() == nil {
		return fmt.Errorf("missing next")
	}
	next := a.GetTape().GetNext()
	seen := map[[2]uint64]struct{}{}
	for _, ev := range a.GetTape().GetEvents() {
		seen[[2]uint64{ev.GetConnEpoch(), ev.GetSeq()}] = struct{}{}
	}
	for _, ev := range b.GetTape().GetEvents() {
		if _, ok := seen[[2]uint64{ev.GetConnEpoch(), ev.GetSeq()}]; ok {
			return fmt.Errorf("overlap %d/%d", ev.GetConnEpoch(), ev.GetSeq())
		}
		if !cursorLess(ev.GetConnEpoch(), ev.GetSeq(), next.GetConnEpoch(), next.GetSeq()) {
			return fmt.Errorf("page2 %d/%d is not before %d/%d", ev.GetConnEpoch(), ev.GetSeq(), next.GetConnEpoch(), next.GetSeq())
		}
	}
	return nil
}
