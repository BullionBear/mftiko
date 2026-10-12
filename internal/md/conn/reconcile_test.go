package conn

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

func TestReconcileDerivesTheAtomDiff(t *testing.T) {
	ticker := symbol.UniversalTicker{Venue: "Binance", Category: symbol.Spot, Symbol: "BTCUSDT"}
	info := &symbol.Info{
		Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Tradable,
		Filters: symbol.Filters{symbol.PriceTick: 0.1},
	}
	c := New(Config{
		Venue: "Binance", Route: "public", Instance: "md",
		Market:  &reconMarket{},
		Symbols: stubSymbols{info: info},
	})
	delta := "orderbook_delta.Binance_Spot_BTCUSDT"
	trade := "trade.Binance_Spot_BTCUSDT"
	ch := "btcusdt@depth"
	c.SetDesired(feedsDesired(1, delta, trade))
	if len(c.wanted[ch]) != 2 || c.exch[ch] != "BTCUSDT" || c.tick[ch] != 0.1 {
		t.Fatalf("derived %+v exch %v tick %v", c.wanted, c.exch, c.tick)
	}

	sess := &memSession{}
	c.mu.Lock()
	c.live = newLink(sess)
	c.mu.Unlock()
	if frames := c.plan(); len(frames) != 1 || len(sess.sub) != 1 || len(sess.sub[0]) != 1 || sess.sub[0][0] != ch {
		t.Fatalf("subscribe %+v frames %d", sess.sub, len(frames))
	}
	c.mu.Lock()
	for id := range c.live.subInflight {
		c.Ack(id, nil)
	}
	c.mu.Unlock()

	c.SetDesired(feedsDesired(2, delta))
	if len(c.wanted[ch]) != 1 {
		t.Fatalf("shared atom dropped %+v", c.wanted)
	}
	if frames := c.plan(); len(frames) != 0 || len(sess.unsub) != 0 {
		t.Fatalf("unsubscribed a channel that still has a feed: %+v", sess.unsub)
	}

	c.symbols = stubSymbols{err: errors.New("sym down")}
	c.SetDesired(feedsDesired(3, delta))
	if len(c.wanted[ch]) != 1 || c.exch[ch] != "BTCUSDT" {
		t.Fatalf("transient lookup dropped the subscription %+v %+v", c.wanted, c.exch)
	}

	c.symbols = stubSymbols{info: &symbol.Info{Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Halted}}
	c.SetDesired(feedsDesired(4, delta))
	if len(c.wanted) != 0 {
		t.Fatalf("halted stayed subscribed %+v", c.wanted)
	}
	if frames := c.plan(); len(frames) != 1 || len(sess.unsub) != 1 || sess.unsub[0][0] != ch {
		t.Fatalf("unsubscribe %+v", sess.unsub)
	}
}

func TestReconcileFollowsTheTable(t *testing.T) {
	ticker := symbol.UniversalTicker{Venue: "Binance", Category: symbol.Spot, Symbol: "BTCUSDT"}
	tab := &switchSymbols{cur: stubSymbols{info: &symbol.Info{
		Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Tradable,
		Filters: symbol.Filters{symbol.PriceTick: 0.1},
	}}}
	c := New(Config{
		Venue: "Binance", Route: "public", Instance: "md",
		Market: &reconMarket{}, Symbols: tab,
	})
	c.reconEvery = 15 * time.Millisecond
	c.SetDesired(feedsDesired(1, "orderbook_delta.Binance_Spot_BTCUSDT"))
	if c.exch["btcusdt@depth"] != "BTCUSDT" {
		t.Fatalf("before %+v", c.exch)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.reconcile(ctx)

	tab.mu.Lock()
	tab.cur = stubSymbols{info: &symbol.Info{Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Halted}}
	tab.mu.Unlock()

	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		n := len(c.wanted)
		c.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("table change did not unsubscribe")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestFeedReport(t *testing.T) {
	ticker := symbol.UniversalTicker{Venue: "Binance", Category: symbol.Spot, Symbol: "BTCUSDT"}
	book := "orderbook_delta.Binance_Spot_BTCUSDT"
	retired := "orderbook.Binance_Spot_BTCUSDT"
	missing := "trade.Binance_Spot_NOSUCHCOIN"
	var mu sync.Mutex
	var got []*mdctlv1.Report
	c := New(Config{
		Venue: "Binance", Route: "public", Instance: "md",
		Market: &reconMarket{},
		Symbols: stubSymbols{info: &symbol.Info{
			Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Tradable,
			Filters: symbol.Filters{symbol.PriceTick: 0.1},
		}},
		Report: func(rep *mdctlv1.Report) error {
			mu.Lock()
			got = append(got, proto.Clone(rep).(*mdctlv1.Report))
			mu.Unlock()
			return nil
		},
	})
	c.SetDesired(feedsDesired(1, book, missing, retired))
	mu.Lock()
	if len(got) != 1 {
		t.Fatalf("reports %d", len(got))
	}
	byFeed := map[string]*mdctlv1.FeedStatus{}
	for _, f := range got[0].GetFeeds() {
		byFeed[f.GetFeed()] = f
	}
	mu.Unlock()
	if byFeed[book].GetState() != mdctlv1.FeedState_FEED_STATE_PENDING || byFeed[book].GetReason() != "" {
		t.Fatalf("book %+v", byFeed[book])
	}
	if byFeed[missing].GetState() != mdctlv1.FeedState_FEED_STATE_REJECTED || byFeed[missing].GetReason() != feedNotListed {
		t.Fatalf("missing %+v", byFeed[missing])
	}
	if byFeed[retired].GetState() != mdctlv1.FeedState_FEED_STATE_REJECTED || byFeed[retired].GetReason() != feedInvalid {
		t.Fatalf("retired %+v", byFeed[retired])
	}

	sess := &memSession{}
	c.mu.Lock()
	c.live = newLink(sess)
	c.epoch = 3
	c.mu.Unlock()
	if frames := c.plan(); len(frames) != 1 {
		t.Fatalf("frames %d", len(frames))
	}
	c.mu.Lock()
	for id := range c.live.subInflight {
		c.Ack(id, nil)
	}
	dirty := c.statusDirty
	c.mu.Unlock()
	if !dirty {
		t.Fatal("ack did not ask for a report")
	}
	c.publishStatus()
	mu.Lock()
	last := got[len(got)-1]
	mu.Unlock()
	if last.GetConnEpoch() != 3 || last.GetState() != mdctlv1.ConnState_CONN_STATE_LIVE {
		t.Fatalf("subscribed report %+v", last)
	}
	for _, f := range last.GetFeeds() {
		if f.GetFeed() != book {
			continue
		}
		if f.GetState() != mdctlv1.FeedState_FEED_STATE_SUBSCRIBED {
			t.Fatalf("book after ack %+v", f)
		}
	}

	c.symbols = stubSymbols{info: &symbol.Info{Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Halted}}
	c.SetDesired(feedsDesired(2, book))
	mu.Lock()
	halted := got[len(got)-1]
	mu.Unlock()
	if len(halted.GetFeeds()) != 1 || halted.GetFeeds()[0].GetState() != mdctlv1.FeedState_FEED_STATE_PENDING || halted.GetFeeds()[0].GetReason() != "HALTED" {
		t.Fatalf("halted %+v", halted.GetFeeds())
	}
}

func feedsDesired(seq uint64, feeds ...string) *mdctlv1.Desired {
	return &mdctlv1.Desired{
		Version: &commonv1.Version{Seq: seq},
		Atoms:   []*mdctlv1.AtomAssignment{{Feeds: feeds}},
	}
}

type switchSymbols struct {
	mu  sync.Mutex
	cur stubSymbols
}

func (s *switchSymbols) Get(ctx context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error) {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	return cur.Get(ctx, t, includeInactive)
}

type reconMarket struct{ stubMarket }

func (*reconMarket) Atom(_ exchange.Topic, info *symbol.Info) (exchange.AtomRef, error) {
	if info == nil || info.ExchTicker == "" {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	return exchange.AtomRef{Route: "public", Channel: strings.ToLower(info.ExchTicker) + "@depth"}, nil
}

func (*reconMarket) Book(string) (exchange.BookSync, bool) { return streamBook{}, true }

type streamBook struct{}

func (streamBook) Snapshot() exchange.SnapshotSource { return exchange.SnapshotStream }
func (streamBook) Depth() int                        { return 0 }
func (streamBook) Check(exchange.BookSeq, *exchange.BookUpdate) exchange.BookVerdict {
	return exchange.BookApply
}
