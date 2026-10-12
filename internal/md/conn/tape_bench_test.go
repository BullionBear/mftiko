package conn

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

func BenchmarkTapePublishQuiet(b *testing.B) {
	benchPublish(b, 0)
}

func BenchmarkTapePublishWhileReading(b *testing.B) {
	benchPublish(b, 4)
}

func BenchmarkTapePage(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		_, ring := fillTape(b, n)
		for _, limit := range []int{100, 1000, 5000} {
			b.Run(fmt.Sprintf("n=%d/limit=%d", n, limit), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					ring.page(nil, 0, false, limit)
				}
			})
		}
	}
}

func benchPublish(b *testing.B, readers int) {
	c, ring := fillTape(b, 100_000)
	if readers > 0 {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for range readers {
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
		b.Cleanup(func() {
			close(stop)
			wg.Wait()
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.mu.Lock()
		c.Trade(tapeCh, &exchange.Trade{TradeID: strconv.Itoa(i), Price: 1, Qty: 1})
		c.mu.Unlock()
	}
}

func fillTape(b *testing.B, n int) (*Conn, *tapeRing) {
	b.Helper()
	c := New(Config{
		Venue: "Binance", Route: "public", Instance: "md",
		Market: &reconMarket{}, Symbols: stubSymbols{info: tapeInfo(symbol.Tradable)},
		TapeMaxEvents: 100_000, TapeBudget: 1 << 30, TapeWindow: time.Hour,
		Publish: func(string, []byte) error { return nil },
	})
	c.SetDesired(feedsDesired(1, "trade."+tapeTicker))
	c.mu.Lock()
	c.epoch = 1
	c.live = newLink(nil)
	c.onUpLocked()
	for i := 1; i <= n; i++ {
		c.Trade(tapeCh, &exchange.Trade{TradeID: strconv.Itoa(i), Price: 1, Qty: 1})
	}
	ring := c.views["trade."+tapeTicker].ring
	c.mu.Unlock()
	if ring == nil || ring.events != n {
		b.Fatalf("ring events %v", ring)
	}
	return c, ring
}
