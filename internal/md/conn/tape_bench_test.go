package conn

import (
	"fmt"
	"runtime"
	"sort"
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
	readers, restore := publishBenchReaders()
	defer restore()
	b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
		benchPublish(b, readers)
	})
}

// publishBenchReaders leaves the publisher a core and one core idle:
// readers <= GOMAXPROCS-2, and never more than two. A machine with fewer
// than three cores is raised to 3 for the duration of the returned restore.
func publishBenchReaders() (readers int, restore func()) {
	procs := runtime.GOMAXPROCS(0)
	readers = procs - 2
	if readers > 2 {
		readers = 2
	}
	if readers >= 1 {
		return readers, func() {}
	}
	prev := runtime.GOMAXPROCS(3)
	return 1, func() { runtime.GOMAXPROCS(prev) }
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
	samples := make([]time.Duration, b.N)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cpu0 := threadCPU()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		c.mu.Lock()
		c.Trade(tapeCh, &exchange.Trade{TradeID: strconv.Itoa(i), Price: 1, Qty: 1})
		c.mu.Unlock()
		samples[i] = time.Since(start)
	}
	b.StopTimer()
	if cpu1 := threadCPU(); b.N > 0 && cpu1 > cpu0 {
		b.ReportMetric(float64(cpu1-cpu0)/float64(b.N), "cpu-ns/op")
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	b.ReportMetric(float64(durationPercentile(samples, 0.50)), "p50-ns")
	b.ReportMetric(float64(durationPercentile(samples, 0.99)), "p99-ns")
}

func durationPercentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	i := int(float64(len(samples)-1) * p)
	return samples[i]
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
