package book

import "testing"

func seedPrices(n int) []Level {
	ls := make([]Level, n)
	for i := range ls {
		ls[i] = Level{Price: float64(10_000 - i), Size: 1}
	}
	return ls
}

func BenchmarkIncrement(b *testing.B) {
	const levels = 1000
	seed := seedPrices(levels)
	inc := []Level{
		{Price: 10000, Size: 2},
		{Price: 9999, Size: 3},
		{Price: 9990, Size: 4},
		{Price: 9600, Size: 5},
	}
	ob, err := New(DefaultN, 1)
	if err != nil {
		b.Fatal(err)
	}
	ob.Apply(seed, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inc[0].Size = float64(i&1) + 1
		ob.Apply(inc, nil)
	}
	tick, _ := ob.BestBidTick()
	benchSink += tick
}

func BenchmarkTopN(b *testing.B) {
	const levels = 1000
	const top = 20
	seed := seedPrices(levels)
	ob, err := New(DefaultN, 1)
	if err != nil {
		b.Fatal(err)
	}
	ob.Apply(seed, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := ob.Bids(top)
		for {
			tick, sz, ok := it.Next()
			if !ok {
				break
			}
			benchSink += tick + int64(sz)
		}
	}
}

var benchSink int64
