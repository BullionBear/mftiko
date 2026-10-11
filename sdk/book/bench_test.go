package book

import (
	"sort"
	"testing"
)

// The map book is the previous conn implementation: a price map, a full sort, and a full diff.
// Increment/map does that work. Increment/ring only writes the slots. TopN/map sorts every level.
// TopN/ring walks from the best and stops.

type mapLevel struct {
	price float64
	size  float64
}

type mapBook struct {
	bids map[float64]float64
}

func (m *mapBook) apply(ls []Level) {
	for _, l := range ls {
		if l.Size == 0 {
			delete(m.bids, l.Price)
			continue
		}
		m.bids[l.Price] = l.Size
	}
}

func (m *mapBook) sorted() []mapLevel {
	out := make([]mapLevel, 0, len(m.bids))
	for p, q := range m.bids {
		out = append(out, mapLevel{p, q})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].price > out[j].price })
	return out
}

func (m *mapBook) diff(prev map[float64]float64, next []mapLevel) {
	n := make(map[float64]float64, len(next))
	for _, l := range next {
		n[l.price] = l.size
		if prev[l.price] != l.size {
			benchSink += int64(l.price) + int64(l.size)
		}
	}
	for p, q := range prev {
		if _, ok := n[p]; !ok {
			benchSink += int64(p) + int64(q)
		}
	}
}

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
	b.Run("ring", func(b *testing.B) {
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
	})
	b.Run("map", func(b *testing.B) {
		m := &mapBook{bids: map[float64]float64{}}
		m.apply(seed)
		prev := map[float64]float64{}
		for p, q := range m.bids {
			prev[p] = q
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inc[0].Size = float64(i&1) + 1
			m.apply(inc)
			sorted := m.sorted()
			m.diff(prev, sorted)
			prev = make(map[float64]float64, len(sorted))
			for _, l := range sorted {
				prev[l.price] = l.size
			}
		}
	})
}

func BenchmarkTopN(b *testing.B) {
	const levels = 1000
	const top = 20
	seed := seedPrices(levels)
	b.Run("ring", func(b *testing.B) {
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
	})
	b.Run("map", func(b *testing.B) {
		m := &mapBook{bids: map[float64]float64{}}
		m.apply(seed)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sorted := m.sorted()
			n := top
			if n > len(sorted) {
				n = len(sorted)
			}
			for _, l := range sorted[:n] {
				benchSink += int64(l.price) + int64(l.size)
			}
		}
	})
}

var benchSink int64
