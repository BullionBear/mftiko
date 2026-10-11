package book

import (
	"math"
	"testing"
)

func newBook(t *testing.T, n int, tick float64) *OrderBook {
	t.Helper()
	b, err := New(n, tick)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNewRejects(t *testing.T) {
	if _, err := New(3, 1); err == nil {
		t.Fatal("n")
	}
	if _, err := New(0, 1); err == nil {
		t.Fatal("zero n")
	}
	if _, err := New(4, 0); err == nil {
		t.Fatal("tick")
	}
	if _, err := New(4, math.NaN()); err == nil {
		t.Fatal("nan")
	}
}

func TestTickRound(t *testing.T) {
	if got := Tick(0.05, 0.1); got != 1 {
		t.Fatal(got)
	}
	if got := Tick(-0.05, 0.1); got != -1 {
		t.Fatal(got)
	}
	if got := Tick(100, 0.01); got != 10000 {
		t.Fatal(got)
	}
}

func TestOutOfWindowDrop(t *testing.T) {
	b := newBook(t, 8, 1)
	b.Apply([]Level{{Price: 100, Size: 1}, {Price: 93, Size: 2}}, nil)
	if q, ok := b.Qty(true, 93); !ok || q != 2 {
		t.Fatalf("inside %+v %v", q, ok)
	}
	// 100-8 is N ticks worse and shares the best's index.
	b.Apply([]Level{{Price: 92, Size: 9}}, nil)
	if _, ok := b.Qty(true, 92); ok {
		t.Fatal("outside level stored")
	}
	if q, ok := b.Qty(true, 100); !ok || q != 1 {
		t.Fatal("best overwritten")
	}
	best, ok := b.BestBidTick()
	if !ok || best != 100 {
		t.Fatal(best, ok)
	}
}

func TestBestMoves(t *testing.T) {
	b := newBook(t, 8, 1)
	b.Apply([]Level{{Price: 100, Size: 1}}, []Level{{Price: 110, Size: 1}})
	b.Apply([]Level{{Price: 105, Size: 4}}, []Level{{Price: 108, Size: 3}})
	bid, ok := b.BestBidTick()
	if !ok || bid != 105 {
		t.Fatal("bid", bid, ok)
	}
	ask, ok := b.BestAskTick()
	if !ok || ask != 108 {
		t.Fatal("ask", ask, ok)
	}
	if q, ok := b.Qty(true, 105); !ok || q != 4 {
		t.Fatal(q, ok)
	}
	if q, ok := b.Qty(false, 108); !ok || q != 3 {
		t.Fatal(q, ok)
	}
}

func TestWrappedSlotIsNotMisread(t *testing.T) {
	b := newBook(t, 8, 1)
	// Tick -3 lives at index 5. A later best at 6 puts tick 5, the same index, inside the window.
	b.Apply([]Level{{Price: 0, Size: 1}, {Price: -3, Size: 7}}, nil)
	b.Apply([]Level{{Price: 6, Size: 2}}, nil)
	if _, ok := b.Qty(true, 5); ok {
		t.Fatal("stale tick -3 was read as tick 5")
	}
	if q, ok := b.Qty(true, -3); ok {
		t.Fatalf("tick -3 is outside the new window, qty %v", q)
	}
	var got []int64
	it := b.Bids(b.Window())
	for {
		tick, _, ok := it.Next()
		if !ok {
			break
		}
		got = append(got, tick)
		if tick == -3 {
			t.Fatal("iterator yielded the stale tick")
		}
	}
	if len(got) == 0 || got[0] != 6 {
		t.Fatal(got)
	}
	for _, tick := range got {
		if tick == 5 {
			t.Fatal(got)
		}
	}
}

func TestScanAfterBestDeleted(t *testing.T) {
	b := newBook(t, 8, 1)
	b.Apply([]Level{{Price: 100, Size: 1}, {Price: 99, Size: 2}, {Price: 97, Size: 3}}, nil)
	b.Apply([]Level{{Price: 100, Size: 0}}, nil)
	best, ok := b.BestBidTick()
	if !ok || best != 99 {
		t.Fatal("next", best, ok)
	}
	b.Apply([]Level{{Price: 99, Size: 0}}, nil)
	best, ok = b.BestBidTick()
	if !ok || best != 97 {
		t.Fatal("skip empty", best, ok)
	}
	if _, ok := b.Qty(true, 98); ok {
		t.Fatal("gap is a level")
	}
	b.Apply([]Level{{Price: 97, Size: 0}}, nil)
	if _, ok := b.BestBidTick(); ok {
		t.Fatal("side still has a best")
	}
}

func TestBestSideFirstKeepsLevelThatEnters(t *testing.T) {
	b := newBook(t, 8, 1)
	b.Apply([]Level{{Price: 100, Size: 1}, {Price: 99, Size: 2}}, nil)
	// 92 is 8 ticks under 100, so it is outside until 100 is deleted. The delete is the
	// better price and is applied first; 92 is then one tick inside the new best.
	b.Apply([]Level{{Price: 92, Size: 4}, {Price: 100, Size: 0}}, nil)
	best, ok := b.BestBidTick()
	if !ok || best != 99 {
		t.Fatal(best, ok)
	}
	if q, ok := b.Qty(true, 92); !ok || q != 4 {
		t.Fatalf("level that entered the window: %v %v", q, ok)
	}
}

func TestDroppedLevelStaysGoneUntilRewritten(t *testing.T) {
	b := newBook(t, 8, 1)
	b.Apply([]Level{{Price: 100, Size: 1}, {Price: 99, Size: 2}}, nil)
	b.Apply([]Level{{Price: 92, Size: 4}}, nil)
	if _, ok := b.Qty(true, 92); ok {
		t.Fatal("stored while outside")
	}
	b.Apply([]Level{{Price: 100, Size: 0}}, nil)
	if _, ok := b.Qty(true, 92); ok {
		t.Fatal("window slide resurrected a dropped level")
	}
	b.Apply([]Level{{Price: 92, Size: 4}}, nil)
	if q, ok := b.Qty(true, 92); !ok || q != 4 {
		t.Fatal(q, ok)
	}
}

func TestAskWindow(t *testing.T) {
	b := newBook(t, 8, 1)
	b.Apply(nil, []Level{{Price: 10, Size: 1}, {Price: 17, Size: 2}})
	b.Apply(nil, []Level{{Price: 18, Size: 3}})
	if _, ok := b.Qty(false, 18); ok {
		t.Fatal("ask outside stored")
	}
	b.Apply(nil, []Level{{Price: 9, Size: 4}})
	best, ok := b.BestAskTick()
	if !ok || best != 9 {
		t.Fatal(best, ok)
	}
}

func TestIterTopAndAllocs(t *testing.T) {
	b := newBook(t, 16, 1)
	var lv []Level
	for i := 0; i < 10; i++ {
		lv = append(lv, Level{Price: float64(100 - i), Size: float64(i + 1)})
	}
	b.Apply(lv, nil)
	var got []int64
	it := b.Bids(3)
	for {
		tick, _, ok := it.Next()
		if !ok {
			break
		}
		got = append(got, tick)
	}
	if len(got) != 3 || got[0] != 100 || got[1] != 99 || got[2] != 98 {
		t.Fatal(got)
	}
	allocs := testing.AllocsPerRun(50, func() {
		it := b.Bids(3)
		for {
			_, _, ok := it.Next()
			if !ok {
				return
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("iter allocs %v", allocs)
	}
	inc := []Level{{Price: 100, Size: 2}, {Price: 97, Size: 1}}
	allocs = testing.AllocsPerRun(50, func() {
		b.Apply(inc, nil)
	})
	if allocs != 0 {
		t.Fatalf("apply allocs %v", allocs)
	}
}

func TestAffectsTop(t *testing.T) {
	b := newBook(t, 16, 1)
	b.Apply([]Level{{Price: 100, Size: 1}, {Price: 99, Size: 1}, {Price: 90, Size: 1}}, nil)
	if b.AffectsTop([]Level{{Price: 90, Size: 2}}, nil, 1) {
		t.Fatal("deep level touched top-1")
	}
	if !b.AffectsTop([]Level{{Price: 100, Size: 2}}, nil, 1) {
		t.Fatal("best did not touch")
	}
	if !b.AffectsTop([]Level{{Price: 99, Size: 0}}, nil, 2) {
		t.Fatal("deleting the nth did not touch")
	}
	if b.AffectsTop([]Level{{Price: 80, Size: 1}}, nil, 2) {
		t.Fatal("outside the top and the side is already full")
	}
}
