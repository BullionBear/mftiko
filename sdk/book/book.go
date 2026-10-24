package book

import (
	"errors"
	"math"
)

var (
	errN    = errors.New("book: n must be a power of two")
	errTick = errors.New("book: tick size must be positive")
)

// DefaultN is the in-window ring length used when a caller does not choose one (E04-D030).
// It is not a venue snapshot depth.
const DefaultN = 2048

// stackN is how many levels Apply orders without a heap allocation.
// A snapshot is larger than this and allocates; an increment is not.
const stackN = 32

// Level is one price update. Size 0 deletes the level.
type Level struct {
	Price float64
	Size  float64
}

// Slot is one ring entry. There is no validity flag: a read is empty when Tick is not the
// requested tick, and Size 0 is a delete (E04-D030). Slots are never cleared.
type Slot struct {
	Tick int64
	Size float64
}

// OrderBook is one instrument's book. Bids and asks each have their own ring and best tick.
type OrderBook struct {
	bids, asks side
	tickSize   float64
}

type side struct {
	slots []Slot
	best  int64
	n     int
	mask  int64
}

// Tick is round(price/tickSize), half away from zero.
func Tick(price, tickSize float64) int64 {
	return int64(math.Round(price / tickSize))
}

// New builds a book of n slots per side. n must be a power of two and at least 1.
// tickSize must be positive.
func New(n int, tickSize float64) (*OrderBook, error) {
	if n < 1 || n&(n-1) != 0 {
		return nil, errN
	}
	if !(tickSize > 0) || math.IsNaN(tickSize) || math.IsInf(tickSize, 0) {
		return nil, errTick
	}
	return &OrderBook{bids: newSide(n), asks: newSide(n), tickSize: tickSize}, nil
}

// Window is the ring length, the most levels one side can hold.
func (b *OrderBook) Window() int {
	if b == nil {
		return 0
	}
	return b.bids.n
}

// TickSize is the price grid the book was built with.
func (b *OrderBook) TickSize() float64 {
	if b == nil {
		return 0
	}
	return b.tickSize
}

// Price is the grid price of a tick.
func (b *OrderBook) Price(tick int64) float64 {
	return float64(tick) * b.tickSize
}

// BestBidTick is the best bid tick. ok is false when that side has no level.
func (b *OrderBook) BestBidTick() (tick int64, ok bool) {
	if b == nil || !b.bids.hasBest() {
		return 0, false
	}
	return b.bids.best, true
}

// BestAskTick is the best ask tick. ok is false when that side has no level.
func (b *OrderBook) BestAskTick() (tick int64, ok bool) {
	if b == nil || !b.asks.hasBest() {
		return 0, false
	}
	return b.asks.best, true
}

// Qty is the size at price, if that tick is inside the current window and the slot holds it.
func (b *OrderBook) Qty(bid bool, price float64) (float64, bool) {
	if b == nil {
		return 0, false
	}
	if bid {
		return b.bids.qty(Tick(price, b.tickSize), true)
	}
	return b.asks.qty(Tick(price, b.tickSize), false)
}

// Apply writes one increment. Each side is applied from its own best price first, and each
// level is kept or dropped against the best tick at that moment (E04-D031, E04-D032). The slice
// order is left as the caller passed it.
func (b *OrderBook) Apply(bids, asks []Level) {
	if b == nil {
		return
	}
	b.bids.apply(bids, b.tickSize, true)
	b.asks.apply(asks, b.tickSize, false)
}

// Bids walks bids from the best outward, at most limit occupied levels, and at most one window.
func (b *OrderBook) Bids(limit int) Iter {
	if b == nil {
		return Iter{}
	}
	return b.bids.iter(limit, true)
}

// Asks walks asks from the best outward, at most limit occupied levels, and at most one window.
func (b *OrderBook) Asks(limit int) Iter {
	if b == nil {
		return Iter{}
	}
	return b.asks.iter(limit, false)
}

// Iter yields occupied levels. Next does not allocate.
type Iter struct {
	slots []Slot
	mask  int64
	tick  int64
	step  int64
	need  int
	span  int
}

// Next returns the next occupied level. ok is false when the walk is finished.
func (it *Iter) Next() (tick int64, size float64, ok bool) {
	for it.need > 0 && it.span > 0 {
		sl := it.slots[it.tick&it.mask]
		t := it.tick
		it.tick += it.step
		it.span--
		if sl.Tick == t && sl.Size > 0 {
			it.need--
			return t, sl.Size, true
		}
	}
	return 0, 0, false
}

func newSide(n int) side {
	return side{slots: make([]Slot, n), n: n, mask: int64(n - 1)}
}

func (s *side) hasBest() bool {
	sl := &s.slots[s.best&s.mask]
	return sl.Tick == s.best && sl.Size > 0
}

func (s *side) write(tick int64, size float64) {
	sl := &s.slots[tick&s.mask]
	sl.Tick = tick
	sl.Size = size
}

func (s *side) apply(levels []Level, tickSize float64, bid bool) {
	n := len(levels)
	if n == 0 {
		return
	}
	if n > stackN {
		s.applyN(levels, tickSize, bid)
		return
	}
	var ticks [stackN]int64
	var idx [stackN]int
	for i := 0; i < n; i++ {
		ticks[i] = Tick(levels[i].Price, tickSize)
		idx[i] = i
	}
	for i := 1; i < n; i++ {
		v := idx[i]
		j := i
		for j > 0 && worse(bid, ticks[idx[j-1]], ticks[v]) {
			idx[j] = idx[j-1]
			j--
		}
		idx[j] = v
	}
	for k := 0; k < n; k++ {
		i := idx[k]
		s.applyTick(ticks[i], levels[i].Size, bid)
	}
}

func (s *side) applyN(levels []Level, tickSize float64, bid bool) {
	n := len(levels)
	ticks := make([]int64, n)
	idx := make([]int, n)
	for i := range levels {
		ticks[i] = Tick(levels[i].Price, tickSize)
		idx[i] = i
	}
	for i := 1; i < n; i++ {
		v := idx[i]
		j := i
		for j > 0 && worse(bid, ticks[idx[j-1]], ticks[v]) {
			idx[j] = idx[j-1]
			j--
		}
		idx[j] = v
	}
	for _, i := range idx {
		s.applyTick(ticks[i], levels[i].Size, bid)
	}
}

// applyTick stores one level against the best tick as it stands now.
// A price better than the best moves the best. A price N or more ticks worse is outside
// the ring and is not written: that tick shares an index with the best.
func (s *side) applyTick(tick int64, size float64, bid bool) {
	if size < 0 || math.IsNaN(size) || math.IsInf(size, 0) {
		size = 0
	}
	if !s.hasBest() {
		if size == 0 {
			return
		}
		s.write(tick, size)
		s.best = tick
		return
	}
	if better(bid, tick, s.best) {
		if size == 0 {
			return
		}
		s.write(tick, size)
		s.best = tick
		return
	}
	if tick != s.best {
		dist := s.best - tick
		if !bid {
			dist = tick - s.best
		}
		if dist >= int64(s.n) {
			return
		}
	}
	s.write(tick, size)
	if tick == s.best && size == 0 {
		s.retreat(bid)
	}
}

// retreat finds the next occupied tick after the best was deleted.
func (s *side) retreat(bid bool) {
	step := int64(-1)
	if !bid {
		step = 1
	}
	t := s.best + step
	for i := 1; i < s.n; i++ {
		sl := &s.slots[t&s.mask]
		if sl.Tick == t && sl.Size > 0 {
			s.best = t
			return
		}
		t += step
	}
}

func (s *side) qty(tick int64, bid bool) (float64, bool) {
	if !s.hasBest() || better(bid, tick, s.best) {
		return 0, false
	}
	dist := s.best - tick
	if !bid {
		dist = tick - s.best
	}
	if dist >= int64(s.n) {
		return 0, false
	}
	sl := &s.slots[tick&s.mask]
	if sl.Tick != tick || sl.Size <= 0 {
		return 0, false
	}
	return sl.Size, true
}

// keeps reports whether applyTick would write this level against the best as it stands now.
// It does not look ahead at earlier levels of the same update.
func (s *side) keeps(tick int64, size float64, bid bool) bool {
	if !s.hasBest() {
		return size > 0
	}
	if better(bid, tick, s.best) {
		return size > 0
	}
	if tick == s.best {
		return true
	}
	dist := s.best - tick
	if !bid {
		dist = tick - s.best
	}
	return dist < int64(s.n)
}

func (s *side) iter(limit int, bid bool) Iter {
	if !s.hasBest() || limit <= 0 {
		return Iter{}
	}
	if limit > s.n {
		limit = s.n
	}
	step := int64(-1)
	if !bid {
		step = 1
	}
	return Iter{slots: s.slots, mask: s.mask, tick: s.best, step: step, need: limit, span: s.n}
}

func better(bid bool, tick, best int64) bool {
	if bid {
		return tick > best
	}
	return tick < best
}

// worse orders one update so the better price is applied first (E04-D031).
func worse(bid bool, a, b int64) bool {
	return better(bid, b, a)
}
