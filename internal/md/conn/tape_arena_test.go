package conn

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
)

func TestTapeChunkGeometry(t *testing.T) {
	if unsafe.Sizeof(tapeChunkHead{}) != tapeChunkHeader {
		t.Fatalf("header %d", unsafe.Sizeof(tapeChunkHead{}))
	}
	if unsafe.Sizeof(tapeChunk{}) != tapeChunkSize {
		t.Fatalf("chunk %d", unsafe.Sizeof(tapeChunk{}))
	}
	if n := chunkPointerLen(defaultTapeMaxEvents, defaultTapeBudget); n != int(defaultTapeBudget/tapeChunkSize) {
		t.Fatalf("pointer len %d", n)
	}
}

func TestTapeEventDoesNotSpanChunks(t *testing.T) {
	a := newTapeArena(1 << 20)
	r := a.newRing(100, "trade.k")
	// One record fits. Two do not, so the second starts a chunk and the
	// first chunk's tail stays unused.
	n := (tapeChunkSize - tapeChunkHeader) / 2
	raw := validFeedRaw(t, n)
	window := int64(time.Hour)
	r.appendEvent(1, 1, 1, raw, window)
	r.appendEvent(1, 2, 2, raw, window)
	if r.chunkWritten != 2 || r.events != 2 {
		t.Fatalf("chunks %d events %d", r.chunkWritten, r.events)
	}
	rep := decodeTape(t, r.page(nil, 0, false, 10))
	if strings.Join(seqsOf(rep), ",") != "2,1" {
		t.Fatalf("seqs %+v", seqsOf(rep))
	}
}

func TestTapeOversizeIsAGap(t *testing.T) {
	a := newTapeArena(1 << 20)
	r := a.newRing(100, "trade.k")
	r.setEmptyHead(1)
	window := int64(time.Hour)
	exact := validFeedRaw(t, tapeEventMax)
	over := bytes.Repeat([]byte{'c'}, tapeEventMax+1)
	r.appendEvent(1, 1, 10, exact, window)
	r.appendEvent(1, 2, 20, over, window)
	r.appendEvent(1, 3, 30, []byte{1, 2, 3, 4, 5, 6, 7, 8}, window)

	rep := decodeTape(t, r.page(nil, 0, false, 10))
	if strings.Join(seqsOf(rep), ",") != "3,1" {
		t.Fatalf("seqs %+v", seqsOf(rep))
	}
	gaps := rep.GetTape().GetGaps()
	if len(gaps) != 1 || gaps[0].GetStartNs() != 20 || gaps[0].GetEndNs() != 20 {
		t.Fatalf("gap %+v", gaps)
	}
	if gaps[0].GetLast().GetConnEpoch() != 1 || gaps[0].GetLast().GetSeq() != 1 {
		t.Fatalf("last %+v", gaps[0].GetLast())
	}
	if gaps[0].GetFirst().GetConnEpoch() != 1 || gaps[0].GetFirst().GetSeq() != 3 {
		t.Fatalf("first %+v", gaps[0].GetFirst())
	}
	if rep.GetTape().GetHead().GetSeq() != 3 || rep.GetTape().GetContinuousSinceNs() != 20 {
		t.Fatalf("head %+v continuous %d", rep.GetTape().GetHead(), rep.GetTape().GetContinuousSinceNs())
	}

	// Nothing retained before the gap: no last cursor, and head still moves.
	empty := a.newRing(100, "trade.empty")
	empty.setEmptyHead(4)
	empty.appendEvent(4, 7, 50, over, window)
	rep = decodeTape(t, empty.page(nil, 0, false, 10))
	if len(rep.GetTape().GetEvents()) != 0 || len(rep.GetTape().GetGaps()) != 1 {
		t.Fatalf("empty oversize %+v", rep.GetTape())
	}
	if rep.GetTape().GetGaps()[0].GetLast() != nil {
		t.Fatalf("last set on an empty ring %+v", rep.GetTape().GetGaps()[0])
	}
	if rep.GetTape().GetHead().GetConnEpoch() != 4 || rep.GetTape().GetHead().GetSeq() != 7 {
		t.Fatalf("head %+v", rep.GetTape().GetHead())
	}
}

func TestTapeOversizeTradeIsAGap(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{})
	h.want("trade")
	h.up(1)
	h.at(100)
	h.trade("1/1")
	h.at(200)
	h.trade(strings.Repeat("Z", 40_000))
	h.at(300)
	h.trade("1/3")
	rep, ok := h.ask("trade", nil)
	if !ok {
		t.Fatal("no responder")
	}
	if strings.Join(tapeSeqs(t, rep), ",") != "3,1" {
		t.Fatalf("seqs %+v", tapeSeqs(t, rep))
	}
	gaps := rep.GetTape().GetGaps()
	if len(gaps) != 1 || gaps[0].GetStartNs() != 200 || gaps[0].GetEndNs() != 200 {
		t.Fatalf("gap %+v", gaps)
	}
	if gaps[0].GetLast().GetSeq() != 1 || gaps[0].GetFirst().GetSeq() != 3 {
		t.Fatalf("cursors %+v", gaps[0])
	}
	if rep.GetTape().GetHead().GetConnEpoch() != 1 || rep.GetTape().GetHead().GetSeq() != 3 {
		t.Fatalf("head %+v", rep.GetTape().GetHead())
	}
	if rep.GetTape().GetContinuousSinceNs() != 200 {
		t.Fatalf("continuous %d", rep.GetTape().GetContinuousSinceNs())
	}
}

func TestTapeReclaimsGloballyOldestChunk(t *testing.T) {
	chunk := int64(unsafe.Sizeof(tapeChunk{}))
	a := newTapeArena(4 * chunk)
	older := a.newRing(1000, "aggtrade.old")
	newer := a.newRing(1000, "trade.new")
	if a.used > a.budget {
		t.Fatalf("index used %d budget %d", a.used, a.budget)
	}
	raw := bytes.Repeat([]byte{'q'}, tapeEventMax)
	window := int64(time.Hour)
	older.appendEvent(1, 1, 1, raw, window)
	older.appendEvent(1, 2, 2, raw, window)
	if older.events != 2 || older.chunkWritten < 2 {
		t.Fatalf("setup events %d chunks %d", older.events, older.chunkWritten)
	}
	lost := false
	for i := uint64(1); i <= 8 && !lost; i++ {
		newer.appendEvent(1, i, int64(100+i), raw, window)
		if older.events < 2 {
			lost = true
		}
	}
	if !lost || older.events != 1 {
		t.Fatalf("reclaim lost %v events %d", lost, older.events)
	}
	rec, ok := older.load(older.pub.Load().tail)
	if !ok || rec.seqNo != 2 {
		t.Fatalf("kept %+v %v", rec, ok)
	}
	if newer.events == 0 {
		t.Fatal("newer ring stored nothing")
	}
	// The open chunk of the older ring stays. Flooding the newer ring
	// reclaims sealed chunks and does not take it.
	for i := uint64(0); i < 8; i++ {
		newer.appendEvent(1, 100+i, int64(200+i), raw, window)
	}
	if older.events != 1 {
		t.Fatalf("floor events %d", older.events)
	}
	if a.used > a.budget {
		t.Fatalf("used %d budget %d", a.used, a.budget)
	}
}

func TestTapeIdleRingsCostAboutOneChunk(t *testing.T) {
	const n = 100
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	a := newTapeArena(defaultTapeBudget)
	rings := make([]*tapeRing, n)
	for i := range rings {
		rings[i] = a.newRing(defaultTapeMaxEvents, fmt.Sprintf("trade.Binance_Spot_T%dUSDT", i))
		if rings[i].open != nil {
			t.Fatalf("idle ring %d allocated a chunk", i)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	limit := int64(n) * tapeChunkSize
	if a.used > limit {
		t.Fatalf("used %d limit %d", a.used, limit)
	}
	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if delta > limit {
		t.Fatalf("heap %d limit %d used %d", delta, limit, a.used)
	}
	runtime.KeepAlive(rings)
}

func TestTapeHeapStaysWithinBudget(t *testing.T) {
	const budget = 2 << 20
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	rings := fillArena(budget)
	a := rings[0].arena
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if a.used > budget {
		t.Fatalf("used %d budget %d", a.used, budget)
	}
	if a.used+int64(unsafe.Sizeof(tapeChunk{})) <= budget && len(a.free) == 0 {
		t.Fatal("budget was not reached")
	}
	kept := 0
	for _, r := range rings {
		kept += r.events
	}
	if kept == 0 {
		t.Fatal("no events retained")
	}
	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if delta > budget {
		t.Fatalf("heap %d budget %d used %d", delta, budget, a.used)
	}
	runtime.KeepAlive(rings)
}

func fillArena(budget int64) []*tapeRing {
	a := newTapeArena(budget)
	rings := make([]*tapeRing, 4)
	for i := range rings {
		rings[i] = a.newRing(10_000, fmt.Sprintf("trade.f%d", i))
	}
	raw := bytes.Repeat([]byte{'x'}, tapeEventMax)
	window := int64(time.Hour)
	// Several passes over the budget. Reclaim recycles chunks, so an append
	// keeps succeeding; the cap is used, not a failed write.
	n := int(budget/int64(tapeChunkSize))*len(rings)*2 + len(rings)
	var seq uint64
	for i := 0; i < n; i++ {
		seq++
		rings[i%len(rings)].appendEvent(1, seq, int64(seq), raw, window)
	}
	return rings
}

func TestTapeChunkReuseRace(t *testing.T) {
	h := newTapeBox(t, tapeBoxOptions{max: 100_000, budget: 256 << 10, window: time.Hour})
	h.want("trade")
	h.up(1)
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
			}
		}()
	}
	for i := 1; i <= 4000; i++ {
		h.c.mu.Lock()
		next := h.c.live.seq["trade."+tapeTicker] + 1
		h.c.Trade(tapeCh, &exchange.Trade{TradeID: fmt.Sprintf("%d/%d", h.c.epoch, next), Price: 1, Qty: 1})
		h.c.mu.Unlock()
	}
	h.c.mu.Lock()
	written := h.c.views["trade."+tapeTicker].ring.chunkWritten
	h.c.mu.Unlock()
	close(stop)
	wg.Wait()
	if written < 2 {
		t.Fatalf("chunks were not reused (chunkWritten %d)", written)
	}
}

// validFeedRaw is an md.f message of exactly n bytes whose payload is a
// FeedEvent protobuf can parse. The bytes past the envelope are an unknown
// field, so the page path can unmarshal it.
func validFeedRaw(t *testing.T, n int) []byte {
	t.Helper()
	if n < envelope.Size+5 {
		t.Fatalf("raw %d shorter than a padded feed event", n)
	}
	payload := n - envelope.Size
	content := payload - 5 // 2-byte tag, 3-byte length
	if content < 16384 || content >= 1<<21 {
		t.Fatalf("content %d is not a 3-byte varint", content)
	}
	body := make([]byte, payload)
	body[0], body[1] = 0xC2, 0x0C // field 200, length-delimited
	body[2] = byte(content) | 0x80
	body[3] = byte(content>>7) | 0x80
	body[4] = byte(content >> 14)
	for i := 5; i < len(body); i++ {
		body[i] = 'a'
	}
	raw := make([]byte, n)
	copy(raw[envelope.Size:], body)
	if err := proto.Unmarshal(body, &mdv1.FeedEvent{}); err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodeTape(t *testing.T, raw []byte) *mdv1.ViewReply {
	t.Helper()
	rep := &mdv1.ViewReply{}
	if err := proto.Unmarshal(raw, rep); err != nil {
		t.Fatal(err)
	}
	return rep
}
