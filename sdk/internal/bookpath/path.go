package bookpath

import (
	"errors"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/book"
)

const (
	// bufferMax and bufferAge are the snapshot-wait limits (D424, D858). Reaching either
	// one drops the buffer and discards the snapshot that is in flight.
	bufferMax = 10000
	bufferAge = 10 * time.Second

	// backoffMin is the first wait after a snapshot read fails. It doubles until backoffMax
	// (D424, D441). 16s doubled is 32s, which is past the cap, so the next wait is 30s.
	backoffMin = time.Second
	backoffMax = 30 * time.Second
)

var errBehind = errors.New("bookpath: snapshot is behind the feed")

// Update is one orderbook_delta that was applied to a synced book and is fresh enough
// to hand to OnBook. Deltas buffered while the snapshot was outstanding are not updates.
type Update struct {
	UID       uint32
	ConnEpoch uint64
	Seq       uint64
	MdTsNs    int64
	ExchTsNs  int64
}

// Config opens a Path.
//
// Snapshot asks md.view for uid. The callback runs on the session dispatcher, which is
// also where OnMessage and Schedule's fn run. Schedule must not call fn before it returns.
// Info reads the instruments this session has resolved. It does not call SYM (D859, D823).
// Hook nil still maintains the book. Now nil uses time.Now. Log nil discards lines.
// RecvWindow is InitPayload.recv_window_ms and must be positive.
type Config struct {
	Snapshot   func(uid uint32, fn func(*mdv1.BookSnapshot, error)) error
	Info       func(uid uint32) (symbol.Info, error)
	Hook       func(Update)
	RecvWindow time.Duration
	Now        func() time.Time
	Schedule   func(delay time.Duration, fn func())
	Log        func(string)
}

// Path is one session's local books, one per instrument. Close makes View report unsynced
// and ignores later messages. Close does not wait for a snapshot that is already in flight.
type Path struct {
	snapshot func(uint32, func(*mdv1.BookSnapshot, error)) error
	info     func(uint32) (symbol.Info, error)
	hook     func(Update)
	recv     time.Duration
	now      func() time.Time
	schedule func(time.Duration, func())
	log      func(string)

	by       map[uint32]*inst
	toldZero bool
	closed   bool
}

// inst is one instrument. synced is false until a snapshot has been installed and the
// buffer that followed it was continuous. ob is nil whenever synced is false (D860).
type inst struct {
	uid    uint32
	ob     *book.OrderBook
	synced bool
	epoch  uint64
	seq    uint64

	buf    []pending
	bufAt  time.Time
	have   bool
	bufGen uint64

	floorEpoch uint64
	floorSeq   uint64
	haveFloor  bool

	gen      uint64
	inflight bool
	wait     time.Duration
	backing  bool

	told string
}

// pending is one delta kept while the book is unsynced. The levels are copies: the
// dispatcher's ring reuses the message bytes after OnMessage returns.
type pending struct {
	epoch  uint64
	seq    uint64
	mdTs   int64
	exchTs int64
	bids   []book.Level
	asks   []book.Level
}

// Open checks the config and returns a path with no books yet. The first delta for an
// instrument asks for the snapshot (D857).
func Open(cfg Config) (*Path, error) {
	if cfg.Snapshot == nil {
		return nil, errors.New("bookpath: nil snapshot")
	}
	if cfg.Info == nil {
		return nil, errors.New("bookpath: nil info")
	}
	if cfg.Schedule == nil {
		return nil, errors.New("bookpath: nil schedule")
	}
	if cfg.RecvWindow <= 0 {
		return nil, errors.New("bookpath: recv window must be positive")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	p := &Path{
		snapshot: cfg.Snapshot,
		info:     cfg.Info,
		hook:     cfg.Hook,
		recv:     cfg.RecvWindow,
		now:      cfg.Now,
		schedule: cfg.Schedule,
		log:      cfg.Log,
		by:       map[uint32]*inst{},
	}
	if p.log == nil {
		p.log = func(string) {}
	}
	return p, nil
}

// Close ignores later messages and makes View return (nil, false).
func (p *Path) Close() {
	if p == nil {
		return
	}
	p.closed = true
}

// View is the instrument's book when it is synced. The pointer is the live ring and stays
// valid only until the hook that called it returns (D803, D860).
func (p *Path) View(uid uint32) (*book.OrderBook, bool) {
	if p == nil || p.closed {
		return nil, false
	}
	b := p.by[uid]
	if b == nil || !b.synced || b.ob == nil {
		return nil, false
	}
	return b.ob, true
}

// OnMessage is one orderbook_delta, envelope and payload. The bytes alias the ring and
// are valid only until OnMessage returns. A delta is applied however old it is. OnBook
// runs only for a delta that was applied to a synced book and is still inside recv_window_ms.
func (p *Path) OnMessage(msg []byte) {
	if p == nil || p.closed {
		return
	}
	mdTs, epoch, seq, _, payload, err := envelope.Parse(msg)
	if err != nil {
		p.log("book: envelope: " + err.Error())
		return
	}
	ev := &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		// The cursor belongs to some instrument, but the uid is in the payload. The next
		// delta of that instrument skips this seq and resyncs.
		p.log("book: payload: " + err.Error())
		return
	}
	uid := ev.GetUid()
	if uid == 0 {
		if !p.toldZero {
			p.toldZero = true
			p.log("book: uid 0")
		}
		return
	}
	delta := ev.GetOrderBookDelta()
	if delta == nil {
		b := p.inst(uid)
		if _, ok := p.gate(b); !ok {
			return
		}
		p.lose(b, epoch, seq, nil)
		return
	}
	p.onDelta(uid, epoch, seq, mdTs, delta)
}

func (p *Path) onDelta(uid uint32, epoch, seq uint64, mdTs int64, d *mdv1.OrderBookDelta) {
	b := p.inst(uid)
	if _, ok := p.gate(b); !ok {
		return
	}
	if d.GetReset_() {
		p.onReset(b, epoch, seq)
		return
	}
	item := pending{
		epoch: epoch, seq: seq, mdTs: mdTs, exchTs: exchTs(d),
		bids: levels(d.GetBids()), asks: levels(d.GetAsks()),
	}
	if !b.synced {
		if !p.buffer(b, item) {
			p.fetch(b)
		}
		return
	}
	if !newer(epoch, seq, b.epoch, b.seq) {
		return
	}
	if epoch != b.epoch || seq != b.seq+1 {
		p.lose(b, epoch, seq, &item)
		return
	}
	b.ob.Apply(item.bids, item.asks)
	b.epoch, b.seq = epoch, seq
	p.emit(b, item)
}

// onReset clears the book and asks md.view again (D861). The levels on the reset are
// not applied. A snapshot older than this cursor is not the reset's book.
func (p *Path) onReset(b *inst, epoch, seq uint64) {
	if b.synced && !newer(epoch, seq, b.epoch, b.seq) {
		return
	}
	if !b.synced {
		if b.haveFloor && !newer(epoch, seq, b.floorEpoch, b.floorSeq) {
			return
		}
		if last, ok := maxPending(b.buf); ok && !newer(epoch, seq, last.epoch, last.seq) {
			return
		}
	}
	b.synced = false
	b.ob = nil
	b.buf = nil
	b.have = false
	b.bufGen++
	p.raiseFloor(b, epoch, seq)
	p.invalidate(b)
	p.fetch(b)
}

// lose marks the book unsynced because seq was not the next cursor, or the body could
// not be applied. item nil means there is nothing to keep; the snapshot still has to
// reach this cursor.
func (p *Path) lose(b *inst, epoch, seq uint64, item *pending) {
	b.synced = false
	b.ob = nil
	p.raiseFloor(b, epoch, seq)
	p.invalidate(b)
	if item != nil && p.buffer(b, *item) {
		return
	}
	p.fetch(b)
}

func (p *Path) emit(b *inst, item pending) {
	if p.hook == nil || !p.fresh(item.mdTs) {
		return
	}
	p.hook(Update{
		UID:       b.uid,
		ConnEpoch: item.epoch,
		Seq:       item.seq,
		MdTsNs:    item.mdTs,
		ExchTsNs:  item.exchTs,
	})
}

// fresh reports whether mdTs is still inside the recv window. The comparison is `>`,
// so an event exactly recv_window_ms old is delivered (D803). now is the dispatcher
// clock at the moment the delta is applied.
func (p *Path) fresh(mdTs int64) bool {
	return p.now().Sub(time.Unix(0, mdTs)) <= p.recv
}

// buffer keeps one delta from the unsynced interval. It reports whether the cap dropped
// the buffer and started a new snapshot, so the caller does not start a second one.
func (p *Path) buffer(b *inst, item pending) bool {
	if !b.have {
		b.bufGen++
		b.bufAt = p.now()
		b.have = true
		p.armAge(b)
	}
	b.buf = append(b.buf, item)
	if !p.over(b) {
		return false
	}
	p.dropBuf(b)
	p.invalidate(b)
	p.fetch(b)
	return true
}

func (p *Path) over(b *inst) bool {
	if len(b.buf) >= bufferMax {
		return true
	}
	return b.have && p.now().Sub(b.bufAt) >= bufferAge
}

func (p *Path) dropBuf(b *inst) {
	if last, ok := maxPending(b.buf); ok {
		p.raiseFloor(b, last.epoch, last.seq)
	}
	b.buf = nil
	b.have = false
	b.bufGen++
}

func (p *Path) armAge(b *inst) {
	gen := b.bufGen
	uid := b.uid
	p.schedule(bufferAge, func() {
		if p.closed {
			return
		}
		b := p.by[uid]
		if b == nil || b.bufGen != gen || !b.have {
			return
		}
		if p.now().Sub(b.bufAt) < bufferAge {
			return
		}
		p.dropBuf(b)
		p.invalidate(b)
		p.fetch(b)
	})
}

func (p *Path) fetch(b *inst) {
	if p.closed || b.backing || b.inflight {
		return
	}
	if _, ok := p.gate(b); !ok {
		return
	}
	gen := b.gen
	b.inflight = true
	uid := b.uid
	called := false
	err := p.snapshot(uid, func(snap *mdv1.BookSnapshot, err error) {
		called = true
		p.onSnap(uid, gen, snap, err)
	})
	if err != nil && !called && b.gen == gen {
		b.inflight = false
		p.fail(b, err)
	}
}

func (p *Path) onSnap(uid uint32, gen uint64, snap *mdv1.BookSnapshot, err error) {
	if p.closed {
		return
	}
	b := p.by[uid]
	if b == nil || b.gen != gen || !b.inflight {
		return
	}
	b.inflight = false
	if err != nil || snap == nil {
		if err == nil {
			err = errors.New("bookpath: empty snapshot")
		}
		p.fail(b, err)
		return
	}
	if u := snap.GetUid(); u != 0 && u != uid {
		p.fail(b, fmt.Errorf("bookpath: snapshot uid %d", u))
		return
	}
	if b.haveFloor && !atLeast(snap.GetConnEpoch(), snap.GetSeq(), b.floorEpoch, b.floorSeq) {
		p.fail(b, errBehind)
		return
	}
	tick, ok := p.gate(b)
	if !ok {
		return
	}
	ob, nerr := book.New(book.DefaultN, tick)
	if nerr != nil {
		p.fail(b, nerr)
		return
	}
	ob.Apply(levels(snap.GetBids()), levels(snap.GetAsks()))
	epoch, seq, ok := p.fold(b, ob, snap.GetConnEpoch(), snap.GetSeq())
	if !ok {
		p.fail(b, errBehind)
		return
	}
	b.ob = ob
	b.synced = true
	b.epoch, b.seq = epoch, seq
	b.haveFloor = false
	b.wait = 0
	b.told = ""
}

// fold applies buffered deltas that continue from the snapshot. A hole keeps the tail,
// raises the floor to that delta, and leaves the book uninstalled.
func (p *Path) fold(b *inst, ob *book.OrderBook, epoch, seq uint64) (uint64, uint64, bool) {
	i := 0
	for i < len(b.buf) {
		item := b.buf[i]
		if !newer(item.epoch, item.seq, epoch, seq) {
			i++
			continue
		}
		if item.epoch != epoch || item.seq != seq+1 {
			b.buf = append([]pending(nil), b.buf[i:]...)
			p.raiseFloor(b, item.epoch, item.seq)
			return epoch, seq, false
		}
		ob.Apply(item.bids, item.asks)
		epoch, seq = item.epoch, item.seq
		i++
	}
	b.buf = nil
	b.have = false
	return epoch, seq, true
}

func (p *Path) fail(b *inst, err error) {
	if err != nil {
		p.log(fmt.Sprintf("book: uid %d: %v", b.uid, err))
	}
	p.backoff(b)
}

func (p *Path) backoff(b *inst) {
	if p.closed || b.backing {
		return
	}
	b.wait = grow(b.wait)
	b.backing = true
	gen := b.gen
	uid := b.uid
	delay := b.wait
	p.schedule(delay, func() {
		if p.closed {
			return
		}
		b := p.by[uid]
		if b == nil || b.gen != gen {
			return
		}
		b.backing = false
		p.fetch(b)
	})
}

func (p *Path) invalidate(b *inst) {
	b.gen++
	b.inflight = false
	b.backing = false
}

func (p *Path) raiseFloor(b *inst, epoch, seq uint64) {
	if b.haveFloor && !newer(epoch, seq, b.floorEpoch, b.floorSeq) {
		return
	}
	b.haveFloor = true
	b.floorEpoch, b.floorSeq = epoch, seq
}

// gate is the tick size from SYM Info. A UID this session has not resolved, or a tick
// that cannot build a ring, stays unsynced and is logged once (D859, D433).
func (p *Path) gate(b *inst) (float64, bool) {
	info, err := p.info(b.uid)
	if err != nil {
		p.once(b, "unresolved", fmt.Sprintf("book: uid %d is not resolved", b.uid))
		return 0, false
	}
	tick := info.Filters[symbol.PriceTick]
	if !(tick > 0) || math.IsNaN(tick) || math.IsInf(tick, 0) {
		p.once(b, "tick", fmt.Sprintf("book: uid %d has no price tick", b.uid))
		return 0, false
	}
	return tick, true
}

func (p *Path) once(b *inst, kind, msg string) {
	if b.told == kind {
		return
	}
	b.told = kind
	p.log(msg)
}

func (p *Path) inst(uid uint32) *inst {
	b := p.by[uid]
	if b == nil {
		b = &inst{uid: uid}
		p.by[uid] = b
	}
	return b
}

func levels(ls []*mdv1.Level) []book.Level {
	if len(ls) == 0 {
		return nil
	}
	out := make([]book.Level, 0, len(ls))
	for _, l := range ls {
		if l == nil {
			continue
		}
		out = append(out, book.Level{Price: l.GetPrice(), Size: l.GetQty()})
	}
	return out
}

func exchTs(d *mdv1.OrderBookDelta) int64 {
	if d == nil {
		return 0
	}
	return d.GetMeta().GetExchTsNs()
}

func maxPending(buf []pending) (pending, bool) {
	if len(buf) == 0 {
		return pending{}, false
	}
	best := buf[0]
	for _, item := range buf[1:] {
		if newer(item.epoch, item.seq, best.epoch, best.seq) {
			best = item
		}
	}
	return best, true
}

func newer(epoch, seq, floorEpoch, floorSeq uint64) bool {
	if epoch != floorEpoch {
		return epoch > floorEpoch
	}
	return seq > floorSeq
}

func atLeast(epoch, seq, floorEpoch, floorSeq uint64) bool {
	if epoch != floorEpoch {
		return epoch > floorEpoch
	}
	return seq >= floorSeq
}

func grow(d time.Duration) time.Duration {
	if d <= 0 {
		return backoffMin
	}
	d *= 2
	if d > backoffMax {
		return backoffMax
	}
	return d
}
