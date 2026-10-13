package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/book"
)

type cursor struct{ epoch, seq uint64 }

func (a cursor) String() string { return fmt.Sprintf("(%d,%d)", a.epoch, a.seq) }

// atOrBefore orders cursors: a conn_epoch first, then seq within it.
func (a cursor) atOrBefore(b cursor) bool {
	return a.epoch < b.epoch || (a.epoch == b.epoch && a.seq <= b.seq)
}

// next reports whether b may follow a with nothing missing: the next seq, or seq 1 of a later
// epoch (a reconnect, which a book sees as a reset).
func (a cursor) next(b cursor) bool {
	return (b.epoch == a.epoch && b.seq == a.seq+1) || (b.epoch > a.epoch && b.seq == 1)
}

type liveMsg struct {
	c       cursor
	payload []byte
}

// late is a late joiner (PROTOCOL §9.4, D437): subscribe md.v1.f for one feed and flush, then ask
// md.v1.view. A book starts from the BookSnapshot at (epoch, S); a tape pages back to -since and
// joins at TapePage.head. Live messages at or before the join cursor are dropped, and the first
// one applied must follow it. It then follows live for -follow. A book is folded with sdk/book,
// as a strategy would, and compared level by level with a second md.v1.view at the same cursor.
func late() error {
	feed := flag.String("feed", "", "topic.UniversalTicker, e.g. orderbook_delta.BinanceUM_Perp_BTCUSDT")
	follow := flag.Duration("follow", 20*time.Second, "how long to follow live after joining")
	since := flag.Duration("since", 10*time.Minute, "tape: how far back to read")
	tick := flag.Float64("tick", 0, "book: price tick; 0 asks SYM")
	flag.Parse()
	topic, ticker, ok := strings.Cut(*feed, ".")
	if !ok || topic == "" || ticker == "" {
		return errors.New("-feed is topic.UniversalTicker")
	}
	isBook := topic == "orderbook_delta"
	if isBook && *tick == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		info, err := symbolInfo(ctx, ticker)
		cancel()
		if err != nil {
			return fmt.Errorf("price tick from SYM: %w", err)
		}
		*tick = info.GetFilters()[symbol.PriceTick].GetValue()
		if *tick <= 0 {
			return fmt.Errorf("%s has no price_tick in SYM", ticker)
		}
	}
	nc, err := dial()
	if err != nil {
		return err
	}
	defer nc.Close()
	live := make(chan liveMsg, 1<<16)
	feedSubject := "md.v1.f." + *instance + "." + *feed
	sub, err := nc.Subscribe(feedSubject, func(m *nats.Msg) {
		_, epoch, seq, _, payload, err := envelope.Parse(m.Data)
		if err != nil {
			return
		}
		live <- liveMsg{c: cursor{epoch, seq}, payload: append([]byte(nil), payload...)}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe() //nolint:errcheck
	if err := nc.Flush(); err != nil {
		return err
	}
	fmt.Printf("%s subscribed %s\n", stamp(), feedSubject)
	v := viewer{nc: nc, subject: "md.v1.view." + strings.TrimPrefix(feedSubject, "md.v1.f.")}

	var join cursor
	var ob *book.OrderBook
	if isBook {
		snap, err := v.book()
		if err != nil {
			return err
		}
		join = cursor{snap.GetConnEpoch(), snap.GetSeq()}
		ob = newBook(*tick, snap)
		fmt.Printf("%s view book at %s: %s\n", stamp(), join, describeBook(ob))
	} else {
		head, err := v.tape(time.Now().Add(-*since).UnixNano())
		if err != nil {
			return err
		}
		join = head
	}

	dropped, applied, gaps := 0, 0, 0
	last := join
	var first []cursor
	deadline := time.After(*follow)
follow:
	for {
		select {
		case m := <-live:
			if m.c.atOrBefore(join) {
				dropped++
				continue
			}
			if !last.next(m.c) {
				gaps++
				fmt.Printf("%s GAP after %s got %s\n", stamp(), last, m.c)
			}
			if len(first) < 3 {
				first = append(first, m.c)
			}
			last = m.c
			applied++
			if isBook {
				if err := applyDelta(&ob, *tick, m.payload); err != nil {
					return err
				}
			}
		case <-deadline:
			break follow
		}
	}
	fmt.Printf("%s live: dropped (at or before %s)=%d applied=%d first=%v last=%s gaps=%d\n",
		stamp(), join, dropped, applied, first, last, gaps)
	if gaps > 0 {
		return fmt.Errorf("%d gaps at or after the join", gaps)
	}
	if !isBook {
		return nil
	}
	fmt.Printf("%s local book: %s\n", stamp(), describeBook(ob))
	return compareBook(v, ob, *tick, live, last)
}

type viewer struct {
	nc      *nats.Conn
	subject string
}

// ask sends one ViewRequest and retries not_synced (D441) for up to ten seconds.
func (v viewer) ask(req *mdv1.ViewRequest) (*mdv1.ViewReply, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	for range 20 {
		m, err := v.nc.Request(v.subject, b, 5*time.Second)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.subject, err)
		}
		rep := &mdv1.ViewReply{}
		if err := proto.Unmarshal(m.Data, rep); err != nil {
			return nil, err
		}
		switch rep.GetError() {
		case "":
			return rep, nil
		case "not_synced":
			fmt.Printf("%s view: not_synced, retrying\n", stamp())
			time.Sleep(500 * time.Millisecond)
		default:
			return nil, fmt.Errorf("view: %s", rep.GetError())
		}
	}
	return nil, errors.New("view: still not_synced after 10s")
}

func (v viewer) book() (*mdv1.BookSnapshot, error) {
	rep, err := v.ask(&mdv1.ViewRequest{})
	if err != nil {
		return nil, err
	}
	if rep.GetBook() == nil {
		return nil, errors.New("view: no book in the reply")
	}
	return rep.GetBook(), nil
}

// tape reads every page back to sinceNs, prints the gaps, checks that the newest event is not
// past head, and returns head.
func (v viewer) tape(sinceNs int64) (cursor, error) {
	var before *mdv1.Cursor
	var head, newest, oldest cursor
	events, pages := 0, 0
	for {
		rep, err := v.ask(&mdv1.ViewRequest{Before: before, SinceNs: &sinceNs})
		if err != nil {
			return cursor{}, err
		}
		page := rep.GetTape()
		if page == nil {
			return cursor{}, errors.New("view: no tape in the reply")
		}
		evs := page.GetEvents()
		if pages == 0 {
			head = cursor{page.GetHead().GetConnEpoch(), page.GetHead().GetSeq()}
			if len(evs) > 0 {
				newest = cursor{evs[0].GetConnEpoch(), evs[0].GetSeq()}
			}
		}
		if len(evs) > 0 {
			oldest = cursor{evs[len(evs)-1].GetConnEpoch(), evs[len(evs)-1].GetSeq()}
		}
		for _, g := range page.GetGaps() {
			fmt.Printf("%s tape gap: %s to %s, last %s, first %s\n", stamp(),
				time.Unix(0, g.GetStartNs()).UTC().Format(time.RFC3339Nano), time.Unix(0, g.GetEndNs()).UTC().Format(time.RFC3339Nano),
				cursor{g.GetLast().GetConnEpoch(), g.GetLast().GetSeq()}, cursor{g.GetFirst().GetConnEpoch(), g.GetFirst().GetSeq()})
		}
		pages++
		events += len(evs)
		if !page.GetMore() {
			break
		}
		before = page.GetNext()
	}
	fmt.Printf("%s view tape: %d events in %d pages, newest %s, oldest %s, head %s\n", stamp(), events, pages, newest, oldest, head)
	if events > 0 && !newest.atOrBefore(head) {
		return cursor{}, fmt.Errorf("newest tape event %s is past head %s", newest, head)
	}
	return head, nil
}

func levels(lv []*mdv1.Level) []book.Level {
	out := make([]book.Level, len(lv))
	for i, l := range lv {
		out[i] = book.Level{Price: l.GetPrice(), Size: l.GetQty()}
	}
	return out
}

func newBook(tick float64, snap *mdv1.BookSnapshot) *book.OrderBook {
	ob, err := book.New(book.DefaultN, tick)
	if err != nil {
		panic(err) // tick was checked positive
	}
	ob.Apply(levels(snap.GetBids()), levels(snap.GetAsks()))
	return ob
}

// applyDelta folds one md.f orderbook_delta. A reset replaces the book.
func applyDelta(ob **book.OrderBook, tick float64, payload []byte) error {
	ev := &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		return err
	}
	d := ev.GetOrderBookDelta()
	if d == nil {
		return fmt.Errorf("not an orderbook_delta: %T", ev.GetEvent())
	}
	if d.GetReset_() {
		nb, err := book.New(book.DefaultN, tick)
		if err != nil {
			return err
		}
		*ob = nb
	}
	(*ob).Apply(levels(d.GetBids()), levels(d.GetAsks()))
	return nil
}

// ring is the book's window as tick -> size, one map per side (bids first).
func ring(ob *book.OrderBook) [2]map[int64]float64 {
	out := [2]map[int64]float64{{}, {}}
	for i, it := range []book.Iter{ob.Bids(1 << 30), ob.Asks(1 << 30)} {
		for {
			t, size, ok := it.Next()
			if !ok {
				break
			}
			out[i][t] = size
		}
	}
	return out
}

func describeBook(ob *book.OrderBook) string {
	r := ring(ob)
	bb, okb := ob.BestBidTick()
	ba, oka := ob.BestAskTick()
	crossed := okb && oka && bb >= ba
	return fmt.Sprintf("%d bids, %d asks, best %s/%s, crossed=%v", len(r[0]), len(r[1]), price(ob, bb), price(ob, ba), crossed)
}

// compareBook asks md.view again, brings the local book to the same cursor, and compares the
// windows by tick. View prices are tick × price_tick (D430) and need not be the float md.f
// carried, so the comparison is by tick, as sdk/book keys them. Live can pass the view's cursor
// before the reply arrives; that is retried.
func compareBook(v viewer, ob *book.OrderBook, tick float64, live <-chan liveMsg, last cursor) error {
	for attempt := 1; attempt <= 5; attempt++ {
		snap, err := v.book()
		if err != nil {
			return err
		}
		at := cursor{snap.GetConnEpoch(), snap.GetSeq()}
		for last.atOrBefore(at) && last != at {
			select {
			case m := <-live:
				if m.c.atOrBefore(last) {
					continue
				}
				if err := applyDelta(&ob, tick, m.payload); err != nil {
					return err
				}
				last = m.c
			case <-time.After(5 * time.Second):
				return fmt.Errorf("live did not reach the view's cursor %s", at)
			}
		}
		if last != at {
			fmt.Printf("%s compare: live at %s is past the view's %s, asking again\n", stamp(), last, at)
			continue
		}
		want := ring(newBook(tick, snap))
		got := ring(ob)
		bb, _ := ob.BestBidTick()
		ba, _ := ob.BestAskTick()
		best := [2]int64{bb, ba}
		core := int64(ob.Window() / 2)
		inner, outer := 0, 0
		for side := range 2 {
			for t := range union(want[side], got[side]) {
				if want[side][t] == got[side][t] {
					continue
				}
				where := "outer"
				if d := t - best[side]; -core < d && d < core {
					where = "core"
					inner++
				} else {
					outer++
				}
				if inner+outer <= 10 {
					fmt.Printf("  differs (%s): bid=%v price=%s view=%v local=%v\n", where, side == 0, price(ob, t), want[side][t], got[side][t])
				}
			}
		}
		fmt.Printf("%s compare at %s: view %d/%d levels, local %d/%d; differing within %d ticks of the best: %d, farther: %d\n",
			stamp(), at, len(want[0]), len(want[1]), len(got[0]), len(got[1]), core, inner, outer)
		if outer > 0 {
			fmt.Println("  farther levels can differ by design (D431): a level dropped outside the window, or never in a")
			fmt.Println("  joiner's snapshot, stays unknown to the joiner until its next update, while the worker's ring keeps its slot.")
		}
		if inner > 0 {
			return fmt.Errorf("%d levels near the best differ from md.view at %s", inner, at)
		}
		return nil
	}
	return errors.New("live kept passing md.view's cursor; no comparison made")
}

// price spells a tick with the tick size's decimals: tick × price_tick is not always the
// shortest float.
func price(ob *book.OrderBook, tick int64) string {
	ts := strconv.FormatFloat(ob.TickSize(), 'f', -1, 64)
	dec := 0
	if _, frac, ok := strings.Cut(ts, "."); ok {
		dec = len(frac)
	}
	return strconv.FormatFloat(ob.Price(tick), 'f', dec, 64)
}

func union(a, b map[int64]float64) map[int64]struct{} {
	out := make(map[int64]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}
