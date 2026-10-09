package conformance

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"sort"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// book is a reference local book: price -> qty, folded the way the conn worker folds (D402).
type book struct{ bids, asks map[float64]float64 }

func newBook() *book { return &book{bids: map[float64]float64{}, asks: map[float64]float64{}} }

func (b *book) reset(u *exchange.BookUpdate) {
	b.bids, b.asks = map[float64]float64{}, map[float64]float64{}
	b.apply(u)
}

func (b *book) apply(u *exchange.BookUpdate) {
	for _, l := range u.Bids {
		set(b.bids, l)
	}
	for _, l := range u.Asks {
		set(b.asks, l)
	}
}

func set(m map[float64]float64, l exchange.Level) {
	if l.Qty == 0 {
		delete(m, l.Price)
	} else {
		m[l.Price] = l.Qty
	}
}

// levels is a side sorted best first.
func levels(m map[float64]float64, desc bool) []exchange.Level {
	out := make([]exchange.Level, 0, len(m))
	for p, q := range m {
		out = append(out, exchange.Level{Price: p, Qty: q})
	}
	sort.Slice(out, func(i, j int) bool {
		if desc {
			return out[i].Price > out[j].Price
		}
		return out[i].Price < out[j].Price
	})
	return out
}

// item is one step of a book replay: a stream update or a REST snapshot.
type item struct {
	frame int
	u     exchange.BookUpdate
	rest  bool // a REST depth snapshot (SnapshotREST), not a stream update
	check bool // compare with the folded book instead of replacing it
}

// bookReplay is the outcome of replaying one channel.
type bookReplay struct {
	resyncAt    []int // frames where Check said BookResync
	checksumBad []int // frames where BookChecksum.Verify failed
	mismatch    []string
	applied     []int // indices into items of the updates applied after the first snapshot
}

// replayBook folds items through bs, as the conn worker does: stream updates wait until a snapshot
// is in (SnapshotREST), then go through Check. corruptAfter >= 0 changes a level of the book after
// the update at that item index, to see the checksum catch it.
func replayBook(bs exchange.BookSync, items []item, corruptAfter int) bookReplay {
	var res bookReplay
	b := newBook()
	var seq exchange.BookSeq
	var pending []int
	cs, hasCS := bs.(exchange.BookChecksum)
	step := func(i int) {
		it := &items[i]
		switch v := bs.Check(seq, &it.u); v {
		case exchange.BookApply:
			b.apply(&it.u)
			seq.Last = it.u.Last
			res.applied = append(res.applied, i)
			if i == corruptAfter {
				if best := levels(b.bids, true); len(best) > 0 {
					b.bids[best[0].Price] = best[0].Qty * 2
				}
			}
			if hasCS && !cs.Verify(levels(b.bids, true), levels(b.asks, false), &it.u) {
				res.checksumBad = append(res.checksumBad, it.frame)
			}
		case exchange.BookSkip:
		case exchange.BookResync:
			res.resyncAt = append(res.resyncAt, it.frame)
			seq = exchange.BookSeq{}
			b = newBook()
		default:
			res.mismatch = append(res.mismatch, fmt.Sprintf("frame %d: Check returned %d", it.frame, v))
		}
	}
	for i := range items {
		it := &items[i]
		switch {
		case it.check:
			if seq.Valid {
				res.mismatch = append(res.mismatch, compareBook(b, &it.u)...)
			}
		case it.rest || it.u.Snapshot:
			b.reset(&it.u)
			seq = exchange.BookSeq{Last: it.u.Last, Valid: true}
			if hasCS && !it.rest && !cs.Verify(levels(b.bids, true), levels(b.asks, false), &it.u) {
				res.checksumBad = append(res.checksumBad, it.frame)
			}
			for _, j := range pending {
				step(j)
			}
			pending = nil
		case !seq.Valid:
			pending = append(pending, i)
		default:
			step(i)
		}
	}
	return res
}

// compareBook: every level of the snapshot is in the folded book, and the folded book has nothing
// inside the snapshot's price range that the snapshot lacks.
func compareBook(b *book, snap *exchange.BookUpdate) []string {
	var out []string
	side := func(name string, m map[float64]float64, want []exchange.Level, desc bool) {
		if len(want) == 0 {
			return
		}
		inSnap := map[float64]float64{}
		worst := want[0].Price
		for _, l := range want {
			inSnap[l.Price] = l.Qty
			if (desc && l.Price < worst) || (!desc && l.Price > worst) {
				worst = l.Price
			}
		}
		for p, q := range inSnap {
			if got, ok := m[p]; !ok || got != q {
				out = append(out, fmt.Sprintf("%s %v: snapshot %v, folded %v", name, p, q, got))
			}
		}
		for p := range m {
			inside := (desc && p >= worst) || (!desc && p <= worst)
			if _, ok := inSnap[p]; inside && !ok {
				out = append(out, fmt.Sprintf("%s %v: folded book has it, the snapshot does not", name, p))
			}
		}
	}
	side("bid", b.bids, snap.Bids, true)
	side("ask", b.asks, snap.Asks, false)
	return out
}

// checkBooks: each book channel in the market sessions replays through its BookSync without a
// resync, ends equal to the closing snapshot when the fixture has one, notices a dropped update,
// and (with a checksum) notices a corrupted book.
func (r *runner) checkBooks() []Problem {
	m := r.spec.Market
	if m == nil {
		return nil
	}
	names, _ := Sessions(r.fsys)
	var out []Problem
	for _, name := range names {
		d, err := r.decode(name)
		if err != nil || d.mkt == nil || len(d.mkt.books) == 0 {
			continue
		}
		byCh := map[string][]item{}
		var order []string
		for _, be := range d.mkt.books {
			if _, ok := byCh[be.ch]; !ok {
				order = append(order, be.ch)
			}
			byCh[be.ch] = append(byCh[be.ch], item{frame: be.frame, u: be.u})
		}
		for i, rec := range d.sess.Records {
			if rec.Kind != KindHTTP || rec.Ch == "" {
				continue
			}
			u, err := r.depth(rec.Ch, rec.Exchange)
			if err != nil {
				out = append(out, Problem{"books", name, fmt.Sprintf("record %d: %v", i, err)})
				continue
			}
			byCh[rec.Ch] = append(byCh[rec.Ch], item{frame: i, u: *u, rest: true, check: rec.Check})
		}
		for _, ch := range order {
			where := name + " " + ch
			bs, ok := m.Book(ch)
			if !ok {
				out = append(out, Problem{"books", where, "Book events on a channel Market.Book does not know"})
				continue
			}
			items := byCh[ch]
			sort.SliceStable(items, func(i, j int) bool { return items[i].frame < items[j].frame })
			res := replayBook(bs, items, -1)
			for _, f := range res.resyncAt {
				out = append(out, Problem{"books", where, fmt.Sprintf("BookResync at frame %d on a clean recording", f)})
			}
			for _, f := range res.checksumBad {
				out = append(out, Problem{"books", where, fmt.Sprintf("checksum fails at frame %d on a clean recording", f)})
			}
			for _, s := range res.mismatch {
				out = append(out, Problem{"books", where, s})
			}
			if len(res.applied) < 3 {
				out = append(out, Problem{"books", where, fmt.Sprintf(
					"only %d updates applied after the snapshot; record a longer session so the gap check means something", len(res.applied))})
				continue
			}
			// Drop one update from the middle: the sequence or the checksum must notice.
			gap := res.applied[len(res.applied)/2]
			cut := slices.Delete(slices.Clone(items), gap, gap+1)
			if g := replayBook(bs, cut, -1); len(g.resyncAt) == 0 && len(g.checksumBad) == 0 {
				out = append(out, Problem{"books", where, fmt.Sprintf("dropping the update of frame %d went unnoticed", items[gap].frame)})
			}
			if _, ok := bs.(exchange.BookChecksum); ok {
				if c := replayBook(bs, items, res.applied[0]); len(c.checksumBad) == 0 {
					out = append(out, Problem{"books", where, "a corrupted book passed the checksum"})
				}
			}
		}
	}
	return out
}

// depth parses a recorded depth snapshot of channel ch with the venue's own Depth call.
func (r *runner) depth(ch string, ex *Exchange) (*exchange.BookUpdate, error) {
	if ex == nil || ex.Response == nil {
		return nil, errors.New("an http record without a response")
	}
	if r.spec.REST == nil {
		return nil, errors.New("a depth snapshot and no Spec.REST to parse it")
	}
	inst := r.instFor(ch)
	if inst == nil {
		return nil, fmt.Errorf("no instrument whose orderbook atom is %q: add it to Spec.Instruments or a listing case", ch)
	}
	bs, _ := r.spec.Market.Book(ch)
	limit := 0
	if bs != nil {
		limit = bs.Depth()
	}
	c, err := r.spec.REST.Depth(inst, limit)
	if err != nil {
		return nil, fmt.Errorf("Depth: %w", err)
	}
	u, err := c.Parse(ex.Response.HTTP())
	if err != nil {
		return nil, fmt.Errorf("Depth parse: %w", err)
	}
	return u, nil
}

func (r *runner) instFor(ch string) *symbol.Info {
	for _, inst := range r.instruments() {
		for _, t := range []exchange.Topic{exchange.TopicOrderBook, exchange.TopicOrderBookDelta} {
			if a, err := r.spec.Market.Atom(t, inst); err == nil && a.Channel == ch {
				return inst
			}
		}
	}
	return nil
}

// Platform topics: what Atom must refuse when Topics does not list it.
var platformTopics = []exchange.Topic{
	exchange.TopicBestQuote, exchange.TopicTrade, exchange.TopicAggTrade, exchange.TopicOrderBook,
	exchange.TopicOrderBookDelta, exchange.TopicTicker, exchange.TopicLiquidation, exchange.TopicFundingRate,
	exchange.TopicOpenInterest, exchange.TopicGreeks, exchange.TopicMarkPrice,
}

// checkPlanning: the planning half of Market is consistent (D401, D403). Atom is a function of its
// arguments; its route is one of Routes; one channel fits a connection; a topic Topics does not list
// is ErrUnsupported; every route has an endpoint, and the override replaces its host.
func (r *runner) checkPlanning() []Problem {
	m := r.spec.Market
	if m == nil {
		return nil
	}
	var out []Problem
	routes := m.Routes()
	if len(routes) == 0 {
		out = append(out, Problem{"planning", "Routes", "no routes"})
	}
	for i, rt := range routes {
		if rt == "" || slices.Contains(routes[:i], rt) {
			out = append(out, Problem{"planning", "Routes", fmt.Sprintf("route %q is empty or repeated", rt)})
		}
		ep, err := m.Endpoint(rt, "")
		if err != nil {
			out = append(out, Problem{"planning", "route " + rt, "Endpoint: " + err.Error()})
			continue
		}
		u, err := url.Parse(ep)
		if err != nil || (u.Scheme != "wss" && u.Scheme != "ws") || u.Host == "" {
			out = append(out, Problem{"planning", "route " + rt, fmt.Sprintf("Endpoint %q is not a websocket URL", ep)})
			continue
		}
		over, err := m.Endpoint(rt, "wss://override.example")
		if ou, perr := url.Parse(over); err != nil || perr != nil || ou.Host != "override.example" {
			out = append(out, Problem{"planning", "route " + rt, fmt.Sprintf("Endpoint with an override gives %q (%v)", over, err)})
		}
	}
	listed := map[exchange.Topic]bool{}
	for _, c := range m.Topics() {
		listed[c.Topic] = true
	}
	insts := r.instruments()
	if len(insts) == 0 {
		out = append(out, Problem{"planning", "instruments", "no instruments: add a listing case or Spec.Instruments"})
	}
	topics := append([]exchange.Topic(nil), platformTopics...)
	for t := range listed {
		if !slices.Contains(topics, t) {
			topics = append(topics, t)
		}
	}
	for _, inst := range insts {
		found := false
		for _, t := range topics {
			where := fmt.Sprintf("%s %s", t, inst.Ticker)
			a, err := m.Atom(t, inst)
			if !listed[t] {
				if !errors.Is(err, exchange.ErrUnsupported) {
					out = append(out, Problem{"planning", where, fmt.Sprintf("Topics does not list it, and Atom gives %v, %v", a, err)})
				}
				continue
			}
			if errors.Is(err, exchange.ErrUnsupported) {
				continue // listed, but not for this instrument's category
			}
			if err != nil {
				out = append(out, Problem{"planning", where, "Atom: " + err.Error()})
				continue
			}
			found = true
			if b, err2 := m.Atom(t, inst); err2 != nil || b != a {
				out = append(out, Problem{"planning", where, "Atom gives a different answer the second time"})
			}
			if !slices.Contains(routes, a.Route) {
				out = append(out, Problem{"planning", where, fmt.Sprintf("route %q is not in Routes", a.Route)})
			}
			if a.Channel == "" {
				out = append(out, Problem{"planning", where, "empty channel"})
			}
			cost, budget := m.Cost(a.Channel), m.Budget(a.Route)
			if cost <= 0 || (budget > 0 && cost > budget) {
				out = append(out, Problem{"planning", where, fmt.Sprintf("Cost %d against Budget %d", cost, budget)})
			}
		}
		if !found && inst.Status.IsTradable() {
			out = append(out, Problem{"planning", inst.Ticker.String(), "no listed topic has an atom for this instrument"})
		}
	}
	return out
}

// checkSubscribe: Subscribe and Unsubscribe cover every channel once, within the session limits,
// with distinct request ids.
func (r *runner) checkSubscribe() []Problem {
	m := r.spec.Market
	if m == nil {
		return nil
	}
	byRoute := map[string][]string{}
	for _, inst := range r.instruments() {
		for _, c := range m.Topics() {
			if a, err := m.Atom(c.Topic, inst); err == nil && !slices.Contains(byRoute[a.Route], a.Channel) {
				byRoute[a.Route] = append(byRoute[a.Route], a.Channel)
			}
		}
	}
	var out []Problem
	for route, chs := range byRoute {
		ids := map[uint64]string{} // ids are per connection
		s, err := m.NewSession(route)
		if err != nil {
			out = append(out, Problem{"subscribe", route, err.Error()})
			continue
		}
		lim := s.Limits()
		for _, op := range []struct {
			name string
			f    func([]string) ([]exchange.Request, error)
		}{{"Subscribe", s.Subscribe}, {"Unsubscribe", s.Unsubscribe}} {
			where := route + " " + op.name
			reqs, err := op.f(chs)
			if err != nil {
				out = append(out, Problem{"subscribe", where, err.Error()})
				continue
			}
			var got []string
			for _, q := range reqs {
				if prev, dup := ids[q.ID]; dup {
					out = append(out, Problem{"subscribe", where, fmt.Sprintf("request id %d repeats (%s)", q.ID, prev)})
				}
				ids[q.ID] = where
				if len(q.Frame) == 0 {
					out = append(out, Problem{"subscribe", where, fmt.Sprintf("request %d has no frame", q.ID)})
				}
				if lim.PerRequest > 0 && len(q.Channels) > lim.PerRequest {
					out = append(out, Problem{"subscribe", where, fmt.Sprintf("request %d has %d channels; PerRequest is %d", q.ID, len(q.Channels), lim.PerRequest)})
				}
				n := 0
				for _, c := range q.Channels {
					n += len(c)
				}
				if lim.PerRequestBytes > 0 && n > lim.PerRequestBytes {
					out = append(out, Problem{"subscribe", where, fmt.Sprintf("request %d has %d bytes of channels; PerRequestBytes is %d", q.ID, n, lim.PerRequestBytes)})
				}
				got = append(got, q.Channels...)
				r.saw(string(q.Frame))
			}
			want := slices.Clone(chs)
			slices.Sort(want)
			slices.Sort(got)
			if !reflect.DeepEqual(want, got) {
				out = append(out, Problem{"subscribe", where, fmt.Sprintf("requests cover %v, want each of %v once", got, want)})
			}
		}
	}
	return out
}
