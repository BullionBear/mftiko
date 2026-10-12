package conn

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// SetDesired replaces the subscription when d is strictly newer than the one already applied.
// The first call applies even when its version is 0, or absent: "have a version" is tracked on
// its own, so a real 0 is not mistaken for "nothing yet". An older or equal version is ignored,
// which is what makes a stale read from the agent harmless. Atoms for another venue or route are
// dropped. Record is ignored (D447). Tape follows the feed list on the setFeeds path:
// trade, aggtrade and liquidation are recorded when project applies that list (D452).
//
// Safe to call while Run is in progress. A change is picked up by the write loop, which diffs
// against what the venue has already acknowledged.
func (c *Conn) SetDesired(d *mdctlv1.Desired) {
	if feeds, ok := feedPayload(d); ok {
		c.setFeeds(d, feeds)
		return
	}
	if c.inFeedMode() && (d == nil || len(d.GetAtoms()) == 0) {
		c.setFeeds(d, nil)
		return
	}
	over := c.symbolOverlay(d)
	c.mu.Lock()
	applied := c.apply(d, over)
	fatal := c.fatal
	cancel := c.stop
	live := c.live != nil
	c.mu.Unlock()
	if fatal != nil {
		if cancel != nil {
			cancel()
		}
		return
	}
	if applied && live {
		c.poke()
		c.wakeBook()
	}
}

func (c *Conn) inFeedMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.feedMode
}

// feedPayload is the agent's topic.UniversalTicker list. An assignment with an atom id is the
// older shape, applied as channels directly.
func feedPayload(d *mdctlv1.Desired) ([]string, bool) {
	if d == nil || len(d.GetAtoms()) == 0 {
		return nil, false
	}
	var feeds []string
	for _, a := range d.Atoms {
		if a == nil {
			continue
		}
		if a.GetAtomId() != "" {
			return nil, false
		}
		feeds = append(feeds, a.GetFeeds()...)
	}
	return feeds, true
}

// symHit is one channel's exch ticker and price tick from the venue table. set is false when the
// lookup failed open: the assignment's values stay.
type symHit struct {
	exch string
	tick float64
	set  bool
}

// symbolOverlay reads the venue table for each book atom. Nil means this worker has no table, so
// apply keeps the assignment. The lookup is outside the connection lock: a reload can call SYM.
func (c *Conn) symbolOverlay(d *mdctlv1.Desired) map[string]symHit {
	if c.symbols == nil || d == nil {
		return nil
	}
	out := map[string]symHit{}
	for _, a := range d.Atoms {
		if a == nil {
			continue
		}
		_, _, ch, ok := splitAtom(a.AtomId)
		if !ok || !assignmentBook(a) {
			continue
		}
		info, keep := c.lookupFeed(firstTicker(a))
		if keep {
			continue
		}
		hit := symHit{set: true}
		if info != nil {
			hit.exch = info.ExchTicker
			hit.tick = priceTick(info)
		}
		out[ch] = hit
	}
	return out
}

func assignmentBook(a *mdctlv1.AtomAssignment) bool {
	for _, raw := range a.Feeds {
		topic, _, ok := splitFeed(raw)
		if ok && (topic == string(exchange.TopicOrderBook) || topic == string(exchange.TopicOrderBookDelta)) {
			return true
		}
	}
	return false
}

func firstTicker(a *mdctlv1.AtomAssignment) string {
	for _, raw := range a.Feeds {
		_, ticker, ok := splitFeed(raw)
		if ok {
			return ticker
		}
	}
	return ""
}

// lookupFeed reads one ticker from the venue table. keep means a transient failure: leave the
// assignment in place. A missing row returns nil, false so the book is not kept.
func (c *Conn) lookupFeed(ticker string) (*symbol.Info, bool) {
	if ticker == "" {
		return nil, false
	}
	t, err := symbol.Parse(ticker)
	if err != nil {
		return nil, false
	}
	info, err := c.symbols.Get(context.Background(), t, false)
	if err == nil {
		return info, false
	}
	if errors.Is(err, symbol.ErrNotFound) {
		return nil, false
	}
	c.log.Warn().Err(err).Str("ticker", ticker).Msg("symbol")
	return nil, true
}

// refreshSymbols copies the venue table onto the channels already desired. A table change is then
// visible to the next adopt. A transient lookup leaves that channel as it was.
func (c *Conn) refreshSymbols() {
	if c.symbols == nil {
		return
	}
	c.mu.Lock()
	tickers := map[string]string{}
	for ch, feeds := range c.wanted {
		if !bookFeed(feeds) || len(feeds) == 0 {
			continue
		}
		tickers[ch] = feeds[0].ticker
	}
	c.mu.Unlock()
	got := map[string]symHit{}
	for ch, ticker := range tickers {
		info, keep := c.lookupFeed(ticker)
		if keep {
			continue
		}
		hit := symHit{set: true}
		if info != nil {
			hit.exch = info.ExchTicker
			hit.tick = priceTick(info)
		}
		got[ch] = hit
	}
	if len(got) == 0 {
		return
	}
	c.mu.Lock()
	if c.exch == nil {
		c.exch = map[string]string{}
	}
	if c.tick == nil {
		c.tick = map[string]float64{}
	}
	for ch, hit := range got {
		c.exch[ch] = hit.exch
		c.tick[ch] = hit.tick
	}
	c.mu.Unlock()
}

func priceTick(info *symbol.Info) float64 {
	if info == nil {
		return 0
	}
	tick := info.Filters[symbol.PriceTick]
	if tick <= 0 || math.IsNaN(tick) || math.IsInf(tick, 0) {
		return 0
	}
	return tick
}

func (c *Conn) apply(d *mdctlv1.Desired, over map[string]symHit) bool {
	if d == nil {
		return false
	}
	if c.have && !newerVersion(&c.ver, d.Version) {
		return false
	}
	c.have = true
	c.ver = commonv1.Version{}
	if d.Version != nil {
		c.ver = commonv1.Version{Epoch: d.Version.Epoch, Seq: d.Version.Seq}
	}
	wanted := make(map[string][]feed)
	exch := make(map[string]string)
	tick := make(map[string]float64)
	for _, a := range d.Atoms {
		if a == nil {
			continue
		}
		venue, route, ch, ok := splitAtom(a.AtomId)
		if !ok {
			c.log.Warn().Str("atom", a.AtomId).Msg("atom id")
			continue
		}
		if venue != c.venue || route != c.route {
			continue
		}
		chKey := strings.Clone(ch)
		book := false
		for _, raw := range a.Feeds {
			topic, ticker, ok := splitFeed(raw)
			if !ok {
				c.log.Warn().Str("feed", raw).Str("atom", a.AtomId).Msg("feed key")
				continue
			}
			if topic == string(exchange.TopicOrderBook) || topic == string(exchange.TopicOrderBookDelta) {
				book = true
			}
			addFeed(wanted, chKey, feed{topic: strings.Clone(topic), ticker: strings.Clone(ticker)})
		}
		if hit, ok := over[chKey]; ok && hit.set {
			if hit.exch != "" {
				exch[chKey] = strings.Clone(hit.exch)
			}
			if book {
				tick[chKey] = hit.tick
			}
		} else {
			if t := a.GetExchTicker(); t != "" {
				exch[chKey] = strings.Clone(t)
			}
			// The last book assignment on the channel wins, including a missing tick: that stalls the book.
			if book {
				tick[chKey] = a.GetPriceTick()
			}
		}
	}
	if err := c.vetBooks(wanted); err != nil {
		c.abort(err)
	}
	c.wanted = wanted
	c.exch = exch
	c.tick = tick
	return true
}

// abort records an error Run returns instead of dialing again. The cancel happens after the
// caller drops c.mu: canceling while the lock is held can deadlock a goroutine that needs it to exit.
func (c *Conn) abort(err error) {
	if err == nil || c.fatal != nil {
		return
	}
	c.fatal = err
	c.log.Error().Err(err).Msg("book")
}

// newerVersion reports whether next is strictly after old. A nil version is 0.
func newerVersion(old, next *commonv1.Version) bool {
	var epoch int64
	var seq uint64
	if next != nil {
		epoch, seq = next.Epoch, next.Seq
	}
	if epoch != old.Epoch {
		return epoch > old.Epoch
	}
	return seq > old.Seq
}

// splitAtom splits venue:route:channel on the first two colons. The channel keeps any further
// colon, and any dot.
func splitAtom(id string) (venue, route, channel string, ok bool) {
	i := strings.IndexByte(id, ':')
	if i <= 0 {
		return "", "", "", false
	}
	rest := id[i+1:]
	j := strings.IndexByte(rest, ':')
	if j <= 0 || j == len(rest)-1 {
		return "", "", "", false
	}
	return id[:i], rest[:j], rest[j+1:], true
}

// splitFeed splits a feed key on the first dot. The topic is the part before it, so a topic
// cannot contain a dot; the ticker is the rest and can.
func splitFeed(s string) (topic, ticker string, ok bool) {
	topic, ticker, ok = strings.Cut(s, ".")
	if !ok || topic == "" || ticker == "" {
		return "", "", false
	}
	return topic, ticker, true
}

func addFeed(m map[string][]feed, ch string, f feed) {
	for _, e := range m[ch] {
		if e == f {
			return
		}
	}
	m[ch] = append(m[ch], f)
}

// plan is the diff between wanted and what this connection has acknowledged, plus anything
// already in flight. Subscribe and Unsubscribe page by the session's own limits; the frames
// come back owned by us so the write loop can send them after releasing the lock. The session
// is not safe for concurrent use, and Decode holds the same lock.
//
// A Subscribe error is logged and not retried until the next kick. Retrying here would spin if
// the session rejects the channels outright.
func (c *Conn) plan() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.live
	if l == nil || l.session == nil {
		return nil
	}
	var sub, unsub []string
	for ch := range c.wanted {
		if _, ok := l.observed[ch]; ok {
			continue
		}
		if _, ok := l.subPending[ch]; ok {
			continue
		}
		sub = append(sub, ch)
	}
	for ch := range l.observed {
		_, want := c.wanted[ch]
		_, resub := l.resub[ch]
		// A channel still in desired stays, unless this book is cycling it (D434).
		if want && !resub {
			continue
		}
		if _, ok := l.unsubPending[ch]; ok {
			continue
		}
		unsub = append(unsub, ch)
	}
	sort.Strings(sub)
	sort.Strings(unsub)
	frames := c.queue(l, true, sub)
	return append(frames, c.queue(l, false, unsub)...)
}

func (c *Conn) queue(l *link, subscribe bool, chs []string) [][]byte {
	if len(chs) == 0 {
		return nil
	}
	var (
		reqs []exchange.Request
		err  error
	)
	if subscribe {
		reqs, err = l.session.Subscribe(chs)
	} else {
		reqs, err = l.session.Unsubscribe(chs)
	}
	if err != nil {
		c.log.Warn().Err(err).Bool("subscribe", subscribe).Msg("request")
		return nil
	}
	frames := make([][]byte, 0, len(reqs))
	for _, r := range reqs {
		copied := append([]string(nil), r.Channels...)
		if subscribe {
			l.subInflight[r.ID] = copied
			for _, ch := range copied {
				l.subPending[ch] = struct{}{}
			}
		} else {
			l.unsubInflight[r.ID] = copied
			for _, ch := range copied {
				l.unsubPending[ch] = struct{}{}
			}
		}
		frames = append(frames, bytes.Clone(r.Frame))
	}
	return frames
}

// Ack is a session callback and runs with c.mu held. An id this connection did not send is
// dropped: after a reconnect the new link's maps are empty, so a late ack from the old epoch
// cannot mark a channel observed. A failed ack leaves the channel unobserved, and the poke
// makes the next diff send it again, still paced by the session's rate limits.
func (c *Conn) Ack(id uint64, err error) {
	l := c.live
	if l == nil {
		return
	}
	if chs, ok := l.subInflight[id]; ok {
		delete(l.subInflight, id)
		var acked []string
		for _, ch := range chs {
			delete(l.subPending, ch)
			if err != nil {
				continue
			}
			// Accepted. If desired dropped the channel while the request was in flight, keep
			// it observed so the next diff unsubscribes what the venue actually has.
			l.observed[ch] = struct{}{}
			acked = append(acked, ch)
			// Only the replacement subscribe ends the cycle. The original ack must not
			// cancel a resubscribe that was armed while that request was in flight.
			if _, cycling := l.resub[ch]; cycling {
				delete(l.resub, ch)
				if sh := l.books[ch]; sh != nil {
					sh.holdResub = false
				}
			}
		}
		if c.noteObserved(acked, true) {
			c.statusDirty = true
		}
		c.poke()
		return
	}
	if chs, ok := l.unsubInflight[id]; ok {
		delete(l.unsubInflight, id)
		var gone []string
		for _, ch := range chs {
			delete(l.unsubPending, ch)
			if err != nil {
				continue
			}
			delete(l.observed, ch)
			gone = append(gone, ch)
		}
		if c.noteObserved(gone, false) {
			c.statusDirty = true
		}
		c.poke()
	}
}
