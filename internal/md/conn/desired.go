package conn

import (
	"bytes"
	"sort"
	"strings"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/exchange"
)

// SetDesired replaces the subscription when d is strictly newer than the one already applied.
// The first call applies even when its version is 0, or absent: "have a version" is tracked on
// its own, so a real 0 is not mistaken for "nothing yet". An older or equal version is ignored,
// which is what makes a stale read from the agent harmless. Atoms for another venue or route are
// dropped. Record is not read; tape is a later ticket.
//
// Safe to call while Run is in progress. A change is picked up by the write loop, which diffs
// against what the venue has already acknowledged.
func (c *Conn) SetDesired(d *mdctlv1.Desired) {
	c.mu.Lock()
	applied := c.apply(d)
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

func (c *Conn) apply(d *mdctlv1.Desired) bool {
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
		if t := a.GetExchTicker(); t != "" {
			exch[chKey] = strings.Clone(t)
		}
		// The last book assignment on the channel wins, including a missing tick: that stalls the book.
		if book {
			tick[chKey] = a.GetPriceTick()
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
		if _, ok := c.wanted[ch]; ok {
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
		for _, ch := range chs {
			delete(l.subPending, ch)
			if err != nil {
				continue
			}
			// Accepted. If desired dropped the channel while the request was in flight, keep
			// it observed so the next diff unsubscribes what the venue actually has.
			l.observed[ch] = struct{}{}
		}
		c.poke()
		return
	}
	if chs, ok := l.unsubInflight[id]; ok {
		delete(l.unsubInflight, id)
		for _, ch := range chs {
			delete(l.unsubPending, ch)
			if err != nil {
				continue
			}
			delete(l.observed, ch)
		}
		c.poke()
	}
}
