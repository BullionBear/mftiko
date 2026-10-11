package conn

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/exchange"
)

// reconcile re-reads the venue table and derives the atom diff again. A feed-list change is
// applied immediately by setFeeds; this loop is what follows sym.changed without a new Desired.
func (c *Conn) reconcile(ctx context.Context) {
	if c.symbols == nil {
		return
	}
	every := c.reconEvery
	if every <= 0 {
		every = time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.project()
		}
	}
}

// setFeeds stores the agent's topic.UniversalTicker list when d is strictly newer, then derives
// the atoms. feeds may be empty: that unsubscribes whatever the previous list had subscribed.
func (c *Conn) setFeeds(d *mdctlv1.Desired, feeds []string) {
	if c.symbols == nil {
		c.log.Warn().Msg("feeds without a symbol table")
		return
	}
	feeds = normalizeFeeds(feeds)
	c.mu.Lock()
	if c.have && !newerVersion(&c.ver, versionOf(d)) {
		c.mu.Unlock()
		return
	}
	c.have = true
	c.feedMode = true
	c.ver = commonv1.Version{}
	if d != nil && d.Version != nil {
		c.ver = commonv1.Version{Epoch: d.Version.Epoch, Seq: d.Version.Seq}
	}
	c.wantFeeds = feeds
	c.mu.Unlock()
	c.project()
}

func versionOf(d *mdctlv1.Desired) *commonv1.Version {
	if d == nil {
		return nil
	}
	return d.GetVersion()
}

func normalizeFeeds(feeds []string) []string {
	if len(feeds) == 0 {
		return nil
	}
	out := append([]string(nil), feeds...)
	sort.Strings(out)
	return slices.Compact(out)
}

// project derives atoms from the stored feeds and the venue table, then replaces the subscription.
// A transient table read leaves the previous atoms in place. The write loop sends the difference.
func (c *Conn) project() {
	c.mu.Lock()
	if !c.feedMode || c.symbols == nil || c.market == nil {
		c.mu.Unlock()
		return
	}
	feeds := slices.Clone(c.wantFeeds)
	c.mu.Unlock()

	wanted, exch, tick, hold := c.atomsFrom(feeds)
	if hold {
		return
	}
	c.mu.Lock()
	if !c.feedMode || !slices.Equal(feeds, c.wantFeeds) {
		c.mu.Unlock()
		return
	}
	if sameWanted(c.wanted, wanted) && sameStrings(c.exch, exch) && sameFloats(c.tick, tick) {
		c.mu.Unlock()
		return
	}
	sub, unsub := channelDiff(c.wanted, wanted)
	if err := c.vetBooks(wanted); err != nil {
		c.abort(err)
	}
	c.wanted = wanted
	c.exch = exch
	c.tick = tick
	fatal := c.fatal
	cancel := c.stop
	live := c.live != nil
	c.mu.Unlock()
	if len(sub) > 0 || len(unsub) > 0 {
		c.log.Info().Strs("subscribe", sub).Strs("unsubscribe", unsub).Msg("reconcile")
	}
	if fatal != nil {
		if cancel != nil {
			cancel()
		}
		return
	}
	if live {
		c.poke()
		c.wakeBook()
	}
}

// atomsFrom maps feeds to channels with the venue table. hold means a transient lookup: the caller
// keeps the subscription it already has.
func (c *Conn) atomsFrom(feeds []string) (wanted map[string][]feed, exch map[string]string, tick map[string]float64, hold bool) {
	wanted = map[string][]feed{}
	exch = map[string]string{}
	tick = map[string]float64{}
	for _, raw := range feeds {
		topic, ticker, ok := splitFeed(raw)
		if !ok {
			continue
		}
		info, keep := c.lookupFeed(ticker)
		if keep {
			return nil, nil, nil, true
		}
		if info == nil || !info.IsTradable() {
			continue
		}
		ref, err := c.market.Atom(exchange.Topic(topic), info)
		if err != nil || ref.Route != c.route || ref.Channel == "" {
			if err != nil {
				c.log.Debug().Err(err).Str("feed", raw).Msg("atom")
			}
			continue
		}
		ch := strings.Clone(ref.Channel)
		addFeed(wanted, ch, feed{topic: strings.Clone(topic), ticker: strings.Clone(ticker)})
		if info.ExchTicker != "" {
			exch[ch] = strings.Clone(info.ExchTicker)
		}
		if topic == string(exchange.TopicOrderBook) || topic == string(exchange.TopicOrderBookDelta) {
			tick[ch] = priceTick(info)
		}
	}
	return wanted, exch, tick, false
}

func sameWanted(a, b map[string][]feed) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if !slices.Equal(av, b[k]) {
			return false
		}
	}
	return true
}

func sameStrings(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func sameFloats(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func channelDiff(prev, next map[string][]feed) (sub, unsub []string) {
	for ch := range next {
		if _, ok := prev[ch]; !ok {
			sub = append(sub, ch)
		}
	}
	for ch := range prev {
		if _, ok := next[ch]; !ok {
			unsub = append(unsub, ch)
		}
	}
	sort.Strings(sub)
	sort.Strings(unsub)
	return sub, unsub
}
