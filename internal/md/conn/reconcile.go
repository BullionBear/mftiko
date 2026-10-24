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
// applied immediately by setFeeds; this loop is what follows sym.v1.changed without a new Desired.
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

	wanted, exch, tick, status, hold := c.atomsFrom(feeds)
	if hold {
		return
	}
	c.mu.Lock()
	if !c.feedMode || !slices.Equal(feeds, c.wantFeeds) {
		c.mu.Unlock()
		return
	}
	c.observeStatus(status)
	statusChanged := !sameStatus(c.status, status)
	if statusChanged {
		c.status = status
		c.statusDirty = true
		for name, st := range status {
			if st.state != mdctlv1.FeedState_FEED_STATE_REJECTED && st.reason == "" {
				continue
			}
			c.log.Debug().Str("feed", name).Str("state", st.state.String()).Str("reason", st.reason).Str("channel", st.ch).Msg("feed")
		}
	}
	subSame := sameWanted(c.wanted, wanted) && sameStrings(c.exch, exch) && sameFloats(c.tick, tick)
	var sub, unsub []string
	if !subSame {
		sub, unsub = channelDiff(c.wanted, wanted)
		if err := c.vetBooks(wanted); err != nil {
			c.abort(err)
		}
		c.wanted = wanted
		c.exch = exch
		c.tick = tick
	}
	// Same critical section as the wanted swap: tape follows the feed list (E04-D048).
	c.syncViewsLocked()
	fatal := c.fatal
	cancel := c.stop
	live := c.live != nil
	dirty := c.statusDirty
	c.mu.Unlock()
	c.rebindViews()
	if len(sub) > 0 || len(unsub) > 0 {
		c.log.Info().Strs("subscribe", sub).Strs("unsubscribe", unsub).Msg("reconcile")
	}
	if fatal != nil {
		if cancel != nil {
			cancel()
		}
		return
	}
	if !subSame && live {
		c.poke()
		c.wakeBook()
	}
	if dirty {
		c.publishStatus()
	}
}

const (
	feedNotListed   = "not listed"
	feedUnsupported = "unsupported"
	feedBad         = "bad feed"
	feedInvalid     = "invalid"
)

// feedState is one feed's place in the next Report. ch is the channel it rides on; empty when
// the feed is not subscribed.
type feedState struct {
	state  mdctlv1.FeedState
	reason string
	ch     string
}

// atomsFrom maps feeds to channels with the venue table. hold means a transient lookup: the caller
// keeps the subscription and the previous feed snapshot. status is the classification of every
// feed, including those that are not subscribed.
func (c *Conn) atomsFrom(feeds []string) (wanted map[string][]feed, exch map[string]string, tick map[string]float64, status map[string]feedState, hold bool) {
	wanted = map[string][]feed{}
	exch = map[string]string{}
	tick = map[string]float64{}
	status = map[string]feedState{}
	for _, raw := range feeds {
		topic, ticker, ok := splitFeed(raw)
		if !ok {
			status[raw] = feedState{state: mdctlv1.FeedState_FEED_STATE_REJECTED, reason: feedBad}
			continue
		}
		if exchange.RetiredTopic(topic) {
			status[raw] = feedState{state: mdctlv1.FeedState_FEED_STATE_REJECTED, reason: feedInvalid}
			continue
		}
		info, keep := c.lookupFeed(ticker)
		if keep {
			return nil, nil, nil, nil, true
		}
		if info == nil {
			status[raw] = feedState{state: mdctlv1.FeedState_FEED_STATE_REJECTED, reason: feedNotListed}
			continue
		}
		if !info.IsTradable() {
			reason := string(info.Status)
			if reason == "" {
				reason = "not tradable"
			}
			status[raw] = feedState{state: mdctlv1.FeedState_FEED_STATE_PENDING, reason: reason}
			continue
		}
		ref, err := c.market.Atom(exchange.Topic(topic), info)
		if err != nil || ref.Route != c.route || ref.Channel == "" {
			if err != nil {
				c.log.Debug().Err(err).Str("feed", raw).Msg("atom")
			}
			status[raw] = feedState{state: mdctlv1.FeedState_FEED_STATE_REJECTED, reason: feedUnsupported}
			continue
		}
		ch := strings.Clone(ref.Channel)
		status[raw] = feedState{state: mdctlv1.FeedState_FEED_STATE_PENDING, ch: ch}
		addFeed(wanted, ch, feed{topic: strings.Clone(topic), ticker: strings.Clone(ticker), uid: info.UID})
		if info.ExchTicker != "" {
			exch[ch] = strings.Clone(info.ExchTicker)
		}
		if bookTopic(topic) {
			tick[ch] = priceTick(info)
		}
	}
	return wanted, exch, tick, status, false
}

// observeStatus marks a feed SUBSCRIBED when its channel is already acked on the connection
// that is up. The caller holds c.mu.
func (c *Conn) observeStatus(status map[string]feedState) {
	if c.live == nil {
		return
	}
	for name, st := range status {
		if st.ch == "" || st.state != mdctlv1.FeedState_FEED_STATE_PENDING {
			continue
		}
		if _, ok := c.live.observed[st.ch]; !ok {
			continue
		}
		st.state = mdctlv1.FeedState_FEED_STATE_SUBSCRIBED
		st.reason = ""
		status[name] = st
	}
}

// noteObserved moves the feeds on chs after an exchange ack. on means the channel is now
// observed; otherwise a successful unsubscribe put a subscribed feed back to PENDING.
// The caller holds c.mu.
func (c *Conn) noteObserved(chs []string, on bool) bool {
	if len(chs) == 0 || len(c.status) == 0 {
		return false
	}
	set := map[string]struct{}{}
	for _, ch := range chs {
		set[ch] = struct{}{}
	}
	changed := false
	for name, st := range c.status {
		if st.ch == "" {
			continue
		}
		if _, ok := set[st.ch]; !ok {
			continue
		}
		next := st.state
		reason := st.reason
		if on {
			next = mdctlv1.FeedState_FEED_STATE_SUBSCRIBED
			reason = ""
		} else if st.state == mdctlv1.FeedState_FEED_STATE_SUBSCRIBED {
			next = mdctlv1.FeedState_FEED_STATE_PENDING
		}
		if next == st.state && reason == st.reason {
			continue
		}
		st.state = next
		st.reason = reason
		c.status[name] = st
		changed = true
	}
	return changed
}

func sameStatus(a, b map[string]feedState) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || av != bv {
			return false
		}
	}
	return true
}

// snapshot is the Report to send. The caller holds c.mu.
func (c *Conn) snapshot(epoch uint64, state mdctlv1.ConnState) *mdctlv1.Report {
	names := make([]string, 0, len(c.status))
	for name := range c.status {
		names = append(names, name)
	}
	sort.Strings(names)
	feeds := make([]*mdctlv1.FeedStatus, 0, len(names))
	for _, name := range names {
		st := c.status[name]
		feeds = append(feeds, &mdctlv1.FeedStatus{Feed: name, State: st.state, Reason: st.reason})
	}
	return &mdctlv1.Report{ConnEpoch: epoch, State: state, Feeds: feeds}
}

// currentReport is the snapshot for the connection as it is now. The caller holds c.mu.
func (c *Conn) currentReport() *mdctlv1.Report {
	epoch := c.epoch
	if c.announced > epoch {
		epoch = c.announced
	}
	state := mdctlv1.ConnState_CONN_STATE_DOWN
	switch {
	case c.live != nil:
		state = mdctlv1.ConnState_CONN_STATE_LIVE
	case c.announced > c.epoch || c.epoch == 0:
		state = mdctlv1.ConnState_CONN_STATE_CONNECTING
	}
	return c.snapshot(epoch, state)
}

// publishStatus sends the feed snapshot when one is waiting. A classification that lands while
// the send is in flight is sent before this returns, so the shim's latest report is the newest
// snapshot. It does not run with c.mu held.
func (c *Conn) publishStatus() {
	c.reportMu.Lock()
	defer c.reportMu.Unlock()
	for {
		c.mu.Lock()
		if !c.statusDirty {
			c.mu.Unlock()
			return
		}
		if c.report == nil {
			c.statusDirty = false
			c.mu.Unlock()
			return
		}
		rep := c.currentReport()
		report := c.report
		c.statusDirty = false
		c.mu.Unlock()
		if err := report(rep); err != nil {
			c.log.Warn().Err(err).Msg("report")
			c.mu.Lock()
			c.statusDirty = true
			c.mu.Unlock()
			return
		}
	}
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
