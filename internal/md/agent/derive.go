package agent

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Markets is the venue lookup planning needs. MarketsFrom adapts an *exchange.Registry.
type Markets interface {
	Market(venue string) (exchange.Market, bool)
}

// placed is one atom as the last commit left it.
type placed struct {
	Conn       string
	Venue      string
	Route      string
	Channel    string
	Feeds      []string
	Record     bool
	ExchTicker string
	PriceTick  float64
	Cost       int
	URL        string
}

// snapshot is the placement commit last published. ByHash is filled only from etcd at start, so a
// restart sticks an atom back on its conn, and a hash with no owner is forgotten after that.
type snapshot struct {
	Atoms  map[string]placed // atom id -> placement
	Linger map[string]time.Time
	ByHash map[string]string // atom hash -> conn, initial load only
}

// outcome is what one demand key became. Reject is an ack reason; otherwise the feed rides Atom
// on Worker.
type outcome struct {
	Reject  string
	Subject string
	Worker  agent.WorkerKey
	Atom    string
}

// feedIn is one demand key plus the resolution commit decided, or a Reject that skips the market.
type feedIn struct {
	Key    string
	Entry  Entry
	Info   *symbol.Info
	Reject string
}

type derived struct {
	Atoms    map[string]placed
	Linger   map[string]time.Time
	Plans    map[agent.WorkerKey]agent.Plan[Spec, Live]
	Outcomes map[string]outcome
}

type owner struct {
	key  string
	feed string
}

type atomGroup struct {
	venue, route, channel string
	cost                  int
	url                   string
	exch                  string
	tick                  float64
	record                bool
	owners                []owner
}

type routeKey struct {
	venue, route string
}

// assign places feeds onto conn workers. prev is the last commit. An atom whose owners just hit
// zero keeps its previous feeds until linger has passed, so the worker does not unsubscribe in
// the meantime. A later commit at the same moment still sees the timer as just started.
func assign(inst string, now time.Time, linger time.Duration, markets Markets, overrides map[string]string, bookDepth uint32, feeds []feedIn, prev snapshot) derived {
	if linger < 0 {
		linger = 0
	}
	out := derived{
		Atoms:    map[string]placed{},
		Linger:   map[string]time.Time{},
		Plans:    map[agent.WorkerKey]agent.Plan[Spec, Live]{},
		Outcomes: map[string]outcome{},
	}
	groups := map[string]*atomGroup{}
	var ids []string
	for _, f := range feeds {
		sub := feedSubject(inst, f.Entry.Feed)
		if f.Reject != "" {
			out.Outcomes[f.Key] = outcome{Reject: f.Reject, Subject: sub}
			continue
		}
		id, g, reason := classify(markets, f)
		if reason != "" {
			out.Outcomes[f.Key] = outcome{Reject: reason, Subject: sub}
			continue
		}
		cur := groups[id]
		if cur == nil {
			groups[id] = g
			ids = append(ids, id)
			continue
		}
		cur.owners = append(cur.owners, g.owners...)
		if g.record {
			cur.record = true
		}
		if cur.exch == "" {
			cur.exch = g.exch
		}
		if cur.tick <= 0 {
			cur.tick = g.tick
		}
	}
	slices.Sort(ids)

	urls := map[routeKey]string{}
	failed := map[routeKey]bool{}
	var wanted []string
	for _, id := range ids {
		g := groups[id]
		u, ok := endpoint(markets, overrides, urls, failed, g.venue, g.route, storedURL(prev, id))
		if !ok {
			for _, o := range g.owners {
				out.Outcomes[o.key] = outcome{Reject: "endpoint", Subject: feedSubject(inst, o.feed)}
			}
			delete(groups, id)
			continue
		}
		g.url = u
		wanted = append(wanted, id)
	}

	var lingerIDs []string
	for id := range prev.Atoms {
		if _, ok := groups[id]; ok {
			continue
		}
		lingerIDs = append(lingerIDs, id)
	}
	slices.Sort(lingerIDs)
	for _, id := range lingerIDs {
		a := prev.Atoms[id]
		if _, _, _, ok := parseConn(a.Conn); !ok {
			continue
		}
		since, waiting := prev.Linger[id]
		if waiting && now.Sub(since) >= linger {
			continue
		}
		if !waiting {
			since = now
		}
		u, ok := endpoint(markets, overrides, urls, failed, a.Venue, a.Route, a.URL)
		if !ok {
			continue
		}
		a.Feeds = slices.Clone(a.Feeds)
		a.URL = u
		out.Atoms[id] = a
		out.Linger[id] = since
	}

	used := map[routeKey]map[uint32]int{}
	add := func(venue, route string, n uint32, cost int) {
		k := routeKey{venue, route}
		m := used[k]
		if m == nil {
			m = map[uint32]int{}
			used[k] = m
		}
		m[n] += cost
	}
	for _, a := range out.Atoms {
		if v, r, n, ok := parseConn(a.Conn); ok {
			add(v, r, n, a.Cost)
		}
	}

	var fresh []string
	for _, id := range wanted {
		g := groups[id]
		conn := stickyConn(prev, id, g.venue, g.route)
		v, r, n, ok := parseConn(conn)
		if !ok || v != g.venue || r != g.route {
			fresh = append(fresh, id)
			continue
		}
		add(v, r, n, g.cost)
		out.Atoms[id] = g.placed(conn)
	}
	for _, id := range fresh {
		g := groups[id]
		budget := 0
		if m, ok := marketOf(markets, g.venue); ok {
			budget = m.Budget(g.route)
		}
		n, ok := lowest(used[routeKey{g.venue, g.route}], g.cost, budget)
		if !ok {
			for _, o := range g.owners {
				out.Outcomes[o.key] = outcome{Reject: "connection budget", Subject: feedSubject(inst, o.feed)}
			}
			continue
		}
		conn := workerKey(g.venue, g.route, n)
		add(g.venue, g.route, n, g.cost)
		out.Atoms[id] = g.placed(conn)
	}

	for _, id := range wanted {
		a, ok := out.Atoms[id]
		if !ok {
			continue
		}
		wk := agent.WorkerKey(a.Conn)
		for _, o := range groups[id].owners {
			out.Outcomes[o.key] = outcome{Subject: feedSubject(inst, o.feed), Worker: wk, Atom: id}
		}
	}

	byConn := map[string][]string{}
	var conns []string
	for id, a := range out.Atoms {
		if _, ok := byConn[a.Conn]; !ok {
			conns = append(conns, a.Conn)
		}
		byConn[a.Conn] = append(byConn[a.Conn], id)
	}
	slices.Sort(conns)
	for _, conn := range conns {
		atomIDs := byConn[conn]
		slices.Sort(atomIDs)
		v, r, n, ok := parseConn(conn)
		if !ok {
			continue
		}
		live := Live{}
		for _, id := range atomIDs {
			a := out.Atoms[id]
			live.Atoms = append(live.Atoms, LiveAtom{
				ID: id, Feeds: slices.Clone(a.Feeds), Record: a.Record,
				ExchTicker: a.ExchTicker, PriceTick: a.PriceTick,
			})
		}
		out.Plans[agent.WorkerKey(conn)] = agent.Plan[Spec, Live]{
			Spawn: Spec{Venue: v, Route: r, N: n, URL: out.Atoms[atomIDs[0]].URL, BookDepth: bookDepth},
			Live:  live,
			Start: true,
		}
	}
	return out
}

func classify(markets Markets, f feedIn) (string, *atomGroup, string) {
	if f.Info == nil || f.Info.Ticker.Venue == "" {
		return "", nil, "unknown venue"
	}
	venue := f.Info.Ticker.Venue
	m, ok := marketOf(markets, venue)
	if !ok {
		return "", nil, "unknown venue"
	}
	ref, err := m.Atom(f.Entry.Topic, f.Info)
	if err != nil {
		if errors.Is(err, exchange.ErrUnsupported) {
			return "", nil, "unsupported"
		}
		return "", nil, err.Error()
	}
	if ref.Route == "" || ref.Channel == "" {
		return "", nil, "unsupported"
	}
	cost := m.Cost(ref.Channel)
	if cost < 0 {
		cost = 0
	}
	exch, tick := symbolFields(f.Info)
	id := venue + ":" + ref.Route + ":" + ref.Channel
	g := &atomGroup{
		venue: venue, route: ref.Route, channel: ref.Channel, cost: cost,
		exch: exch, tick: tick, record: records(f.Entry.Topic),
		owners: []owner{{key: f.Key, feed: f.Entry.Feed}},
	}
	return id, g, ""
}

func marketOf(markets Markets, venue string) (exchange.Market, bool) {
	if markets == nil {
		return nil, false
	}
	m, ok := markets.Market(venue)
	if !ok || m == nil {
		return nil, false
	}
	return m, true
}

func records(t exchange.Topic) bool {
	switch t {
	case exchange.TopicTrade, exchange.TopicAggTrade, exchange.TopicLiquidation:
		return true
	default:
		return false
	}
}

func (g *atomGroup) placed(conn string) placed {
	seen := map[string]bool{}
	var feeds []string
	for _, o := range g.owners {
		if o.feed == "" || seen[o.feed] {
			continue
		}
		seen[o.feed] = true
		feeds = append(feeds, o.feed)
	}
	slices.Sort(feeds)
	return placed{
		Conn: conn, Venue: g.venue, Route: g.route, Channel: g.channel,
		Feeds: feeds, Record: g.record, ExchTicker: g.exch, PriceTick: g.tick, Cost: g.cost, URL: g.url,
	}
}

// symbolFields is what the conn worker needs and does not look up itself (D426, D433). A missing
// or non-positive price_tick is zero: the worker logs that book and does not keep it.
func symbolFields(info *symbol.Info) (exch string, tick float64) {
	if info == nil {
		return "", 0
	}
	tick = info.Filters[symbol.PriceTick]
	if tick <= 0 || math.IsNaN(tick) || math.IsInf(tick, 0) {
		tick = 0
	}
	return info.ExchTicker, tick
}

func storedURL(prev snapshot, id string) string {
	if prev.Atoms == nil {
		return ""
	}
	return prev.Atoms[id].URL
}

func stickyConn(prev snapshot, id, venue, route string) string {
	if prev.Atoms != nil {
		if a, ok := prev.Atoms[id]; ok && connMatches(a.Conn, venue, route) {
			return a.Conn
		}
	}
	if prev.ByHash == nil {
		return ""
	}
	c := prev.ByHash[state.WorkerHash(id)]
	if connMatches(c, venue, route) {
		return c
	}
	return ""
}

func connMatches(conn, venue, route string) bool {
	v, r, _, ok := parseConn(conn)
	return ok && v == venue && r == route
}

// endpoint returns the websocket URL for a route. A failure keeps a URL already stored for the
// atom; with none, the feeds are rejected and the rest of the commit still runs.
func endpoint(markets Markets, overrides map[string]string, cache map[routeKey]string, failed map[routeKey]bool, venue, route, stored string) (string, bool) {
	k := routeKey{venue, route}
	if u, ok := cache[k]; ok {
		return u, true
	}
	if failed[k] {
		if stored != "" {
			return stored, true
		}
		return "", false
	}
	m, ok := marketOf(markets, venue)
	if !ok {
		if stored != "" {
			return stored, true
		}
		failed[k] = true
		return "", false
	}
	override := ""
	if overrides != nil {
		override = overrides[venue]
	}
	u, err := m.Endpoint(route, override)
	if err != nil || u == "" {
		if stored != "" {
			cache[k] = stored
			return stored, true
		}
		failed[k] = true
		return "", false
	}
	cache[k] = u
	return u, true
}

// lowest is the smallest n on a route with room for cost. budget <= 0 means no limit, so every
// new atom shares n = 0. A cost above a positive budget fits nowhere.
func lowest(used map[uint32]int, cost, budget int) (uint32, bool) {
	if budget > 0 && cost > budget {
		return 0, false
	}
	var maxN uint32
	have := false
	for n := range used {
		if !have || n > maxN {
			maxN = n
			have = true
		}
	}
	var n uint32
	for {
		u := 0
		if used != nil {
			u = used[n]
		}
		if budget <= 0 || u+cost <= budget {
			return n, true
		}
		if n == math.MaxUint32 {
			return 0, false
		}
		if !have || n >= maxN {
			return n + 1, true
		}
		n++
	}
}

func workerKey(venue, route string, n uint32) string {
	return fmt.Sprintf("md/%s/%s/%d", venue, route, n)
}

func parseConn(key string) (venue, route string, n uint32, ok bool) {
	rest, ok := strings.CutPrefix(key, "md/")
	if !ok {
		return "", "", 0, false
	}
	venue, rest, ok = strings.Cut(rest, "/")
	if !ok || venue == "" {
		return "", "", 0, false
	}
	route, ns, ok := strings.Cut(rest, "/")
	if !ok || route == "" || strings.Contains(ns, "/") {
		return "", "", 0, false
	}
	u, err := strconv.ParseUint(ns, 10, 32)
	if err != nil {
		return "", "", 0, false
	}
	return venue, route, uint32(u), true
}
