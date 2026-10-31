package binance

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// market is the public-market codec. UM and spot are implemented; CM and EO still refuse.
type market struct{ p product }

var _ exchange.Market = (*market)(nil)

// klineInterval is one UM kline the venue pushes and serves on REST.
// canon is the platform interval (kline_<canon>); wire is the stream and the REST parameter.
type klineInterval struct{ canon, wire string }

// Kline intervals UM and spot share, in the venue's order. 1M is the month; the platform spells it 1mo.
// Spot also offers 1s. It is not in this table, so neither venue subscribes it.
var klineIntervals = []klineInterval{
	{"1m", "1m"}, {"3m", "3m"}, {"5m", "5m"}, {"15m", "15m"}, {"30m", "30m"},
	{"1h", "1h"}, {"2h", "2h"}, {"4h", "4h"}, {"6h", "6h"}, {"8h", "8h"}, {"12h", "12h"},
	{"1d", "1d"}, {"3d", "3d"}, {"1w", "1w"}, {"1mo", "1M"},
}

func wireInterval(canon string) (string, bool) {
	for _, iv := range klineIntervals {
		if iv.canon == canon {
			return iv.wire, true
		}
	}
	return "", false
}

func canonInterval(wire string) string {
	if wire == "1M" {
		return "1mo"
	}
	return wire
}

func (m *market) Topics() []exchange.TopicCaps {
	switch m.p.venue.Name {
	case VenueUM:
		return umTopics()
	case VenueSpot:
		return spotTopics()
	default:
		return nil
	}
}

func umTopics() []exchange.TopicCaps {
	out := []exchange.TopicCaps{
		{Topic: exchange.TopicBestQuote},
		{Topic: exchange.TopicTrade},
		{Topic: exchange.TopicAggTrade},
		{Topic: exchange.TopicOrderBookDelta},
		{Topic: exchange.TopicTicker},
		{Topic: exchange.TopicLiquidation},
		// markPrice carries the funding rate and not the interval between fundings.
		{Topic: exchange.TopicFundingRate, Absent: []string{"IntervalS"}},
		{Topic: exchange.TopicMarkPrice},
	}
	return appendKlines(out)
}

func spotTopics() []exchange.TopicCaps {
	return appendKlines([]exchange.TopicCaps{
		{Topic: exchange.TopicBestQuote},
		{Topic: exchange.TopicTrade},
		{Topic: exchange.TopicAggTrade},
		{Topic: exchange.TopicOrderBookDelta},
		{Topic: exchange.TopicTicker},
	})
}

func appendKlines(out []exchange.TopicCaps) []exchange.TopicCaps {
	for _, iv := range klineIntervals {
		out = append(out, exchange.TopicCaps{Topic: exchange.KlineTopic(iv.canon)})
	}
	return out
}

// Atom: the channel is the venue's subscribe parameter, lowercase symbol included.
// bookTicker, depth and trade are the public route; everything else is market.
// orderbook_delta subscribes btcusdt@depth@100ms. mark_price and funding_rate
// share btcusdt@markPrice@1s. Open interest is not pushed (E04-D001). There is no top-N topic (E04-D053).
func (m *market) Atom(topic exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	switch m.p.venue.Name {
	case VenueUM:
		return m.umAtom(topic, inst)
	case VenueSpot:
		return m.spotAtom(topic, inst)
	default:
		return exchange.AtomRef{}, exchange.ErrNotImplemented
	}
}

func (m *market) umAtom(topic exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	if err := instrument(inst); err != nil {
		return exchange.AtomRef{}, err
	}
	sym := strings.ToLower(inst.ExchTicker)
	switch topic {
	case exchange.TopicBestQuote:
		return atom("public", sym+"@bookTicker"), nil
	case exchange.TopicOrderBookDelta:
		return atom("public", sym+"@depth@100ms"), nil
	case exchange.TopicTrade:
		return atom("public", sym+"@trade"), nil
	case exchange.TopicAggTrade:
		return atom("market", sym+"@aggTrade"), nil
	case exchange.TopicTicker:
		return atom("market", sym+"@ticker"), nil
	case exchange.TopicLiquidation:
		return atom("market", sym+"@forceOrder"), nil
	case exchange.TopicMarkPrice, exchange.TopicFundingRate:
		return atom("market", sym+"@markPrice@1s"), nil
	default:
		return klineAtom("market", sym, topic)
	}
}

// spotAtom subscribes the one spot route. There is no mark price, liquidation, or open interest.
func (m *market) spotAtom(topic exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	if err := instrumentVenue(inst, VenueSpot); err != nil {
		return exchange.AtomRef{}, err
	}
	sym := strings.ToLower(inst.ExchTicker)
	switch topic {
	case exchange.TopicBestQuote:
		return atom(exchange.RouteDefault, sym+"@bookTicker"), nil
	case exchange.TopicOrderBookDelta:
		return atom(exchange.RouteDefault, sym+"@depth@100ms"), nil
	case exchange.TopicTrade:
		return atom(exchange.RouteDefault, sym+"@trade"), nil
	case exchange.TopicAggTrade:
		return atom(exchange.RouteDefault, sym+"@aggTrade"), nil
	case exchange.TopicTicker:
		return atom(exchange.RouteDefault, sym+"@ticker"), nil
	default:
		return klineAtom(exchange.RouteDefault, sym, topic)
	}
}

func klineAtom(route, sym string, topic exchange.Topic) (exchange.AtomRef, error) {
	rest, ok := strings.CutPrefix(string(topic), exchange.KlinePrefix)
	if !ok {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	wire, ok := wireInterval(rest)
	if !ok {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	return atom(route, sym+"@kline_"+wire), nil
}

func atom(route, channel string) exchange.AtomRef {
	return exchange.AtomRef{Route: route, Channel: channel}
}

func (m *market) Routes() []string { return m.p.routes }

func (m *market) Budget(string) int { return m.p.budget }

// Cost: Binance counts streams.
func (m *market) Cost(string) int { return 1 }

func (m *market) Endpoint(route, override string) (string, error) {
	raw, err := m.endpoint(route)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if override == "" {
		return u.String(), nil
	}
	o, err := url.Parse(override)
	if err != nil || o.Host == "" || (o.Scheme != "ws" && o.Scheme != "wss") {
		return "", fmt.Errorf("binance: endpoint override %q", override)
	}
	u.Scheme, u.Host = o.Scheme, o.Host
	return u.String(), nil
}

func (m *market) endpoint(route string) (string, error) {
	switch m.p.venue.Name {
	case VenueUM:
		if route != "public" && route != "market" {
			return "", fmt.Errorf("binance: route %q", route)
		}
		return "wss://fstream.binance.com/" + route + "/ws", nil
	case VenueSpot:
		if route != exchange.RouteDefault {
			return "", fmt.Errorf("binance: route %q", route)
		}
		return "wss://stream.binance.com:9443/ws", nil
	default:
		return "", exchange.ErrNotImplemented
	}
}

func (m *market) NewSession(route string) (exchange.Session, error) {
	if _, err := m.endpoint(route); err != nil {
		return nil, err
	}
	s := &session{per: m.p.budget, spot: m.p.venue.Name == VenueSpot}
	if s.spot {
		// Five incoming messages a second, and a ping counts. The server pings every 20s and
		// drops the socket if no pong comes back within a minute. A client ping would spend
		// that budget, so the idle limit is the pong deadline and nothing is sent on a timer.
		s.rateN = 5
		s.idle = time.Minute
	}
	return s, nil
}

// Book: a diff depth channel. UM accepts @depth and @100ms / @250ms / @500ms.
// Spot accepts @depth and @100ms. Partial books (depth5, depth10, depth20) are snapshots
// and are not this sync.
func (m *market) Book(channel string) (exchange.BookSync, bool) {
	switch m.p.venue.Name {
	case VenueUM:
		if diffDepth(channel) {
			return umBook{}, true
		}
	case VenueSpot:
		if spotDiff(channel) {
			return spotBookSync{}, true
		}
	}
	return nil, false
}

func diffDepth(ch string) bool {
	i := strings.Index(ch, "@depth")
	if i <= 0 {
		return false
	}
	switch ch[i+len("@depth"):] {
	case "", "@100ms", "@250ms", "@500ms":
		return true
	default:
		return false
	}
}

// umBook sequences USD-M diff depth. The snapshot's lastUpdateId is BookSeq.Last.
// An update whose u is still below that is already inside the snapshot.
// The first update that still overlaps it bridges the gap; after that, pu must equal
// the previous u. A break is a resync.
type umBook struct{}

func (umBook) Snapshot() exchange.SnapshotSource { return exchange.SnapshotREST }

func (umBook) Depth() int { return 1000 }

func (umBook) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if !last.Valid || u == nil {
		return exchange.BookResync
	}
	if u.Last < last.Last {
		return exchange.BookSkip
	}
	if u.Prev == last.Last || (u.First <= last.Last && u.Last >= last.Last) {
		return exchange.BookApply
	}
	return exchange.BookResync
}

// spotBookSync sequences spot diff depth. The snapshot's lastUpdateId is BookSeq.Last.
// An event with u <= lastUpdateId is already inside the snapshot. The next event's U
// is at most one past the previous u: that covers the overlapping first event and the
// exact continuation. There is no pu. A gap is a resync.
type spotBookSync struct{}

func (spotBookSync) Snapshot() exchange.SnapshotSource { return exchange.SnapshotREST }

func (spotBookSync) Depth() int { return 1000 }

func (spotBookSync) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if !last.Valid || u == nil {
		return exchange.BookResync
	}
	if u.Last <= last.Last {
		return exchange.BookSkip
	}
	if u.First > last.Last+1 {
		return exchange.BookResync
	}
	return exchange.BookApply
}

func spotDiff(ch string) bool {
	i := strings.Index(ch, "@depth")
	if i <= 0 {
		return false
	}
	switch ch[i+len("@depth"):] {
	case "", "@100ms":
		return true
	default:
		return false
	}
}

// session is one public connection. Ids start at 1 and are not reused on it.
// A zero rateN and idle are the UM socket: 10 messages a second, 10 minutes to pong.
// A zero spot is UM too: a trade is sent only when its X is MARKET.
type session struct {
	per   int
	n     uint64
	rateN int
	idle  time.Duration
	spot  bool
	// lastTrade is the last t on each @trade channel. The value is a pointer so that an update
	// never assigns the map: that would store the frame's key in place of the cloned one.
	lastTrade map[string]*int64
	// dropped counts UM trades whose X is not MARKET.
	dropped uint64
}

func (s *session) Limits() exchange.SessionLimits {
	n, idle := s.rateN, s.idle
	if n == 0 {
		n = 10
	}
	if idle == 0 {
		idle = 10 * time.Minute
	}
	return exchange.SessionLimits{
		// One subscribe stays within the connection's stream budget.
		PerRequest: s.per,
		Rate:       []exchange.RateLimit{{N: n, Per: time.Second}},
		// The server pings. Timeout is how long a missing pong may last.
		Keepalive:   exchange.Keepalive{ServerPing: true, Timeout: idle},
		MaxLifetime: 24 * time.Hour,
		// An all-market array is the largest frame this socket sends.
		ReadLimit: 8 << 20,
	}
}

func (s *session) Ping() []byte { return nil }
