package binance

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// market is the public-market codec. UM is implemented; the other products still refuse.
type market struct{ p product }

var _ exchange.Market = (*market)(nil)

// klineInterval is one UM kline the venue pushes and serves on REST.
// canon is the platform interval (kline_<canon>); wire is the stream and the REST parameter.
type klineInterval struct{ canon, wire string }

// UM kline intervals, in the venue's order. 1M is the month; the platform spells it 1mo.
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
	if m.p.venue.Name != VenueUM {
		return nil
	}
	out := []exchange.TopicCaps{
		{Topic: exchange.TopicBestQuote},
		{Topic: exchange.TopicAggTrade},
		{Topic: exchange.TopicOrderBook},
		{Topic: exchange.TopicOrderBookDelta},
		{Topic: exchange.TopicTicker},
		{Topic: exchange.TopicLiquidation},
		// markPrice carries the funding rate and not the interval between fundings.
		{Topic: exchange.TopicFundingRate, Absent: []string{"IntervalS"}},
		{Topic: exchange.TopicMarkPrice},
	}
	for _, iv := range klineIntervals {
		out = append(out, exchange.TopicCaps{Topic: exchange.KlineTopic(iv.canon)})
	}
	return out
}

// Atom: the channel is the venue's subscribe parameter, lowercase symbol included.
// bookTicker and depth are the public route; everything else is market.
// orderbook and orderbook_delta share btcusdt@depth@100ms. mark_price and funding_rate
// share btcusdt@markPrice@1s. Open interest is not pushed (D401).
func (m *market) Atom(topic exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	if m.p.venue.Name != VenueUM {
		return exchange.AtomRef{}, exchange.ErrNotImplemented
	}
	if err := instrument(inst); err != nil {
		return exchange.AtomRef{}, err
	}
	sym := strings.ToLower(inst.ExchTicker)
	switch topic {
	case exchange.TopicBestQuote:
		return atom("public", sym+"@bookTicker"), nil
	case exchange.TopicOrderBook, exchange.TopicOrderBookDelta:
		return atom("public", sym+"@depth@100ms"), nil
	case exchange.TopicAggTrade:
		return atom("market", sym+"@aggTrade"), nil
	case exchange.TopicTicker:
		return atom("market", sym+"@ticker"), nil
	case exchange.TopicLiquidation:
		return atom("market", sym+"@forceOrder"), nil
	case exchange.TopicMarkPrice, exchange.TopicFundingRate:
		return atom("market", sym+"@markPrice@1s"), nil
	default:
		rest, ok := strings.CutPrefix(string(topic), exchange.KlinePrefix)
		if !ok {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		wire, ok := wireInterval(rest)
		if !ok {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		return atom("market", sym+"@kline_"+wire), nil
	}
}

func atom(route, channel string) exchange.AtomRef {
	return exchange.AtomRef{Route: route, Channel: channel}
}

func (m *market) Routes() []string { return m.p.routes }

func (m *market) Budget(string) int { return m.p.budget }

// Cost: Binance counts streams.
func (m *market) Cost(string) int { return 1 }

func (m *market) Endpoint(route, override string) (string, error) {
	if m.p.venue.Name != VenueUM {
		return "", exchange.ErrNotImplemented
	}
	if route != "public" && route != "market" {
		return "", fmt.Errorf("binance: route %q", route)
	}
	u, err := url.Parse("wss://fstream.binance.com/" + route + "/ws")
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

func (m *market) NewSession(route string) (exchange.Session, error) {
	if m.p.venue.Name != VenueUM {
		return nil, exchange.ErrNotImplemented
	}
	if route != "public" && route != "market" {
		return nil, fmt.Errorf("binance: route %q", route)
	}
	return &session{per: m.p.budget}, nil
}

// Book: a diff depth channel (btcusdt@depth, or @100ms / @250ms / @500ms).
// Partial books (depth5, depth10, depth20) are snapshots and are not this sync.
func (m *market) Book(channel string) (exchange.BookSync, bool) {
	if m.p.venue.Name != VenueUM || !diffDepth(channel) {
		return nil, false
	}
	return umBook{}, true
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

// session is one UM public connection. Ids start at 1 and are not reused on it.
type session struct {
	per int
	n   uint64
}

func (s *session) Limits() exchange.SessionLimits {
	return exchange.SessionLimits{
		// One subscribe stays within the connection's stream budget.
		PerRequest: s.per,
		// The venue accepts 10 client messages a second.
		Rate: []exchange.RateLimit{{N: 10, Per: time.Second}},
		// The server pings every 3 minutes and drops a connection that does not pong within 10.
		Keepalive:   exchange.Keepalive{ServerPing: true, Timeout: 10 * time.Minute},
		MaxLifetime: 24 * time.Hour,
		// An all-market array is the largest frame this socket sends.
		ReadLimit: 8 << 20,
	}
}

func (s *session) Ping() []byte { return nil }
