package bybit

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

const (
	// argBudget is the character budget of one public connection: the sum of the subscribed
	// arg lengths. Cost is len(channel).
	argBudget = 21000

	// spotArgs is how many args one spot subscribe message may carry.
	spotArgs = 10

	// bookLevels is the order book depth orderbook and orderbook_delta share. Depth 1 is the
	// best quote: every push is a snapshot, so it is not a BookSync.
	bookLevels = 50

	prodPublic = "wss://stream.bybit.com/v5/public/"
)

// market is the public codec for spot and linear. Inverse and option have their own public
// paths and are not served here.
type market struct{}

var _ exchange.Market = (*market)(nil)

// klineIntervals are the intervals both public routes push, in the venue's order.
// canon is the platform interval; wire is the arg (kline.<wire>.<symbol>).
var klineIntervals = []struct{ canon, wire string }{
	{"1m", "1"}, {"3m", "3"}, {"5m", "5"}, {"15m", "15"}, {"30m", "30"},
	{"1h", "60"}, {"2h", "120"}, {"4h", "240"}, {"6h", "360"}, {"12h", "720"},
	{"1d", "D"}, {"1w", "W"}, {"1mo", "M"},
}

func wireInterval(canon string) (string, bool) {
	for _, iv := range klineIntervals {
		if iv.canon == canon {
			return iv.wire, true
		}
	}
	return "", false
}

func canonInterval(wire string) (string, bool) {
	for _, iv := range klineIntervals {
		if iv.wire == wire {
			return iv.canon, true
		}
	}
	return "", false
}

func (m *market) Topics() []exchange.TopicCaps {
	out := []exchange.TopicCaps{
		{Topic: exchange.TopicBestQuote},
		{Topic: exchange.TopicTrade},
		{Topic: exchange.TopicOrderBook},
		{Topic: exchange.TopicOrderBookDelta},
		{Topic: exchange.TopicTicker},
		{Topic: exchange.TopicLiquidation},
		{Topic: exchange.TopicFundingRate},
		{Topic: exchange.TopicOpenInterest},
		{Topic: exchange.TopicMarkPrice},
	}
	for _, iv := range klineIntervals {
		out = append(out, exchange.TopicCaps{Topic: exchange.KlineTopic(iv.canon)})
	}
	return out
}

// Atom: the route is spot or linear. The channel is the venue arg, and the same string is a
// different atom on each route (D202): publicTrade.BTCUSDT is both.
// orderbook and orderbook_delta share orderbook.50. On linear, ticker, funding_rate,
// open_interest and mark_price share tickers.<symbol>. Liquidation, funding, open interest
// and mark price are linear only.
func (m *market) Atom(topic exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	route, err := publicRoute(inst)
	if err != nil {
		return exchange.AtomRef{}, err
	}
	sym := inst.ExchTicker
	switch topic {
	case exchange.TopicBestQuote:
		return atom(route, "orderbook.1."+sym), nil
	case exchange.TopicOrderBook, exchange.TopicOrderBookDelta:
		return atom(route, fmt.Sprintf("orderbook.%d.%s", bookLevels, sym)), nil
	case exchange.TopicTrade:
		return atom(route, "publicTrade."+sym), nil
	case exchange.TopicTicker:
		return atom(route, "tickers."+sym), nil
	case exchange.TopicLiquidation:
		if route != "linear" {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		return atom(route, "allLiquidation."+sym), nil
	case exchange.TopicFundingRate, exchange.TopicOpenInterest, exchange.TopicMarkPrice:
		if route != "linear" {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		return atom(route, "tickers."+sym), nil
	default:
		return klineAtom(route, sym, topic)
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
	return atom(route, "kline."+wire+"."+sym), nil
}

func atom(route, channel string) exchange.AtomRef {
	return exchange.AtomRef{Route: route, Channel: channel}
}

// publicRoute is spot or linear. Perp and Future are both linear. Inverse and option are
// not on these sockets.
func publicRoute(inst *symbol.Info) (string, error) {
	cat, err := product(inst)
	if err != nil {
		return "", err
	}
	if cat != "spot" && cat != "linear" {
		return "", fmt.Errorf("bybit: %s has no public route: %w", cat, exchange.ErrUnsupported)
	}
	return cat, nil
}

func (m *market) Routes() []string { return []string{"spot", "linear"} }

func (m *market) Budget(string) int { return argBudget }

func (m *market) Cost(channel string) int { return len(channel) }

func (m *market) Endpoint(route, override string) (string, error) {
	if route != "spot" && route != "linear" {
		return "", fmt.Errorf("bybit: route %q", route)
	}
	u, err := url.Parse(prodPublic + route)
	if err != nil {
		return "", err
	}
	if override == "" {
		return u.String(), nil
	}
	o, err := url.Parse(override)
	if err != nil || o.Host == "" || (o.Scheme != "ws" && o.Scheme != "wss") {
		return "", fmt.Errorf("bybit: endpoint override %q", override)
	}
	u.Scheme, u.Host = o.Scheme, o.Host
	return u.String(), nil
}

func (m *market) NewSession(route string) (exchange.Session, error) {
	switch route {
	case "spot":
		return &pubSession{per: spotArgs, bytes: argBudget}, nil
	case "linear":
		return &pubSession{bytes: argBudget}, nil
	default:
		return nil, fmt.Errorf("bybit: route %q", route)
	}
}

// Book: orderbook.<depth>.<symbol> for depth greater than 1. Snapshot replaces the book.
// A delta applies when its u is the previous u plus one.
func (m *market) Book(channel string) (exchange.BookSync, bool) {
	if bookDepth(channel) > 1 {
		return bybitBook{}, true
	}
	return nil, false
}

func bookDepth(channel string) int {
	rest, ok := strings.CutPrefix(channel, "orderbook.")
	if !ok {
		return 0
	}
	n, _, ok := strings.Cut(rest, ".")
	if !ok {
		return 0
	}
	d, err := strconv.Atoi(n)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// bybitBook sequences orderbook.<depth> for depth above 1. The snapshot's u is the sequence.
// A later delta applies only when its u is one past that. A gap is a resync; an older u is
// already inside the snapshot.
type bybitBook struct{}

func (bybitBook) Snapshot() exchange.SnapshotSource { return exchange.SnapshotStream }

func (bybitBook) Depth() int { return 0 }

func (bybitBook) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if !last.Valid || u == nil {
		return exchange.BookResync
	}
	if u.Last <= last.Last {
		return exchange.BookSkip
	}
	if u.Last == last.Last+1 {
		return exchange.BookApply
	}
	return exchange.BookResync
}

// pubSession is one public connection. Ids start at 1 and are not reused on it.
type pubSession struct {
	per   int
	bytes int
	n     uint64
}

func (s *pubSession) Limits() exchange.SessionLimits {
	return exchange.SessionLimits{
		PerRequest:      s.per,
		PerRequestBytes: s.bytes,
		Rate:            []exchange.RateLimit{{N: 5, Per: time.Second}},
		Keepalive:       exchange.Keepalive{Interval: 20 * time.Second, Timeout: time.Minute},
		ReadLimit:       1 << 20,
	}
}

func (s *pubSession) Ping() []byte { return []byte(`{"op":"ping"}`) }
