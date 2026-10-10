package okx

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

const (
	prodPublicWS   = "wss://ws.okx.com:8443/ws/v5/public"
	prodBusinessWS = "wss://ws.okx.com:8443/ws/v5/business"

	// channelBytes is the channel text one subscribe message may carry.
	// OKX allows 64 KB of channel names. The JSON args are larger than those
	// strings, so the codec stops at 32 KB of "<channel>:<instId>".
	channelBytes = 32 << 10

	// subPerHour is subscribe, unsubscribe and login requests on one connection.
	subPerHour = 480
)

// market is the public codec. Spot, swap, futures and the coin-margined perp share the two
// sockets; the instId is what differs. Candles are the business route (D403).
type market struct{}

var _ exchange.Market = (*market)(nil)

// klineIntervals are the candles both the business socket and the REST reader serve.
// canon is the platform interval; wire is OKX's bar and the candle channel suffix.
var klineIntervals = []struct{ canon, wire string }{
	{"1m", "1m"}, {"3m", "3m"}, {"5m", "5m"}, {"15m", "15m"}, {"30m", "30m"},
	{"1h", "1H"}, {"2h", "2H"}, {"4h", "4H"}, {"6h", "6H"}, {"12h", "12H"},
	{"1d", "1D"}, {"1w", "1W"}, {"1mo", "1M"},
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
		{Topic: exchange.TopicTicker, Absent: []string{"ChangePct24h"}},
		{Topic: exchange.TopicFundingRate},
		{Topic: exchange.TopicOpenInterest},
		{Topic: exchange.TopicMarkPrice, Absent: []string{"IndexPrice"}},
	}
	for _, iv := range klineIntervals {
		out = append(out, exchange.TopicCaps{Topic: exchange.KlineTopic(iv.canon)})
	}
	return out
}

// Atom: the channel is "<channel>:<instId>", and instId is the instrument's ExchTicker.
// orderbook and orderbook_delta share books. Candles go to business. Funding, open interest
// and mark price are contracts only. Liquidation is not an atom: OKX's liquidation-orders
// channel is one feed per instType, and the event has no instrument to route it by.
func (m *market) Atom(topic exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	id, err := needInst(inst)
	if err != nil {
		return exchange.AtomRef{}, err
	}
	switch topic {
	case exchange.TopicBestQuote:
		return atom("public", "bbo-tbt:"+id), nil
	case exchange.TopicOrderBook, exchange.TopicOrderBookDelta:
		return atom("public", "books:"+id), nil
	case exchange.TopicTrade:
		return atom("public", "trades:"+id), nil
	case exchange.TopicTicker:
		return atom("public", "tickers:"+id), nil
	case exchange.TopicFundingRate:
		if !contractInst(inst) {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		return atom("public", "funding-rate:"+id), nil
	case exchange.TopicOpenInterest:
		if !contractInst(inst) {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		return atom("public", "open-interest:"+id), nil
	case exchange.TopicMarkPrice:
		if !contractInst(inst) {
			return exchange.AtomRef{}, exchange.ErrUnsupported
		}
		return atom("public", "mark-price:"+id), nil
	default:
		return klineAtom(id, topic)
	}
}

func klineAtom(id string, topic exchange.Topic) (exchange.AtomRef, error) {
	rest, ok := strings.CutPrefix(string(topic), exchange.KlinePrefix)
	if !ok {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	wire, ok := wireInterval(rest)
	if !ok {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	return atom("business", "candle"+wire+":"+id), nil
}

func atom(route, channel string) exchange.AtomRef {
	return exchange.AtomRef{Route: route, Channel: channel}
}

func needInst(inst *symbol.Info) (string, error) {
	if inst == nil || inst.ExchTicker == "" {
		return "", errors.New("okx: instrument has no exch ticker")
	}
	return inst.ExchTicker, nil
}

func contractInst(inst *symbol.Info) bool { return inst != nil && inst.ContractSize > 0 }

func (m *market) Routes() []string { return []string{"public", "business"} }

func (m *market) Budget(string) int { return 0 }

func (m *market) Cost(string) int { return 1 }

func (m *market) Endpoint(route, override string) (string, error) {
	base := prodPublicWS
	switch route {
	case "public":
	case "business":
		base = prodBusinessWS
	default:
		return "", fmt.Errorf("okx: route %q", route)
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if override == "" {
		return u.String(), nil
	}
	o, err := url.Parse(override)
	if err != nil || o.Host == "" || (o.Scheme != "ws" && o.Scheme != "wss") {
		return "", fmt.Errorf("okx: endpoint override %q", override)
	}
	u.Scheme, u.Host = o.Scheme, o.Host
	return u.String(), nil
}

func (m *market) NewSession(route string) (exchange.Session, error) {
	switch route {
	case "public", "business":
		return &pubSession{}, nil
	default:
		return nil, fmt.Errorf("okx: route %q", route)
	}
}

// Book: books:<instId> only. The first push is the snapshot (prevSeqId -1, or action
// snapshot). An update applies when its prevSeqId is the previous seqId. OKX's seqId is not
// dense. The checksum field has been fixed at 0 since 2026-06-23, so this is not a BookChecksum;
// a broken chain is BookResync.
func (m *market) Book(channel string) (exchange.BookSync, bool) {
	rest, ok := strings.CutPrefix(channel, "books:")
	if ok && rest != "" && !strings.Contains(rest, ":") {
		return okxBook{}, true
	}
	return nil, false
}

// okxBook sequences books:<instId>. Snapshot replaces the book. A later update applies only
// when Prev is the sequence already held. A gap is a resync; an older seqId is already in.
type okxBook struct{}

func (okxBook) Snapshot() exchange.SnapshotSource { return exchange.SnapshotStream }

func (okxBook) Depth() int { return 0 }

func (okxBook) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if !last.Valid || u == nil {
		return exchange.BookResync
	}
	if u.Prev == last.Last {
		return exchange.BookApply
	}
	if u.Last <= last.Last {
		return exchange.BookSkip
	}
	return exchange.BookResync
}

// pubSession is one public connection. Ids start at 1 and are not reused on it.
type pubSession struct{ n uint64 }

func (s *pubSession) Limits() exchange.SessionLimits {
	return exchange.SessionLimits{
		PerRequestBytes: channelBytes,
		Rate:            []exchange.RateLimit{{N: subPerHour, Per: time.Hour}},
		Keepalive:       exchange.Keepalive{Interval: 20 * time.Second, Timeout: 30 * time.Second},
		ReadLimit:       1 << 20,
	}
}

func (s *pubSession) Ping() []byte { return []byte("ping") }
