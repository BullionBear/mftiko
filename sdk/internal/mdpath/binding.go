package mdpath

import (
	"errors"
	"fmt"
	"strings"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
)

const (
	feedPrefix = "md.v1.f."
	viewPrefix = "md.v1.view."
	fetchRoot  = "md.v1.fetch."

	topicBook = "orderbook_delta"
)

// spec is one feed in a snapshot, before it owns a subscription.
type spec struct {
	key     string
	topic   string
	subject string
	view    string // empty when subject does not start with md.v1.f.
}

func indexFeeds(feeds []*stsctlv1.FeedBinding) (map[string]*spec, error) {
	out := make(map[string]*spec, len(feeds))
	keySubject := make(map[string]string, len(feeds))
	for _, fb := range feeds {
		s, err := parseFeed(fb)
		if err != nil {
			return nil, err
		}
		if prev, ok := keySubject[s.key]; ok && prev != s.subject {
			return nil, fmt.Errorf("mdpath: feed %s is on two subjects", s.key)
		}
		keySubject[s.key] = s.subject
		if prev, ok := out[s.subject]; ok {
			if prev.key != s.key {
				return nil, fmt.Errorf("mdpath: subject %s is listed for two feeds", s.subject)
			}
			continue
		}
		out[s.subject] = s
	}
	return out, nil
}

func parseFeed(fb *stsctlv1.FeedBinding) (*spec, error) {
	if fb == nil {
		return nil, errors.New("mdpath: nil feed")
	}
	key := fb.GetFeed()
	topic, ticker, ok := strings.Cut(key, ".")
	if !ok || topic == "" || ticker == "" {
		return nil, fmt.Errorf("mdpath: feed %q is not topic.ticker", key)
	}
	subject := fb.GetSubject()
	if subject == "" {
		return nil, errors.New("mdpath: empty subject")
	}
	view, _ := viewSubject(subject)
	return &spec{key: key, topic: topic, subject: subject, view: view}, nil
}

// viewSubject replaces a leading md.v1.f. with md.v1.view. (E04-D055). The rest of the
// subject is copied as given. ok is false when the prefix is absent.
func viewSubject(subject string) (string, bool) {
	rest, ok := strings.CutPrefix(subject, feedPrefix)
	if !ok || rest == "" {
		return "", false
	}
	return viewPrefix + rest, true
}

func fetchSubject(venue string) (string, error) {
	if !token(venue) {
		return "", fmt.Errorf("mdpath: venue %q is not a subject token", venue)
	}
	return fetchRoot + venue, nil
}

// token is one NATS subject token (E08-D019): not empty, and no dot, star, greater-than, or space.
func token(s string) bool {
	if s == "" || strings.ContainsAny(s, ".*> \t\n\r") {
		return false
	}
	return true
}

// classify diffs the held subjects against the snapshot (E08-D046). A subject whose feed key
// and topic are unchanged keeps its subscription. Anything else is a drop plus an add.
func classify(cur map[string]*feed, next map[string]*spec) (add, drop []*feed, merged map[string]*feed) {
	merged = make(map[string]*feed, len(next))
	for sub, n := range next {
		if old, ok := cur[sub]; ok && old.key == n.key && old.topic == n.topic && old.view == n.view && old.sub != nil {
			merged[sub] = old
			continue
		}
		f := &feed{key: n.key, topic: n.topic, subject: n.subject, view: n.view}
		f.live.Store(true)
		add = append(add, f)
		merged[sub] = f
		if old, ok := cur[sub]; ok {
			drop = append(drop, old)
		}
	}
	for sub, old := range cur {
		if _, ok := next[sub]; !ok {
			drop = append(drop, old)
		}
	}
	return add, drop, merged
}

func keysOf(bySub map[string]*feed) map[string]*feed {
	out := make(map[string]*feed, len(bySub))
	for _, f := range bySub {
		out[f.key] = f
	}
	return out
}

// newer reports whether next is strictly after the held (epoch, seq). A nil version is zero.
// Epoch is compared first (the same rule as a conn worker's desired version).
func newer(oldEpoch int64, oldSeq uint64, next *commonv1.Version) bool {
	var epoch int64
	var seq uint64
	if next != nil {
		epoch = next.GetEpoch()
		seq = next.GetSeq()
	}
	if epoch != oldEpoch {
		return epoch > oldEpoch
	}
	return seq > oldSeq
}

// wants reports whether topic has a hook. orderbook_delta is not a hook (E08-D005).
func wants(h Hooks, topic string) bool {
	switch topic {
	case "bestquote":
		return h.BestQuote != nil
	case "trade":
		return h.Trade != nil
	case "aggtrade":
		return h.AggTrade != nil
	case "ticker":
		return h.Ticker != nil
	case "liquidation":
		return h.Liquidation != nil
	case "funding_rate":
		return h.FundingRate != nil
	case "open_interest":
		return h.OpenInterest != nil
	case "greeks":
		return h.Greeks != nil
	case "mark_price":
		return h.MarkPrice != nil
	default:
		return strings.HasPrefix(topic, "kline_") && len(topic) > len("kline_") && h.Kline != nil
	}
}
