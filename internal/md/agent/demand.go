package agent

import (
	"errors"
	"strings"

	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// Reasons Decode returns. Sync writes them on the ack.
var (
	errBadDemand    = errors.New("bad demand")
	errNotFeed      = errors.New("not a feed")
	errBadFeed      = errors.New("bad feed key")
	errFeedMismatch = errors.New("feed key mismatch")
)

// Entry is one feed demand. Feed is topic.UniversalTicker, the etcd entry's name.
type Entry struct {
	Session string
	Feed    string
	Topic   exchange.Topic
	Ticker  symbol.UniversalTicker
}

// Decode reads one demand entry. A selector or an account entry is not a feed: this agent does
// not evaluate it. The value is a state.v1.DemandEntry, and its feed must be the key's.
func Decode(key string, value []byte) (Entry, error) {
	session, entry, ok := splitDemand(key)
	if !ok {
		return Entry{}, errBadDemand
	}
	kind, feedKey, ok := strings.Cut(entry, "/")
	if !ok || kind != "feed" || feedKey == "" {
		return Entry{}, errNotFeed
	}
	d := &statev1.DemandEntry{}
	if err := proto.Unmarshal(value, d); err != nil {
		return Entry{}, errBadDemand
	}
	if d.GetFeed() == "" {
		return Entry{}, errNotFeed
	}
	if d.GetFeed() != feedKey {
		return Entry{}, errFeedMismatch
	}
	topic, ticker, ok := splitFeed(feedKey)
	if !ok {
		return Entry{}, errBadFeed
	}
	t, err := symbol.Parse(ticker)
	if err != nil {
		return Entry{}, errBadFeed
	}
	return Entry{Session: session, Feed: feedKey, Topic: exchange.Topic(topic), Ticker: t}, nil
}

// splitDemand splits /mftiko/v1/md/{inst}/demand/{session}/{entry}. entry is feed/{feed key} or
// select/{name}, kept whole for the ack key.
func splitDemand(key string) (session, entry string, ok bool) {
	const mark = "/demand/"
	i := strings.Index(key, mark)
	if i < 0 {
		return "", "", false
	}
	rest := key[i+len(mark):]
	session, entry, ok = strings.Cut(rest, "/")
	if !ok || session == "" || entry == "" {
		return "", "", false
	}
	return session, entry, true
}

// splitFeed matches the conn worker: the first dot separates the topic from the ticker.
func splitFeed(s string) (topic, ticker string, ok bool) {
	topic, ticker, ok = strings.Cut(s, ".")
	if !ok || topic == "" || ticker == "" {
		return "", "", false
	}
	return topic, ticker, true
}

// feedSubject is the ack subject. The feed key is already topic.ticker (P14).
func feedSubject(inst, feed string) string {
	if feed == "" {
		return ""
	}
	return "md.f." + inst + "." + feed
}
