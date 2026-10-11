package conn

import (
	"google.golang.org/protobuf/proto"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
)

// The methods below are exchange.Sink. Decode calls them with c.mu held, on the read goroutine,
// and the frame they read is invalid once Decode returns, so the protobuf is marshaled before
// they return. Strings the decoder borrowed from the frame are copied by the marshal.

var _ exchange.Sink = (*Conn)(nil)

func (c *Conn) BestQuote(ch string, e *exchange.BestQuote) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicBestQuote), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_BestQuote{BestQuote: &mdv1.BestQuote{
			Meta: meta(e.ExchTsNs), Bid: e.Bid, BidQty: e.BidQty, Ask: e.Ask, AskQty: e.AskQty,
		}},
	})
}

func (c *Conn) Trade(ch string, e *exchange.Trade) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicTrade), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_Trade{Trade: &mdv1.Trade{
			Meta: meta(e.ExchTsNs), TradeId: e.TradeID, Price: e.Price, Qty: e.Qty, Side: side(e.Side),
		}},
	})
}

func (c *Conn) AggTrade(ch string, e *exchange.AggTrade) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicAggTrade), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_AggTrade{AggTrade: &mdv1.AggTrade{
			Meta: meta(e.ExchTsNs), TradeId: e.TradeID, Price: e.Price, Qty: e.Qty, Side: side(e.Side),
			FirstTradeId: e.FirstTradeID, LastTradeId: e.LastTradeID, MatchCount: e.MatchCount,
		}},
	})
}

// Book folds one venue update into the local book. The read goroutine is still inside Decode, so
// this only buffers or applies; Depth runs on the book's own goroutine.
func (c *Conn) Book(ch string, u *exchange.BookUpdate) { c.onBook(ch, u) }

func (c *Conn) Kline(ch string, e *exchange.Kline) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.KlineTopic(e.Interval)), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_Kline{Kline: &mdv1.Kline{
			Meta: meta(e.ExchTsNs), Interval: e.Interval, OpenTimeNs: e.OpenTimeNs,
			Open: e.Open, High: e.High, Low: e.Low, Close: e.Close, Volume: e.Volume,
			QuoteVolume: f64p(e.QuoteVolume), Closed: e.Closed,
		}},
	})
}

func (c *Conn) Ticker(ch string, e *exchange.Ticker) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicTicker), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_Ticker{Ticker: &mdv1.Ticker{
			Meta: meta(e.ExchTsNs), Last: f64p(e.Last),
			Open_24H: f64p(e.Open24h), High_24H: f64p(e.High24h), Low_24H: f64p(e.Low24h),
			Volume_24H: f64p(e.Volume24h), QuoteVolume_24H: f64p(e.QuoteVolume24h),
			ChangePct_24H: f64p(e.ChangePct24h),
		}},
	})
}

func (c *Conn) Liquidation(ch string, e *exchange.Liquidation) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicLiquidation), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_Liquidation{Liquidation: &mdv1.Liquidation{
			Meta: meta(e.ExchTsNs), Side: side(e.Side), Price: e.Price, Qty: e.Qty,
		}},
	})
}

func (c *Conn) FundingRate(ch string, e *exchange.FundingRate) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicFundingRate), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_FundingRate{FundingRate: &mdv1.FundingRate{
			Meta: meta(e.ExchTsNs), Rate: e.Rate, NextFundingNs: e.NextFundingNs, IntervalS: u32p(e.IntervalS),
		}},
	})
}

func (c *Conn) OpenInterest(ch string, e *exchange.OpenInterest) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicOpenInterest), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_OpenInterest{OpenInterest: &mdv1.OpenInterest{
			Meta: meta(e.ExchTsNs), OpenInterest: e.OpenInterest, OpenInterestValue: f64p(e.OpenInterestValue),
		}},
	})
}

func (c *Conn) Greeks(ch string, e *exchange.Greeks) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicGreeks), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_Greeks{Greeks: &mdv1.Greeks{
			Meta:  meta(e.ExchTsNs),
			Delta: f64p(e.Delta), Gamma: f64p(e.Gamma), Vega: f64p(e.Vega), Theta: f64p(e.Theta), Rho: f64p(e.Rho),
			MarkIv: f64p(e.MarkIV), BidIv: f64p(e.BidIV), AskIv: f64p(e.AskIV),
			MarkPrice: f64p(e.MarkPrice), UnderlyingPrice: f64p(e.UnderlyingPrice),
		}},
	})
}

func (c *Conn) MarkPrice(ch string, e *exchange.MarkPrice) {
	if e == nil {
		return
	}
	c.publishTopic(ch, string(exchange.TopicMarkPrice), &mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_MarkPrice{MarkPrice: &mdv1.MarkPrice{
			Meta: meta(e.ExchTsNs), MarkPrice: e.MarkPrice, IndexPrice: f64p(e.IndexPrice),
		}},
	})
}

// FeedEnd is not a topic of its own. The instrument stopped, so every feed assigned to the
// channel gets a copy, and each copy takes the next sequence number of that feed.
func (c *Conn) FeedEnd(ch string, e *exchange.FeedEnd) {
	if e == nil || c.live == nil {
		return
	}
	feeds := c.wanted[ch]
	if len(feeds) == 0 {
		return
	}
	payload, err := proto.Marshal(&mdv1.FeedEvent{
		Event: &mdv1.FeedEvent_FeedEnd{FeedEnd: &mdv1.FeedEnd{Reason: e.Reason, AtNs: e.AtNs}},
	})
	if err != nil {
		c.log.Warn().Err(err).Msg("marshal")
		return
	}
	for _, f := range feeds {
		// FeedEnd takes a seq, and therefore moves head, but it is not tape (D446).
		c.deliver(f, payload, false)
	}
}

// publishTopic sends ev on every feed of ch whose topic is topic. A channel that is not in the
// current desired set, or whose assigned feeds are a different topic, is dropped and does not
// consume a sequence number. Acknowledgement is not required: desired is enough, so a trade that
// arrives before the ack is still published.
func (c *Conn) publishTopic(ch, topic string, ev *mdv1.FeedEvent) {
	if c.live == nil {
		return
	}
	var (
		payload []byte
		built   bool
	)
	for _, f := range c.wanted[ch] {
		if f.topic != topic {
			continue
		}
		if !built {
			var err error
			payload, err = proto.Marshal(ev)
			if err != nil {
				c.log.Warn().Err(err).Msg("marshal")
				return
			}
			built = true
		}
		c.send(f, payload)
	}
}

// send takes the next seq for the feed and then publishes. A publish error still consumes the
// number: the subscriber sees a gap instead of a silent duplicate once the publisher recovers.
func (c *Conn) send(f feed, payload []byte) {
	c.deliver(f, payload, true)
}

// deliver hands out the next seq, notes it as head, and optionally stores the message. keep is
// false for FeedEnd. The cursor is noted even when the publish fails, because the seq is spent.
// Decode holds c.mu, so the ring write and the md.f publish share that critical section (D437).
func (c *Conn) deliver(f feed, payload []byte, keep bool) {
	l := c.live
	if l == nil || c.pub == nil {
		return
	}
	key := f.topic + "." + f.ticker
	l.seq[key]++
	seq := l.seq[key]
	mdTs := c.now().UnixNano()
	msg := envelope.Append(nil, mdTs, c.epoch, seq, payload)
	c.notePublish(key, c.epoch, seq, mdTs)
	if keep {
		c.recordTape(key, f.topic, mdTs, c.epoch, seq, msg)
	}
	if err := c.pub(subject(c.inst, f.topic, f.ticker), msg); err != nil {
		c.log.Warn().Err(err).Str("subject", subject(c.inst, f.topic, f.ticker)).Uint64("seq", seq).Msg("publish")
	}
}

func subject(inst, topic, ticker string) string {
	return "md.f." + inst + "." + topic + "." + ticker
}

func meta(ns int64) *mdv1.Meta { return &mdv1.Meta{ExchTsNs: ns} }

func side(s exchange.Side) commonv1.Side { return commonv1.Side(s) }

// f64p is the optional-number encoding: Absent (NaN) is an unset field, never a NaN on the wire.
func f64p(v float64) *float64 {
	if !decimal.Has(v) {
		return nil
	}
	return &v
}

// u32p: a zero interval was not sent (exchange.FundingRate).
func u32p(v uint32) *uint32 {
	if v == 0 {
		return nil
	}
	return &v
}
