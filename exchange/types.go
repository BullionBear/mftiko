package exchange

// Topic is a platform market-data topic: the <topic> of a feed key <topic>.<UniversalTicker> and of
// the md.f subject (D401). A topic cannot contain '.', it is one NATS subject token.
type Topic string

// Platform topics (PROTOCOL §2), mftik's names plus orderbook_delta and mark_price.
// orderbook, the old top-N name, is not a topic and not an alias (D453).
const (
	TopicBestQuote      Topic = "bestquote"
	TopicTrade          Topic = "trade"
	TopicAggTrade       Topic = "aggtrade"
	TopicOrderBookDelta Topic = "orderbook_delta"
	TopicTicker         Topic = "ticker"
	TopicLiquidation    Topic = "liquidation"
	TopicFundingRate    Topic = "funding_rate"
	TopicOpenInterest   Topic = "open_interest"
	TopicGreeks         Topic = "greeks"
	TopicMarkPrice      Topic = "mark_price"
)

// KlinePrefix: a feed key is only <topic>.<ticker>, so a kline's interval rides in the topic,
// "kline_" + the canonical interval (1m, 4h, 1mo, as in md.fetch): kline_1m.Binance_Spot_BTCUSDT.
const KlinePrefix = "kline_"

func KlineTopic(interval string) Topic { return Topic(KlinePrefix + interval) }

// RetiredTopic reports a name the platform no longer accepts (D453).
// "orderbook" was the top-N book. A config or subscription of that name is invalid.
// It is not rewritten to orderbook_delta.
func RetiredTopic(topic string) bool { return topic == "orderbook" }

// TopicCaps is one row of a venue's capability table (D401): the topic is supported, and these
// optional fields of its event are never filled on this venue.
type TopicCaps struct {
	Topic  Topic
	Absent []string // field names of the event type, e.g. "QuoteVolume24h"
}

type Side uint8

const (
	SideUnspecified Side = iota
	SideBuy
	SideSell
)

type OrderType uint8

const (
	OrderTypeUnspecified OrderType = iota
	OrderTypeLimit
	OrderTypeMarket
)

type TimeInForce uint8

const (
	TIFUnspecified TimeInForce = iota
	TIFGTC
	TIFIOC
	TIFFOK
	TIFPostOnly
)

// OrderStatus follows td.v1.OrderStatus: the order as the venue holds it (D610). A cancel in flight
// is the account worker's flag, not a status, so there is no pending-cancel value.
type OrderStatus uint8

const (
	OrderStatusUnspecified OrderStatus = iota
	// OrderStatusPendingNew is the account worker's own mark for a request the venue has not
	// answered. An adapter does not report it.
	OrderStatusPendingNew
	// OrderStatusNew: the venue has the request and did not refuse it at once. It is not on the
	// book yet and may still be rejected.
	OrderStatusNew
	// OrderStatusAccepted: on the book, nothing filled.
	OrderStatusAccepted
	OrderStatusPartiallyFilled
	OrderStatusFilled
	OrderStatusCancelled
	OrderStatusRejected
	OrderStatusExpired
	OrderStatusUnknown
)

// OnBook is the status of an order resting on the book: Accepted, or PartiallyFilled once part of
// it filled. A venue's own pending-cancel state maps here (D610).
func OnBook(filled float64) OrderStatus {
	if filled > 0 {
		return OrderStatusPartiallyFilled
	}
	return OrderStatusAccepted
}

// Level is one price level. Qty 0 deletes the level.
type Level struct {
	Price float64
	Qty   float64
}
