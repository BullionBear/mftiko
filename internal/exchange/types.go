package exchange

// Topic is a platform market-data topic: the <topic> of a feed key <topic>.<UniversalTicker> and of
// the md.f subject (D12). A topic cannot contain '.', it is one NATS subject token.
type Topic string

// Platform topics (PROTOCOL §2), mftik's names plus orderbook_delta and mark_price.
const (
	TopicBestQuote      Topic = "bestquote"
	TopicTrade          Topic = "trade"
	TopicAggTrade       Topic = "aggtrade"
	TopicOrderBook      Topic = "orderbook"
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

// TopicCaps is one row of a venue's capability table (D12): the topic is supported, and these
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

// OrderStatus follows td.v1.OrderStatus.
type OrderStatus uint8

const (
	OrderStatusUnspecified OrderStatus = iota
	OrderStatusPendingNew
	OrderStatusNew
	OrderStatusPartiallyFilled
	OrderStatusFilled
	OrderStatusPendingCancel
	OrderStatusCancelled
	OrderStatusRejected
	OrderStatusExpired
	OrderStatusUnknown
)

// Level is one price level. Qty "0" deletes the level.
type Level struct {
	Price string
	Qty   string
}
