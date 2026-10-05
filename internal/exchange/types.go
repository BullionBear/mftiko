package exchange

// Symbol is an instrument as the venue names it.
//
// Category is the <Category> token of UniversalTicker (D11). Most venues could do without it, but on
// a unified-account venue the native name alone is ambiguous: Bybit's spot and linear BTCUSDT are
// both "BTCUSDT".
type Symbol struct {
	Category string
	Native   string
}

// Topic is a platform market-data topic: the <topic> of a feed key <topic>.<UniversalTicker> and of
// the md.f subject (D12). A topic cannot contain '.', it is one NATS subject token.
type Topic string

// Platform topics. PROTOCOL has no topic table yet (DECISIONS Q19); these follow the names in D12, D21.
const (
	TopicBestQuote      Topic = "bestquote"
	TopicTrade          Topic = "trade"
	TopicAggTrade       Topic = "aggtrade"
	TopicOrderBook      Topic = "orderbook"
	TopicOrderBookDelta Topic = "orderbook_delta"
	TopicTicker         Topic = "ticker"
	TopicLiquidation    Topic = "liquidation"
	TopicFundingRate    Topic = "fundingrate"
	TopicOpenInterest   Topic = "openinterest"
	TopicGreeks         Topic = "greeks"
	TopicMarkPrice      Topic = "markprice"
)

// KlineTopic returns the topic of a kline feed, "kline_" + the canonical interval (1m, 4h, 1mo, as in
// md.fetch). Provisional until Q19 is settled.
func KlineTopic(interval string) Topic { return Topic("kline_" + interval) }

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
