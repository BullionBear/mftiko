package mdpath

import mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"

// MD is what every market data event carries besides its payload. The values match sdk.MD.
// UID comes from the payload (E08-D065), not from the subject.
type MD struct {
	UID       uint32
	MdTsNs    int64
	ConnEpoch uint64
	Seq       uint64
}

// Feed is one market data event. Data stays valid after the hook returns.
type Feed[T any] struct {
	MD
	Data T
}

type (
	BestQuote    = Feed[*mdv1.BestQuote]
	Trade        = Feed[*mdv1.Trade]
	AggTrade     = Feed[*mdv1.AggTrade]
	Kline        = Feed[*mdv1.Kline]
	Ticker       = Feed[*mdv1.Ticker]
	Liquidation  = Feed[*mdv1.Liquidation]
	FundingRate  = Feed[*mdv1.FundingRate]
	OpenInterest = Feed[*mdv1.OpenInterest]
	Greeks       = Feed[*mdv1.Greeks]
	MarkPrice    = Feed[*mdv1.MarkPrice]
)

// Hooks are the strategy callbacks, one per topic (E08-D005). A nil func means that topic is
// skipped before its payload is decoded. OnKline receives every kline_<interval> topic.
// orderbook_delta has no hook here; Config.Book receives it.
type Hooks struct {
	BestQuote    func(BestQuote)
	Trade        func(Trade)
	AggTrade     func(AggTrade)
	Kline        func(Kline)
	Ticker       func(Ticker)
	Liquidation  func(Liquidation)
	FundingRate  func(FundingRate)
	OpenInterest func(OpenInterest)
	Greeks       func(Greeks)
	MarkPrice    func(MarkPrice)
}

// TapeQuery is one page of a feed's tape (E04-D036). Limit 0 is left for the conn worker to
// read as 1000. Before nil asks for the newest page. SinceNs 0 sets no time bound.
type TapeQuery struct {
	Limit   uint32
	Before  *mdv1.Cursor
	SinceNs int64
}
