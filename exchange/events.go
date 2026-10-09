package exchange

// Sink receives what Session.Decode finds in a frame. ch is the atom's channel (AtomRef.Channel);
// the conn worker maps it to the atom and from there to the feeds it publishes (mdctl.AtomAssignment).
//
// Pointers are valid only during the call: the decoder reuses them. Copy what you keep.
type Sink interface {
	// Ack answers a Request. err is nil when the venue accepted it.
	Ack(id uint64, err error)

	BestQuote(ch string, e *BestQuote)
	Trade(ch string, e *Trade)
	AggTrade(ch string, e *AggTrade)
	Book(ch string, e *BookUpdate)
	Kline(ch string, e *Kline)
	Ticker(ch string, e *Ticker)
	Liquidation(ch string, e *Liquidation)
	FundingRate(ch string, e *FundingRate)
	OpenInterest(ch string, e *OpenInterest)
	Greeks(ch string, e *Greeks)
	MarkPrice(ch string, e *MarkPrice)
	FeedEnd(ch string, e *FeedEnd)
}

// The event types follow md.v1 field for field, except where noted. An optional decimal the venue
// does not send is "".

type BestQuote struct {
	ExchTsNs    int64
	Bid, BidQty string
	Ask, AskQty string
}

type Trade struct {
	ExchTsNs   int64
	TradeID    string
	Price, Qty string
	Side       Side // taker
}

type AggTrade struct {
	ExchTsNs                  int64
	TradeID                   string
	Price, Qty                string
	Side                      Side
	FirstTradeID, LastTradeID string
	MatchCount                uint32
}

// BookUpdate is a venue book message, before the conn worker folds it into the local book. Unlike
// md.v1.OrderBookDelta it carries the venue's sequence fields; they are what BookSync.Check reads.
type BookUpdate struct {
	ExchTsNs   int64
	Bids, Asks []Level
	Snapshot   bool   // a full book that replaces the local state (SnapshotStream venues, or a REST depth)
	First      uint64 // first sequence id covered (Binance U)
	Last       uint64 // last sequence id covered (Binance u, OKX seqId, Bybit u)
	Prev       uint64 // the Last of the previous update, when the venue sends it (Binance futures pu, OKX prevSeqId)
	Checksum   int64  // OKX crc32 as a signed 32-bit value; see BookChecksum
}

type Kline struct {
	ExchTsNs               int64
	Interval               string // canonical: 1m, 4h, 1mo
	OpenTimeNs             int64
	Open, High, Low, Close string
	Volume                 string
	QuoteVolume            string
	Closed                 bool
}

// Ticker is 24h statistics only (D401).
type Ticker struct {
	ExchTsNs                  int64
	Last                      string
	Open24h, High24h, Low24h  string
	Volume24h, QuoteVolume24h string
	ChangePct24h              string
}

type Liquidation struct {
	ExchTsNs   int64
	Side       Side // side of the liquidated position
	Price, Qty string
}

type FundingRate struct {
	ExchTsNs      int64
	Rate          string
	NextFundingNs int64
	IntervalS     uint32 // 0 = not sent
}

type OpenInterest struct {
	ExchTsNs          int64
	OpenInterest      string
	OpenInterestValue string
}

type Greeks struct {
	ExchTsNs                       int64
	Delta, Gamma, Vega, Theta, Rho string
	MarkIV, BidIV, AskIV           string
	MarkPrice, UnderlyingPrice     string
}

type MarkPrice struct {
	ExchTsNs   int64
	MarkPrice  string
	IndexPrice string
}

// FeedEnd: the instrument stopped trading.
type FeedEnd struct {
	Reason string // expired | delisted
	AtNs   int64
}
