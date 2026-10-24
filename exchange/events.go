package exchange

// Sink receives what Session.Decode finds in a frame. ch is the atom's channel (AtomRef.Channel);
// the conn worker maps it to the atom and from there to the feeds it publishes (mdctl.AtomAssignment).
//
// Pointers are valid only during the call, and so are ch and the strings in what they point to
// (E03-D002): the decoder reuses its structs, and a string may point into the frame, which the conn
// worker reads the next message into once Decode returns. Copy what you keep (strings.Clone).
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
}

// The event types follow md.v1 field for field, except where noted. Numbers are float64 (E00-D008); an
// optional one the venue does not send is decimal.Absent (NaN), an unset field in md.v1.

type BestQuote struct {
	ExchTsNs    int64
	Bid, BidQty float64
	Ask, AskQty float64
}

type Trade struct {
	ExchTsNs   int64
	TradeID    string
	Price, Qty float64
	Side       Side // taker
}

type AggTrade struct {
	ExchTsNs                  int64
	TradeID                   string
	Price, Qty                float64
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
	Open, High, Low, Close float64
	Volume                 float64
	QuoteVolume            float64 // optional
	Closed                 bool
}

// Ticker is 24h statistics only (E04-D001).
type Ticker struct {
	ExchTsNs                  int64
	Last                      float64 // each optional
	Open24h, High24h, Low24h  float64
	Volume24h, QuoteVolume24h float64
	ChangePct24h              float64
}

type Liquidation struct {
	ExchTsNs   int64
	Side       Side // side of the liquidated position
	Price, Qty float64
}

type FundingRate struct {
	ExchTsNs      int64
	Rate          float64
	NextFundingNs int64
	IntervalS     uint32 // 0 = not sent
}

type OpenInterest struct {
	ExchTsNs          int64
	OpenInterest      float64
	OpenInterestValue float64 // optional
}

type Greeks struct {
	ExchTsNs                       int64
	Delta, Gamma, Vega, Theta, Rho float64 // each optional
	MarkIV, BidIV, AskIV           float64
	MarkPrice, UnderlyingPrice     float64
}

type MarkPrice struct {
	ExchTsNs   int64
	MarkPrice  float64
	IndexPrice float64 // optional
}
