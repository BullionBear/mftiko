package codec

import (
	"os"
	"path/filepath"
	"testing"
)

// The fixtures are recorded-shape payloads from each venue's documentation (no venue was contacted), decoded
// into the structs an adapter would declare: a few fields out of many, numbers as Decimal. Frames decode
// with DecodeFrame into a value reused across frames, as a connection's scratch would be; bodies decode with
// DecodeBody into a new value, as a Parse returns one.

type level [2]Decimal

type binanceDepth struct {
	Stream string `json:"stream"`
	Data   struct {
		Event   string  `json:"e"`
		EventMs int64   `json:"E"`
		TxMs    int64   `json:"T"`
		Symbol  string  `json:"s"`
		First   int64   `json:"U"`
		Last    int64   `json:"u"`
		Prev    int64   `json:"pu"`
		Bids    []level `json:"b"`
		Asks    []level `json:"a"`
	} `json:"data"`
}

type binanceAggTrade struct {
	Stream string `json:"stream"`
	Data   struct {
		Event   string  `json:"e"`
		EventMs int64   `json:"E"`
		AggID   int64   `json:"a"`
		Symbol  string  `json:"s"`
		Price   Decimal `json:"p"`
		Qty     Decimal `json:"q"`
		First   int64   `json:"f"`
		Last    int64   `json:"l"`
		TradeMs int64   `json:"T"`
		Maker   bool    `json:"m"`
	} `json:"data"`
}

type binanceBookTicker struct {
	Stream string `json:"stream"`
	Data   struct {
		Update  int64   `json:"u"`
		Symbol  string  `json:"s"`
		BidPx   Decimal `json:"b"`
		BidQty  Decimal `json:"B"`
		AskPx   Decimal `json:"a"`
		AskQty  Decimal `json:"A"`
		TxMs    int64   `json:"T"`
		EventMs int64   `json:"E"`
	} `json:"data"`
}

type okxBooks struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Action string `json:"action"`
	Data   []struct {
		Asks     [][4]Decimal `json:"asks"`
		Bids     [][4]Decimal `json:"bids"`
		Ts       int64        `json:"ts,string"`
		Checksum int32        `json:"checksum"`
		PrevSeq  int64        `json:"prevSeqId"`
		Seq      int64        `json:"seqId"`
	} `json:"data"`
}

type bybitOrderbook struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  struct {
		Symbol string  `json:"s"`
		Bids   []level `json:"b"`
		Asks   []level `json:"a"`
		Update int64   `json:"u"`
		Seq    int64   `json:"seq"`
	} `json:"data"`
	Cts int64 `json:"cts"`
}

type binanceDepthSnapshot struct {
	LastUpdateID int64   `json:"lastUpdateId"`
	EventMs      int64   `json:"E"`
	TxMs         int64   `json:"T"`
	Bids         []level `json:"bids"`
	Asks         []level `json:"asks"`
}

type binanceExchangeInfo struct {
	ServerTime int64 `json:"serverTime"`
	Symbols    []struct {
		Symbol            string `json:"symbol"`
		Pair              string `json:"pair"`
		ContractType      string `json:"contractType"`
		DeliveryDate      int64  `json:"deliveryDate"`
		OnboardDate       int64  `json:"onboardDate"`
		Status            string `json:"status"`
		BaseAsset         string `json:"baseAsset"`
		QuoteAsset        string `json:"quoteAsset"`
		MarginAsset       string `json:"marginAsset"`
		PricePrecision    int    `json:"pricePrecision"`
		QuantityPrecision int    `json:"quantityPrecision"`
		Filters           []struct {
			FilterType string  `json:"filterType"`
			MinPrice   Decimal `json:"minPrice"`
			MaxPrice   Decimal `json:"maxPrice"`
			TickSize   Decimal `json:"tickSize"`
			StepSize   Decimal `json:"stepSize"`
			MinQty     Decimal `json:"minQty"`
			MaxQty     Decimal `json:"maxQty"`
			Notional   Decimal `json:"notional"`
			Limit      int     `json:"limit"`
		} `json:"filters"`
	} `json:"symbols"`
}

type okxInstruments struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		InstID     string  `json:"instId"`
		InstType   string  `json:"instType"`
		InstFamily string  `json:"instFamily"`
		Uly        string  `json:"uly"`
		CtType     string  `json:"ctType"`
		CtVal      Decimal `json:"ctVal"`
		CtMult     Decimal `json:"ctMult"`
		CtValCcy   string  `json:"ctValCcy"`
		SettleCcy  string  `json:"settleCcy"`
		TickSz     Decimal `json:"tickSz"`
		LotSz      Decimal `json:"lotSz"`
		MinSz      Decimal `json:"minSz"`
		Lever      Decimal `json:"lever"`
		Stk        Decimal `json:"stk"`
		ListTime   string  `json:"listTime"`
		ExpTime    string  `json:"expTime"`
		State      string  `json:"state"`
	} `json:"data"`
}

type binanceOrder struct {
	OrderID       int64   `json:"orderId"`
	ClientOrderID string  `json:"clientOrderId"`
	Symbol        string  `json:"symbol"`
	Status        string  `json:"status"`
	Side          string  `json:"side"`
	Type          string  `json:"type"`
	TimeInForce   string  `json:"timeInForce"`
	Price         Decimal `json:"price"`
	OrigQty       Decimal `json:"origQty"`
	ExecutedQty   Decimal `json:"executedQty"`
	AvgPrice      Decimal `json:"avgPrice"`
	CumQuote      Decimal `json:"cumQuote"`
	ReduceOnly    bool    `json:"reduceOnly"`
	UpdateTime    int64   `json:"updateTime"`
}

// fixture is one payload and what it decodes into.
type fixture struct {
	name  string
	frame bool       // a websocket frame (DecodeFrame), else a REST body (DecodeBody)
	new   func() any // a pointer to a new value of the fixture's type
}

var fixtures = []fixture{
	{"binance_um_aggtrade", true, func() any { return new(binanceAggTrade) }},
	{"binance_um_bookticker", true, func() any { return new(binanceBookTicker) }},
	{"binance_um_depth", true, func() any { return new(binanceDepth) }},
	{"okx_books", true, func() any { return new(okxBooks) }},
	{"bybit_orderbook", true, func() any { return new(bybitOrderbook) }},
	{"binance_um_order", false, func() any { return new(binanceOrder) }},
	{"binance_um_depth_snapshot", false, func() any { return new(binanceDepthSnapshot) }},
	{"okx_instruments", false, func() any { return new(okxInstruments) }},
	{"binance_um_exchange_info", false, func() any { return new(binanceExchangeInfo) }},
}

func (f fixture) load(tb testing.TB) []byte {
	tb.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", f.name+".json"))
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// decode decodes b into v the way f is decoded in an adapter.
func (f fixture) decode(b []byte, v any) error {
	if f.frame {
		return DecodeFrame(b, v)
	}
	return DecodeBody(b, v)
}
