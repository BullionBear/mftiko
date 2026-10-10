package binance

import (
	"fmt"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// tradeStream is one ws-fapi connection (D303). It does not carry private events: UM sends those
// on the listenKey socket. After session.logon the order frames are not signed; the testnet
// rejects a signature on a logged-on session.
type tradeStream struct {
	url string
	key string
	sig *codec.Signer
}

var _ exchange.TradeStream = (*tradeStream)(nil)

func (t *tradeStream) Limits() exchange.TradeLimits {
	return exchange.TradeLimits{
		Rate: []exchange.RateLimit{
			{N: 300, Per: 10 * time.Second},
			{N: 1200, Per: time.Minute},
		},
		Keepalive:   exchange.Keepalive{ServerPing: true, Timeout: 5 * time.Minute},
		MaxLifetime: 24 * time.Hour,
		ReadLimit:   1 << 20,
	}
}

func (t *tradeStream) Ping() []byte { return nil }

func (t *tradeStream) Dial(now time.Time) (*exchange.Dial, error) {
	frame, err := t.logon(now)
	if err != nil {
		return nil, err
	}
	return &exchange.Dial{URL: t.url, Frames: [][]byte{frame}}, nil
}

func (t *tradeStream) logon(now time.Time) ([]byte, error) {
	// The signature payload is the parameters except signature, sorted by name.
	q := codec.Query(nil).Add("apiKey", t.key).AddInt("recvWindow", recvWindow).AddInt("timestamp", now.UnixMilli())
	body := wsCall{
		ID:     "logon",
		Method: "session.logon",
		Params: logonParams{
			APIKey: t.key, RecvWindow: recvWindow,
			Signature: string(t.sig.SignBase64(nil, q)), Timestamp: now.UnixMilli(),
		},
	}
	return codec.Marshal(body)
}

func (t *tradeStream) Submit(now time.Time, id uint64, req *exchange.OrderRequest) ([]byte, error) {
	if id == 0 {
		return nil, fmt.Errorf("binance: request id is 0")
	}
	if err := instrument(req.Instrument); err != nil {
		return nil, err
	}
	if err := checkCloid(req.ClientOrderID); err != nil {
		return nil, err
	}
	if decimal.Has(req.QuoteQty) {
		return nil, fmt.Errorf("binance: USD-M orders take a quantity, not a quote amount: %w", exchange.ErrUnsupported)
	}
	side, err := sideWire(req.Side)
	if err != nil {
		return nil, err
	}
	typ, err := typeWire(req.Type)
	if err != nil {
		return nil, err
	}
	step := req.Instrument.Filters[symbol.QtyStep]
	qty, err := formatDec(req.Qty, step)
	if err != nil {
		return nil, fmt.Errorf("binance: quantity: %w", err)
	}
	p := orderParams{
		NewClientOrderID: req.ClientOrderID,
		NewOrderRespType: "RESULT",
		Quantity:         qty,
		RecvWindow:       recvWindow,
		Side:             side,
		Symbol:           req.Instrument.ExchTicker,
		Timestamp:        now.UnixMilli(),
		Type:             typ,
		PositionSide:     req.Params["positionSide"],
	}
	if req.ReduceOnly {
		p.ReduceOnly = "true"
	}
	if req.Type == exchange.OrderTypeLimit {
		tif, err := tifWire(req.TIF)
		if err != nil {
			return nil, err
		}
		px, err := formatDec(req.Price, req.Instrument.Filters[symbol.PriceTick])
		if err != nil {
			return nil, fmt.Errorf("binance: price: %w", err)
		}
		p.TimeInForce = tif
		p.Price = px
	} else if req.TIF != exchange.TIFUnspecified && req.TIF != exchange.TIFGTC {
		return nil, fmt.Errorf("binance: a market order has no time in force: %w", exchange.ErrUnsupported)
	}
	return codec.Marshal(wsCall{ID: "o" + strconv.FormatUint(id, 10), Method: "order.place", Params: p})
}

func (t *tradeStream) Cancel(now time.Time, id uint64, ref exchange.CancelRef) ([]byte, error) {
	if id == 0 {
		return nil, fmt.Errorf("binance: request id is 0")
	}
	p, err := cancelParamsOf(now, ref)
	if err != nil {
		return nil, err
	}
	return codec.Marshal(wsCall{ID: "c" + strconv.FormatUint(id, 10), Method: "order.cancel", Params: p})
}

func cancelParamsOf(now time.Time, ref exchange.CancelRef) (cancelParams, error) {
	if err := instrument(ref.Instrument); err != nil {
		return cancelParams{}, err
	}
	p := cancelParams{RecvWindow: recvWindow, Symbol: ref.Instrument.ExchTicker, Timestamp: now.UnixMilli()}
	if ref.ClientOrderID != "" {
		if err := checkCloid(ref.ClientOrderID); err != nil {
			return cancelParams{}, err
		}
		p.OrigClientOrderID = ref.ClientOrderID
	}
	if ref.OrderID != "" {
		n, err := strconv.ParseInt(ref.OrderID, 10, 64)
		if err != nil {
			return cancelParams{}, fmt.Errorf("binance: order id %q: %w", ref.OrderID, err)
		}
		p.OrderID = n
	}
	if p.OrigClientOrderID == "" && p.OrderID == 0 {
		return cancelParams{}, fmt.Errorf("binance: cancel names neither order id nor client order id")
	}
	return p, nil
}

func (t *tradeStream) Decode(frame []byte, sink exchange.TradeSink) error {
	var msg wsAnswer
	if err := codec.DecodeFrame(frame, &msg); err != nil || msg.ID == "" {
		return fmt.Errorf("binance: trade frame: %w", errOrUnknown(err))
	}
	if len(msg.RateLimits) > 0 {
		sink.Used(usedOf(msg.RateLimits))
	}
	kind, id, err := requestID(msg.ID)
	if err != nil {
		return err
	}
	ans := answerErr(msg)
	switch kind {
	case 'l':
		if ans != nil {
			return ans
		}
		sink.Ready()
		return nil
	case 'o':
		if ans != nil {
			sink.Submitted(id, nil, ans)
			return nil
		}
		res, err := orderResult(msg.Result)
		if err != nil {
			return err
		}
		sink.Submitted(id, res, nil)
		return nil
	case 'c':
		sink.Canceled(id, ans)
		return nil
	default:
		return fmt.Errorf("binance: trade frame id %q", msg.ID)
	}
}

func errOrUnknown(err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("not a trade response")
}

func requestID(id string) (byte, uint64, error) {
	if id == "logon" {
		return 'l', 0, nil
	}
	if len(id) < 2 || (id[0] != 'o' && id[0] != 'c') {
		return 0, 0, fmt.Errorf("binance: trade frame id %q", id)
	}
	n, err := strconv.ParseUint(id[1:], 10, 64)
	if err != nil || n == 0 {
		return 0, 0, fmt.Errorf("binance: trade frame id %q", id)
	}
	return id[0], n, nil
}

func answerErr(msg wsAnswer) error {
	if msg.Status == 200 && msg.Error == nil {
		return nil
	}
	code, text := 0, ""
	if msg.Error != nil {
		code, text = msg.Error.Code, msg.Error.Msg
	}
	if err := binanceErr(msg.Status, code, text); err != nil {
		return err
	}
	return fmt.Errorf("binance: status %d", msg.Status)
}

func orderResult(raw []byte) (*exchange.OrderResult, error) {
	var o restOrder
	if err := codec.DecodeBody(raw, &o); err != nil {
		return nil, fmt.Errorf("binance: order result: %w", err)
	}
	if o.OrderID == 0 {
		return nil, fmt.Errorf("binance: order result has no order id")
	}
	return &exchange.OrderResult{
		OrderID: strconv.FormatInt(o.OrderID, 10), Status: statusOf(o.Status), ExchTsNs: ms(o.UpdateTime),
	}, nil
}

func usedOf(ls []wsLimit) []exchange.Used {
	out := make([]exchange.Used, len(ls))
	for i, l := range ls {
		bucket := l.RateLimitType
		switch l.RateLimitType {
		case "REQUEST_WEIGHT":
			bucket = weight1m
		case "ORDERS":
			if l.Interval == "SECOND" {
				bucket = orders10s
			} else {
				bucket = orders1m
			}
		}
		out[i] = exchange.Used{Bucket: bucket, N: l.Count}
	}
	return out
}

type wsCall struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type logonParams struct {
	APIKey     string `json:"apiKey"`
	RecvWindow int64  `json:"recvWindow"`
	Signature  string `json:"signature"`
	Timestamp  int64  `json:"timestamp"`
}

type orderParams struct {
	NewClientOrderID string `json:"newClientOrderId,omitempty"`
	NewOrderRespType string `json:"newOrderRespType,omitempty"`
	PositionSide     string `json:"positionSide,omitempty"`
	Price            string `json:"price,omitempty"`
	Quantity         string `json:"quantity,omitempty"`
	RecvWindow       int64  `json:"recvWindow"`
	ReduceOnly       string `json:"reduceOnly,omitempty"`
	Side             string `json:"side"`
	Symbol           string `json:"symbol"`
	TimeInForce      string `json:"timeInForce,omitempty"`
	Timestamp        int64  `json:"timestamp"`
	Type             string `json:"type"`
}

type cancelParams struct {
	OrderID           int64  `json:"orderId,omitempty"`
	OrigClientOrderID string `json:"origClientOrderId,omitempty"`
	RecvWindow        int64  `json:"recvWindow"`
	Symbol            string `json:"symbol"`
	Timestamp         int64  `json:"timestamp"`
}

type wsAnswer struct {
	ID         string    `json:"id"`
	Status     int       `json:"status"`
	Result     codecRaw  `json:"result"`
	Error      *apiError `json:"error"`
	RateLimits []wsLimit `json:"rateLimits"`
}

// codecRaw is a JSON value kept as bytes. sonic decodes it without copying the frame; orderResult
// decodes it again with DecodeBody, which copies the strings the result keeps.
type codecRaw []byte

func (c *codecRaw) UnmarshalJSON(b []byte) error {
	*c = append((*c)[:0], b...)
	return nil
}

type wsLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Count         int    `json:"count"`
}

type restOrder struct {
	OrderID       int64         `json:"orderId"`
	ClientOrderID string        `json:"clientOrderId"`
	Symbol        string        `json:"symbol"`
	Status        string        `json:"status"`
	Side          string        `json:"side"`
	Type          string        `json:"type"`
	OrigQty       codec.Decimal `json:"origQty"`
	Price         codec.Decimal `json:"price"`
	AvgPrice      codec.Decimal `json:"avgPrice"`
	ExecutedQty   codec.Decimal `json:"executedQty"`
	UpdateTime    int64         `json:"updateTime"`
}

func (o restOrder) update() exchange.OrderUpdate {
	return exchange.OrderUpdate{
		Exch: exchOf(o.Symbol), OrderID: strconv.FormatInt(o.OrderID, 10), ClientOrderID: o.ClientOrderID,
		Side: sideOf(o.Side), Type: typeOf(o.Type), Status: statusOf(o.Status),
		Qty: float64(o.OrigQty), Price: float64(o.Price), FilledQty: float64(o.ExecutedQty),
		AvgPrice: float64(o.AvgPrice), ExchTsNs: ms(o.UpdateTime),
	}
}
