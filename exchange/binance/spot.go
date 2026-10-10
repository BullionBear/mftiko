package binance

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// spotTrade is one spot WebSocket API connection (D303). session.logon, then
// userDataStream.subscribe: orders, cancels and private events share the socket.
// After logon the order frames are not signed.
type spotTrade struct {
	url   string
	key   string
	sig   *codec.Signer
	ready bool
}

var _ exchange.TradeStream = (*spotTrade)(nil)

func (t *spotTrade) Limits() exchange.TradeLimits {
	return exchange.TradeLimits{
		Events: true,
		Rate: []exchange.RateLimit{
			{N: 100, Per: 10 * time.Second},
			{N: 200000, Per: 24 * time.Hour},
		},
		// The API answers websocket pings. A quiet session still has to be judged dead.
		Keepalive:   exchange.Keepalive{Interval: 20 * time.Second, Timeout: time.Minute},
		MaxLifetime: 24 * time.Hour,
		ReadLimit:   1 << 20,
	}
}

func (t *spotTrade) Ping() []byte { return nil }

// Dial's frames are session.logon and then userDataStream.subscribe. The worker sends the next
// frame only after the previous one is answered: the venue rejects an order when logon has not
// finished, even if subscribe was written behind it.
func (t *spotTrade) Dial(now time.Time) (*exchange.Dial, error) {
	logon, err := logonFrame(t.key, t.sig, now)
	if err != nil {
		return nil, err
	}
	sub, err := codec.Marshal(wsCall{ID: "subscribe", Method: "userDataStream.subscribe"})
	if err != nil {
		return nil, err
	}
	return &exchange.Dial{URL: t.url, Frames: [][]byte{logon, sub}}, nil
}

func logonFrame(key string, sig *codec.Signer, now time.Time) ([]byte, error) {
	q := codec.Query(nil).Add("apiKey", key).AddInt("recvWindow", recvWindow).AddInt("timestamp", now.UnixMilli())
	body := wsCall{
		ID:     "logon",
		Method: "session.logon",
		Params: logonParams{
			APIKey: key, RecvWindow: recvWindow,
			Signature: string(sig.SignBase64(nil, q)), Timestamp: now.UnixMilli(),
		},
	}
	return codec.Marshal(body)
}

func (t *spotTrade) Submit(now time.Time, id uint64, req *exchange.OrderRequest) ([]byte, error) {
	if id == 0 {
		return nil, fmt.Errorf("binance: request id is 0")
	}
	p, err := spotOrderParamsOf(now, req)
	if err != nil {
		return nil, err
	}
	return codec.Marshal(wsCall{ID: "o" + strconv.FormatUint(id, 10), Method: "order.place", Params: p})
}

func (t *spotTrade) Cancel(now time.Time, id uint64, ref exchange.CancelRef) ([]byte, error) {
	if id == 0 {
		return nil, fmt.Errorf("binance: request id is 0")
	}
	p, err := cancelParamsOf(now, ref, VenueSpot)
	if err != nil {
		return nil, err
	}
	return codec.Marshal(wsCall{ID: "c" + strconv.FormatUint(id, 10), Method: "order.cancel", Params: p})
}

func (t *spotTrade) Decode(frame []byte, sink exchange.TradeSink) error {
	var head struct {
		ID     string    `json:"id"`
		Event  codecRaw  `json:"event"`
		Status int       `json:"status"`
		Error  *apiError `json:"error"`
		Rates  []wsLimit `json:"rateLimits"`
		Result codecRaw  `json:"result"`
	}
	if err := codec.DecodeFrame(frame, &head); err != nil {
		return fmt.Errorf("binance: trade frame: %w", err)
	}
	if len(head.Event) > 0 && head.Event[0] == '{' {
		return t.event(head.Event, sink)
	}
	if head.ID == "" {
		var raw struct {
			E string `json:"e"`
		}
		if err := codec.DecodeFrame(frame, &raw); err == nil && raw.E != "" {
			return t.event(frame, sink)
		}
		return fmt.Errorf("binance: trade frame: %w", errOrUnknown(nil))
	}
	if len(head.Rates) > 0 {
		sink.Used(usedOf(head.Rates))
	}
	kind, id, err := requestID(head.ID)
	if err != nil {
		return err
	}
	ans := answerErr(wsAnswer{Status: head.Status, Error: head.Error})
	switch kind {
	case 'l':
		if ans != nil {
			return ans
		}
		return nil
	case 's':
		if ans != nil {
			return ans
		}
		if !t.ready {
			t.ready = true
			sink.Ready()
		}
		return nil
	case 'o':
		if ans != nil {
			sink.Submitted(id, nil, ans)
			return nil
		}
		res, err := orderResult(head.Result)
		if err != nil {
			return err
		}
		sink.Submitted(id, res, nil)
		return nil
	case 'c':
		sink.Canceled(id, ans)
		return nil
	default:
		return fmt.Errorf("binance: trade frame id %q", head.ID)
	}
}

func (t *spotTrade) event(raw []byte, sink exchange.TradeSink) error {
	var ev struct {
		E string `json:"e"`
	}
	if err := codec.DecodeBody(raw, &ev); err != nil || ev.E == "" {
		if err == nil {
			err = fmt.Errorf("no event name")
		}
		return fmt.Errorf("binance: spot event: %w", err)
	}
	switch ev.E {
	case "executionReport":
		return t.exec(raw, sink)
	case "outboundAccountPosition":
		return t.balances(raw, sink)
	case "eventStreamTerminated":
		sink.Expired("eventStreamTerminated")
		return nil
	default:
		// balanceUpdate is a delta, listStatus is an order list. Neither is a snapshot.
		return nil
	}
}

func (t *spotTrade) exec(raw []byte, sink exchange.TradeSink) error {
	var o spotExec
	if err := codec.DecodeBody(raw, &o); err != nil {
		return fmt.Errorf("binance: executionReport: %w", err)
	}
	cloid := o.ClientID
	if o.OrigID != "" {
		cloid = o.OrigID
	}
	ts := o.Time
	if ts == 0 {
		ts = o.Event
	}
	filled := float64(o.Filled)
	avg := decimal.Absent()
	if filled > 0 && float64(o.Quote) > 0 {
		avg = float64(o.Quote) / filled
	}
	px := float64(o.Price)
	if px == 0 {
		px = decimal.Absent()
	}
	up := exchange.OrderUpdate{
		Exch: spotKey(o.Symbol), OrderID: strconv.FormatInt(o.OrderID, 10), ClientOrderID: cloid,
		Side: sideOf(o.Side), Type: typeOf(o.Type), Status: statusOf(o.Status),
		Qty: float64(o.Qty), Price: px, FilledQty: filled, AvgPrice: avg, ExchTsNs: ms(ts),
	}
	if o.Reason != "" && o.Reason != "NONE" {
		up.RejectReason = o.Reason
	}
	sink.Order(&up)
	if o.Exec != "TRADE" || o.Trade <= 0 || float64(o.LastQty) == 0 {
		return nil
	}
	liq := "taker"
	if o.Maker {
		liq = "maker"
	}
	sink.Fill(&exchange.Fill{
		Exch: spotKey(o.Symbol), FillID: strconv.FormatInt(o.Trade, 10),
		OrderID: strconv.FormatInt(o.OrderID, 10), ClientOrderID: cloid,
		Side: sideOf(o.Side), Price: float64(o.LastPrice), Qty: float64(o.LastQty),
		Fee: float64(o.Fee), FeeAsset: string(o.FeeAsset), Liquidity: liq, ExchTsNs: ms(ts),
	})
	return nil
}

func (t *spotTrade) balances(raw []byte, sink exchange.TradeSink) error {
	var ev struct {
		B []spotWireBal `json:"B"`
	}
	if err := codec.DecodeBody(raw, &ev); err != nil {
		return fmt.Errorf("binance: outboundAccountPosition: %w", err)
	}
	if len(ev.B) == 0 {
		return nil
	}
	bals := make([]exchange.Balance, len(ev.B))
	for i, b := range ev.B {
		bals[i] = exchange.Balance{Asset: b.Asset, Free: float64(b.Free), Locked: float64(b.Locked)}
	}
	sink.Balances(bals)
	return nil
}

func spotKey(sym string) symbol.ExchKey {
	return symbol.ExchKey{Venue: VenueSpot, Category: symbol.Spot, ExchTicker: sym}
}

type spotExec struct {
	Event     int64         `json:"E"`
	Symbol    string        `json:"s"`
	ClientID  string        `json:"c"`
	OrigID    string        `json:"C"`
	Side      string        `json:"S"`
	Type      string        `json:"o"`
	Status    string        `json:"X"`
	Exec      string        `json:"x"`
	Reason    string        `json:"r"`
	Qty       codec.Decimal `json:"q"`
	Price     codec.Decimal `json:"p"`
	Filled    codec.Decimal `json:"z"`
	Quote     codec.Decimal `json:"Z"`
	OrderID   int64         `json:"i"`
	LastQty   codec.Decimal `json:"l"`
	LastPrice codec.Decimal `json:"L"`
	Fee       codec.Decimal `json:"n"`
	FeeAsset  flexString    `json:"N"`
	Time      int64         `json:"T"`
	Trade     int64         `json:"t"`
	Maker     bool          `json:"m"`
}

type spotWireBal struct {
	Asset  string        `json:"a"`
	Free   codec.Decimal `json:"f"`
	Locked codec.Decimal `json:"l"`
}

type spotOrderWire struct {
	NewClientOrderID string `json:"newClientOrderId,omitempty"`
	NewOrderRespType string `json:"newOrderRespType,omitempty"`
	Price            string `json:"price,omitempty"`
	Quantity         string `json:"quantity,omitempty"`
	QuoteOrderQty    string `json:"quoteOrderQty,omitempty"`
	RecvWindow       int64  `json:"recvWindow"`
	Side             string `json:"side"`
	Symbol           string `json:"symbol"`
	TimeInForce      string `json:"timeInForce,omitempty"`
	Timestamp        int64  `json:"timestamp"`
	Type             string `json:"type"`
}

func spotOrderParamsOf(now time.Time, req *exchange.OrderRequest) (spotOrderWire, error) {
	if req == nil {
		return spotOrderWire{}, fmt.Errorf("binance: order is missing")
	}
	if err := instrumentVenue(req.Instrument, VenueSpot); err != nil {
		return spotOrderWire{}, err
	}
	if err := checkCloid(req.ClientOrderID); err != nil {
		return spotOrderWire{}, err
	}
	if req.ReduceOnly {
		return spotOrderWire{}, fmt.Errorf("binance: spot has no reduce-only: %w", exchange.ErrUnsupported)
	}
	if req.Params["positionSide"] != "" {
		return spotOrderWire{}, fmt.Errorf("binance: spot has no position side: %w", exchange.ErrUnsupported)
	}
	side, err := sideWire(req.Side)
	if err != nil {
		return spotOrderWire{}, err
	}
	p := spotOrderWire{
		NewClientOrderID: req.ClientOrderID,
		NewOrderRespType: "RESULT",
		RecvWindow:       recvWindow,
		Side:             side,
		Symbol:           req.Instrument.ExchTicker,
		Timestamp:        now.UnixMilli(),
	}
	switch {
	case req.Type == exchange.OrderTypeLimit && req.TIF == exchange.TIFPostOnly:
		px, err := formatDec(req.Price, req.Instrument.Filters[symbol.PriceTick])
		if err != nil {
			return spotOrderWire{}, fmt.Errorf("binance: price: %w", err)
		}
		qty, err := formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
		if err != nil {
			return spotOrderWire{}, fmt.Errorf("binance: quantity: %w", err)
		}
		p.Type, p.Price, p.Quantity = "LIMIT_MAKER", px, qty
	case req.Type == exchange.OrderTypeLimit:
		tif, err := tifWire(req.TIF)
		if err != nil {
			return spotOrderWire{}, err
		}
		px, err := formatDec(req.Price, req.Instrument.Filters[symbol.PriceTick])
		if err != nil {
			return spotOrderWire{}, fmt.Errorf("binance: price: %w", err)
		}
		qty, err := formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
		if err != nil {
			return spotOrderWire{}, fmt.Errorf("binance: quantity: %w", err)
		}
		p.Type, p.TimeInForce, p.Price, p.Quantity = "LIMIT", tif, px, qty
	case req.Type == exchange.OrderTypeMarket:
		if req.TIF != exchange.TIFUnspecified && req.TIF != exchange.TIFGTC {
			return spotOrderWire{}, fmt.Errorf("binance: a market order has no time in force: %w", exchange.ErrUnsupported)
		}
		p.Type = "MARKET"
		switch {
		case decimal.Has(req.QuoteQty) && !decimal.Has(req.Qty):
			q, err := formatDec(req.QuoteQty, req.Instrument.Filters[symbol.PriceTick])
			if err != nil {
				return spotOrderWire{}, fmt.Errorf("binance: quote quantity: %w", err)
			}
			p.QuoteOrderQty = q
		case decimal.Has(req.Qty) && !decimal.Has(req.QuoteQty):
			qty, err := formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
			if err != nil {
				return spotOrderWire{}, fmt.Errorf("binance: quantity: %w", err)
			}
			p.Quantity = qty
		default:
			return spotOrderWire{}, fmt.Errorf("binance: a market order takes a quantity or a quote amount")
		}
	default:
		_, err := typeWire(req.Type)
		return spotOrderWire{}, err
	}
	return p, nil
}

func spotOrderQuery(req *exchange.OrderRequest) (codec.Query, error) {
	p, err := spotOrderParamsOf(time.Unix(0, 0), req)
	if err != nil {
		return nil, err
	}
	// Timestamp and recvWindow are added by signedCall. The wire struct's copies are not the query.
	q := codec.Query(nil).Add("symbol", p.Symbol).Add("side", p.Side).Add("type", p.Type)
	if p.TimeInForce != "" {
		q = q.Add("timeInForce", p.TimeInForce)
	}
	if p.Quantity != "" {
		q = q.Add("quantity", p.Quantity)
	}
	if p.QuoteOrderQty != "" {
		q = q.Add("quoteOrderQty", p.QuoteOrderQty)
	}
	if p.Price != "" {
		q = q.Add("price", p.Price)
	}
	q = q.Add("newClientOrderId", p.NewClientOrderID).Add("newOrderRespType", p.NewOrderRespType)
	return q, nil
}

func (a *account) cancelQuery(now time.Time, ref exchange.CancelRef) (cancelParams, string, []exchange.Cost, error) {
	venue, path := VenueUM, "/fapi/v1/order"
	cost := orderCost(1, 1)
	switch a.k.name {
	case kindSpot.name:
		venue, path, cost = VenueSpot, "/api/v3/order", spotOrderCost(1, 1)
	case kindUM.name:
	default:
		return cancelParams{}, "", nil, exchange.ErrNotImplemented
	}
	p, err := cancelParamsOf(now, ref, venue)
	if err != nil {
		return cancelParams{}, "", nil, err
	}
	return p, path, cost, nil
}

func (a *account) spotSnapshot(now time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	orders, err := a.signedCall(now, http.MethodGet, "/api/v3/openOrders", nil, weightCost(40), false)
	if err != nil {
		return nil, err
	}
	bal, err := a.signedCall(now, http.MethodGet, "/api/v3/account", nil, weightCost(10), false)
	if err != nil {
		return nil, err
	}
	return []exchange.Call[exchange.Page[exchange.AccountSnapshot]]{
		{Req: orders, Parse: parseSpotOrders},
		{Req: bal, Parse: parseSpotBalances},
	}, nil
}

func parseSpotOrders(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	if resp.Status >= 400 {
		return exchange.Page[exchange.AccountSnapshot]{}, refuse(resp)
	}
	var rows []restOrder
	if err := codec.DecodeBody(resp.Body, &rows); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	orders := make([]exchange.OrderUpdate, len(rows))
	for i, o := range rows {
		orders[i] = o.update(VenueSpot)
	}
	return snapPage(exchange.AccountSnapshot{OpenOrders: orders}), nil
}

func parseSpotBalances(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	if resp.Status >= 400 {
		return exchange.Page[exchange.AccountSnapshot]{}, refuse(resp)
	}
	var body struct {
		Balances []struct {
			Asset  string        `json:"asset"`
			Free   codec.Decimal `json:"free"`
			Locked codec.Decimal `json:"locked"`
		} `json:"balances"`
	}
	if err := codec.DecodeBody(resp.Body, &body); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var bals []exchange.Balance
	for _, b := range body.Balances {
		free, locked := float64(b.Free), float64(b.Locked)
		if free == 0 && locked == 0 {
			continue
		}
		bals = append(bals, exchange.Balance{Asset: b.Asset, Free: free, Locked: locked})
	}
	return snapPage(exchange.AccountSnapshot{Balances: bals}), nil
}

// spotCancelAll is DELETE /api/v3/openOrders. The interface has no account-wide cancel;
// the testnet smoke uses it to leave the account clean.
func (a *account) spotCancelAll(now time.Time, sym string) (exchange.Call[struct{}], error) {
	q := codec.Query(nil).Add("symbol", sym)
	req, err := a.signedCall(now, http.MethodDelete, "/api/v3/openOrders", q, spotOrderCost(1, 1), true)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: req, Parse: parseSpotCancelAll}, nil
}

// parseSpotCancelAll accepts the canceled-order array, and -2011 when the symbol had none.
func parseSpotCancelAll(resp *exchange.HTTPResponse) (struct{}, error) {
	if resp.Status < 400 {
		return struct{}{}, nil
	}
	var e apiError
	if err := codec.DecodeBody(resp.Body, &e); err == nil && e.Code == -2011 {
		return struct{}{}, nil
	}
	return struct{}{}, refuse(resp)
}
