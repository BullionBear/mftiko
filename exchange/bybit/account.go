package bybit

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

type account struct {
	cfg exchange.AccountConfig
	signerState
}

var (
	_ exchange.Account            = (*account)(nil)
	_ exchange.CancelOnDisconnect = (*account)(nil)
)

// Limits: orderLinkId is 36 characters. CancelBatch is 5, the smallest category cap (option 5,
// spot 10, linear and inverse 20). The SDK splits to that; a longer list is still sent and the
// venue rejects it (D625). The order budget is 10/s, the tighter of the category limits (spot's
// own create limit is 20/s). A batch consumes one count per order on that budget.
func (a *account) Limits() exchange.AccountLimits {
	return exchange.AccountLimits{
		ClientOrderIDMaxLen: cloidMax,
		CancelBatch:         5,
		OrderRate:           []exchange.RateLimit{{N: 10, Per: time.Second}},
	}
}

func (a *account) Submit(now time.Time, req *exchange.OrderRequest) (exchange.Call[*exchange.OrderResult], error) {
	body, err := createBodyOf(req)
	if err != nil {
		return exchange.Call[*exchange.OrderResult]{}, err
	}
	httpReq, err := a.postSpan(now, "/v5/order/create", body, orderCost(1), true, req.Validity)
	if err != nil {
		return exchange.Call[*exchange.OrderResult]{}, err
	}
	return exchange.Call[*exchange.OrderResult]{Req: httpReq, Parse: parseCreate}, nil
}

func (a *account) Cancel(now time.Time, ref exchange.CancelRef) (exchange.Call[struct{}], error) {
	body, err := cancelBodyOf(ref)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	httpReq, err := a.postSpan(now, "/v5/order/cancel", body, orderCost(1), true, ref.Validity)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: httpReq, Parse: parseAck}, nil
}

func (a *account) CancelBatch(now time.Time, refs []exchange.CancelRef) (exchange.Call[[]error], error) {
	if len(refs) == 0 {
		return exchange.Call[[]error]{}, fmt.Errorf("bybit: batch cancel is empty")
	}
	cat := ""
	items := make([]cancelItem, 0, len(refs))
	for _, ref := range refs {
		c, err := product(ref.Instrument)
		if err != nil {
			return exchange.Call[[]error]{}, err
		}
		if cat == "" {
			cat = c
		} else if c != cat {
			return exchange.Call[[]error]{}, fmt.Errorf("bybit: batch cancel is one category")
		}
		item, err := cancelItemOf(ref)
		if err != nil {
			return exchange.Call[[]error]{}, err
		}
		items = append(items, item)
	}
	httpReq, err := a.post(now, "/v5/order/cancel-batch", batchBody{Category: cat, Request: items}, orderCost(len(items)), true)
	if err != nil {
		return exchange.Call[[]error]{}, err
	}
	return exchange.Call[[]error]{Req: httpReq, Parse: parseBatch}, nil
}

// QueryOrder is GET /v5/order/realtime by orderLinkId (orderId when the ref has none). With
// either id the venue ignores openOnly and also returns a closed order, among the 500 most recent
// per category. An empty list is ErrNotFound.
func (a *account) QueryOrder(now time.Time, ref exchange.CancelRef) (exchange.Call[*exchange.OrderUpdate], error) {
	cat, err := product(ref.Instrument)
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	item, err := cancelItemOf(ref)
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	pairs := []pair{{"category", cat}, {"symbol", item.Symbol}, {"orderLinkId", item.OrderLinkID}}
	if item.OrderLinkID == "" {
		pairs = append(pairs, pair{"orderId", item.OrderID})
	}
	req, err := a.get(now, "/v5/order/realtime", pairs, readCost())
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	return exchange.Call[*exchange.OrderUpdate]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.OrderUpdate, error) {
		e, err := readEnv(resp)
		if err != nil {
			return nil, err
		}
		var page listPage[orderRow]
		if err := decodeResult(e.Result, &page); err != nil {
			return nil, err
		}
		for _, row := range page.List {
			if (item.OrderLinkID != "" && row.LinkID == item.OrderLinkID) || (item.OrderLinkID == "" && row.OrderID == item.OrderID) {
				up := row.update(page.Category, e.Time)
				return &up, nil
			}
		}
		return nil, fmt.Errorf("bybit: %s %s%s: %w", item.Symbol, item.OrderLinkID, item.OrderID, exchange.ErrNotFound)
	}}, nil
}

// Snapshot reads the unified account. Open orders and positions are one page per category the
// API will list without a symbol: spot, linear USDT, linear USDC, inverse, option. Balances are
// one wallet-balance call. Each page follows nextPageCursor. v5 has no read of the position mode:
// an order or a position with positionIdx 1 or 2 is hedge mode, so the orders and positions pages
// carry HedgeMode (D617).
func (a *account) Snapshot(now time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	orders := []struct{ cat, settle string }{
		{"spot", ""},
		{"linear", "USDT"},
		{"linear", "USDC"},
		{"inverse", ""},
		{"option", ""},
	}
	pos := []struct{ cat, settle string }{
		{"linear", "USDT"},
		{"linear", "USDC"},
		{"inverse", ""},
		{"option", ""},
	}
	out := make([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], 0, len(orders)+len(pos)+1)
	for _, o := range orders {
		c, err := a.openOrders(now, o.cat, o.settle, "")
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	for _, p := range pos {
		c, err := a.positions(now, p.cat, p.settle, "")
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	bal, err := a.balances(now)
	if err != nil {
		return nil, err
	}
	out = append(out, bal)
	return out, nil
}

func (a *account) Leverage(now time.Time, inst *symbol.Info) (exchange.Call[float64], error) {
	cat, err := product(inst)
	if err != nil {
		return exchange.Call[float64]{}, err
	}
	if cat == "spot" {
		return exchange.Call[float64]{}, fmt.Errorf("bybit: spot has no leverage: %w", exchange.ErrUnsupported)
	}
	sym := inst.ExchTicker
	req, err := a.get(now, "/v5/position/list", []pair{{"category", cat}, {"symbol", sym}}, readCost())
	if err != nil {
		return exchange.Call[float64]{}, err
	}
	return exchange.Call[float64]{Req: req, Parse: func(resp *exchange.HTTPResponse) (float64, error) {
		rows, _, err := decodePositions(resp)
		if err != nil {
			return 0, err
		}
		for _, row := range rows {
			if row.Symbol != sym || row.Lev == "" {
				continue
			}
			v, err := strconv.ParseFloat(row.Lev, 64)
			if err != nil {
				return 0, fmt.Errorf("bybit: leverage %q: %w", row.Lev, err)
			}
			return v, nil
		}
		return 0, fmt.Errorf("bybit: %s has no leverage", sym)
	}}, nil
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	sig, err := a.signer()
	if err != nil {
		return nil, err
	}
	return &privateStream{ws: a.privateURL(), key: a.cfg.Credentials.Key, sig: sig}, nil
}

// NewTradeStream is the order socket. The worker sends Submit and Cancel over it. CancelBatch
// stays on REST.
func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	sig, err := a.signer()
	if err != nil {
		return nil, err
	}
	w, err := windowOf(a.cfg)
	if err != nil {
		return nil, err
	}
	return &tradeStream{ws: a.tradeURL(), key: a.cfg.Credentials.Key, sig: sig, window: w}, nil
}

// ArmCancelOnDisconnect sets Disconnected Cancel Protection. syms is ignored: the switch is
// account-wide. The body is timeWindow alone, which is the documented request; product defaults
// to OPTIONS. The window is whole seconds in [3, 300]. Zero is not a disable: the venue's range
// starts at 3, and a UID without DCP enabled is refused with 3500716.
func (a *account) ArmCancelOnDisconnect(now time.Time, timeout time.Duration, _ []*symbol.Info) (exchange.Call[struct{}], error) {
	if timeout%time.Second != 0 {
		return exchange.Call[struct{}]{}, fmt.Errorf("bybit: disconnect window is whole seconds")
	}
	sec := int(timeout / time.Second)
	if sec < 3 || sec > 300 {
		return exchange.Call[struct{}]{}, fmt.Errorf("bybit: disconnect window must be from 3 to 300 seconds, got %d", sec)
	}
	req, err := a.post(now, "/v5/order/disconnected-cancel-all", dcpBody{TimeWindow: sec}, []exchange.Cost{{Bucket: "dcp", N: 1}}, true)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: req, Parse: parseAck}, nil
}

// hedgeIdx: positionIdx 1 (buy side) and 2 (sell side) exist only in hedge mode; 0 is one-way.
func hedgeIdx(idx int) bool { return idx == 1 || idx == 2 }

type createBody struct {
	Category    string `json:"category"`
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`
	OrderType   string `json:"orderType"`
	Qty         string `json:"qty"`
	Price       string `json:"price,omitempty"`
	TimeInForce string `json:"timeInForce,omitempty"`
	OrderLinkID string `json:"orderLinkId"`
	ReduceOnly  bool   `json:"reduceOnly,omitempty"`
	MarketUnit  string `json:"marketUnit,omitempty"`
	PositionIdx *int   `json:"positionIdx,omitempty"`
	IsLeverage  *int   `json:"isLeverage,omitempty"`
}

func createBodyOf(req *exchange.OrderRequest) (createBody, error) {
	if req == nil {
		return createBody{}, fmt.Errorf("bybit: order is missing")
	}
	cat, err := product(req.Instrument)
	if err != nil {
		return createBody{}, err
	}
	if err := checkCloid(req.ClientOrderID); err != nil {
		return createBody{}, err
	}
	side, err := sideWire(req.Side)
	if err != nil {
		return createBody{}, err
	}
	typ, err := typeWire(req.Type)
	if err != nil {
		return createBody{}, err
	}
	if req.ReduceOnly && cat == "spot" {
		return createBody{}, fmt.Errorf("bybit: spot has no reduce-only: %w", exchange.ErrUnsupported)
	}
	body := createBody{
		Category: cat, Symbol: req.Instrument.ExchTicker, Side: side, OrderType: typ,
		OrderLinkID: req.ClientOrderID, ReduceOnly: req.ReduceOnly,
	}
	switch req.Type {
	case exchange.OrderTypeLimit:
		px, err := formatDec(req.Price, req.Instrument.Filters[symbol.PriceTick])
		if err != nil {
			return createBody{}, fmt.Errorf("bybit: price: %w", err)
		}
		tif, err := tifWire(req.TIF)
		if err != nil {
			return createBody{}, err
		}
		qty, err := formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
		if err != nil {
			return createBody{}, fmt.Errorf("bybit: quantity: %w", err)
		}
		if decimal.Has(req.QuoteQty) {
			return createBody{}, fmt.Errorf("bybit: a limit order takes a quantity, not a quote amount: %w", exchange.ErrUnsupported)
		}
		body.Qty, body.Price, body.TimeInForce = qty, px, tif
	case exchange.OrderTypeMarket:
		if req.TIF != exchange.TIFUnspecified && req.TIF != exchange.TIFIOC {
			return createBody{}, fmt.Errorf("bybit: a market order is IOC: %w", exchange.ErrUnsupported)
		}
		qty, unit, err := marketQty(cat, req)
		if err != nil {
			return createBody{}, err
		}
		body.Qty, body.MarketUnit = qty, unit
	}
	if s := req.Params["positionIdx"]; s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return createBody{}, fmt.Errorf("bybit: positionIdx %q: %w", s, err)
		}
		body.PositionIdx = &n
	}
	if s := req.Params["isLeverage"]; s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return createBody{}, fmt.Errorf("bybit: isLeverage %q: %w", s, err)
		}
		body.IsLeverage = &n
	}
	return body, nil
}

func marketQty(cat string, req *exchange.OrderRequest) (qty, unit string, err error) {
	hasQty, hasQuote := decimal.Has(req.Qty), decimal.Has(req.QuoteQty)
	if hasQty && hasQuote {
		return "", "", fmt.Errorf("bybit: an order takes a quantity or a quote amount, not both")
	}
	if cat != "spot" {
		if hasQuote {
			return "", "", fmt.Errorf("bybit: %s orders take a quantity, not a quote amount: %w", cat, exchange.ErrUnsupported)
		}
		qty, err = formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
		if err != nil {
			return "", "", fmt.Errorf("bybit: quantity: %w", err)
		}
		return qty, "", nil
	}
	switch {
	case hasQuote:
		qty, err = formatDec(req.QuoteQty, quoteStep)
		if err != nil {
			return "", "", fmt.Errorf("bybit: quote quantity: %w", err)
		}
		return qty, "quoteCoin", nil
	case hasQty:
		qty, err = formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
		if err != nil {
			return "", "", fmt.Errorf("bybit: quantity: %w", err)
		}
		return qty, "baseCoin", nil
	default:
		return "", "", fmt.Errorf("bybit: quantity: number is absent")
	}
}

type cancelBody struct {
	Category    string `json:"category"`
	Symbol      string `json:"symbol"`
	OrderID     string `json:"orderId,omitempty"`
	OrderLinkID string `json:"orderLinkId,omitempty"`
}

func cancelBodyOf(ref exchange.CancelRef) (cancelBody, error) {
	cat, err := product(ref.Instrument)
	if err != nil {
		return cancelBody{}, err
	}
	item, err := cancelItemOf(ref)
	if err != nil {
		return cancelBody{}, err
	}
	return cancelBody{Category: cat, Symbol: item.Symbol, OrderID: item.OrderID, OrderLinkID: item.OrderLinkID}, nil
}

type cancelItem struct {
	Symbol      string `json:"symbol"`
	OrderID     string `json:"orderId,omitempty"`
	OrderLinkID string `json:"orderLinkId,omitempty"`
}

type batchBody struct {
	Category string       `json:"category"`
	Request  []cancelItem `json:"request"`
}

func cancelItemOf(ref exchange.CancelRef) (cancelItem, error) {
	if err := instrument(ref.Instrument); err != nil {
		return cancelItem{}, err
	}
	item := cancelItem{Symbol: ref.Instrument.ExchTicker, OrderID: ref.OrderID}
	if ref.ClientOrderID != "" {
		if err := checkCloid(ref.ClientOrderID); err != nil {
			return cancelItem{}, err
		}
		item.OrderLinkID = ref.ClientOrderID
	}
	if item.OrderID == "" && item.OrderLinkID == "" {
		return cancelItem{}, fmt.Errorf("bybit: cancel names neither order id nor client order id")
	}
	return item, nil
}

type dcpBody struct {
	TimeWindow int `json:"timeWindow"`
}

func parseCreate(resp *exchange.HTTPResponse) (*exchange.OrderResult, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var r struct {
		OrderID string `json:"orderId"`
	}
	if err := decodeResult(e.Result, &r); err != nil {
		return nil, err
	}
	if r.OrderID == "" {
		return nil, fmt.Errorf("bybit: empty order id")
	}
	// The ack carries no status. retCode 0 means the venue has the request and did not refuse it:
	// New. The private stream is where the order's later status arrives.
	return &exchange.OrderResult{OrderID: r.OrderID, Status: exchange.OrderStatusNew, ExchTsNs: ms(e.Time)}, nil
}

func parseBatch(resp *exchange.HTTPResponse) ([]error, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var ext struct {
		List []struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		} `json:"list"`
	}
	if err := decodeResult(e.Ext, &ext); err != nil {
		return nil, err
	}
	if len(ext.List) == 0 {
		return nil, fmt.Errorf("bybit: batch cancel has no per-order result")
	}
	out := make([]error, len(ext.List))
	for i, item := range ext.List {
		if item.Code == 0 {
			continue
		}
		out[i] = classify(http.StatusOK, item.Code, item.Msg)
	}
	return out, nil
}

type listPage[T any] struct {
	List     []T    `json:"list"`
	Cursor   string `json:"nextPageCursor"`
	Category string `json:"category"`
}

func (a *account) openOrders(now time.Time, cat, settle, cursor string) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	req, err := a.get(now, "/v5/order/realtime", []pair{
		{"category", cat}, {"settleCoin", settle}, {"openOnly", "0"}, {"limit", "50"}, {"cursor", cursor},
	}, readCost())
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{Req: req, Parse: func(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
		e, err := readEnv(resp)
		if err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		var page listPage[orderRow]
		if err := decodeResult(e.Result, &page); err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		orders := make([]exchange.OrderUpdate, len(page.List))
		hedge := false
		for i, row := range page.List {
			orders[i] = row.update(page.Category, e.Time)
			hedge = hedge || hedgeIdx(row.PositionIdx)
		}
		out := snapPage(exchange.AccountSnapshot{OpenOrders: orders, HedgeMode: hedge})
		if page.Cursor != "" && page.Cursor != cursor && len(page.List) > 0 {
			out.Next = func(now time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return a.openOrders(now, cat, settle, page.Cursor)
			}
		}
		return out, nil
	}}, nil
}

func (a *account) positions(now time.Time, cat, settle, cursor string) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	req, err := a.get(now, "/v5/position/list", []pair{
		{"category", cat}, {"settleCoin", settle}, {"limit", "200"}, {"cursor", cursor},
	}, readCost())
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{Req: req, Parse: func(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
		e, err := readEnv(resp)
		if err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		var page listPage[posRow]
		if err := decodeResult(e.Result, &page); err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		var pos []exchange.Position
		hedge := false
		for _, row := range page.List {
			if p, ok := row.position(page.Category); ok {
				pos = append(pos, p)
			}
			hedge = hedge || hedgeIdx(row.PosIdx)
		}
		out := snapPage(exchange.AccountSnapshot{Positions: pos, HedgeMode: hedge})
		if page.Cursor != "" && page.Cursor != cursor && len(page.List) > 0 {
			out.Next = func(now time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return a.positions(now, cat, settle, page.Cursor)
			}
		}
		return out, nil
	}}, nil
}

func (a *account) balances(now time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	req, err := a.get(now, "/v5/account/wallet-balance", []pair{{"accountType", "UNIFIED"}}, readCost())
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{Req: req, Parse: parseBalances}, nil
}

func parseBalances(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	e, err := readEnv(resp)
	if err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var body struct {
		List []struct {
			Coin []coinRow `json:"coin"`
		} `json:"list"`
	}
	if err := decodeResult(e.Result, &body); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var bals []exchange.Balance
	for _, acct := range body.List {
		for _, c := range acct.Coin {
			if b, ok := c.balance(); ok {
				bals = append(bals, b)
			}
		}
	}
	return snapPage(exchange.AccountSnapshot{Balances: bals}), nil
}

func decodePositions(resp *exchange.HTTPResponse) ([]posRow, string, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, "", err
	}
	var page listPage[posRow]
	if err := decodeResult(e.Result, &page); err != nil {
		return nil, "", err
	}
	return page.List, page.Category, nil
}
