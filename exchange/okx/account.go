package okx

import (
	"errors"
	"fmt"
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

// Limits: clOrdId is 32 characters. CancelBatch is 20, which is cancel-batch-orders' cap.
// The order budget is 60 requests per 2 seconds. OKX applies that per instrument; enforcing it
// on the account keeps every instrument inside its own budget. A batch consumes one count per order.
func (a *account) Limits() exchange.AccountLimits {
	return exchange.AccountLimits{
		ClientOrderIDMaxLen: cloidMax,
		CancelBatch:         20,
		OrderRate:           []exchange.RateLimit{{N: 60, Per: 2 * time.Second}},
	}
}

func (a *account) Submit(now time.Time, req *exchange.OrderRequest) (exchange.Call[*exchange.OrderResult], error) {
	body, err := createBodyOf(req)
	if err != nil {
		return exchange.Call[*exchange.OrderResult]{}, err
	}
	httpReq, err := a.postOrder(now, "/api/v5/trade/order", body, orderCost(1))
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
	httpReq, err := a.postOrder(now, "/api/v5/trade/cancel-order", body, orderCost(1))
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: httpReq, Parse: parseAck}, nil
}

func (a *account) CancelBatch(now time.Time, refs []exchange.CancelRef) (exchange.Call[[]error], error) {
	if len(refs) == 0 {
		return exchange.Call[[]error]{}, fmt.Errorf("okx: batch cancel is empty")
	}
	if len(refs) > 20 {
		return exchange.Call[[]error]{}, fmt.Errorf("okx: batch cancel takes at most 20 orders, got %d", len(refs))
	}
	items := make([]cancelBody, 0, len(refs))
	for _, ref := range refs {
		item, err := cancelBodyOf(ref)
		if err != nil {
			return exchange.Call[[]error]{}, err
		}
		items = append(items, item)
	}
	httpReq, err := a.postOrder(now, "/api/v5/trade/cancel-batch-orders", items, orderCost(len(items)))
	if err != nil {
		return exchange.Call[[]error]{}, err
	}
	return exchange.Call[[]error]{Req: httpReq, Parse: parseBatch}, nil
}

// QueryOrder is GET /api/v5/trade/order by instId and clOrdId (ordId when the ref has none), open
// or recently ended. 51603 (order does not exist) is ErrNotFound.
func (a *account) QueryOrder(now time.Time, ref exchange.CancelRef) (exchange.Call[*exchange.OrderUpdate], error) {
	body, err := cancelBodyOf(ref)
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	pairs := []pair{{"instId", body.InstID}, {"clOrdId", body.ClOrdID}}
	if body.ClOrdID == "" {
		pairs = append(pairs, pair{"ordId", body.OrdID})
	}
	req, err := a.get(now, "/api/v5/trade/order", pairs, readCost())
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	return exchange.Call[*exchange.OrderUpdate]{Req: req, Parse: parseQueryOrder}, nil
}

func parseQueryOrder(resp *exchange.HTTPResponse) (*exchange.OrderUpdate, error) {
	e, err := readEnv(resp)
	var rej *exchange.Reject
	if errors.As(err, &rej) && rej.Native == "51603" {
		return nil, fmt.Errorf("okx: %s: %w", rej.Reason, exchange.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	var rows []orderRow
	if err := decodeData(e.Data, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("okx: order lookup returned no order")
	}
	up := rows[0].update()
	return &up, nil
}

// Snapshot reads the unified account: one orders-pending page, one positions page, one balance,
// and the account config for the position mode. Each list follows after when the page is full.
func (a *account) Snapshot(now time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	orders, err := a.openOrders(now, "")
	if err != nil {
		return nil, err
	}
	pos, err := a.positions(now, "")
	if err != nil {
		return nil, err
	}
	bal, err := a.balances(now)
	if err != nil {
		return nil, err
	}
	mode, err := a.get(now, "/api/v5/account/config", nil, readCost())
	if err != nil {
		return nil, err
	}
	return []exchange.Call[exchange.Page[exchange.AccountSnapshot]]{orders, pos, bal, {Req: mode, Parse: parsePosMode}}, nil
}

// parsePosMode reads posMode: long_short_mode is hedge mode, net_mode one-way (D607).
func parsePosMode(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	e, err := readEnv(resp)
	if err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var rows []struct {
		PosMode string `json:"posMode"`
	}
	if err := decodeData(e.Data, &rows); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	if len(rows) == 0 {
		return exchange.Page[exchange.AccountSnapshot]{}, errors.New("okx: account config is empty")
	}
	switch rows[0].PosMode {
	case "long_short_mode":
		return snapPage(exchange.AccountSnapshot{HedgeMode: true}), nil
	case "net_mode":
		return snapPage(exchange.AccountSnapshot{}), nil
	default:
		return exchange.Page[exchange.AccountSnapshot]{}, fmt.Errorf("okx: posMode %q", rows[0].PosMode)
	}
}

// Leverage reads cross leverage. Spot has none. Long and short rows that disagree are an error;
// a net row, or one shared lever, is the answer.
func (a *account) Leverage(now time.Time, inst *symbol.Info) (exchange.Call[float64], error) {
	if err := instrument(inst); err != nil {
		return exchange.Call[float64]{}, err
	}
	if inst.Ticker.Category == symbol.Spot {
		return exchange.Call[float64]{}, fmt.Errorf("okx: spot has no leverage: %w", exchange.ErrUnsupported)
	}
	sym := inst.ExchTicker
	req, err := a.get(now, "/api/v5/account/leverage-info", []pair{
		{"instId", sym}, {"mgnMode", "cross"},
	}, readCost())
	if err != nil {
		return exchange.Call[float64]{}, err
	}
	return exchange.Call[float64]{Req: req, Parse: func(resp *exchange.HTTPResponse) (float64, error) {
		e, err := readEnv(resp)
		if err != nil {
			return 0, err
		}
		var rows []struct {
			PosSide string `json:"posSide"`
			Lever   string `json:"lever"`
		}
		if err := decodeData(e.Data, &rows); err != nil {
			return 0, err
		}
		var lever string
		for _, row := range rows {
			if row.Lever == "" {
				continue
			}
			if row.PosSide == "net" || lever == "" {
				lever = row.Lever
			} else if row.Lever != lever {
				return 0, fmt.Errorf("okx: %s long and short leverage differ", sym)
			}
		}
		if lever == "" {
			return 0, fmt.Errorf("okx: %s has no leverage", sym)
		}
		v, err := strconv.ParseFloat(lever, 64)
		if err != nil {
			return 0, fmt.Errorf("okx: leverage %q: %w", lever, err)
		}
		return v, nil
	}}, nil
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	if err := a.passphrase(); err != nil {
		return nil, err
	}
	sig, err := a.signer()
	if err != nil {
		return nil, err
	}
	return &privateStream{
		ws: a.privateURL(), key: a.cfg.Credentials.Key, pass: a.cfg.Credentials.Passphrase, sig: sig,
	}, nil
}

// NewTradeStream: phase 1 trades over REST. The order channel of the private websocket is not
// used to send orders (D303).
func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	return nil, exchange.ErrUnsupported
}

// ArmCancelOnDisconnect sets cancel-all-after. syms is ignored: the switch is account-wide.
// The window is whole seconds, 0 to turn it off or from 10 to 120.
func (a *account) ArmCancelOnDisconnect(now time.Time, timeout time.Duration, _ []*symbol.Info) (exchange.Call[struct{}], error) {
	if timeout%time.Second != 0 {
		return exchange.Call[struct{}]{}, fmt.Errorf("okx: disconnect window is whole seconds")
	}
	sec := int(timeout / time.Second)
	if sec != 0 && (sec < 10 || sec > 120) {
		return exchange.Call[struct{}]{}, fmt.Errorf("okx: disconnect window must be 0 or from 10 to 120 seconds, got %d", sec)
	}
	req, err := a.post(now, "/api/v5/trade/cancel-all-after", caaBody{TimeOut: strconv.Itoa(sec)}, []exchange.Cost{{Bucket: bucketCAA, N: 1}}, true)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: req, Parse: parseAck}, nil
}

type orderBody struct {
	InstID     string `json:"instId"`
	TdMode     string `json:"tdMode"`
	Side       string `json:"side"`
	OrdType    string `json:"ordType"`
	Sz         string `json:"sz"`
	Px         string `json:"px,omitempty"`
	ClOrdID    string `json:"clOrdId"`
	TgtCcy     string `json:"tgtCcy,omitempty"`
	ReduceOnly string `json:"reduceOnly,omitempty"`
	PosSide    string `json:"posSide,omitempty"`
}

func createBodyOf(req *exchange.OrderRequest) (orderBody, error) {
	if req == nil {
		return orderBody{}, fmt.Errorf("okx: order is missing")
	}
	mode, err := tdMode(req.Instrument, req.Params)
	if err != nil {
		return orderBody{}, err
	}
	if err := checkCloid(req.ClientOrderID); err != nil {
		return orderBody{}, err
	}
	side, err := sideWire(req.Side)
	if err != nil {
		return orderBody{}, err
	}
	typ, err := ordTypeOf(req.Type, req.TIF)
	if err != nil {
		return orderBody{}, err
	}
	if req.ReduceOnly && req.Instrument.Ticker.Category == symbol.Spot {
		return orderBody{}, fmt.Errorf("okx: spot has no reduce-only: %w", exchange.ErrUnsupported)
	}
	body := orderBody{
		InstID: req.Instrument.ExchTicker, TdMode: mode, Side: side, OrdType: typ, ClOrdID: req.ClientOrderID,
	}
	if req.ReduceOnly {
		body.ReduceOnly = "true"
	}
	if s := req.Params["posSide"]; s != "" {
		switch s {
		case "net", "long", "short":
			body.PosSide = s
		default:
			return orderBody{}, fmt.Errorf("okx: posSide %q", s)
		}
	}
	switch req.Type {
	case exchange.OrderTypeLimit:
		px, err := formatDec(req.Price, req.Instrument.Filters[symbol.PriceTick])
		if err != nil {
			return orderBody{}, fmt.Errorf("okx: price: %w", err)
		}
		if decimal.Has(req.QuoteQty) {
			return orderBody{}, fmt.Errorf("okx: a limit order takes a quantity, not a quote amount: %w", exchange.ErrUnsupported)
		}
		qty, err := wireQty(req.Instrument, req.Qty)
		if err != nil {
			return orderBody{}, err
		}
		body.Sz, body.Px = qty, px
	case exchange.OrderTypeMarket:
		qty, tgt, err := marketQty(req)
		if err != nil {
			return orderBody{}, err
		}
		body.Sz, body.TgtCcy = qty, tgt
	}
	return body, nil
}

func marketQty(req *exchange.OrderRequest) (qty, tgt string, err error) {
	hasQty, hasQuote := decimal.Has(req.Qty), decimal.Has(req.QuoteQty)
	if hasQty && hasQuote {
		return "", "", fmt.Errorf("okx: an order takes a quantity or a quote amount, not both")
	}
	if req.Instrument.Ticker.Category != symbol.Spot {
		if hasQuote {
			return "", "", fmt.Errorf("okx: %s orders take a quantity, not a quote amount: %w", req.Instrument.Ticker.Category, exchange.ErrUnsupported)
		}
		qty, err = wireQty(req.Instrument, req.Qty)
		return qty, "", err
	}
	switch {
	case hasQuote:
		qty, err = formatDec(req.QuoteQty, quoteStep)
		if err != nil {
			return "", "", fmt.Errorf("okx: quote quantity: %w", err)
		}
		return qty, "quote_ccy", nil
	case hasQty:
		qty, err = wireQty(req.Instrument, req.Qty)
		if err != nil {
			return "", "", err
		}
		return qty, "base_ccy", nil
	default:
		return "", "", fmt.Errorf("okx: quantity: number is absent")
	}
}

type cancelBody struct {
	InstID  string `json:"instId"`
	OrdID   string `json:"ordId,omitempty"`
	ClOrdID string `json:"clOrdId,omitempty"`
}

func cancelBodyOf(ref exchange.CancelRef) (cancelBody, error) {
	if err := instrument(ref.Instrument); err != nil {
		return cancelBody{}, err
	}
	body := cancelBody{InstID: ref.Instrument.ExchTicker, OrdID: ref.OrderID}
	if ref.ClientOrderID != "" {
		if err := checkCloid(ref.ClientOrderID); err != nil {
			return cancelBody{}, err
		}
		body.ClOrdID = ref.ClientOrderID
	}
	if body.OrdID == "" && body.ClOrdID == "" {
		return cancelBody{}, fmt.Errorf("okx: cancel names neither order id nor client order id")
	}
	return body, nil
}

type caaBody struct {
	TimeOut string `json:"timeOut"`
}

const pageLimit = "100"

func (a *account) openOrders(now time.Time, after string) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	req, err := a.get(now, "/api/v5/trade/orders-pending", []pair{{"after", after}, {"limit", pageLimit}}, readCost())
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{Req: req, Parse: func(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
		e, err := readEnv(resp)
		if err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		var rows []orderRow
		if err := decodeData(e.Data, &rows); err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		orders := make([]exchange.OrderUpdate, len(rows))
		for i, row := range rows {
			orders[i] = row.update()
		}
		out := snapPage(exchange.AccountSnapshot{OpenOrders: orders})
		if len(rows) == 100 && rows[len(rows)-1].OrdID != "" && rows[len(rows)-1].OrdID != after {
			cursor := rows[len(rows)-1].OrdID
			out.Next = func(now time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return a.openOrders(now, cursor)
			}
		}
		return out, nil
	}}, nil
}

func (a *account) positions(now time.Time, after string) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	req, err := a.get(now, "/api/v5/account/positions", []pair{{"after", after}, {"limit", pageLimit}}, readCost())
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{Req: req, Parse: func(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
		e, err := readEnv(resp)
		if err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		var rows []posRow
		if err := decodeData(e.Data, &rows); err != nil {
			return exchange.Page[exchange.AccountSnapshot]{}, err
		}
		var pos []exchange.Position
		for _, row := range rows {
			if p, ok := row.position(); ok {
				pos = append(pos, p)
			}
		}
		out := snapPage(exchange.AccountSnapshot{Positions: pos})
		if len(rows) == 100 {
			cursor := rows[len(rows)-1].PosID
			if cursor != "" && cursor != after {
				out.Next = func(now time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
					return a.positions(now, cursor)
				}
			}
		}
		return out, nil
	}}, nil
}

func (a *account) balances(now time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	req, err := a.get(now, "/api/v5/account/balance", nil, readCost())
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
	var rows []struct {
		Details []balRow `json:"details"`
	}
	if err := decodeData(e.Data, &rows); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var bals []exchange.Balance
	for _, acct := range rows {
		for _, row := range acct.Details {
			if b, ok := row.balance(); ok {
				bals = append(bals, b)
			}
		}
	}
	return snapPage(exchange.AccountSnapshot{Balances: bals}), nil
}
