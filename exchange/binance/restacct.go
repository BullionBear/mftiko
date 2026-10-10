package binance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// Request weights are the USD-M table: an order is 1, openOrders with no symbol is 40,
// positionRisk and account and userTrades are 5, countdownCancelAll is 10. An order also
// takes one count on each order bucket; a batch takes one per order.

func (a *account) Submit(now time.Time, req *exchange.OrderRequest) (exchange.Call[*exchange.OrderResult], error) {
	q, err := orderQuery(req)
	if err != nil {
		return exchange.Call[*exchange.OrderResult]{}, err
	}
	httpReq, err := a.signedCall(now, http.MethodPost, "/fapi/v1/order", q, orderCost(1, 1), true)
	if err != nil {
		return exchange.Call[*exchange.OrderResult]{}, err
	}
	return exchange.Call[*exchange.OrderResult]{Req: httpReq, Parse: parseOrderResult}, nil
}

func (a *account) Cancel(now time.Time, ref exchange.CancelRef) (exchange.Call[struct{}], error) {
	p, err := cancelParamsOf(now, ref)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	q := codec.Query(nil).Add("symbol", p.Symbol)
	if p.OrderID != 0 {
		q = q.AddInt("orderId", p.OrderID)
	}
	if p.OrigClientOrderID != "" {
		q = q.Add("origClientOrderId", p.OrigClientOrderID)
	}
	httpReq, err := a.signedCall(now, http.MethodDelete, "/fapi/v1/order", q, orderCost(1, 1), true)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: httpReq, Parse: parseAck}, nil
}

func (a *account) CancelBatch(now time.Time, refs []exchange.CancelRef) (exchange.Call[[]error], error) {
	if len(refs) == 0 || len(refs) > 10 {
		return exchange.Call[[]error]{}, fmt.Errorf("binance: batch cancel takes 1 to 10 orders, got %d", len(refs))
	}
	sym := ""
	ids := make([]int64, 0, len(refs))
	cloids := make([]string, 0, len(refs))
	useClient := refs[0].ClientOrderID != ""
	for i, ref := range refs {
		if err := instrument(ref.Instrument); err != nil {
			return exchange.Call[[]error]{}, err
		}
		if sym == "" {
			sym = ref.Instrument.ExchTicker
		} else if ref.Instrument.ExchTicker != sym {
			return exchange.Call[[]error]{}, fmt.Errorf("binance: batch cancel is one symbol")
		}
		hasClient := ref.ClientOrderID != ""
		if hasClient != useClient {
			return exchange.Call[[]error]{}, fmt.Errorf("binance: batch cancel takes client order ids or order ids, not both")
		}
		if hasClient {
			if err := checkCloid(ref.ClientOrderID); err != nil {
				return exchange.Call[[]error]{}, err
			}
			cloids = append(cloids, ref.ClientOrderID)
			continue
		}
		n, err := strconv.ParseInt(ref.OrderID, 10, 64)
		if err != nil || n == 0 {
			return exchange.Call[[]error]{}, fmt.Errorf("binance: batch item %d has no order id", i)
		}
		ids = append(ids, n)
	}
	q := codec.Query(nil).Add("symbol", sym)
	if useClient {
		raw, err := json.Marshal(cloids)
		if err != nil {
			return exchange.Call[[]error]{}, err
		}
		q = q.Add("origClientOrderIdList", string(raw))
	} else {
		raw, err := json.Marshal(ids)
		if err != nil {
			return exchange.Call[[]error]{}, err
		}
		q = q.Add("orderIdList", string(raw))
	}
	httpReq, err := a.signedCall(now, http.MethodDelete, "/fapi/v1/batchOrders", q, orderCost(1, len(refs)), true)
	if err != nil {
		return exchange.Call[[]error]{}, err
	}
	return exchange.Call[[]error]{Req: httpReq, Parse: parseBatch}, nil
}

func (a *account) Snapshot(now time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	orders, err := a.signedCall(now, http.MethodGet, "/fapi/v1/openOrders", nil, weightCost(40), false)
	if err != nil {
		return nil, err
	}
	risk, err := a.signedCall(now, http.MethodGet, "/fapi/v2/positionRisk", nil, weightCost(5), false)
	if err != nil {
		return nil, err
	}
	bal, err := a.signedCall(now, http.MethodGet, "/fapi/v2/account", nil, weightCost(5), false)
	if err != nil {
		return nil, err
	}
	return []exchange.Call[exchange.Page[exchange.AccountSnapshot]]{
		{Req: orders, Parse: parseOpenOrders},
		{Req: risk, Parse: parsePositions},
		{Req: bal, Parse: parseBalances},
	}, nil
}

func (a *account) Leverage(now time.Time, inst *symbol.Info) (exchange.Call[float64], error) {
	if err := instrument(inst); err != nil {
		return exchange.Call[float64]{}, err
	}
	q := codec.Query(nil).Add("symbol", inst.ExchTicker)
	req, err := a.signedCall(now, http.MethodGet, "/fapi/v2/positionRisk", q, weightCost(5), false)
	if err != nil {
		return exchange.Call[float64]{}, err
	}
	sym := inst.ExchTicker
	return exchange.Call[float64]{Req: req, Parse: func(resp *exchange.HTTPResponse) (float64, error) {
		rows, err := decodeRisk(resp)
		if err != nil {
			return 0, err
		}
		for _, row := range rows {
			if row.Symbol == sym {
				return float64(row.Lev), nil
			}
		}
		return 0, fmt.Errorf("binance: %s has no position risk", sym)
	}}, nil
}

func (a *account) ArmCancelOnDisconnect(now time.Time, timeout time.Duration, insts []*symbol.Info) (exchange.Call[struct{}], error) {
	if timeout < 0 {
		return exchange.Call[struct{}]{}, fmt.Errorf("binance: countdown is negative")
	}
	if len(insts) != 1 {
		return exchange.Call[struct{}]{}, fmt.Errorf("binance: countdownCancelAll is one symbol, got %d", len(insts))
	}
	if err := instrument(insts[0]); err != nil {
		return exchange.Call[struct{}]{}, err
	}
	q := codec.Query(nil).Add("symbol", insts[0].ExchTicker).AddInt("countdownTime", timeout.Milliseconds())
	req, err := a.signedCall(now, http.MethodPost, "/fapi/v1/countdownCancelAll", q, weightCost(10), true)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: req, Parse: parseAck}, nil
}

// cancelAll is DELETE /fapi/v1/allOpenOrders. The interface has no account-wide cancel; the
// testnet smoke uses it to leave the account clean.
func (a *account) cancelAll(now time.Time, sym string) (exchange.Call[struct{}], error) {
	q := codec.Query(nil).Add("symbol", sym)
	req, err := a.signedCall(now, http.MethodDelete, "/fapi/v1/allOpenOrders", q, orderCost(1, 1), true)
	if err != nil {
		return exchange.Call[struct{}]{}, err
	}
	return exchange.Call[struct{}]{Req: req, Parse: parseAck}, nil
}

func orderQuery(req *exchange.OrderRequest) (codec.Query, error) {
	if req == nil {
		return nil, fmt.Errorf("binance: order is missing")
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
	qty, err := formatDec(req.Qty, req.Instrument.Filters[symbol.QtyStep])
	if err != nil {
		return nil, fmt.Errorf("binance: quantity: %w", err)
	}
	q := codec.Query(nil).Add("symbol", req.Instrument.ExchTicker).Add("side", side).Add("type", typ).Add("quantity", qty)
	if req.Type == exchange.OrderTypeLimit {
		tif, err := tifWire(req.TIF)
		if err != nil {
			return nil, err
		}
		px, err := formatDec(req.Price, req.Instrument.Filters[symbol.PriceTick])
		if err != nil {
			return nil, fmt.Errorf("binance: price: %w", err)
		}
		q = q.Add("price", px).Add("timeInForce", tif)
	} else if req.TIF != exchange.TIFUnspecified && req.TIF != exchange.TIFGTC {
		return nil, fmt.Errorf("binance: a market order has no time in force: %w", exchange.ErrUnsupported)
	}
	if req.ReduceOnly {
		q = q.Add("reduceOnly", "true")
	}
	q = q.Add("newClientOrderId", req.ClientOrderID).Add("newOrderRespType", "RESULT")
	if side := req.Params["positionSide"]; side != "" {
		q = q.Add("positionSide", side)
	}
	return q, nil
}

func parseOrderResult(resp *exchange.HTTPResponse) (*exchange.OrderResult, error) {
	if resp.Status >= 400 {
		return nil, refuse(resp)
	}
	return orderResult(resp.Body)
}

func parseOpenOrders(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	if resp.Status >= 400 {
		return exchange.Page[exchange.AccountSnapshot]{}, refuse(resp)
	}
	var rows []restOrder
	if err := codec.DecodeBody(resp.Body, &rows); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	orders := make([]exchange.OrderUpdate, len(rows))
	for i, o := range rows {
		orders[i] = o.update()
	}
	return snapPage(exchange.AccountSnapshot{OpenOrders: orders}), nil
}

func parsePositions(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	rows, err := decodeRisk(resp)
	if err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var pos []exchange.Position
	for _, row := range rows {
		p := row.position()
		if p.Size == 0 {
			continue
		}
		pos = append(pos, p)
	}
	return snapPage(exchange.AccountSnapshot{Positions: pos}), nil
}

func parseBalances(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
	if resp.Status >= 400 {
		return exchange.Page[exchange.AccountSnapshot]{}, refuse(resp)
	}
	var body struct {
		Assets []umAsset `json:"assets"`
	}
	if err := codec.DecodeBody(resp.Body, &body); err != nil {
		return exchange.Page[exchange.AccountSnapshot]{}, err
	}
	var bals []exchange.Balance
	for _, a := range body.Assets {
		b := a.balance()
		if b.Free == 0 && b.Locked == 0 {
			continue
		}
		bals = append(bals, b)
	}
	return snapPage(exchange.AccountSnapshot{Balances: bals}), nil
}

func snapPage(s exchange.AccountSnapshot) exchange.Page[exchange.AccountSnapshot] {
	return exchange.Page[exchange.AccountSnapshot]{Items: []exchange.AccountSnapshot{s}}
}

func decodeRisk(resp *exchange.HTTPResponse) ([]riskRow, error) {
	if resp.Status >= 400 {
		return nil, refuse(resp)
	}
	var rows []riskRow
	if err := codec.DecodeBody(resp.Body, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func parseBatch(resp *exchange.HTTPResponse) ([]error, error) {
	body := bytes.TrimSpace(resp.Body)
	if len(body) == 0 || body[0] != '[' {
		return nil, refuse(resp)
	}
	var rows []codecRaw
	if err := codec.DecodeBody(body, &rows); err != nil {
		return nil, err
	}
	out := make([]error, len(rows))
	for i, raw := range rows {
		var item struct {
			Code    int    `json:"code"`
			Msg     string `json:"msg"`
			OrderID int64  `json:"orderId"`
		}
		if err := codec.DecodeBody(raw, &item); err != nil {
			return nil, err
		}
		if item.OrderID != 0 {
			continue
		}
		if item.Code != 0 {
			out[i] = binanceErr(resp.Status, item.Code, item.Msg)
			if out[i] == nil {
				out[i] = fmt.Errorf("binance: %s", item.Msg)
			}
			continue
		}
		out[i] = fmt.Errorf("binance: batch item %d has no order", i)
	}
	return out, nil
}

type riskRow struct {
	Symbol string        `json:"symbol"`
	Amt    codec.Decimal `json:"positionAmt"`
	Entry  codec.Decimal `json:"entryPrice"`
	Mark   codec.Decimal `json:"markPrice"`
	UPnl   codec.Decimal `json:"unRealizedProfit"`
	Lev    codec.Decimal `json:"leverage"`
	Side   string        `json:"positionSide"`
}

func (r riskRow) position() exchange.Position {
	return exchange.Position{
		Exch: exchOf(r.Symbol), Size: positionSize(float64(r.Amt), r.Side),
		EntryPrice: float64(r.Entry), MarkPrice: float64(r.Mark),
		UnrealizedPnl: float64(r.UPnl), Leverage: float64(r.Lev),
	}
}

type umAsset struct {
	Asset     string        `json:"asset"`
	Wallet    codec.Decimal `json:"walletBalance"`
	Available codec.Decimal `json:"availableBalance"`
}

func (a umAsset) balance() exchange.Balance {
	wallet, free := float64(a.Wallet), float64(a.Available)
	locked := 0.0
	if wallet > free {
		locked = wallet - free
	}
	return exchange.Balance{Asset: a.Asset, Free: free, Locked: locked}
}
