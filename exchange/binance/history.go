package binance

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

func historyFactory(k kind) exchange.HistoryFactory {
	return func(cfg exchange.AccountConfig) (exchange.History, error) {
		return &history{k: k, cfg: cfg}, nil
	}
}

// history reads userTrades by fromId, ascending. The trade id is the same id the private stream
// reports as ORDER_TRADE_UPDATE.o.t, which is what D601 needs for fill_id.
type history struct {
	k   kind
	cfg exchange.AccountConfig
	signerState
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits {
	if h.k.name != kindUM.name {
		return exchange.HistoryTraits{}
	}
	return exchange.HistoryTraits{
		ByID: true,
		// userTrades rejects a window longer than 7 days.
		MaxWindow: 7 * 24 * time.Hour,
		// The venue only serves fills from the past three months.
		Retention: 90 * 24 * time.Hour,
		PageLimit: 1000,
	}
}

func (h *history) Fills(now time.Time, q exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	if err := instrument(q.Instrument); err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	byID := q.Cursor != "" || q.FromID != ""
	byTime := q.StartNs != 0 || q.EndNs != 0
	if byID && byTime {
		return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("binance: fromId cannot be combined with startTime or endTime")
	}
	if q.StartNs != 0 && q.EndNs != 0 {
		if q.EndNs < q.StartNs {
			return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("binance: fill window ends before it starts")
		}
		if time.Duration(q.EndNs-q.StartNs) > 7*24*time.Hour {
			return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("binance: fill window is longer than 7 days")
		}
	}
	limit := 1000
	if q.Limit > 0 && q.Limit < limit {
		limit = q.Limit
	}
	query := codec.Query(nil).Add("symbol", q.Instrument.ExchTicker)
	if byID {
		id := q.Cursor
		if id == "" {
			id = q.FromID
		}
		query = query.Add("fromId", id)
	} else {
		if q.StartNs != 0 {
			query = query.AddInt("startTime", q.StartNs/int64(time.Millisecond))
		}
		if q.EndNs != 0 {
			query = query.AddInt("endTime", q.EndNs/int64(time.Millisecond))
		}
	}
	query = query.AddInt("limit", int64(limit))
	req, err := signedCall(&h.signerState, h.k, h.cfg, now, http.MethodGet, "/fapi/v1/userTrades", query, weightCost(5), false)
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	return exchange.Call[*exchange.FillPage]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.FillPage, error) {
		return parseFills(resp, limit)
	}}, nil
}

func (h *history) Order(now time.Time, inst *symbol.Info, orderID string) (exchange.Call[*exchange.OrderUpdate], error) {
	if err := instrument(inst); err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	n, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil || n == 0 {
		return exchange.Call[*exchange.OrderUpdate]{}, fmt.Errorf("binance: order id %q", orderID)
	}
	q := codec.Query(nil).Add("symbol", inst.ExchTicker).AddInt("orderId", n)
	req, err := signedCall(&h.signerState, h.k, h.cfg, now, http.MethodGet, "/fapi/v1/order", q, weightCost(1), false)
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	return exchange.Call[*exchange.OrderUpdate]{Req: req, Parse: parseOrder}, nil
}

func parseFills(resp *exchange.HTTPResponse, limit int) (*exchange.FillPage, error) {
	if resp.Status >= 400 {
		return nil, refuse(resp)
	}
	var rows []userTrade
	if err := codec.DecodeBody(resp.Body, &rows); err != nil {
		return nil, err
	}
	fills := make([]exchange.Fill, len(rows))
	for i, r := range rows {
		fills[i] = r.fill()
	}
	page := &exchange.FillPage{Fills: fills}
	if limit > 0 && len(rows) == limit {
		page.Next = strconv.FormatInt(rows[len(rows)-1].ID+1, 10)
	}
	return page, nil
}

func parseOrder(resp *exchange.HTTPResponse) (*exchange.OrderUpdate, error) {
	if resp.Status >= 400 {
		return nil, refuse(resp)
	}
	var o restOrder
	if err := codec.DecodeBody(resp.Body, &o); err != nil {
		return nil, err
	}
	up := o.update()
	return &up, nil
}

type userTrade struct {
	Symbol   string        `json:"symbol"`
	ID       int64         `json:"id"`
	OrderID  int64         `json:"orderId"`
	Side     string        `json:"side"`
	Price    codec.Decimal `json:"price"`
	Qty      codec.Decimal `json:"qty"`
	Fee      codec.Decimal `json:"commission"`
	FeeAsset string        `json:"commissionAsset"`
	Time     int64         `json:"time"`
	Maker    bool          `json:"maker"`
}

func (t userTrade) fill() exchange.Fill {
	liq := "taker"
	if t.Maker {
		liq = "maker"
	}
	return exchange.Fill{
		Exch: exchOf(t.Symbol), FillID: strconv.FormatInt(t.ID, 10),
		OrderID: strconv.FormatInt(t.OrderID, 10),
		Side:    sideOf(t.Side), Price: float64(t.Price), Qty: float64(t.Qty),
		Fee: float64(t.Fee), FeeAsset: t.FeeAsset, Liquidity: liq, ExchTsNs: ms(t.Time),
	}
}
