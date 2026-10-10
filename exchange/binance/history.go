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

// history reads fills by fromId, ascending. USD-M uses userTrades; spot uses myTrades.
// The trade id is the same id the private event reports (D601).
type history struct {
	k   kind
	cfg exchange.AccountConfig
	signerState
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits {
	switch h.k.name {
	case kindUM.name:
		return exchange.HistoryTraits{
			ByID: true,
			// userTrades rejects a window longer than 7 days.
			MaxWindow: 7 * 24 * time.Hour,
			// The venue only serves fills from the past three months.
			Retention: 90 * 24 * time.Hour,
			PageLimit: 1000,
		}
	case kindSpot.name:
		return exchange.HistoryTraits{
			ByID: true,
			// myTrades rejects a startTime/endTime window longer than 24 hours.
			MaxWindow: 24 * time.Hour,
			PageLimit: 1000,
		}
	default:
		return exchange.HistoryTraits{}
	}
}

func (h *history) Fills(now time.Time, q exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	venue, path, weight, window, err := h.fillsRoute()
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	if err := instrumentVenue(q.Instrument, venue); err != nil {
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
		if time.Duration(q.EndNs-q.StartNs) > window {
			return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("binance: fill window is longer than %s", window)
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
	req, err := signedCall(&h.signerState, h.k, h.cfg, now, http.MethodGet, path, query, weightCost(weight), false)
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	return exchange.Call[*exchange.FillPage]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.FillPage, error) {
		return parseFills(resp, limit, venue)
	}}, nil
}

func (h *history) fillsRoute() (venue, path string, weight int, window time.Duration, err error) {
	switch h.k.name {
	case kindUM.name:
		return VenueUM, "/fapi/v1/userTrades", 5, 7 * 24 * time.Hour, nil
	case kindSpot.name:
		return VenueSpot, "/api/v3/myTrades", 10, 24 * time.Hour, nil
	default:
		return "", "", 0, 0, exchange.ErrNotImplemented
	}
}

func (h *history) Order(now time.Time, inst *symbol.Info, orderID string) (exchange.Call[*exchange.OrderUpdate], error) {
	venue, path, weight, err := h.orderRoute()
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	if err := instrumentVenue(inst, venue); err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	n, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil || n == 0 {
		return exchange.Call[*exchange.OrderUpdate]{}, fmt.Errorf("binance: order id %q", orderID)
	}
	q := codec.Query(nil).Add("symbol", inst.ExchTicker).AddInt("orderId", n)
	req, err := signedCall(&h.signerState, h.k, h.cfg, now, http.MethodGet, path, q, weightCost(weight), false)
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	return exchange.Call[*exchange.OrderUpdate]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.OrderUpdate, error) {
		return parseOrder(resp, venue)
	}}, nil
}

func (h *history) orderRoute() (venue, path string, weight int, err error) {
	switch h.k.name {
	case kindUM.name:
		return VenueUM, "/fapi/v1/order", 1, nil
	case kindSpot.name:
		return VenueSpot, "/api/v3/order", 2, nil
	default:
		return "", "", 0, exchange.ErrNotImplemented
	}
}

func parseFills(resp *exchange.HTTPResponse, limit int, venue string) (*exchange.FillPage, error) {
	if resp.Status >= 400 {
		return nil, refuse(resp)
	}
	var rows []userTrade
	if err := codec.DecodeBody(resp.Body, &rows); err != nil {
		return nil, err
	}
	fills := make([]exchange.Fill, len(rows))
	for i, r := range rows {
		fills[i] = r.fill(venue)
	}
	page := &exchange.FillPage{Fills: fills}
	if limit > 0 && len(rows) == limit {
		page.Next = strconv.FormatInt(rows[len(rows)-1].ID+1, 10)
	}
	return page, nil
}

func parseOrder(resp *exchange.HTTPResponse, venue string) (*exchange.OrderUpdate, error) {
	if resp.Status >= 400 {
		return nil, refuse(resp)
	}
	var o restOrder
	if err := codec.DecodeBody(resp.Body, &o); err != nil {
		return nil, err
	}
	up := o.update(venue)
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
	FeeAsset flexString    `json:"commissionAsset"`
	Time     int64         `json:"time"`
	Maker    bool          `json:"maker"`
	IsMaker  bool          `json:"isMaker"`
	IsBuyer  bool          `json:"isBuyer"`
}

func (t userTrade) fill(venue string) exchange.Fill {
	maker := t.Maker || t.IsMaker
	liq := "taker"
	if maker {
		liq = "maker"
	}
	side := sideOf(t.Side)
	if t.Side == "" {
		side = exchange.SideSell
		if t.IsBuyer {
			side = exchange.SideBuy
		}
	}
	return exchange.Fill{
		Exch: exchFor(venue, t.Symbol), FillID: strconv.FormatInt(t.ID, 10),
		OrderID: strconv.FormatInt(t.OrderID, 10),
		Side:    side, Price: float64(t.Price), Qty: float64(t.Qty),
		Fee: float64(t.Fee), FeeAsset: string(t.FeeAsset), Liquidity: liq, ExchTsNs: ms(t.Time),
	}
}

// flexString accepts a JSON string or null. Spot leaves the commission asset null when there is none.
type flexString string

func (s *flexString) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*s = ""
		return nil
	}
	var v string
	if err := codec.DecodeBody(b, &v); err != nil {
		return err
	}
	*s = flexString(v)
	return nil
}
