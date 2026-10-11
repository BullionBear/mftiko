package okx

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

const (
	// fillKeep is how far back fills-history goes. The docs title it "last 3 months".
	fillKeep = 90 * 24 * time.Hour
	// fillLimit is the page cap. The default when Limit is 0, and the most one request accepts.
	fillLimit = 100
)

// history reads /api/v5/trade/fills-history. Rows are newest first. tradeId is the fill id the
// orders channel also reports (D601). Order looks up /api/v5/trade/order by ordId.
type history struct {
	cfg exchange.AccountConfig
	signerState
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits {
	return exchange.HistoryTraits{
		Descending: true,
		Retention:  fillKeep,
		PageLimit:  fillLimit,
	}
}

func (h *history) Fills(now time.Time, q exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	instType, err := instTypeOf(q.Instrument)
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	if q.FromID != "" {
		return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("okx: fills page by bill id and time, not by trade id")
	}
	if q.StartNs != 0 && q.EndNs != 0 && q.EndNs < q.StartNs {
		return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("okx: fill window ends before it starts")
	}
	limit := fillLimit
	if q.Limit > 0 && q.Limit < limit {
		limit = q.Limit
	}
	req, err := h.get(now, "/api/v5/trade/fills-history", []pair{
		{"instType", instType},
		{"instId", q.Instrument.ExchTicker},
		{"begin", msQuery(q.StartNs)},
		{"end", msQuery(q.EndNs)},
		{"limit", strconv.Itoa(limit)},
		{"after", q.Cursor},
	})
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	sent := q.Cursor
	return exchange.Call[*exchange.FillPage]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.FillPage, error) {
		return parseFills(resp, sent, limit)
	}}, nil
}

func (h *history) Order(now time.Time, inst *symbol.Info, orderID string) (exchange.Call[*exchange.OrderUpdate], error) {
	if inst == nil || inst.ExchTicker == "" {
		return exchange.Call[*exchange.OrderUpdate]{}, errors.New("okx: instrument")
	}
	if orderID == "" {
		return exchange.Call[*exchange.OrderUpdate]{}, errors.New("okx: order id is empty")
	}
	req, err := h.get(now, "/api/v5/trade/order", []pair{
		{"instId", inst.ExchTicker},
		{"ordId", orderID},
	})
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	return exchange.Call[*exchange.OrderUpdate]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.OrderUpdate, error) {
		return parseHistoryOrder(resp, orderID)
	}}, nil
}

func (h *history) Usage(*exchange.HTTPResponse, time.Time) []exchange.Used { return nil }

func (h *history) get(now time.Time, path string, pairs []pair) (exchange.HTTPRequest, error) {
	return signCfg(&h.signerState, h.cfg, now, http.MethodGet, path, queryString(pairs), nil, readCost(), false)
}

// instTypeOf is the REST instType for one instrument. Inverse is a coin-margined swap, or a
// dated future when it expires. MARGIN is not a category of its own; spot covers SPOT.
func instTypeOf(inst *symbol.Info) (string, error) {
	if inst == nil || inst.ExchTicker == "" {
		return "", errors.New("okx: instrument")
	}
	switch inst.Ticker.Category {
	case symbol.Spot:
		return "SPOT", nil
	case symbol.Perp:
		return "SWAP", nil
	case symbol.Future:
		return "FUTURES", nil
	case symbol.Option:
		return "OPTION", nil
	case symbol.Inverse:
		if inst.ExpiryNs != 0 {
			return "FUTURES", nil
		}
		return "SWAP", nil
	default:
		return "", fmt.Errorf("okx: %s has no instType", inst.Ticker.Category)
	}
}

func msQuery(ns int64) string {
	if ns <= 0 {
		return ""
	}
	return strconv.FormatInt(ns/int64(time.Millisecond), 10)
}

// fillRow is one fills-history bill. fee is that fill's fee; the orders channel calls the
// same number fillFee.
type fillRow struct {
	InstType string        `json:"instType"`
	InstID   string        `json:"instId"`
	TradeID  string        `json:"tradeId"`
	OrdID    string        `json:"ordId"`
	ClOrdID  string        `json:"clOrdId"`
	BillID   string        `json:"billId"`
	FillPx   codec.Decimal `json:"fillPx"`
	FillSz   codec.Decimal `json:"fillSz"`
	Side     string        `json:"side"`
	ExecType string        `json:"execType"`
	FeeCcy   string        `json:"feeCcy"`
	Fee      codec.Decimal `json:"fee"`
	Ts       string        `json:"ts"`
	FillTime string        `json:"fillTime"`
}

func (r fillRow) fill() (exchange.Fill, bool) {
	return makeFill(r.InstType, r.InstID, r.TradeID, r.OrdID, r.ClOrdID, r.Side, r.ExecType, r.FillTime, r.Ts, r.FeeCcy, r.FillPx, r.FillSz, r.Fee)
}

// parseFills keeps trades and walks older pages by billId. A row with no tradeId, or a zero
// fillSz, is not a fill; the cursor still advances past it. after returns records earlier than
// that billId, so the oldest row on a full page is the next cursor. The same cursor again means
// the venue handed the page back, and the scan stops.
func parseFills(resp *exchange.HTTPResponse, sent string, limit int) (*exchange.FillPage, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var rows []fillRow
	if err := decodeData(e.Data, &rows); err != nil {
		return nil, err
	}
	page := &exchange.FillPage{}
	for _, row := range rows {
		if f, ok := row.fill(); ok {
			page.Fills = append(page.Fills, f)
		}
	}
	if limit > 0 && len(rows) >= limit {
		last := rows[len(rows)-1].BillID
		if last != "" && last != sent {
			page.Next = last
		}
	}
	return page, nil
}

func parseHistoryOrder(resp *exchange.HTTPResponse, orderID string) (*exchange.OrderUpdate, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var rows []orderRow
	if err := decodeData(e.Data, &rows); err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.OrdID == orderID {
			up := row.update()
			return &up, nil
		}
	}
	return nil, fmt.Errorf("okx: order %s is not in history", orderID)
}
