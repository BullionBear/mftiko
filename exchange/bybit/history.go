package bybit

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

const (
	// fillWindow is the widest [startTime, endTime] one execution-list request accepts.
	// Omitting both ends returns the last 7 days; one end extends 7 days from that end.
	fillWindow = 7 * 24 * time.Hour
	// fillKeep is how far back Get Execution keeps rows: the docs title it "2 years".
	fillKeep = 2 * 365 * 24 * time.Hour
	// fillLimit is the execution list's page cap. The default when Limit is 0.
	fillLimit = 100
)

// history reads /v5/execution/list. Rows are newest first. execId is the fill id the
// execution topic also reports (E06-D001). Order looks up /v5/order/history by order id.
type history struct {
	cfg exchange.AccountConfig
	signerState
}

var _ exchange.History = (*history)(nil)

func (h *history) Traits() exchange.HistoryTraits {
	return exchange.HistoryTraits{
		Descending: true,
		MaxWindow:  fillWindow,
		Retention:  fillKeep,
		PageLimit:  fillLimit,
	}
}

func (h *history) Fills(now time.Time, q exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	cat, err := product(q.Instrument)
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	if q.FromID != "" {
		return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("bybit: fills page by cursor and time, not by id")
	}
	if q.StartNs != 0 && q.EndNs != 0 {
		if q.EndNs < q.StartNs {
			return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("bybit: fill window ends before it starts")
		}
		if time.Duration(q.EndNs-q.StartNs) > fillWindow {
			return exchange.Call[*exchange.FillPage]{}, fmt.Errorf("bybit: fill window is longer than %s", fillWindow)
		}
	}
	limit := fillLimit
	if q.Limit > 0 && q.Limit < limit {
		limit = q.Limit
	}
	pairs := []pair{
		{"category", cat},
		{"symbol", q.Instrument.ExchTicker},
		{"startTime", msQuery(q.StartNs)},
		{"endTime", msQuery(q.EndNs)},
		{"limit", strconv.Itoa(limit)},
		{"cursor", q.Cursor},
	}
	req, err := h.get(now, "/v5/execution/list", pairs)
	if err != nil {
		return exchange.Call[*exchange.FillPage]{}, err
	}
	sent := q.Cursor
	return exchange.Call[*exchange.FillPage]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.FillPage, error) {
		return parseFills(resp, sent)
	}}, nil
}

func (h *history) Order(now time.Time, inst *symbol.Info, orderID string) (exchange.Call[*exchange.OrderUpdate], error) {
	cat, err := product(inst)
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	if orderID == "" {
		return exchange.Call[*exchange.OrderUpdate]{}, fmt.Errorf("bybit: order id is empty")
	}
	req, err := h.get(now, "/v5/order/history", []pair{
		{"category", cat}, {"symbol", inst.ExchTicker}, {"orderId", orderID},
	})
	if err != nil {
		return exchange.Call[*exchange.OrderUpdate]{}, err
	}
	return exchange.Call[*exchange.OrderUpdate]{Req: req, Parse: func(resp *exchange.HTTPResponse) (*exchange.OrderUpdate, error) {
		return parseHistoryOrder(resp, orderID)
	}}, nil
}

func (h *history) Usage(resp *exchange.HTTPResponse, now time.Time) []exchange.Used {
	return usedFrom(resp, now)
}

func (h *history) get(now time.Time, path string, pairs []pair) (exchange.HTTPRequest, error) {
	var q codec.Query
	for _, p := range pairs {
		if p.v == "" {
			continue
		}
		q = q.Add(p.k, p.v)
	}
	return signCfg(&h.signerState, h.cfg, now, http.MethodGet, path, string(q), nil, readCost(), false)
}

func msQuery(ns int64) string {
	if ns == 0 {
		return ""
	}
	return strconv.FormatInt(ns/int64(time.Millisecond), 10)
}

func parseFills(resp *exchange.HTTPResponse, sentCursor string) (*exchange.FillPage, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var page listPage[execRow]
	if err := decodeResult(e.Result, &page); err != nil {
		return nil, err
	}
	fills := make([]exchange.Fill, 0, len(page.List))
	for _, row := range page.List {
		if row.Category == "" {
			row.Category = page.Category
		}
		if f, ok := row.fill(); ok {
			fills = append(fills, f)
		}
	}
	out := &exchange.FillPage{Fills: fills}
	if page.Cursor != "" && page.Cursor != sentCursor && len(page.List) > 0 {
		out.Next = page.Cursor
	}
	return out, nil
}

func parseHistoryOrder(resp *exchange.HTTPResponse, orderID string) (*exchange.OrderUpdate, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var page listPage[orderRow]
	if err := decodeResult(e.Result, &page); err != nil {
		return nil, err
	}
	for i := range page.List {
		if page.List[i].OrderID != orderID {
			continue
		}
		up := page.List[i].update(page.Category, e.Time)
		return &up, nil
	}
	return nil, fmt.Errorf("bybit: order %s is not in history", orderID)
}
