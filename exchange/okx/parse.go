package okx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// platformCodes is the 200–299 band in doc.go. A code that is not listed is returned as itself.
var platformCodes = map[string]int32{
	"51400": 200, "51401": 200, "51402": 200,

	"51008": 201,

	"51020": 202, "51121": 202,

	"51122": 203, "51137": 203, "51138": 203,

	"51169": 205,

	"51016": 208,
}

// authCodes are credential refusals. HTTP 401 is the same answer without a code.
var authCodes = map[string]bool{
	"50101": true, "50102": true, "50103": true, "50104": true, "50105": true,
	"50106": true, "50107": true, "50110": true, "50111": true, "50112": true,
	"50113": true, "50114": true,
}

type envelope struct {
	Code    string          `json:"code"`
	Msg     string          `json:"msg"`
	Data    json.RawMessage `json:"data"`
	OutTime string          `json:"outTime"`
}

// readEnv decodes the body. code 0 is success. code 1 and 2 still carry a per-order sCode,
// so they are handed back for the caller to read. Anything else is the request's own refusal.
func readEnv(resp *exchange.HTTPResponse) (envelope, error) {
	var e envelope
	if resp == nil {
		return e, errors.New("okx: empty response")
	}
	if len(resp.Body) > 0 {
		if err := codec.DecodeBody(resp.Body, &e); err != nil {
			return e, statusFallback(resp.Status, err)
		}
	}
	switch e.Code {
	case "", "0", "1", "2":
		if e.Code == "" && resp.Status >= 400 {
			return e, classify(resp.Status, "", httpText(resp.Status))
		}
		return e, nil
	default:
		if err := classify(resp.Status, e.Code, e.Msg); err != nil {
			return e, err
		}
		return e, nil
	}
}

func statusFallback(status int, err error) error {
	switch status {
	case http.StatusTooManyRequests:
		return &exchange.RateLimited{}
	case http.StatusUnauthorized:
		return fmt.Errorf("okx: http %d: %w", status, exchange.ErrAuth)
	default:
		return fmt.Errorf("okx: http %d: %w", status, err)
	}
}

func httpText(status int) string {
	if text := http.StatusText(status); text != "" {
		return text
	}
	return "refused"
}

func classify(status int, code, msg string) error {
	if msg == "" {
		msg = "refused"
	}
	switch {
	case status == http.StatusTooManyRequests || code == "50011":
		return &exchange.RateLimited{}
	case authCodes[code] || status == http.StatusUnauthorized:
		return fmt.Errorf("okx: %s: %w", msg, exchange.ErrAuth)
	case code == "50004":
		return fmt.Errorf("okx: %s: %w", msg, exchange.ErrUnknownOutcome)
	}
	if code == "" || code == "0" {
		if status >= 400 {
			return fmt.Errorf("okx: http %d: %s", status, msg)
		}
		return nil
	}
	mapped := int32(0)
	if n, err := strconv.Atoi(code); err == nil {
		mapped = int32(n)
	}
	if c, ok := platformCodes[code]; ok {
		mapped = c
	}
	return &exchange.Reject{Code: mapped, Native: code, Reason: msg}
}

type ack struct {
	OrdID   string `json:"ordId"`
	ClOrdID string `json:"clOrdId"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
	Ts      string `json:"ts"`
}

func decodeData(raw json.RawMessage, v any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return codec.DecodeBody(raw, v)
}

func parseCreate(resp *exchange.HTTPResponse) (*exchange.OrderResult, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var rows []ack
	if err := decodeData(e.Data, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if e.Code != "0" && e.Code != "" {
			return nil, classify(resp.Status, e.Code, e.Msg)
		}
		return nil, errors.New("okx: empty order ack")
	}
	row := rows[0]
	if err := classify(http.StatusOK, row.SCode, row.SMsg); err != nil {
		return nil, err
	}
	if row.OrdID == "" {
		return nil, errors.New("okx: empty order id")
	}
	// The ack carries an order id and no state. sCode 0 means the venue accepted the request;
	// the private stream is where the order's status arrives.
	return &exchange.OrderResult{
		OrderID: row.OrdID, Status: exchange.OrderStatusNew, ExchTsNs: exchNs(row.Ts, e.OutTime),
	}, nil
}

func parseAck(resp *exchange.HTTPResponse) (struct{}, error) {
	e, err := readEnv(resp)
	if err != nil {
		return struct{}{}, err
	}
	var rows []ack
	if err := decodeData(e.Data, &rows); err != nil {
		return struct{}{}, err
	}
	if len(rows) == 0 {
		if e.Code != "0" && e.Code != "" {
			return struct{}{}, classify(resp.Status, e.Code, e.Msg)
		}
		return struct{}{}, nil
	}
	if err := classify(http.StatusOK, rows[0].SCode, rows[0].SMsg); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, nil
}

func parseBatch(resp *exchange.HTTPResponse) ([]error, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var rows []ack
	if err := decodeData(e.Data, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if e.Code != "0" && e.Code != "" {
			return nil, classify(resp.Status, e.Code, e.Msg)
		}
		return nil, errors.New("okx: batch cancel has no per-order result")
	}
	out := make([]error, len(rows))
	for i, row := range rows {
		out[i] = classify(http.StatusOK, row.SCode, row.SMsg)
	}
	return out, nil
}

type orderRow struct {
	InstType string        `json:"instType"`
	InstID   string        `json:"instId"`
	OrdID    string        `json:"ordId"`
	ClOrdID  string        `json:"clOrdId"`
	Side     string        `json:"side"`
	OrdType  string        `json:"ordType"`
	State    string        `json:"state"`
	Px       codec.Decimal `json:"px"`
	Sz       codec.Decimal `json:"sz"`
	AccFill  codec.Decimal `json:"accFillSz"`
	AvgPx    codec.Decimal `json:"avgPx"`
	FillSz   codec.Decimal `json:"fillSz"`
	FillPx   codec.Decimal `json:"fillPx"`
	FillFee  codec.Decimal `json:"fillFee"`
	FillCcy  string        `json:"fillFeeCcy"`
	TradeID  string        `json:"tradeId"`
	ExecType string        `json:"execType"`
	TgtCcy   string        `json:"tgtCcy"`
	CTime    string        `json:"cTime"`
	UTime    string        `json:"uTime"`
	FillTime string        `json:"fillTime"`
}

func (o orderRow) update() exchange.OrderUpdate {
	qty, quote := num(o.Sz), decimal.Absent()
	if o.TgtCcy == "quote_ccy" {
		quote = num(o.Sz)
		if f := num(o.AccFill); f > 0 {
			qty = f
		} else {
			qty = decimal.Absent()
		}
	}
	ts := msString(o.UTime)
	if ts == 0 {
		ts = msString(o.CTime)
	}
	return exchange.OrderUpdate{
		Exch: exchKey(o.InstType, o.InstID), OrderID: o.OrdID, ClientOrderID: o.ClOrdID,
		Side: sideOf(o.Side), Type: typeOf(o.OrdType), Status: statusOf(o.State),
		Qty: qty, QuoteQty: quote, Price: optNum(o.Px), FilledQty: num(o.AccFill),
		AvgPrice: optNum(o.AvgPx), ExchTsNs: ts,
	}
}

// fill is the last trade on this order push. tradeId is the fill id (E06-D001). An order push
// with no tradeId, or a zero fillSz, is a state change and not a fill.
func (o orderRow) fill() (exchange.Fill, bool) {
	return makeFill(o.InstType, o.InstID, o.TradeID, o.OrdID, o.ClOrdID, o.Side, o.ExecType, o.FillTime, o.UTime, o.FillCcy, o.FillPx, o.FillSz, o.FillFee)
}

// makeFill is one trade, shared by the orders channel and fills-history so the two fill ids
// describe the same price, quantity and fee (E06-D001). fallback is uTime on a push and ts on a
// history row. A charge is negative, which is the sign OKX sends.
func makeFill(instType, instID, tradeID, ordID, clOrdID, side, execType, fillTime, fallback, feeCcy string, px, sz, fee codec.Decimal) (exchange.Fill, bool) {
	if tradeID == "" || num(sz) == 0 {
		return exchange.Fill{}, false
	}
	liq := ""
	switch execType {
	case "M":
		liq = "maker"
	case "T":
		liq = "taker"
	}
	ts := msString(fillTime)
	if ts == 0 {
		ts = msString(fallback)
	}
	price := optNum(px)
	if !decimal.Has(price) {
		price = num(px)
	}
	return exchange.Fill{
		Exch: exchKey(instType, instID), FillID: tradeID, OrderID: ordID, ClientOrderID: clOrdID,
		Side: sideOf(side), Price: price, Qty: num(sz),
		Fee: num(fee), FeeAsset: feeCcy, Liquidity: liq, ExchTsNs: ts,
	}, true
}

type posRow struct {
	InstType string        `json:"instType"`
	InstID   string        `json:"instId"`
	PosID    string        `json:"posId"`
	PosSide  string        `json:"posSide"`
	Pos      codec.Decimal `json:"pos"`
	AvgPx    codec.Decimal `json:"avgPx"`
	MarkPx   codec.Decimal `json:"markPx"`
	UPL      codec.Decimal `json:"upl"`
	Lever    string        `json:"lever"`
}

func (p posRow) position() (exchange.Position, bool) {
	size := num(p.Pos)
	switch p.PosSide {
	case "short":
		if size > 0 {
			size = -size
		}
	case "long":
		if size < 0 {
			size = -size
		}
	}
	lev := decimal.Absent()
	if p.Lever != "" {
		if v, err := strconv.ParseFloat(p.Lever, 64); err == nil {
			lev = v
		}
	}
	return exchange.Position{
		Exch: exchKey(p.InstType, p.InstID), Size: size, EntryPrice: optNum(p.AvgPx),
		MarkPrice: optNum(p.MarkPx), UnrealizedPnl: optNum(p.UPL), Leverage: lev,
	}, size != 0
}

type balRow struct {
	Ccy    string        `json:"ccy"`
	Avail  codec.Decimal `json:"availBal"`
	Frozen codec.Decimal `json:"frozenBal"`
}

func (b balRow) balance() (exchange.Balance, bool) {
	free, locked := num(b.Avail), num(b.Frozen)
	if b.Ccy == "" || (free == 0 && locked == 0) {
		return exchange.Balance{}, false
	}
	return exchange.Balance{Asset: b.Ccy, Free: free, Locked: locked}, true
}

func num(d codec.Decimal) float64 {
	v := float64(d)
	if !decimal.Has(v) {
		return 0
	}
	return v
}

func optNum(d codec.Decimal) float64 {
	v := float64(d)
	if !decimal.Has(v) {
		return decimal.Absent()
	}
	return v
}

func snapPage(s exchange.AccountSnapshot) exchange.Page[exchange.AccountSnapshot] {
	return exchange.Page[exchange.AccountSnapshot]{Items: []exchange.AccountSnapshot{s}}
}
