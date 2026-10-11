package bybit

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// platformCodes is the 200–299 band in doc.go. A code that is not listed is returned as itself.
var platformCodes = map[int]int32{
	110001: 200, 110008: 200, 110010: 200,
	170139: 200, 170142: 200, 170143: 200, 170213: 200,

	110004: 201, 110006: 201, 110007: 201, 110012: 201, 110044: 201, 110045: 201,
	110051: 201, 110052: 201, 110053: 201,
	170033: 201, 170131: 201,

	110017: 202, 110094: 202,
	170124: 202, 170136: 202, 170140: 202, 170148: 202, 170341: 202, 170381: 202, 170382: 202,

	110003: 203, 110120: 203, 110121: 203,
	30208: 203, 30209: 203,
	170132: 203, 170133: 203, 170134: 203, 170137: 203,
	170192: 203, 170193: 203, 170194: 203, 170371: 203, 170372: 203,

	170218: 204,

	110023: 205, 110042: 205,

	110009: 207, 110020: 207,
	170005: 207, 170810: 207,

	110072: 208, 170141: 208,
}

type envelope struct {
	Code   int             `json:"retCode"`
	Msg    string          `json:"retMsg"`
	Result json.RawMessage `json:"result"`
	Ext    json.RawMessage `json:"retExtInfo"`
	Time   int64           `json:"time"`
}

func readEnv(resp *exchange.HTTPResponse) (envelope, error) {
	var e envelope
	if resp == nil {
		return e, errors.New("bybit: empty response")
	}
	if len(resp.Body) > 0 {
		if err := codec.DecodeBody(resp.Body, &e); err != nil {
			return e, stampRetry(resp, statusFallback(resp.Status, err))
		}
	}
	if err := classify(resp.Status, e.Code, e.Msg); err != nil {
		return e, stampRetry(resp, err)
	}
	return e, nil
}

// stampRetry copies Retry-After, in seconds, onto a rate-limit error. Anything that is not a
// positive integer leaves RetryAfterMs at 0.
func stampRetry(resp *exchange.HTTPResponse, err error) error {
	var rl *exchange.RateLimited
	if resp == nil || !errors.As(err, &rl) || rl.RetryAfterMs != 0 {
		return err
	}
	sec, conv := strconv.Atoi(strings.TrimSpace(resp.Get("Retry-After")))
	if conv != nil || sec <= 0 {
		return err
	}
	rl.RetryAfterMs = int64(sec) * 1000
	return err
}

func decodeResult(raw json.RawMessage, v any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return codec.DecodeBody(raw, v)
}

func statusFallback(status int, err error) error {
	switch status {
	case http.StatusTooManyRequests, http.StatusForbidden:
		return &exchange.RateLimited{}
	case http.StatusUnauthorized:
		return fmt.Errorf("bybit: http %d: %w", status, exchange.ErrAuth)
	default:
		return fmt.Errorf("bybit: http %d: %w", status, err)
	}
}

func classify(status, code int, msg string) error {
	if msg == "" {
		msg = "refused"
	}
	switch {
	case status == http.StatusTooManyRequests || code == 10006 || code == 10018 || code == 429 || code == 170222 ||
		code == 10403 || code == 10429:
		return &exchange.RateLimited{}
	case code == 10003 || code == 10004 || code == 10005 || code == 10007 || code == 10010 ||
		code == 33004 || code == -2015 || status == http.StatusUnauthorized:
		return fmt.Errorf("bybit: %s: %w", msg, exchange.ErrAuth)
	case code == 10000 || code == 10016 || code == 10019:
		return fmt.Errorf("bybit: %s: %w", msg, exchange.ErrUnknownOutcome)
	case status == http.StatusForbidden && code == 0:
		return &exchange.RateLimited{}
	}
	if code == 0 {
		if status >= 400 {
			return fmt.Errorf("bybit: http %d: %s", status, msg)
		}
		return nil
	}
	mapped := int32(code)
	if c, ok := platformCodes[code]; ok {
		mapped = c
	}
	return &exchange.Reject{Code: mapped, Native: strconv.Itoa(code), Reason: msg}
}

func parseAck(resp *exchange.HTTPResponse) (struct{}, error) {
	if _, err := readEnv(resp); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, nil
}

type orderRow struct {
	Category   string        `json:"category"`
	Symbol     string        `json:"symbol"`
	OrderID    string        `json:"orderId"`
	LinkID     string        `json:"orderLinkId"`
	Side       string        `json:"side"`
	OrderType  string        `json:"orderType"`
	Status     string        `json:"orderStatus"`
	Qty        codec.Decimal `json:"qty"`
	Price      codec.Decimal `json:"price"`
	Avg        codec.Decimal `json:"avgPrice"`
	Filled     codec.Decimal `json:"cumExecQty"`
	MarketUnit string        `json:"marketUnit"`
	Reject     string        `json:"rejectReason"`
	Created    string        `json:"createdTime"`
	Updated    string        `json:"updatedTime"`
}

func (o orderRow) update(cat string, fallbackMs int64) exchange.OrderUpdate {
	if o.Category != "" {
		cat = o.Category
	}
	qty, quote := qtyOf(o.Qty), decimal.Absent()
	if o.MarketUnit == "quoteCoin" {
		quote = num(o.Qty)
		if f := qtyOf(o.Filled); f > 0 {
			qty = f
		} else {
			qty = decimal.Absent()
		}
	}
	ts := msString(o.Updated)
	if ts == 0 {
		ts = msString(o.Created)
	}
	if ts == 0 {
		ts = ms(fallbackMs)
	}
	return exchange.OrderUpdate{
		Exch: exchKey(cat, o.Symbol), OrderID: o.OrderID, ClientOrderID: o.LinkID,
		Side: sideOf(o.Side), Type: typeOf(o.OrderType), Status: statusOf(o.Status),
		Qty: qty, QuoteQty: quote, Price: optNum(o.Price), FilledQty: qtyOf(o.Filled),
		AvgPrice: optNum(o.Avg), RejectReason: rejectReason(o.Status, o.Reject), ExchTsNs: ts,
	}
}

type execRow struct {
	Category  string        `json:"category"`
	Symbol    string        `json:"symbol"`
	ExecID    string        `json:"execId"`
	ExecType  string        `json:"execType"`
	ExecPrice codec.Decimal `json:"execPrice"`
	ExecQty   codec.Decimal `json:"execQty"`
	ExecFee   codec.Decimal `json:"execFee"`
	FeeCcy    string        `json:"feeCurrency"`
	OrderID   string        `json:"orderId"`
	LinkID    string        `json:"orderLinkId"`
	Side      string        `json:"side"`
	IsMaker   bool          `json:"isMaker"`
	ExecTime  string        `json:"execTime"`
}

// fillExec is a trade the history endpoint will also report under the same execId (D601).
func fillExec(t string) bool {
	switch t {
	case "Trade", "AdlTrade", "BustTrade", "BlockTrade":
		return true
	default:
		return false
	}
}

func (e execRow) fill() (exchange.Fill, bool) {
	if !fillExec(e.ExecType) || e.ExecID == "" || qtyOf(e.ExecQty) == 0 {
		return exchange.Fill{}, false
	}
	liq := "taker"
	if e.IsMaker {
		liq = "maker"
	}
	return exchange.Fill{
		Exch: exchKey(e.Category, e.Symbol), FillID: e.ExecID, OrderID: e.OrderID, ClientOrderID: e.LinkID,
		Side: sideOf(e.Side), Price: num(e.ExecPrice), Qty: qtyOf(e.ExecQty),
		Fee: num(e.ExecFee), FeeAsset: e.FeeCcy, Liquidity: liq, ExchTsNs: msString(e.ExecTime),
	}, true
}

type posRow struct {
	Symbol   string        `json:"symbol"`
	Category string        `json:"category"`
	Side     string        `json:"side"`
	Size     codec.Decimal `json:"size"`
	Avg      codec.Decimal `json:"avgPrice"`
	Entry    codec.Decimal `json:"entryPrice"`
	Mark     codec.Decimal `json:"markPrice"`
	UPL      codec.Decimal `json:"unrealisedPnl"`
	Lev      string        `json:"leverage"`
	Updated  string        `json:"updatedTime"`
}

func (p posRow) position(cat string) (exchange.Position, bool) {
	size := qtyOf(p.Size)
	if size == 0 {
		return exchange.Position{}, false
	}
	if p.Side == "Sell" {
		size = -size
	}
	if p.Category != "" {
		cat = p.Category
	}
	// REST sends avgPrice. The private topic sends entryPrice and leaves avgPrice out, which
	// decodes as 0, so a zero avgPrice falls through.
	entry := optNum(p.Avg)
	if alt := optNum(p.Entry); (!decimal.Has(entry) || entry == 0) && decimal.Has(alt) && alt != 0 {
		entry = alt
	}
	lev := decimal.Absent()
	if p.Lev != "" {
		if v, err := strconv.ParseFloat(p.Lev, 64); err == nil {
			lev = v
		}
	}
	return exchange.Position{
		Exch: exchKey(cat, p.Symbol), Size: size, EntryPrice: entry,
		MarkPrice: optNum(p.Mark), UnrealizedPnl: optNum(p.UPL), Leverage: lev,
	}, true
}

type coinRow struct {
	Coin    string        `json:"coin"`
	Wallet  codec.Decimal `json:"walletBalance"`
	Locked  codec.Decimal `json:"locked"`
	OrderIM codec.Decimal `json:"totalOrderIM"`
}

// balance reports the venue's own locks. Free is the wallet minus the spot-order lock and the
// order initial margin. Position margin stays in the wallet: it is the position, not an order
// lock. The account worker adds prelock on top (D603).
func (c coinRow) balance() (exchange.Balance, bool) {
	if c.Coin == "" {
		return exchange.Balance{}, false
	}
	wallet := qtyOf(c.Wallet)
	locked := qtyOf(c.Locked) + qtyOf(c.OrderIM)
	if wallet == 0 && locked == 0 {
		return exchange.Balance{}, false
	}
	free := wallet - locked
	if free < 0 {
		free = 0
	}
	return exchange.Balance{Asset: c.Coin, Free: free, Locked: locked}, true
}

func snapPage(s exchange.AccountSnapshot) exchange.Page[exchange.AccountSnapshot] {
	return exchange.Page[exchange.AccountSnapshot]{Items: []exchange.AccountSnapshot{s}}
}
