package bybit

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// rest is the public REST reader for spot and linear. Depth exists for a caller that wants a
// book without the stream; the conn worker does not use it, because the book is SnapshotStream.
type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

const (
	klinePage  = 1000
	depthLimit = 50
)

func (r *rest) Listing(c symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	switch c {
	case symbol.Spot:
		return r.instruments("spot", c, "")
	case symbol.Perp, symbol.Future:
		return r.instruments("linear", c, "")
	default:
		return exchange.Call[exchange.Page[symbol.Listing]]{}, fmt.Errorf("bybit: public listing does not cover %s: %w", c, exchange.ErrUnsupported)
	}
}

func (r *rest) instruments(cat string, want symbol.Category, cursor string) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	req := marketGet("/v5/market/instruments-info", []pair{
		{"category", cat}, {"limit", "1000"}, {"cursor", cursor},
	})
	return exchange.Call[exchange.Page[symbol.Listing]]{
		Req: req,
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[symbol.Listing], error) {
			e, err := readEnv(resp)
			if err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			var page instPage
			if err := decodeResult(e.Result, &page); err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			items := make([]symbol.Listing, 0, len(page.List))
			for _, row := range page.List {
				if item, ok := oneInstrument(row, want); ok {
					items = append(items, item)
				}
			}
			out := exchange.Page[symbol.Listing]{Items: items}
			next := string(page.Cursor)
			if next != "" && next != cursor && len(page.List) > 0 {
				out.Next = func(time.Time) (exchange.Call[exchange.Page[symbol.Listing]], error) {
					return r.instruments(cat, want, next)
				}
			}
			return out, nil
		},
	}, nil
}

func (r *rest) Depth(inst *symbol.Info, limit int) (exchange.Call[*exchange.BookUpdate], error) {
	route, err := publicRoute(inst)
	if err != nil {
		return exchange.Call[*exchange.BookUpdate]{}, err
	}
	if limit <= 0 {
		limit = depthLimit
	}
	return exchange.Call[*exchange.BookUpdate]{
		Req: marketGet("/v5/market/orderbook", []pair{
			{"category", route}, {"symbol", inst.ExchTicker}, {"limit", strconv.Itoa(limit)},
		}),
		Parse: parseDepth,
	}, nil
}

func (r *rest) Klines(inst *symbol.Info, interval string, endNs int64, limit int) (exchange.Call[[]exchange.Kline], error) {
	route, err := publicRoute(inst)
	if err != nil {
		return exchange.Call[[]exchange.Kline]{}, err
	}
	wire, ok := wireInterval(interval)
	if !ok {
		return exchange.Call[[]exchange.Kline]{}, fmt.Errorf("bybit: kline interval %q: %w", interval, exchange.ErrUnsupported)
	}
	if limit <= 0 || limit > klinePage {
		limit = klinePage
	}
	pairs := []pair{
		{"category", route}, {"symbol", inst.ExchTicker}, {"interval", wire}, {"limit", strconv.Itoa(limit)},
	}
	if endNs != 0 {
		pairs = append(pairs, pair{"end", strconv.FormatInt(endNs/int64(time.Millisecond), 10)})
	}
	return exchange.Call[[]exchange.Kline]{
		Req: marketGet("/v5/market/kline", pairs),
		Parse: func(resp *exchange.HTTPResponse) ([]exchange.Kline, error) {
			return parseKlines(resp, interval, endNs)
		},
	}, nil
}

func (r *rest) KlinePage() int { return klinePage }

// OpenInterest: the linear ticker carries it (D401). Spot has none.
func (r *rest) OpenInterest(inst *symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	if _, err := publicRoute(inst); err != nil {
		return exchange.Call[*exchange.OpenInterest]{}, err
	}
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrUnsupported
}

func (r *rest) Usage(resp *exchange.HTTPResponse) []exchange.Used { return usedFrom(resp) }

func marketGet(path string, pairs []pair) exchange.HTTPRequest {
	var q codec.Query
	for _, p := range pairs {
		if p.v == "" {
			continue
		}
		q = q.Add(p.k, p.v)
	}
	return exchange.HTTPRequest{
		Method: http.MethodGet, Base: prodREST, Path: path, Query: []byte(q),
		Cost: []exchange.Cost{{Bucket: "ip", N: 1}},
	}
}

type instPage struct {
	List   []instRow  `json:"list"`
	Cursor wireCursor `json:"nextPageCursor"`
}

type instRow struct {
	Symbol       string      `json:"symbol"`
	ContractType string      `json:"contractType"`
	Status       string      `json:"status"`
	Base         string      `json:"baseCoin"`
	Quote        string      `json:"quoteCoin"`
	Settle       string      `json:"settleCoin"`
	Delivery     string      `json:"deliveryTime"`
	Pre          bool        `json:"isPreListing"`
	Price        priceFilter `json:"priceFilter"`
	Lot          lotFilter   `json:"lotSizeFilter"`
}

type priceFilter struct {
	Min  string `json:"minPrice"`
	Max  string `json:"maxPrice"`
	Tick string `json:"tickSize"`
}

type lotFilter struct {
	QtyStep     string `json:"qtyStep"`
	BasePrec    string `json:"basePrecision"`
	MinQty      string `json:"minOrderQty"`
	MaxQty      string `json:"maxOrderQty"`
	MinAmt      string `json:"minOrderAmt"`
	MaxAmt      string `json:"maxOrderAmt"`
	MinNotional string `json:"minNotionalValue"`
}

// wireCursor is nextPageCursor. Spot sends null when there is no next page; linear sends "".
type wireCursor string

func (c *wireCursor) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*c = ""
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*c = wireCursor(s)
	return nil
}

func oneInstrument(row instRow, want symbol.Category) (symbol.Listing, bool) {
	switch want {
	case symbol.Spot:
		return oneSpotRow(row)
	case symbol.Perp, symbol.Future:
		return oneLinearRow(row, want)
	default:
		return symbol.Listing{}, false
	}
}

// oneSpotRow keeps a spot symbol SYM can store. A row with no positive tick or step, an asset
// that is not canonical, or a tick or step that does not fit 15 significant digits (D008) is
// left out. Spot has no contract size and no settlement asset.
func oneSpotRow(row instRow) (symbol.Listing, bool) {
	base, quote, ok := pairAssets(row.Base, row.Quote)
	if !ok {
		return symbol.Listing{}, false
	}
	filters, ok := rowFilters(row, true)
	if !ok {
		return symbol.Listing{}, false
	}
	item := symbol.Listing{
		Venue: Name, Category: symbol.Spot, Base: base, Quote: quote, ExchTicker: row.Symbol,
		ContractValueCcy: base, PriceCcy: quote,
		Status: instStatus(row.Status, row.Pre), Filters: filters,
	}
	return keepListing(item)
}

// oneLinearRow keeps a linear perpetual or a dated future. Perp and Future share the linear
// page and are split on contractType. A quanto (settlement neither base nor quote), a future
// whose suffix is not DDMMMYY, and a row that fails the spot checks are left out.
func oneLinearRow(row instRow, want symbol.Category) (symbol.Listing, bool) {
	switch want {
	case symbol.Perp:
		if row.ContractType != "LinearPerpetual" {
			return symbol.Listing{}, false
		}
	case symbol.Future:
		if row.ContractType != "LinearFutures" {
			return symbol.Listing{}, false
		}
	default:
		return symbol.Listing{}, false
	}
	base, quote, ok := pairAssets(row.Base, row.Quote)
	if !ok {
		return symbol.Listing{}, false
	}
	settle, err := symbol.CanonicalAsset(row.Settle)
	if err != nil || (settle != base && settle != quote) {
		return symbol.Listing{}, false
	}
	filters, ok := rowFilters(row, false)
	if !ok {
		return symbol.Listing{}, false
	}
	item := symbol.Listing{
		Venue: Name, Category: want, Base: base, Quote: quote, ExchTicker: row.Symbol,
		ContractSize: 1, ContractValueCcy: base, PriceCcy: quote, Settle: settle,
		Status: instStatus(row.Status, row.Pre), Filters: filters,
	}
	if want == symbol.Future {
		code, ok := datedExpiry(row.Symbol)
		if !ok {
			return symbol.Listing{}, false
		}
		item.ExpiryCode = code
		item.ExpiryNs = msString(row.Delivery)
	}
	return keepListing(item)
}

func pairAssets(base, quote string) (string, string, bool) {
	b, err := symbol.CanonicalAsset(base)
	if err != nil {
		return "", "", false
	}
	q, err := symbol.CanonicalAsset(quote)
	if err != nil {
		return "", "", false
	}
	return b, q, true
}

func keepListing(item symbol.Listing) (symbol.Listing, bool) {
	if item.Validate() != nil {
		return symbol.Listing{}, false
	}
	if _, err := item.Ticker(); err != nil {
		return symbol.Listing{}, false
	}
	return item, true
}

func instStatus(s string, pre bool) symbol.Status {
	switch s {
	case "Trading":
		if pre {
			return symbol.Prelisting
		}
		return symbol.Tradable
	case "PreLaunch":
		return symbol.Prelisting
	case "Delivering", "Settling":
		return symbol.Settling
	case "Closed":
		return symbol.Delisted
	default:
		return symbol.Unknown
	}
}

func rowFilters(row instRow, spot bool) (symbol.Filters, bool) {
	tick, ok := posNum(row.Price.Tick)
	if !ok {
		return nil, false
	}
	stepSrc := row.Lot.QtyStep
	if spot {
		stepSrc = row.Lot.BasePrec
	}
	step, ok := posNum(stepSrc)
	if !ok {
		return nil, false
	}
	f := symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step}
	putPos(f, symbol.MinPrice, row.Price.Min)
	putPos(f, symbol.MaxPrice, row.Price.Max)
	putPos(f, symbol.MinQty, row.Lot.MinQty)
	putPos(f, symbol.MaxQty, row.Lot.MaxQty)
	if spot {
		putPos(f, symbol.MinNotional, row.Lot.MinAmt)
		putPos(f, symbol.MaxNotional, row.Lot.MaxAmt)
	} else {
		putPos(f, symbol.MinNotional, row.Lot.MinNotional)
	}
	if !fitsFloat(f) {
		return nil, false
	}
	return f, true
}

func putPos(f symbol.Filters, key, s string) {
	if v, ok := posNum(s); ok {
		f[key] = v
	}
}

func posNum(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || !(v > 0) || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}

// fitsFloat is the D008 bound conformance checks: the largest price and quantity, written at
// the step's decimals, have at most 15 significant digits and survive a text round trip.
func fitsFloat(f symbol.Filters) bool {
	for _, pair := range [][2]string{{symbol.PriceTick, symbol.MaxPrice}, {symbol.QtyStep, symbol.MaxQty}} {
		step := f[pair[0]]
		top := f[pair[1]]
		if !(top > 0) || math.IsInf(top, 0) {
			top = 1e7
		}
		if !(step > 0) || math.IsInf(step, 0) {
			return false
		}
		digits := len(strconv.FormatFloat(math.Floor(top), 'f', 0, 64)) + decimal.Places(step)
		if digits > 15 {
			return false
		}
		v := decimal.Snap(top, step)
		back, err := decimal.Parse(decimal.Format(v, step))
		if err != nil || decimal.Format(back, step) != decimal.Format(v, step) {
			return false
		}
	}
	return true
}

// datedExpiry reads the DDMMMYY suffix of a linear future (BTCUSDT-16OCT26 → 261016).
// ExpiryCode is that date, not a value computed from deliveryTime.
func datedExpiry(sym string) (string, bool) {
	i := strings.LastIndexByte(sym, '-')
	if i < 0 || len(sym)-i-1 != 7 {
		return "", false
	}
	suf := sym[i+1:]
	dd, mon, yy := suf[:2], strings.ToUpper(suf[2:5]), suf[5:]
	mm, ok := monthNum[mon]
	if !ok || !allDigits(dd) || !allDigits(yy) {
		return "", false
	}
	return yy + mm + dd, true
}

var monthNum = map[string]string{
	"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
	"MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
	"SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func parseDepth(resp *exchange.HTTPResponse) (*exchange.BookUpdate, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var body struct {
		Bids [][]string `json:"b"`
		Asks [][]string `json:"a"`
		Ts   int64      `json:"ts"`
		U    uint64     `json:"u"`
	}
	if err := decodeResult(e.Result, &body); err != nil {
		return nil, err
	}
	bids, err := levels(body.Bids)
	if err != nil {
		return nil, err
	}
	asks, err := levels(body.Asks)
	if err != nil {
		return nil, err
	}
	return &exchange.BookUpdate{
		ExchTsNs: ms(body.Ts), Bids: bids, Asks: asks, Snapshot: true, Last: body.U,
	}, nil
}

// parseKlines reads a newest-first page. A request with no end includes the candle that is
// still forming, which is the first row and is dropped. The rest come out oldest first.
func parseKlines(resp *exchange.HTTPResponse, interval string, endNs int64) ([]exchange.Kline, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	var body struct {
		List [][]string `json:"list"`
	}
	if err := decodeResult(e.Result, &body); err != nil {
		return nil, err
	}
	rows := body.List
	if endNs == 0 && len(rows) > 0 {
		rows = rows[1:]
	}
	endMs := endNs / int64(time.Millisecond)
	out := make([]exchange.Kline, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		if len(row) < 7 {
			return nil, fmt.Errorf("bybit: kline row %d has %d fields", i, len(row))
		}
		start, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bybit: kline row %d start %q", i, row[0])
		}
		if endNs != 0 && start > endMs {
			continue
		}
		open, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("bybit: kline row %d open", i)
		}
		high, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, fmt.Errorf("bybit: kline row %d high", i)
		}
		low, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			return nil, fmt.Errorf("bybit: kline row %d low", i)
		}
		cls, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, fmt.Errorf("bybit: kline row %d close", i)
		}
		vol, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			return nil, fmt.Errorf("bybit: kline row %d volume", i)
		}
		out = append(out, exchange.Kline{
			ExchTsNs: ms(start), Interval: interval, OpenTimeNs: ms(start),
			Open: open, High: high, Low: low, Close: cls, Volume: vol,
			QuoteVolume: parseNum(row[6]), Closed: true,
		})
	}
	return out, nil
}
