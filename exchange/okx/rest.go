package okx

import (
	"encoding/json"
	"errors"
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

// rest is the public REST reader. Depth exists for a caller that wants a book without the
// stream; the conn worker does not use it, because the book is SnapshotStream.
type rest struct{}

var _ exchange.PublicREST = (*rest)(nil)

const (
	klineLimit = 300
	depthLimit = 400
)

func (r *rest) Listing(c symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	switch c {
	case symbol.Spot:
		return r.instruments("SPOT", c)
	case symbol.Perp:
		return r.instruments("SWAP", c)
	case symbol.Future:
		return r.instruments("FUTURES", c)
	case symbol.Inverse:
		return r.instruments("SWAP", c)
	default:
		return exchange.Call[exchange.Page[symbol.Listing]]{}, fmt.Errorf("okx: listing %s: %w", c, exchange.ErrUnsupported)
	}
}

// instruments reads one instType. OKX ignores limit and after on this path: a second request
// with a cursor returns the same page. Perp and Inverse are both SWAP, split by ctType.
// Coin-margined dated futures are not listed. An Inverse ticker cannot carry an expiry, and
// the linear USD_UM future already uses Okx_Future_<pair>-<date>.
func (r *rest) instruments(instType string, want symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return exchange.Call[exchange.Page[symbol.Listing]]{
		Req: marketGet("/api/v5/public/instruments", []pair{{"instType", instType}}),
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[symbol.Listing], error) {
			raw, err := publicData(resp)
			if err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			var rows []instRow
			if err := codec.DecodeBody(raw, &rows); err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			items := make([]symbol.Listing, 0, len(rows))
			for _, row := range rows {
				if item, ok := oneInstrument(row, want); ok {
					items = append(items, item)
				}
			}
			return exchange.Page[symbol.Listing]{Items: items}, nil
		},
	}, nil
}

func (r *rest) Depth(inst *symbol.Info, limit int) (exchange.Call[*exchange.BookUpdate], error) {
	if _, err := needInst(inst); err != nil {
		return exchange.Call[*exchange.BookUpdate]{}, err
	}
	if limit <= 0 || limit > depthLimit {
		limit = depthLimit
	}
	return exchange.Call[*exchange.BookUpdate]{
		Req: marketGet("/api/v5/market/books", []pair{
			{"instId", inst.ExchTicker}, {"sz", strconv.Itoa(limit)},
		}),
		Parse: parseDepth,
	}, nil
}

func (r *rest) Klines(inst *symbol.Info, interval string, endNs int64, limit int) (exchange.Call[[]exchange.Kline], error) {
	if _, err := needInst(inst); err != nil {
		return exchange.Call[[]exchange.Kline]{}, err
	}
	wire, ok := wireInterval(interval)
	if !ok {
		return exchange.Call[[]exchange.Kline]{}, fmt.Errorf("okx: kline interval %q: %w", interval, exchange.ErrUnsupported)
	}
	if limit <= 0 || limit > klineLimit {
		limit = klineLimit
	}
	pairs := []pair{
		{"instId", inst.ExchTicker}, {"bar", wire}, {"limit", strconv.Itoa(limit)},
	}
	if endNs != 0 {
		// after asks for candles earlier than this millisecond.
		pairs = append(pairs, pair{"after", strconv.FormatInt(endNs/int64(time.Millisecond), 10)})
	}
	return exchange.Call[[]exchange.Kline]{
		Req: marketGet("/api/v5/market/candles", pairs),
		Parse: func(resp *exchange.HTTPResponse) ([]exchange.Kline, error) {
			return parseKlines(resp, interval, inst.ExchTicker)
		},
	}, nil
}

func (r *rest) KlinePage() int { return klineLimit }

// OpenInterest: the open-interest channel pushes it (D401).
func (r *rest) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, fmt.Errorf("okx: open interest is pushed on open-interest: %w", exchange.ErrUnsupported)
}

func (r *rest) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }

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
		Header: []exchange.Header{{Name: "User-Agent", Value: userAgent}},
	}
}

func publicData(resp *exchange.HTTPResponse) (json.RawMessage, error) {
	e, err := readEnv(resp)
	if err != nil {
		return nil, err
	}
	if e.Code != "0" {
		return nil, fmt.Errorf("okx: code %s: %s", e.Code, e.Msg)
	}
	return e.Data, nil
}

type instRow struct {
	InstID     string `json:"instId"`
	InstType   string `json:"instType"`
	Uly        string `json:"uly"`
	InstFamily string `json:"instFamily"`
	BaseCcy    string `json:"baseCcy"`
	QuoteCcy   string `json:"quoteCcy"`
	SettleCcy  string `json:"settleCcy"`
	CtVal      string `json:"ctVal"`
	CtValCcy   string `json:"ctValCcy"`
	CtType     string `json:"ctType"`
	LotSz      string `json:"lotSz"`
	MinSz      string `json:"minSz"`
	TickSz     string `json:"tickSz"`
	MaxLmtSz   string `json:"maxLmtSz"`
	MaxLmtAmt  string `json:"maxLmtAmt"`
	State      string `json:"state"`
	ExpTime    string `json:"expTime"`
}

func oneInstrument(row instRow, want symbol.Category) (symbol.Listing, bool) {
	if row.InstID == "" {
		return symbol.Listing{}, false
	}
	var item symbol.Listing
	var ok bool
	switch want {
	case symbol.Spot:
		if row.InstType != "SPOT" {
			return symbol.Listing{}, false
		}
		item, ok = spotListing(row)
	case symbol.Perp:
		if row.InstType != "SWAP" || row.CtType != "linear" {
			return symbol.Listing{}, false
		}
		item, ok = contractListing(row, symbol.Perp, false)
	case symbol.Future:
		if row.InstType != "FUTURES" || row.CtType != "linear" {
			return symbol.Listing{}, false
		}
		item, ok = contractListing(row, symbol.Future, true)
	case symbol.Inverse:
		if row.InstType != "SWAP" || row.CtType != "inverse" {
			return symbol.Listing{}, false
		}
		item, ok = contractListing(row, symbol.Inverse, false)
	default:
		return symbol.Listing{}, false
	}
	if !ok {
		return symbol.Listing{}, false
	}
	if !fitsSteps(item.Filters) {
		return symbol.Listing{}, false
	}
	return keepListing(item)
}

func spotListing(row instRow) (symbol.Listing, bool) {
	base, ok := canonAsset(row.BaseCcy)
	if !ok {
		return symbol.Listing{}, false
	}
	quote, ok := canonAsset(row.QuoteCcy)
	if !ok || base == quote {
		return symbol.Listing{}, false
	}
	f, ok := spotFilters(row)
	if !ok {
		return symbol.Listing{}, false
	}
	return symbol.Listing{
		Venue: Name, Category: symbol.Spot, Base: base, Quote: quote, ExchTicker: row.InstID,
		Status: instStatus(row.State), Filters: f,
	}, true
}

func contractListing(row instRow, want symbol.Category, dated bool) (symbol.Listing, bool) {
	uly := row.Uly
	if uly == "" {
		uly = row.InstFamily
	}
	base, quote, ok := splitPair(uly)
	if !ok {
		return symbol.Listing{}, false
	}
	base, ok = canonAsset(base)
	if !ok {
		return symbol.Listing{}, false
	}
	quote, ok = canonAsset(quote)
	if !ok || base == quote {
		return symbol.Listing{}, false
	}
	size, ok := posNum(row.CtVal)
	if !ok {
		return symbol.Listing{}, false
	}
	settle, ok := canonAsset(row.SettleCcy)
	if !ok {
		return symbol.Listing{}, false
	}
	var value string
	switch {
	case row.CtType == "linear" && settle == quote && sameAsset(row.CtValCcy, base):
		value = base
	case row.CtType == "inverse" && settle == base && sameAsset(row.CtValCcy, quote):
		value = quote
	default:
		return symbol.Listing{}, false // quanto, or the currencies disagree with ctType
	}
	f, ok := contractFilters(row)
	if !ok {
		return symbol.Listing{}, false
	}
	item := symbol.Listing{
		Venue: Name, Category: want, Base: base, Quote: quote, ExchTicker: row.InstID,
		ContractSize: size, ContractValueCcy: value, PriceCcy: quote, Settle: settle,
		Status: instStatus(row.State), Filters: f,
	}
	if dated {
		code, ok := expiryCode(row.InstID)
		if !ok {
			return symbol.Listing{}, false
		}
		item.ExpiryCode = code
		item.ExpiryNs = expNS(row.ExpTime)
	}
	return item, true
}

func spotFilters(row instRow) (symbol.Filters, bool) {
	tick, ok := posNum(row.TickSz)
	if !ok {
		return nil, false
	}
	step, ok := posNum(row.LotSz)
	if !ok {
		return nil, false
	}
	f := symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step}
	if min, ok := posNum(row.MinSz); ok {
		f[symbol.MinQty] = min
	}
	if !putMax(f, symbol.MaxQty, step, posOrZero(row.MaxLmtSz)) {
		return nil, false
	}
	if !putMax(f, symbol.MaxPrice, tick, 0) {
		return nil, false
	}
	if amt, ok := posNum(row.MaxLmtAmt); ok {
		f[symbol.MaxNotional] = amt
	}
	return qtyRange(f)
}

func contractFilters(row instRow) (symbol.Filters, bool) {
	tick, ok := posNum(row.TickSz)
	if !ok {
		return nil, false
	}
	step, ok := mulStr(row.LotSz, row.CtVal)
	if !ok {
		return nil, false
	}
	f := symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step}
	if min, ok := mulStr(row.MinSz, row.CtVal); ok {
		f[symbol.MinQty] = min
	}
	maxQty := 0.0
	if mx, ok := mulStr(row.MaxLmtSz, row.CtVal); ok {
		maxQty = mx
	}
	if !putMax(f, symbol.MaxQty, step, maxQty) {
		return nil, false
	}
	if !putMax(f, symbol.MaxPrice, tick, 0) {
		return nil, false
	}
	if amt, ok := posNum(row.MaxLmtAmt); ok {
		f[symbol.MaxNotional] = amt
	}
	return qtyRange(f)
}

func qtyRange(f symbol.Filters) (symbol.Filters, bool) {
	min, hasMin := f[symbol.MinQty]
	max, hasMax := f[symbol.MaxQty]
	if hasMin && hasMax && min > max {
		return nil, false
	}
	return f, true
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

// fitsSteps is the D008 check conformance runs on every stored listing.
func fitsSteps(f symbol.Filters) bool {
	for _, pair := range [][2]string{{symbol.PriceTick, symbol.MaxPrice}, {symbol.QtyStep, symbol.MaxQty}} {
		step, ok := f[pair[0]]
		if !ok || !decimal.Has(step) || !(step > 0) || math.IsInf(step, 0) {
			return false
		}
		top := f[pair[1]]
		if !(top > 0) || math.IsInf(top, 0) {
			top = 1e7
		}
		if len(strconv.FormatFloat(math.Floor(top), 'f', 0, 64))+decimal.Places(step) > 15 {
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

func instStatus(s string) symbol.Status {
	switch s {
	case "live":
		return symbol.Tradable
	case "suspend":
		return symbol.Halted
	case "preopen":
		return symbol.Prelisting
	case "expired":
		return symbol.Expired
	default:
		return symbol.Unknown
	}
}

func splitPair(s string) (string, string, bool) {
	base, quote, ok := strings.Cut(s, "-")
	if !ok || base == "" || quote == "" || strings.Contains(quote, "-") {
		return "", "", false
	}
	return base, quote, true
}

func canonAsset(s string) (string, bool) {
	c, err := symbol.CanonicalAsset(s)
	if err != nil {
		return "", false
	}
	return c, true
}

func sameAsset(native, canon string) bool {
	c, ok := canonAsset(native)
	return ok && c == canon
}

func expiryCode(id string) (string, bool) {
	i := strings.LastIndexByte(id, '-')
	if i < 0 || !isDate(id[i+1:]) {
		return "", false
	}
	return id[i+1:], true
}

func expNS(s string) int64 {
	if s == "" || s == "0" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v * int64(time.Millisecond)
}

func posOrZero(s string) float64 {
	v, ok := posNum(s)
	if !ok {
		return 0
	}
	return v
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

// mulStr is a * b, rounded to the sum of their decimal places, so 0.01 * 0.01 is 0.0001
// and not a float that prints with a tail.
func mulStr(a, b string) (float64, bool) {
	af, err := strconv.ParseFloat(a, 64)
	if err != nil {
		return 0, false
	}
	bf, err := strconv.ParseFloat(b, 64)
	if err != nil {
		return 0, false
	}
	places := decPlaces(a) + decPlaces(b)
	if places > 15 {
		return 0, false
	}
	p := af * bf
	if places > 0 {
		scale := math.Pow10(places)
		p = math.Round(p*scale) / scale
	}
	if !(p > 0) || math.IsInf(p, 0) {
		return 0, false
	}
	return p, true
}

func decPlaces(s string) int {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return len(s) - i - 1
	}
	return 0
}

// putMax stores the largest bound that still fits float64 at step (D008). OKX's own max can
// be too wide: spot lotSz 1e-8 with maxLmtSz 9999999999 needs 18 digits, and float64 holds 15.
// When the venue publishes no max and ten million fits, the bound is left unset.
func putMax(f symbol.Filters, maxKey string, step, published float64) bool {
	top, ok := boundMax(step, published)
	if !ok {
		return false
	}
	if !(published > 0) {
		if len(strconv.FormatFloat(math.Floor(1e7), 'f', 0, 64))+decimal.Places(step) <= 15 {
			return true
		}
	}
	f[maxKey] = top
	return true
}

func boundMax(step, published float64) (float64, bool) {
	if !(step > 0) || math.IsInf(step, 0) {
		return 0, false
	}
	places := decimal.Places(step)
	if places >= 15 {
		return 0, false
	}
	cap := math.Pow10(15-places) - 1
	top := published
	if !(top > 0) || math.IsInf(top, 0) || top > cap {
		top = cap
	}
	top = decimal.SnapDown(top, step)
	for top > 0 && len(strconv.FormatFloat(math.Floor(top), 'f', 0, 64))+places > 15 {
		top = decimal.SnapDown(top/10, step)
	}
	if !(top > 0) {
		return 0, false
	}
	return top, true
}

func parseDepth(resp *exchange.HTTPResponse) (*exchange.BookUpdate, error) {
	raw, err := publicData(resp)
	if err != nil {
		return nil, err
	}
	var rows []bookRow
	if err := codec.DecodeBody(raw, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("okx: depth is empty")
	}
	row := rows[0]
	bids, err := levels(row.Bids)
	if err != nil {
		return nil, err
	}
	asks, err := levels(row.Asks)
	if err != nil {
		return nil, err
	}
	ts, err := textMS(row.TS)
	if err != nil {
		return nil, err
	}
	u := &exchange.BookUpdate{ExchTsNs: ts, Bids: bids, Asks: asks, Snapshot: true}
	if row.Seq > 0 {
		u.Last = uint64(row.Seq)
	}
	return u, nil
}

func parseKlines(resp *exchange.HTTPResponse, interval, inst string) ([]exchange.Kline, error) {
	raw, err := publicData(resp)
	if err != nil {
		return nil, err
	}
	var rows [][]string
	if err := codec.DecodeBody(raw, &rows); err != nil {
		return nil, err
	}
	out := make([]exchange.Kline, 0, len(rows))
	for _, row := range rows {
		k, err := klineFrom(row, interval, inst)
		if err != nil {
			return nil, err
		}
		if !k.Closed {
			continue
		}
		out = append(out, k)
	}
	// The venue returns the newest candle first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}
