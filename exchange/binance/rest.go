package binance

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// rest is the public REST reader. UM is implemented (fapi); spot, CM and EO are not.
type rest struct{ p product }

var _ exchange.PublicREST = (*rest)(nil)

const (
	klinePage = 1500
	depthFull = 1000
)

func (r *rest) Listing(c symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	if r.p.venue.Name != VenueUM {
		return exchange.Call[exchange.Page[symbol.Listing]]{}, exchange.ErrNotImplemented
	}
	if c != symbol.Perp && c != symbol.Future {
		return exchange.Call[exchange.Page[symbol.Listing]]{}, fmt.Errorf("binance: %s does not list %s", VenueUM, c)
	}
	req := publicCall("/fapi/v1/exchangeInfo", nil, 1)
	return exchange.Call[exchange.Page[symbol.Listing]]{
		Req: req,
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[symbol.Listing], error) {
			return parseListing(resp, c)
		},
	}, nil
}

func (r *rest) Depth(inst *symbol.Info, limit int) (exchange.Call[*exchange.BookUpdate], error) {
	if r.p.venue.Name != VenueUM {
		return exchange.Call[*exchange.BookUpdate]{}, exchange.ErrNotImplemented
	}
	if err := instrument(inst); err != nil {
		return exchange.Call[*exchange.BookUpdate]{}, err
	}
	if limit <= 0 {
		limit = depthFull
	}
	w := depthWeight(limit)
	if w == 0 {
		return exchange.Call[*exchange.BookUpdate]{}, fmt.Errorf("binance: depth limit %d", limit)
	}
	q := codec.Query(nil).Add("symbol", inst.ExchTicker).AddInt("limit", int64(limit))
	return exchange.Call[*exchange.BookUpdate]{
		Req:   publicCall("/fapi/v1/depth", q, w),
		Parse: parseDepth,
	}, nil
}

func (r *rest) Klines(inst *symbol.Info, interval string, endNs int64, limit int) (exchange.Call[[]exchange.Kline], error) {
	if r.p.venue.Name != VenueUM {
		return exchange.Call[[]exchange.Kline]{}, exchange.ErrNotImplemented
	}
	if err := instrument(inst); err != nil {
		return exchange.Call[[]exchange.Kline]{}, err
	}
	wire, ok := wireInterval(interval)
	if !ok {
		return exchange.Call[[]exchange.Kline]{}, fmt.Errorf("binance: kline interval %q: %w", interval, exchange.ErrUnsupported)
	}
	if limit <= 0 || limit > klinePage {
		limit = klinePage
	}
	q := codec.Query(nil).Add("symbol", inst.ExchTicker).Add("interval", wire).AddInt("limit", int64(limit))
	if endNs != 0 {
		q = q.AddInt("endTime", endNs/int64(msUnit))
	}
	return exchange.Call[[]exchange.Kline]{
		Req: publicCall("/fapi/v1/klines", q, klineWeight(limit)),
		Parse: func(resp *exchange.HTTPResponse) ([]exchange.Kline, error) {
			return parseKlines(resp, interval, endNs)
		},
	}, nil
}

func (r *rest) KlinePage() int {
	if r.p.venue.Name != VenueUM {
		return 0
	}
	return klinePage
}

func (r *rest) OpenInterest(inst *symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	if r.p.venue.Name != VenueUM {
		return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrNotImplemented
	}
	if err := instrument(inst); err != nil {
		return exchange.Call[*exchange.OpenInterest]{}, err
	}
	q := codec.Query(nil).Add("symbol", inst.ExchTicker)
	return exchange.Call[*exchange.OpenInterest]{
		Req:   publicCall("/fapi/v1/openInterest", q, 1),
		Parse: parseOpenInterest,
	}, nil
}

func (r *rest) Usage(resp *exchange.HTTPResponse) []exchange.Used { return usedFrom(resp) }

func publicCall(path string, q codec.Query, weight int) exchange.HTTPRequest {
	return exchange.HTTPRequest{
		Method: http.MethodGet,
		Base:   prodREST,
		Path:   path,
		Query:  []byte(q),
		Cost:   weightCost(weight),
	}
}

// msUnit avoids importing time into the query math. A millisecond in nanoseconds.
const msUnit = 1_000_000

func depthWeight(limit int) int {
	switch limit {
	case 5, 10, 20, 50:
		return 2
	case 100:
		return 5
	case 500:
		return 10
	case 1000:
		return 20
	default:
		return 0
	}
}

func klineWeight(limit int) int {
	switch {
	case limit < 100:
		return 1
	case limit < 500:
		return 2
	case limit <= 1000:
		return 5
	default:
		return 10
	}
}

func okStatus(resp *exchange.HTTPResponse) error {
	if resp == nil {
		return fmt.Errorf("binance: no response")
	}
	if resp.Status != http.StatusOK {
		return refuse(resp)
	}
	return nil
}

type infoSymbol struct {
	Symbol       string       `json:"symbol"`
	ContractType string       `json:"contractType"`
	DeliveryDate int64        `json:"deliveryDate"`
	Status       string       `json:"status"`
	Base         string       `json:"baseAsset"`
	Quote        string       `json:"quoteAsset"`
	Margin       string       `json:"marginAsset"`
	Filters      []infoFilter `json:"filters"`
}

type infoFilter struct {
	Type     string        `json:"filterType"`
	MinPrice codec.Decimal `json:"minPrice"`
	MaxPrice codec.Decimal `json:"maxPrice"`
	Tick     codec.Decimal `json:"tickSize"`
	Step     codec.Decimal `json:"stepSize"`
	MinQty   codec.Decimal `json:"minQty"`
	MaxQty   codec.Decimal `json:"maxQty"`
	Notional codec.Decimal `json:"notional"`
}

func parseListing(resp *exchange.HTTPResponse, want symbol.Category) (exchange.Page[symbol.Listing], error) {
	if err := okStatus(resp); err != nil {
		return exchange.Page[symbol.Listing]{}, err
	}
	var body struct {
		Symbols []infoSymbol `json:"symbols"`
	}
	if err := codec.DecodeBody(resp.Body, &body); err != nil {
		return exchange.Page[symbol.Listing]{}, err
	}
	items := make([]symbol.Listing, 0, len(body.Symbols))
	for _, s := range body.Symbols {
		item, ok, err := oneListing(s)
		if err != nil {
			return exchange.Page[symbol.Listing]{}, err
		}
		if ok && item.Category == want {
			items = append(items, item)
		}
	}
	return exchange.Page[symbol.Listing]{Items: items}, nil
}

func oneListing(s infoSymbol) (symbol.Listing, bool, error) {
	cat, ok := listingCategory(s.ContractType)
	if !ok {
		return symbol.Listing{}, false, nil
	}
	// PnL is credited in the margin asset. When that is neither base nor quote it is quanto
	// and is not stored (D203).
	if s.Margin != s.Base && s.Margin != s.Quote {
		return symbol.Listing{}, false, nil
	}
	filters, err := listingFilters(s.Filters)
	if err != nil {
		return symbol.Listing{}, false, fmt.Errorf("binance: %s: %w", s.Symbol, err)
	}
	item := symbol.Listing{
		Venue: VenueUM, Category: cat, Base: s.Base, Quote: s.Quote, ExchTicker: s.Symbol,
		ContractSize: 1, ContractValueCcy: s.Base, PriceCcy: s.Quote, Settle: s.Margin,
		Status: listingStatus(s.Status), Filters: filters,
	}
	if cat == symbol.Future {
		code, ok := expiryCode(s.Symbol)
		if !ok {
			return symbol.Listing{}, false, fmt.Errorf("binance: %s has no YYMMDD suffix", s.Symbol)
		}
		item.ExpiryCode = code
		item.ExpiryNs = ms(s.DeliveryDate)
	}
	return item, true, nil
}

func listingCategory(contractType string) (symbol.Category, bool) {
	switch contractType {
	case "PERPETUAL", "TRADIFI_PERPETUAL":
		return symbol.Perp, true
	case "CURRENT_QUARTER", "NEXT_QUARTER":
		return symbol.Future, true
	default:
		return "", false
	}
}

func listingStatus(s string) symbol.Status {
	switch s {
	case "PENDING_TRADING":
		return symbol.Prelisting
	case "TRADING":
		return symbol.Tradable
	case "PRE_DELIVERING", "DELIVERING", "PRE_SETTLE", "SETTLING":
		return symbol.Settling
	case "DELIVERED":
		return symbol.Expired
	case "CLOSE":
		return symbol.Delisted
	default:
		return symbol.Unknown
	}
}

func expiryCode(symbol string) (string, bool) {
	i := strings.LastIndex(symbol, "_")
	if i < 0 || i == len(symbol)-1 {
		return "", false
	}
	code := symbol[i+1:]
	if len(code) != 6 {
		return "", false
	}
	for _, c := range code {
		if c < '0' || c > '9' {
			return "", false
		}
	}
	return code, true
}

func listingFilters(in []infoFilter) (symbol.Filters, error) {
	out := symbol.Filters{}
	var tick, step float64
	for _, f := range in {
		switch f.Type {
		case "PRICE_FILTER":
			tick = float64(f.Tick)
			out[symbol.PriceTick] = tick
			out[symbol.MinPrice] = float64(f.MinPrice)
			out[symbol.MaxPrice] = float64(f.MaxPrice)
		case "LOT_SIZE":
			step = float64(f.Step)
			out[symbol.QtyStep] = step
			out[symbol.MinQty] = float64(f.MinQty)
			out[symbol.MaxQty] = float64(f.MaxQty)
		case "MIN_NOTIONAL":
			out[symbol.MinNotional] = float64(f.Notional)
		}
	}
	if !(tick > 0) || !(step > 0) {
		return nil, fmt.Errorf("price tick %v qty step %v", tick, step)
	}
	return out, nil
}

func parseDepth(resp *exchange.HTTPResponse) (*exchange.BookUpdate, error) {
	if err := okStatus(resp); err != nil {
		return nil, err
	}
	var body struct {
		Last int64              `json:"lastUpdateId"`
		E    int64              `json:"E"`
		T    int64              `json:"T"`
		Bids [][2]codec.Decimal `json:"bids"`
		Asks [][2]codec.Decimal `json:"asks"`
	}
	if err := codec.DecodeBody(resp.Body, &body); err != nil {
		return nil, err
	}
	if body.Last < 0 {
		return nil, fmt.Errorf("binance: lastUpdateId %d", body.Last)
	}
	return &exchange.BookUpdate{
		ExchTsNs: eventNs(body.T, body.E),
		Bids:     levels(body.Bids), Asks: levels(body.Asks),
		Snapshot: true, Last: uint64(body.Last),
	}, nil
}

func parseKlines(resp *exchange.HTTPResponse, interval string, endNs int64) ([]exchange.Kline, error) {
	if err := okStatus(resp); err != nil {
		return nil, err
	}
	var rows [][]codec.Decimal
	if err := codec.DecodeBody(resp.Body, &rows); err != nil {
		return nil, err
	}
	endMs := endNs / msUnit
	out := make([]exchange.Kline, 0, len(rows))
	for i, row := range rows {
		if len(row) < 8 {
			return nil, fmt.Errorf("binance: kline row %d has %d fields", i, len(row))
		}
		// A latest request ends with the candle that is still forming.
		if endNs == 0 && i == len(rows)-1 {
			continue
		}
		closeMs := int64(row[6])
		if endNs != 0 && closeMs > endMs {
			continue
		}
		out = append(out, exchange.Kline{
			ExchTsNs: ms(closeMs), Interval: interval, OpenTimeNs: ms(int64(row[0])),
			Open: float64(row[1]), High: float64(row[2]), Low: float64(row[3]), Close: float64(row[4]),
			Volume: float64(row[5]), QuoteVolume: float64(row[7]), Closed: true,
		})
	}
	return out, nil
}

func parseOpenInterest(resp *exchange.HTTPResponse) (*exchange.OpenInterest, error) {
	if err := okStatus(resp); err != nil {
		return nil, err
	}
	var body struct {
		OI   codec.Decimal `json:"openInterest"`
		Time int64         `json:"time"`
	}
	if err := codec.DecodeBody(resp.Body, &body); err != nil {
		return nil, err
	}
	return &exchange.OpenInterest{
		ExchTsNs: ms(body.Time), OpenInterest: float64(body.OI), OpenInterestValue: decimal.Absent(),
	}, nil
}
