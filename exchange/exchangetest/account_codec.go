package exchangetest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

// Account is this venue's adapter for cfg: REST Calls to cfg.RESTEndpoint (the venue's own base when
// empty), signed with cfg.Credentials.Key, and a stream codec that dials cfg.StreamEndpoint.
func (v *AccountVenue) Account(cfg exchange.AccountConfig) exchange.Account {
	base := cfg.RESTEndpoint
	if base == "" {
		base = v.base
	}
	stream := cfg.StreamEndpoint
	if stream == "" {
		stream = "ws://stream.exchangetest.invalid"
	}
	return &account{v: v, base: base, stream: strings.TrimSuffix(stream, "/"), key: cfg.Credentials.Key}
}

type account struct {
	v      *AccountVenue
	base   string
	stream string
	key    string
}

var (
	_ exchange.Account            = (*account)(nil)
	_ exchange.CancelOnDisconnect = (*account)(nil)
)

func (a *account) Limits() exchange.AccountLimits {
	l := a.v.opts.Limits
	l.OrderRate = append([]exchange.RateLimit(nil), l.OrderRate...)
	return l
}

func (a *account) req(method, path string, query url.Values, body []byte, mutates bool) exchange.HTTPRequest {
	r := exchange.HTTPRequest{
		Method: method, Base: a.base, Path: path, Body: body, Mutates: mutates,
		Header: []exchange.Header{{Name: "X-Key", Value: a.key}},
		Cost:   []exchange.Cost{{Bucket: "requests", N: 1}},
	}
	if len(query) > 0 {
		r.Query = []byte(query.Encode())
	}
	return r
}

func keyOf(r *exchange.HTTPRequest) string {
	for _, h := range r.Header {
		if h.Name == "X-Key" {
			return h.Value
		}
	}
	return ""
}

// classifyResp turns a response other than 200 into the error Parse returns.
func classifyResp(resp *exchange.HTTPResponse) error {
	switch resp.Status {
	case 200:
		return nil
	case 401:
		return exchange.ErrAuth
	case 429:
		e := &exchange.RateLimited{}
		if s, err := strconv.ParseInt(resp.Get("Retry-After"), 10, 64); err == nil {
			e.RetryAfterMs = s * 1000
		}
		return e
	case 400:
		var b batchEntry
		if err := json.Unmarshal(resp.Body, &b); err != nil || b.Code == 0 {
			return fmt.Errorf("exchangetest: status 400: %s", resp.Body)
		}
		return venueReject(b.Code, b.Msg)
	default:
		return fmt.Errorf("exchangetest: status %d", resp.Status)
	}
}

func parseJSON[T any](resp *exchange.HTTPResponse) (T, error) {
	var out T
	if err := classifyResp(resp); err != nil {
		return out, err
	}
	err := json.Unmarshal(resp.Body, &out)
	return out, err
}

func (a *account) Submit(now time.Time, req *exchange.OrderRequest) (exchange.Call[*exchange.OrderResult], error) {
	if req == nil || req.Instrument == nil {
		return exchange.Call[*exchange.OrderResult]{}, errors.New("exchangetest: order without an instrument")
	}
	body := mustJSON(orderReq{
		Ts: now.UnixNano(), Cat: req.Instrument.Ticker.Category, Sym: req.Instrument.ExchTicker,
		Side: req.Side, Type: req.Type, TIF: req.TIF, Qty: num(req.Qty), Quote: num(req.QuoteQty),
		Price: num(req.Price), Reduce: req.ReduceOnly, Cid: req.ClientOrderID, Params: req.Params,
	})
	return exchange.Call[*exchange.OrderResult]{
		Req: a.req("POST", "/order", nil, body, true),
		Parse: func(resp *exchange.HTTPResponse) (*exchange.OrderResult, error) {
			ans, err := parseJSON[submitAns](resp)
			if err != nil {
				return nil, err
			}
			return &exchange.OrderResult{OrderID: ans.ID, Status: ans.Status, ExchTsNs: ans.Ts}, nil
		},
	}, nil
}

func cancelOf(now time.Time, ref exchange.CancelRef) cancelReq {
	r := cancelReq{Ts: now.UnixNano(), ID: ref.OrderID, Cid: ref.ClientOrderID}
	if ref.Instrument != nil {
		r.Cat, r.Sym = ref.Instrument.Ticker.Category, ref.Instrument.ExchTicker
	}
	return r
}

func (a *account) Cancel(now time.Time, ref exchange.CancelRef) (exchange.Call[struct{}], error) {
	if ref.OrderID == "" && ref.ClientOrderID == "" {
		return exchange.Call[struct{}]{}, errors.New("exchangetest: cancel names no order")
	}
	return exchange.Call[struct{}]{
		Req: a.req("DELETE", "/order", nil, mustJSON(cancelOf(now, ref)), true),
		Parse: func(resp *exchange.HTTPResponse) (struct{}, error) {
			return struct{}{}, classifyResp(resp)
		},
	}, nil
}

func (a *account) CancelBatch(now time.Time, refs []exchange.CancelRef) (exchange.Call[[]error], error) {
	if len(refs) == 0 || len(refs) > a.v.opts.Limits.CancelBatch {
		return exchange.Call[[]error]{}, fmt.Errorf("exchangetest: %d orders in one batch, limit %d", len(refs), a.v.opts.Limits.CancelBatch)
	}
	body := make([]cancelReq, len(refs))
	for i, r := range refs {
		body[i] = cancelOf(now, r)
	}
	n := len(refs)
	return exchange.Call[[]error]{
		Req: a.req("DELETE", "/batch", nil, mustJSON(body), true),
		Parse: func(resp *exchange.HTTPResponse) ([]error, error) {
			ans, err := parseJSON[batchAns](resp)
			if err != nil {
				return nil, err
			}
			if len(ans.Results) != n {
				return nil, fmt.Errorf("exchangetest: %d results for %d orders", len(ans.Results), n)
			}
			out := make([]error, n)
			for i, e := range ans.Results {
				if e.Code != 0 {
					out[i] = venueReject(e.Code, e.Msg)
				}
			}
			return out, nil
		},
	}, nil
}

func (a *account) Snapshot(now time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	return []exchange.Call[exchange.Page[exchange.AccountSnapshot]]{
		a.snapPage(SnapshotOrders, 0), a.snapPage(SnapshotPositions, 0), a.snapPage(SnapshotBalances, 0),
	}, nil
}

func (a *account) snapPage(part SnapshotPart, page int) exchange.Call[exchange.Page[exchange.AccountSnapshot]] {
	q := url.Values{"page": {strconv.Itoa(page)}}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{
		Req: a.req("GET", snapshotPaths[part], q, nil, false),
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[exchange.AccountSnapshot], error) {
			ans, err := parseJSON[snapAns](resp)
			if err != nil {
				return exchange.Page[exchange.AccountSnapshot]{}, err
			}
			var s exchange.AccountSnapshot
			for _, o := range ans.Orders {
				s.OpenOrders = append(s.OpenOrders, o.update())
			}
			for _, p := range ans.Positions {
				s.Positions = append(s.Positions, p.position())
			}
			for _, b := range ans.Balances {
				s.Balances = append(s.Balances, b.balance())
			}
			s.HedgeMode = ans.Hedge
			p := exchange.Page[exchange.AccountSnapshot]{Items: []exchange.AccountSnapshot{s}}
			if ans.More {
				p.Next = func(time.Time) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
					return a.snapPage(part, page+1), nil
				}
			}
			return p, nil
		},
	}
}

func (a *account) Leverage(now time.Time, inst *symbol.Info) (exchange.Call[float64], error) {
	if inst == nil {
		return exchange.Call[float64]{}, errors.New("exchangetest: leverage without an instrument")
	}
	q := url.Values{"cat": {string(inst.Ticker.Category)}, "sym": {inst.ExchTicker}}
	return exchange.Call[float64]{
		Req: a.req("GET", "/leverage", q, nil, false),
		Parse: func(resp *exchange.HTTPResponse) (float64, error) {
			ans, err := parseJSON[map[string]string](resp)
			if err != nil {
				return 0, err
			}
			return decimal.Parse(ans["lev"])
		},
	}, nil
}

// ArmCancelOnDisconnect records the arm at the venue (Arms); the venue does not run the timer.
func (a *account) ArmCancelOnDisconnect(now time.Time, timeout time.Duration, insts []*symbol.Info) (exchange.Call[struct{}], error) {
	r := codReq{Ts: now.UnixNano(), TimeoutMs: timeout.Milliseconds()}
	for _, in := range insts {
		r.Syms = append(r.Syms, in.ExchTicker)
	}
	return exchange.Call[struct{}]{
		Req:   a.req("POST", "/cod", nil, mustJSON(r), true),
		Parse: func(resp *exchange.HTTPResponse) (struct{}, error) { return struct{}{}, classifyResp(resp) },
	}, nil
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	return &privateStream{a: a}, nil
}

func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	if a.v.opts.Trade == nil {
		return nil, exchange.ErrUnsupported
	}
	return a.v.opts.Trade.Stream(), nil
}

// Usage reads the X-Used-<bucket> and X-Reset-<bucket> (unix ms) headers the venue sends.
func (a *account) Usage(resp *exchange.HTTPResponse, now time.Time) []exchange.Used {
	var out []exchange.Used
	for _, h := range resp.Header {
		bucket, ok := strings.CutPrefix(h.Name, "X-Used-")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(h.Value)
		if err != nil {
			continue
		}
		u := exchange.Used{Bucket: bucket, N: n}
		if ms, err := strconv.ParseInt(resp.Get("X-Reset-"+bucket), 10, 64); err == nil {
			u.ResetAt = time.UnixMilli(ms)
		}
		out = append(out, u)
	}
	return out
}

// ---------- the private stream's codec ----------

type privateStream struct{ a *account }

var _ exchange.PrivateStream = (*privateStream)(nil)

func (s *privateStream) Prepare(now time.Time) (*exchange.Call[string], error) {
	if !s.a.v.opts.ListenKey {
		return nil, nil
	}
	return &exchange.Call[string]{
		Req: s.a.req("POST", "/listenKey", nil, nil, false),
		Parse: func(resp *exchange.HTTPResponse) (string, error) {
			ans, err := parseJSON[map[string]string](resp)
			return ans["listenKey"], err
		},
	}, nil
}

func (s *privateStream) Dial(now time.Time, token string) (*exchange.Dial, error) {
	u := s.a.stream + "/private"
	if s.a.v.opts.ListenKey {
		if token == "" {
			return nil, errors.New("exchangetest: the stream needs a listen key")
		}
		u += "?listenKey=" + url.QueryEscape(token)
	}
	login := mustJSON(streamFrame{Op: "login", Key: s.a.key, Ts: now.UnixMilli()})
	return &exchange.Dial{URL: u, Frames: [][]byte{login}}, nil
}

func (s *privateStream) Refresh(now time.Time) (*exchange.Call[struct{}], error) {
	if !s.a.v.opts.ListenKey {
		return nil, nil
	}
	// As on Binance, the key is the account's: the request extends whichever one the venue issued.
	return &exchange.Call[struct{}]{
		Req:   s.a.req("PUT", "/listenKey", nil, nil, false),
		Parse: func(resp *exchange.HTTPResponse) (struct{}, error) { return struct{}{}, classifyResp(resp) },
	}, nil
}

func (s *privateStream) Limits() exchange.StreamLimits { return s.a.v.opts.Stream }
func (s *privateStream) Ping() []byte                  { return nil }

func (s *privateStream) Decode(frame []byte, sink exchange.AccountSink) error {
	var f streamFrame
	if err := json.Unmarshal(frame, &f); err != nil {
		return err
	}
	switch {
	case f.Op == "login":
		if f.Status != 200 {
			return fmt.Errorf("exchangetest: login refused: %w", exchange.ErrAuth)
		}
		sink.Ready()
	case f.Event == "expired":
		sink.Expired(f.Reason)
	case f.Event == "order" && f.Order != nil:
		u := f.Order.update()
		sink.Order(&u)
	case f.Event == "fill" && f.Fill != nil:
		x := f.Fill.fill()
		sink.Fill(&x)
	case f.Event == "balances":
		out := make([]exchange.Balance, len(f.Balances))
		for i, b := range f.Balances {
			out[i] = b.balance()
		}
		sink.Balances(out)
	case f.Event == "positions":
		out := make([]exchange.Position, len(f.Positions))
		for i, p := range f.Positions {
			out[i] = p.position()
		}
		sink.Positions(out)
	default:
		return errors.New("exchangetest: unknown frame")
	}
	return nil
}

// ---------- wire ----------

// num is v as the venue's wire carries it: shortest round-trip decimal, "" when absent.
func num(v float64) string {
	if !decimal.Has(v) || math.IsInf(v, 0) {
		return ""
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func parseNum(s string) float64 {
	if s == "" {
		return decimal.Absent()
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return decimal.Absent()
	}
	return v
}

type orderReq struct {
	Ts     int64                `json:"ts"`
	Cat    symbol.Category      `json:"cat"`
	Sym    string               `json:"sym"`
	Side   exchange.Side        `json:"side"`
	Type   exchange.OrderType   `json:"type"`
	TIF    exchange.TimeInForce `json:"tif"`
	Qty    string               `json:"qty,omitempty"`
	Quote  string               `json:"quote,omitempty"`
	Price  string               `json:"px,omitempty"`
	Reduce bool                 `json:"reduce,omitempty"`
	Cid    string               `json:"cid,omitempty"`
	Params map[string]string    `json:"params,omitempty"`
}

type cancelReq struct {
	Ts  int64           `json:"ts"`
	Cat symbol.Category `json:"cat,omitempty"`
	Sym string          `json:"sym,omitempty"`
	ID  string          `json:"id,omitempty"`
	Cid string          `json:"cid,omitempty"`
}

type codReq struct {
	Ts        int64    `json:"ts"`
	TimeoutMs int64    `json:"timeout_ms"`
	Syms      []string `json:"syms,omitempty"`
}

type submitAns struct {
	ID     string               `json:"id"`
	Status exchange.OrderStatus `json:"status"`
	Ts     int64                `json:"ts"`
}

type batchEntry struct {
	Code int    `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

type batchAns struct {
	Results []batchEntry `json:"results"`
}

type snapAns struct {
	Orders    []wireOrder    `json:"orders,omitempty"`
	Positions []wirePosition `json:"positions,omitempty"`
	Balances  []wireBalance  `json:"balances,omitempty"`
	Hedge     bool           `json:"hedge,omitempty"`
	More      bool           `json:"more,omitempty"`
}

type streamFrame struct {
	Op        string         `json:"op,omitempty"`
	Status    int            `json:"status,omitempty"`
	Key       string         `json:"key,omitempty"`
	Ts        int64          `json:"ts,omitempty"`
	Event     string         `json:"event,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	Order     *wireOrder     `json:"order,omitempty"`
	Fill      *wireFill      `json:"fill,omitempty"`
	Balances  []wireBalance  `json:"balances,omitempty"`
	Positions []wirePosition `json:"positions,omitempty"`
}

type wireOrder struct {
	Venue  string               `json:"venue"`
	Cat    symbol.Category      `json:"cat"`
	Sym    string               `json:"sym"`
	ID     string               `json:"id"`
	Cid    string               `json:"cid,omitempty"`
	Side   exchange.Side        `json:"side"`
	Type   exchange.OrderType   `json:"type"`
	Status exchange.OrderStatus `json:"status"`
	Qty    string               `json:"qty,omitempty"`
	Quote  string               `json:"quote,omitempty"`
	Price  string               `json:"px,omitempty"`
	Filled string               `json:"filled,omitempty"`
	Avg    string               `json:"avg,omitempty"`
	Reason string               `json:"reason,omitempty"`
	Ts     int64                `json:"ts"`
}

func toWireOrder(o *exchange.OrderUpdate) wireOrder {
	return wireOrder{
		Venue: o.Exch.Venue, Cat: o.Exch.Category, Sym: o.Exch.ExchTicker, ID: o.OrderID, Cid: o.ClientOrderID,
		Side: o.Side, Type: o.Type, Status: o.Status, Qty: num(o.Qty), Quote: num(o.QuoteQty),
		Price: num(o.Price), Filled: num(o.FilledQty), Avg: num(o.AvgPrice), Reason: o.RejectReason, Ts: o.ExchTsNs,
	}
}

func (w wireOrder) update() exchange.OrderUpdate {
	filled := parseNum(w.Filled)
	if !decimal.Has(filled) {
		filled = 0
	}
	return exchange.OrderUpdate{
		Exch:    symbol.ExchKey{Venue: w.Venue, Category: w.Cat, ExchTicker: w.Sym},
		OrderID: w.ID, ClientOrderID: w.Cid, Side: w.Side, Type: w.Type, Status: w.Status,
		Qty: parseNum(w.Qty), QuoteQty: parseNum(w.Quote), Price: parseNum(w.Price),
		FilledQty: filled, AvgPrice: parseNum(w.Avg), RejectReason: w.Reason, ExchTsNs: w.Ts,
	}
}

func orderFrame(o *exchange.OrderUpdate) []byte {
	w := toWireOrder(o)
	return mustJSON(streamFrame{Event: "order", Order: &w})
}

type wireFill struct {
	Venue string          `json:"venue"`
	Cat   symbol.Category `json:"cat"`
	Sym   string          `json:"sym"`
	FID   string          `json:"fid"`
	ID    string          `json:"id"`
	Cid   string          `json:"cid,omitempty"`
	Side  exchange.Side   `json:"side"`
	Price string          `json:"px"`
	Qty   string          `json:"qty"`
	Fee   string          `json:"fee,omitempty"`
	Asset string          `json:"fee_asset,omitempty"`
	Liq   string          `json:"liq,omitempty"`
	Ts    int64           `json:"ts"`
}

func fillFrame(f *exchange.Fill) []byte {
	w := wireFill{
		Venue: f.Exch.Venue, Cat: f.Exch.Category, Sym: f.Exch.ExchTicker, FID: f.FillID, ID: f.OrderID,
		Cid: f.ClientOrderID, Side: f.Side, Price: num(f.Price), Qty: num(f.Qty), Fee: num(f.Fee),
		Asset: f.FeeAsset, Liq: f.Liquidity, Ts: f.ExchTsNs,
	}
	return mustJSON(streamFrame{Event: "fill", Fill: &w})
}

func (w wireFill) fill() exchange.Fill {
	return exchange.Fill{
		Exch:   symbol.ExchKey{Venue: w.Venue, Category: w.Cat, ExchTicker: w.Sym},
		FillID: w.FID, OrderID: w.ID, ClientOrderID: w.Cid, Side: w.Side, Price: parseNum(w.Price),
		Qty: parseNum(w.Qty), Fee: parseNum(w.Fee), FeeAsset: w.Asset, Liquidity: w.Liq, ExchTsNs: w.Ts,
	}
}

type wireBalance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

func toWireBalance(b exchange.Balance) wireBalance {
	return wireBalance{Asset: b.Asset, Free: num(b.Free), Locked: num(b.Locked)}
}

func (w wireBalance) balance() exchange.Balance {
	return exchange.Balance{Asset: w.Asset, Free: parseNum(w.Free), Locked: parseNum(w.Locked)}
}

type wirePosition struct {
	Venue string          `json:"venue"`
	Cat   symbol.Category `json:"cat"`
	Sym   string          `json:"sym"`
	Size  string          `json:"size"`
	Entry string          `json:"entry,omitempty"`
	Mark  string          `json:"mark,omitempty"`
	Upnl  string          `json:"upnl,omitempty"`
	Lev   string          `json:"lev,omitempty"`
}

func toWirePosition(p exchange.Position) wirePosition {
	return wirePosition{
		Venue: p.Exch.Venue, Cat: p.Exch.Category, Sym: p.Exch.ExchTicker, Size: num(p.Size),
		Entry: num(p.EntryPrice), Mark: num(p.MarkPrice), Upnl: num(p.UnrealizedPnl), Lev: num(p.Leverage),
	}
}

func (w wirePosition) position() exchange.Position {
	size := parseNum(w.Size)
	if !decimal.Has(size) {
		size = 0
	}
	return exchange.Position{
		Exch: symbol.ExchKey{Venue: w.Venue, Category: w.Cat, ExchTicker: w.Sym}, Size: size,
		EntryPrice: parseNum(w.Entry), MarkPrice: parseNum(w.Mark), UnrealizedPnl: parseNum(w.Upnl), Leverage: parseNum(w.Lev),
	}
}
