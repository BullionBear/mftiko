package binance

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

const (
	prodREST    = "https://fapi.binance.com"
	prodTrade   = "wss://ws-fapi.binance.com/ws-fapi/v1"
	prodPrivate = "wss://fstream.binance.com/private/ws"

	testHost    = "testnet.binancefuture.com"
	testTrade   = "wss://testnet.binancefuture.com/ws-fapi/v1"
	testPrivate = "wss://stream.binancefuture.com/private/ws"

	spotREST      = "https://api.binance.com"
	spotProdTrade = "wss://ws-api.binance.com/ws-api/v3"
	spotTestHost  = "testnet.binance.vision"
	spotTestTrade = "wss://ws-api.testnet.binance.vision/ws-api/v3"

	// cloidMax is newClientOrderId's limit. A E06-D002 id is at most 19 digits and fits.
	cloidMax = 36

	weight1m  = "weight_1m"
	orders10s = "orders_10s"
	orders1m  = "orders_1m"
	orders1d  = "orders_1d"
)

func (a *account) um() error {
	if a.k.name != kindUM.name {
		return exchange.ErrNotImplemented
	}
	return nil
}

func (a *account) signer() (*codec.Signer, error) {
	return a.get(a.k.name, a.cfg.Credentials)
}

func (s *signerState) get(name string, c exchange.Credentials) (*codec.Signer, error) {
	s.once.Do(func() {
		if c.Type != exchange.KeyEd25519 {
			s.signErr = fmt.Errorf("binance: %s signs with ed25519: %w", name, exchange.ErrUnsupported)
			return
		}
		s.sig, s.signErr = codec.NewSigner(c)
	})
	return s.sig, s.signErr
}

func (a *account) restBase() string {
	if a.cfg.RESTEndpoint != "" {
		return strings.TrimRight(a.cfg.RESTEndpoint, "/")
	}
	return prodREST
}

func (a *account) onTestnet() bool {
	return strings.Contains(a.restBase(), testHost)
}

func (a *account) tradeURL() string {
	if a.k.name == kindSpot.name {
		if ep := strings.TrimRight(a.cfg.StreamEndpoint, "/"); ep != "" {
			return ep
		}
		if strings.Contains(a.restBase(), spotTestHost) {
			return spotTestTrade
		}
		return spotProdTrade
	}
	if a.onTestnet() {
		return testTrade
	}
	return prodTrade
}

func (a *account) privateBase() string {
	if ep := strings.TrimRight(a.cfg.StreamEndpoint, "/"); ep != "" {
		return ep
	}
	if a.onTestnet() {
		return testPrivate
	}
	return prodPrivate
}

func (a *account) signedCall(now time.Time, method, path string, q codec.Query, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	return a.signedCallSpan(now, method, path, q, cost, mutates, 0)
}

// signedCallSpan is signedCall with a per-request window. span 0 uses AccountConfig.Validity (E06-D020).
func (a *account) signedCallSpan(now time.Time, method, path string, q codec.Query, cost []exchange.Cost, mutates bool, span time.Duration) (exchange.HTTPRequest, error) {
	return signedCallSpan(&a.signerState, a.k, a.cfg, now, method, path, q, cost, mutates, span)
}

// windowMS is recvWindow in milliseconds: the stream's account window, or span when the request
// carries its own (E06-D020).
func windowMS(accountMs int64, span time.Duration) int64 {
	return exchange.RequestWindow(time.Duration(accountMs)*time.Millisecond, span).Milliseconds()
}

func signedCall(st *signerState, k kind, cfg exchange.AccountConfig, now time.Time, method, path string, q codec.Query, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	return signedCallSpan(st, k, cfg, now, method, path, q, cost, mutates, 0)
}

func signedCallSpan(st *signerState, k kind, cfg exchange.AccountConfig, now time.Time, method, path string, q codec.Query, cost []exchange.Cost, mutates bool, span time.Duration) (exchange.HTTPRequest, error) {
	base, err := defaultREST(k)
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	sig, err := st.get(k.name, cfg.Credentials)
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	if cfg.RESTEndpoint != "" {
		base = strings.TrimRight(cfg.RESTEndpoint, "/")
	}
	w, err := cfg.Window()
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	w = exchange.RequestWindow(w, span)
	q = q.AddInt("recvWindow", w.Milliseconds()).AddInt("timestamp", now.UnixMilli())
	q = q.AddBytes("signature", sig.SignBase64(nil, q))
	return exchange.HTTPRequest{
		Method:  method,
		Base:    base,
		Path:    path,
		Query:   []byte(q),
		Header:  []exchange.Header{{Name: "X-MBX-APIKEY", Value: cfg.Credentials.Key}},
		Cost:    cost,
		Mutates: mutates,
	}, nil
}

func defaultREST(k kind) (string, error) {
	switch k.name {
	case kindUM.name:
		return prodREST, nil
	case kindSpot.name:
		return spotREST, nil
	default:
		return "", exchange.ErrNotImplemented
	}
}

func weightCost(n int) []exchange.Cost {
	return []exchange.Cost{{Bucket: weight1m, N: n}}
}

// spotOrderCost is one spot order: request weight, plus one count on the 10s and 1d order buckets.
func spotOrderCost(w, n int) []exchange.Cost {
	return []exchange.Cost{
		{Bucket: weight1m, N: w},
		{Bucket: orders10s, N: n},
		{Bucket: orders1d, N: n},
	}
}

// orderCost is one order request: request weight plus one count on each order bucket.
// n is how many orders the venue charges (a batch charges one per order).
func orderCost(w, n int) []exchange.Cost {
	return []exchange.Cost{
		{Bucket: weight1m, N: w},
		{Bucket: orders10s, N: n},
		{Bucket: orders1m, N: n},
	}
}

func apiKeyCall(base, key, method, path string, q codec.Query) exchange.HTTPRequest {
	return exchange.HTTPRequest{
		Method: method,
		Base:   base,
		Path:   path,
		Query:  []byte(q),
		Header: []exchange.Header{{Name: "X-MBX-APIKEY", Value: key}},
		Cost:   []exchange.Cost{{Bucket: weight1m, N: 1}},
	}
}

func (a *account) Usage(resp *exchange.HTTPResponse, now time.Time) []exchange.Used {
	return usedFrom(resp, now)
}

func (h *history) Usage(resp *exchange.HTTPResponse, now time.Time) []exchange.Used {
	return usedFrom(resp, now)
}

// bucketWindow is the counting window named by the header. The live windows are confirmed
// with the venue in E4-09; until then these are the intervals the header names state.
var bucketWindow = map[string]time.Duration{
	weight1m:  time.Minute,
	orders10s: 10 * time.Second,
	orders1m:  time.Minute,
	orders1d:  24 * time.Hour,
}

func usedFrom(resp *exchange.HTTPResponse, now time.Time) []exchange.Used {
	if resp == nil {
		return nil
	}
	var out []exchange.Used
	if n, ok := headerInt(resp, "X-MBX-USED-WEIGHT-1M"); ok {
		out = append(out, counted(weight1m, n, now))
	}
	if n, ok := headerInt(resp, "X-MBX-ORDER-COUNT-10S"); ok {
		out = append(out, counted(orders10s, n, now))
	}
	if n, ok := headerInt(resp, "X-MBX-ORDER-COUNT-1M"); ok {
		out = append(out, counted(orders1m, n, now))
	}
	if n, ok := headerInt(resp, "X-MBX-ORDER-COUNT-1D"); ok {
		out = append(out, counted(orders1d, n, now))
	}
	return out
}

func counted(bucket string, n int, now time.Time) exchange.Used {
	return exchange.Used{Bucket: bucket, N: n, ResetAt: windowReset(now, bucketWindow[bucket])}
}

// windowReset is the next window boundary at or after the instant just past now, aligned to the
// Unix epoch. A response that lands on a boundary belongs to the window that just opened, so the
// counter resets at the following boundary.
func windowReset(now time.Time, window time.Duration) time.Time {
	if now.IsZero() || window <= 0 {
		return time.Time{}
	}
	step := window.Nanoseconds()
	return time.Unix(0, (now.UnixNano()/step+1)*step).UTC()
}

func headerInt(resp *exchange.HTTPResponse, name string) (int, bool) {
	s := resp.Get(name)
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

// platformCodes is the 200–299 band in doc.go. A code that is not listed is returned as itself.
var platformCodes = map[int]int32{
	-2011: 200, -2013: 200,
	-2010: 201, -2018: 201, -2019: 201,
	-1013: 202, -1100: 202, -4003: 202, -4004: 202, -4005: 202, -4164: 202,
	-1111: 203, -4001: 203, -4002: 203, -4014: 203, -4131: 203,
	-2021: 204, -5022: 204,
	-2022: 205, -2024: 205, -4118: 205,
	-4061: 206,
	-1015: 207, -2025: 207, -2027: 207,
	-4015: 208,
}

func binanceErr(status, code int, msg string) error {
	switch {
	case status == http.StatusTooManyRequests || status == 418 || code == -1003:
		rl := &exchange.RateLimited{}
		if status == http.StatusTooManyRequests || status == 418 {
			rl.Status = int32(status)
		}
		return rl
	case code == -1022 || code == -2014 || code == -2015 || status == http.StatusUnauthorized || status == http.StatusForbidden:
		return fmt.Errorf("binance: %s: %w", msg, exchange.ErrAuth)
	}
	if code == 0 {
		if status >= 400 {
			return fmt.Errorf("binance: http %d: %s", status, msg)
		}
		return nil
	}
	mapped := int32(code)
	if c, ok := platformCodes[code]; ok {
		mapped = c
	}
	return &exchange.Reject{Code: mapped, Native: strconv.Itoa(code), Reason: msg}
}

type apiError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func refuse(resp *exchange.HTTPResponse) error {
	var e apiError
	if err := codec.DecodeBody(resp.Body, &e); err != nil {
		switch resp.Status {
		case http.StatusTooManyRequests, 418:
			return limited(resp)
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("binance: http %d: %w", resp.Status, exchange.ErrAuth)
		default:
			return fmt.Errorf("binance: http %d: %w", resp.Status, err)
		}
	}
	if err := binanceErr(resp.Status, e.Code, e.Msg); err != nil {
		return stampRetry(resp, err)
	}
	return fmt.Errorf("binance: http %d", resp.Status)
}

// limited is a 429 or 418. Retry-After is a delay in seconds; anything else leaves RetryAfterMs at 0.
func limited(resp *exchange.HTTPResponse) error {
	return &exchange.RateLimited{RetryAfterMs: retryAfterMs(resp), Status: int32(resp.Status)}
}

func stampRetry(resp *exchange.HTTPResponse, err error) error {
	var rl *exchange.RateLimited
	if errors.As(err, &rl) && rl.RetryAfterMs == 0 {
		rl.RetryAfterMs = retryAfterMs(resp)
	}
	return err
}

func retryAfterMs(resp *exchange.HTTPResponse) int64 {
	if resp == nil {
		return 0
	}
	sec, err := strconv.Atoi(strings.TrimSpace(resp.Get("Retry-After")))
	if err != nil || sec <= 0 {
		return 0
	}
	return int64(sec) * 1000
}

func ms(v int64) int64 {
	if v == 0 {
		return 0
	}
	return v * int64(time.Millisecond)
}

func formatDec(v, step float64) (string, error) {
	if !decimal.Has(v) {
		return "", errors.New("binance: number is absent")
	}
	if !(step > 0) {
		return "", errors.New("binance: step is not set")
	}
	return decimal.Format(v, step), nil
}

func checkCloid(s string) error {
	if s == "" {
		return errors.New("binance: client order id is empty")
	}
	if len(s) > cloidMax {
		return fmt.Errorf("binance: client order id is %d characters, limit %d", len(s), cloidMax)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-', c == ':', c == '/':
		default:
			return fmt.Errorf("binance: client order id contains %q", c)
		}
	}
	return nil
}

func instrument(inst *symbol.Info) error { return instrumentVenue(inst, VenueUM) }

func instrumentVenue(inst *symbol.Info, venue string) error {
	if inst == nil || inst.ExchTicker == "" {
		return errors.New("binance: instrument is missing")
	}
	if v := inst.Ticker.Venue; v != "" && v != venue {
		return fmt.Errorf("binance: %s belongs to %s", inst.ExchTicker, v)
	}
	return nil
}

func exchFor(venue, sym string) symbol.ExchKey {
	if venue == VenueSpot {
		return symbol.ExchKey{Venue: VenueSpot, Category: symbol.Spot, ExchTicker: sym}
	}
	return exchOf(sym)
}

func exchOf(sym string) symbol.ExchKey {
	cat := symbol.Perp
	if strings.Contains(sym, "_") {
		cat = symbol.Future
	}
	return symbol.ExchKey{Venue: VenueUM, Category: cat, ExchTicker: sym}
}

func sideOf(s string) exchange.Side {
	switch s {
	case "BUY":
		return exchange.SideBuy
	case "SELL":
		return exchange.SideSell
	default:
		return exchange.SideUnspecified
	}
}

func sideWire(s exchange.Side) (string, error) {
	switch s {
	case exchange.SideBuy:
		return "BUY", nil
	case exchange.SideSell:
		return "SELL", nil
	default:
		return "", errors.New("binance: side is missing")
	}
}

func typeOf(s string) exchange.OrderType {
	switch s {
	case "LIMIT", "LIMIT_MAKER":
		return exchange.OrderTypeLimit
	case "MARKET":
		return exchange.OrderTypeMarket
	default:
		return exchange.OrderTypeUnspecified
	}
}

func typeWire(t exchange.OrderType) (string, error) {
	switch t {
	case exchange.OrderTypeLimit:
		return "LIMIT", nil
	case exchange.OrderTypeMarket:
		return "MARKET", nil
	default:
		return "", errors.New("binance: order type is missing")
	}
}

func tifWire(t exchange.TimeInForce) (string, error) {
	switch t {
	case exchange.TIFGTC:
		return "GTC", nil
	case exchange.TIFIOC:
		return "IOC", nil
	case exchange.TIFFOK:
		return "FOK", nil
	case exchange.TIFPostOnly:
		return "GTX", nil
	default:
		return "", errors.New("binance: time in force is missing")
	}
}

// statusOf maps the venue's order status. NEW is the matching engine's acceptance, so it is Accepted:
// Binance has no separate state for a request it has only received.
func statusOf(s string) exchange.OrderStatus {
	switch s {
	case "NEW":
		return exchange.OrderStatusAccepted
	case "PARTIALLY_FILLED":
		return exchange.OrderStatusPartiallyFilled
	case "FILLED":
		return exchange.OrderStatusFilled
	case "CANCELED":
		return exchange.OrderStatusCancelled
	case "REJECTED":
		return exchange.OrderStatusRejected
	case "EXPIRED", "EXPIRED_IN_MATCH":
		return exchange.OrderStatusExpired
	default:
		return exchange.OrderStatusUnknown
	}
}

// orderStatus is statusOf for an order report, which carries the filled quantity. PENDING_CANCEL
// (unused by the venue today) is an order still on the book: a cancel in flight is the account
// worker's flag, not a status (E06-D010).
func orderStatus(s string, filled float64) exchange.OrderStatus {
	if s == "PENDING_CANCEL" {
		return exchange.OnBook(filled)
	}
	return statusOf(s)
}

// positionSize applies hedge mode: a SHORT row's amount is positive on the wire and short here.
func positionSize(amt float64, side string) float64 {
	if side == "SHORT" {
		if amt < 0 {
			return amt
		}
		return -amt
	}
	return amt
}

// account holds the once-parsed key. The fields below are written by signer and then only read.
type signerState struct {
	once    sync.Once
	sig     *codec.Signer
	signErr error
}
