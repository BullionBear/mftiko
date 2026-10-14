package bybit

import (
	"bytes"
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
	prodREST    = "https://api.bybit.com"
	testREST    = "https://api-testnet.bybit.com"
	prodPrivate = "wss://stream.bybit.com/v5/private"
	testPrivate = "wss://stream-testnet.bybit.com/v5/private"
	prodTrade   = "wss://stream.bybit.com/v5/trade"
	testTrade   = "wss://stream-testnet.bybit.com/v5/trade"

	// cloidMax is orderLinkId's limit. A D602 id is at most 19 digits and fits.
	cloidMax = 36

	// authLead is how far ahead Dial's expires sits. The venue rejects an expires that is
	// not still in the future when the frame arrives.
	authLead = 10 * time.Second

	// quoteStep formats a spot market order's quote amount. The instrument has no quote
	// step; 1e-8 is the finest Bybit prints.
	quoteStep = 1e-8

	bucketOrders   = "orders"
	bucketRead     = "read"
	bucketEndpoint = "endpoint"
)

func orderCost(n int) []exchange.Cost {
	return []exchange.Cost{{Bucket: bucketOrders, N: n}}
}

func readCost() []exchange.Cost {
	return []exchange.Cost{{Bucket: bucketRead, N: 1}}
}

// account holds the once-parsed key. signer writes it; after that it is only read.
type signerState struct {
	once    sync.Once
	sig     *codec.Signer
	signErr error
}

func (s *signerState) signer(c exchange.Credentials) (*codec.Signer, error) {
	s.once.Do(func() {
		if c.Type != exchange.KeyHMAC {
			s.signErr = fmt.Errorf("bybit: signs with HMAC-SHA256: %w", exchange.ErrUnsupported)
			return
		}
		s.sig, s.signErr = codec.NewSigner(c)
	})
	return s.sig, s.signErr
}

func (a *account) signer() (*codec.Signer, error) {
	return a.signerState.signer(a.cfg.Credentials)
}

func (a *account) restBase() string {
	if a.cfg.RESTEndpoint != "" {
		return strings.TrimRight(a.cfg.RESTEndpoint, "/")
	}
	return prodREST
}

func (a *account) onTestnet() bool {
	return strings.Contains(a.restBase(), "api-testnet.bybit.com")
}

func (a *account) privateURL() string {
	if ep := strings.TrimRight(a.cfg.StreamEndpoint, "/"); ep != "" {
		return ep
	}
	if a.onTestnet() {
		return testPrivate
	}
	return prodPrivate
}

// tradeURL is the order socket. It follows the REST host the way the private socket does.
// StreamEndpoint replaces only the private URL.
func (a *account) tradeURL() string {
	if a.onTestnet() {
		return testTrade
	}
	return prodTrade
}

// sign builds one signed request. GET signs the query string; POST signs body, which is sent
// unchanged (D008). The timestamp and the recv window travel in headers, not in the query.
func (a *account) sign(now time.Time, method, path, query string, body []byte, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	return signCfg(&a.signerState, a.cfg, now, method, path, query, body, cost, mutates)
}

func signCfg(s *signerState, cfg exchange.AccountConfig, now time.Time, method, path, query string, body []byte, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	sig, err := s.signer(cfg.Credentials)
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	recvWindow, err := windowOf(cfg)
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	ts := strconv.FormatInt(now.UnixMilli(), 10)
	payload := make([]byte, 0, len(ts)+len(cfg.Credentials.Key)+len(recvWindow)+len(query)+len(body))
	payload = append(payload, ts...)
	payload = append(payload, cfg.Credentials.Key...)
	payload = append(payload, recvWindow...)
	if method == http.MethodPost {
		payload = append(payload, body...)
	} else {
		payload = append(payload, query...)
	}
	header := []exchange.Header{
		{Name: "X-BAPI-API-KEY", Value: cfg.Credentials.Key},
		{Name: "X-BAPI-TIMESTAMP", Value: ts},
		{Name: "X-BAPI-RECV-WINDOW", Value: recvWindow},
		{Name: "X-BAPI-SIGN", Value: string(sig.SignHex(nil, payload))},
	}
	if method == http.MethodPost {
		header = append(header, exchange.Header{Name: "Content-Type", Value: "application/json"})
	}
	base := prodREST
	if ep := strings.TrimRight(cfg.RESTEndpoint, "/"); ep != "" {
		base = ep
	}
	return exchange.HTTPRequest{
		Method: method, Base: base, Path: path,
		Query: []byte(query), Header: header, Body: body, Cost: cost, Mutates: mutates,
	}, nil
}

// windowOf is X-BAPI-RECV-WINDOW: AccountConfig.Validity in milliseconds (D605).
func windowOf(cfg exchange.AccountConfig) (string, error) {
	w, err := cfg.Window()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(w.Milliseconds(), 10), nil
}

func (a *account) get(now time.Time, path string, pairs []pair, cost []exchange.Cost) (exchange.HTTPRequest, error) {
	var q codec.Query
	for _, p := range pairs {
		if p.v == "" {
			continue
		}
		q = q.Add(p.k, p.v)
	}
	return a.sign(now, http.MethodGet, path, string(q), nil, cost, false)
}

func (a *account) post(now time.Time, path string, body any, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	raw, err := marshalBody(body)
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	return a.sign(now, http.MethodPost, path, "", raw, cost, mutates)
}

// marshalBody is the JSON that is signed and sent. sonic may end it with a newline; that
// byte is not part of the body.
func marshalBody(v any) ([]byte, error) {
	b, err := codec.Marshal(v)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b, []byte("\n")), nil
}

type pair struct{ k, v string }

func (a *account) Usage(resp *exchange.HTTPResponse, now time.Time) []exchange.Used {
	return usedFrom(resp, now)
}

// usedFrom reads X-Bapi-Limit and X-Bapi-Limit-Status. Status is how many requests remain in
// this endpoint's own window, so the used count is limit minus status. The window is not shared
// across paths: bucket "endpoint" is that one response's window, not a counter to add up.
// X-Bapi-Limit-Reset-Timestamp is that counter's reset, in milliseconds. The header is confirmed
// with the venue in E4-10. now is unused: the venue sends the absolute time.
func usedFrom(resp *exchange.HTTPResponse, _ time.Time) []exchange.Used {
	if resp == nil {
		return nil
	}
	limit, okL := headerInt(resp, "X-Bapi-Limit")
	left, okS := headerInt(resp, "X-Bapi-Limit-Status")
	if !okL || !okS || left > limit {
		return nil
	}
	return []exchange.Used{{Bucket: bucketEndpoint, N: limit - left, ResetAt: bybitReset(resp)}}
}

func bybitReset(resp *exchange.HTTPResponse) time.Time {
	if resp == nil {
		return time.Time{}
	}
	return resetMillis(resp.Get("X-Bapi-Limit-Reset-Timestamp"))
}

func resetMillis(s string) time.Time {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
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

func ms(v int64) int64 {
	if v == 0 {
		return 0
	}
	return v * int64(time.Millisecond)
}

func msString(s string) int64 {
	if s == "" || s == "0" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n == 0 {
		return 0
	}
	return n * int64(time.Millisecond)
}

func formatDec(v, step float64) (string, error) {
	if !decimal.Has(v) {
		return "", errors.New("bybit: number is absent")
	}
	if !(step > 0) {
		return "", errors.New("bybit: step is not set")
	}
	return decimal.Format(v, step), nil
}

func checkCloid(s string) error {
	if s == "" {
		return errors.New("bybit: client order id is empty")
	}
	if len(s) > cloidMax {
		return fmt.Errorf("bybit: client order id is %d characters, limit %d", len(s), cloidMax)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return fmt.Errorf("bybit: client order id contains %q", c)
		}
	}
	return nil
}

func instrument(inst *symbol.Info) error {
	if inst == nil || inst.ExchTicker == "" {
		return errors.New("bybit: instrument is missing")
	}
	if v := inst.Ticker.Venue; v != "" && v != Name {
		return fmt.Errorf("bybit: %s belongs to %s", inst.ExchTicker, v)
	}
	return nil
}

// product is the v5 category of inst. Perp and Future are both linear (the listing splits them
// by contractType). Inverse is its own category, dated inverse symbols included.
func product(inst *symbol.Info) (string, error) {
	if err := instrument(inst); err != nil {
		return "", err
	}
	switch inst.Ticker.Category {
	case symbol.Spot:
		return "spot", nil
	case symbol.Perp, symbol.Future:
		return "linear", nil
	case symbol.Inverse:
		return "inverse", nil
	case symbol.Option:
		return "option", nil
	default:
		return "", fmt.Errorf("bybit: %s has no category", inst.ExchTicker)
	}
}

// exchKey stamps a wire category and symbol. A linear symbol with '-' is a dated future
// (BTCUSDT-16OCT26); the rest of linear is a perpetual (BTCUSDT, BTCPERP).
func exchKey(cat, sym string) symbol.ExchKey {
	var c symbol.Category
	switch cat {
	case "spot":
		c = symbol.Spot
	case "option":
		c = symbol.Option
	case "inverse":
		c = symbol.Inverse
	case "linear":
		c = symbol.Perp
		if strings.Contains(sym, "-") {
			c = symbol.Future
		}
	}
	return symbol.ExchKey{Venue: Name, Category: c, ExchTicker: sym}
}

func sideOf(s string) exchange.Side {
	switch s {
	case "Buy":
		return exchange.SideBuy
	case "Sell":
		return exchange.SideSell
	default:
		return exchange.SideUnspecified
	}
}

func sideWire(s exchange.Side) (string, error) {
	switch s {
	case exchange.SideBuy:
		return "Buy", nil
	case exchange.SideSell:
		return "Sell", nil
	default:
		return "", errors.New("bybit: side is missing")
	}
}

func typeOf(s string) exchange.OrderType {
	switch s {
	case "Limit":
		return exchange.OrderTypeLimit
	case "Market":
		return exchange.OrderTypeMarket
	default:
		return exchange.OrderTypeUnspecified
	}
}

func typeWire(t exchange.OrderType) (string, error) {
	switch t {
	case exchange.OrderTypeLimit:
		return "Limit", nil
	case exchange.OrderTypeMarket:
		return "Market", nil
	default:
		return "", errors.New("bybit: order type is missing")
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
		return "PostOnly", nil
	default:
		return "", errors.New("bybit: time in force is missing")
	}
}

// statusOf maps the venue's order status. Created, Untriggered and Triggered are held by the venue
// but not on the book: New. The venue's New is on the book: Accepted.
func statusOf(s string) exchange.OrderStatus {
	switch s {
	case "Created", "Untriggered", "Triggered":
		return exchange.OrderStatusNew
	case "New":
		return exchange.OrderStatusAccepted
	case "PartiallyFilled":
		return exchange.OrderStatusPartiallyFilled
	case "Filled":
		return exchange.OrderStatusFilled
	case "Cancelled", "PartiallyFilledCanceled", "Deactivated":
		return exchange.OrderStatusCancelled
	case "Rejected":
		return exchange.OrderStatusRejected
	default:
		return exchange.OrderStatusUnknown
	}
}

// orderStatus is statusOf for an order report, which carries the filled quantity. PendingCancel is
// an order still on the book: a cancel in flight is the account worker's flag, not a status (D610).
func orderStatus(s string, filled float64) exchange.OrderStatus {
	if s == "PendingCancel" {
		return exchange.OnBook(filled)
	}
	return statusOf(s)
}

// rejectReason keeps a reason only on a rejected order. EC_NoError is the venue's empty value.
func rejectReason(status, reason string) string {
	if status != "Rejected" {
		return ""
	}
	switch reason {
	case "", "EC_NoError":
		return ""
	default:
		return reason
	}
}

func num(d codec.Decimal) float64 { return float64(d) }

func qtyOf(d codec.Decimal) float64 {
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
