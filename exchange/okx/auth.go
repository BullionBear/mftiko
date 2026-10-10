package okx

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
	prodREST    = "https://www.okx.com"
	prodPrivate = "wss://ws.okx.com:8443/ws/v5/private"
	demoPrivate = "wss://wspap.okx.com:8443/ws/v5/private"

	// cloidMax is clOrdId's limit. A D602 id is at most 19 digits and fits.
	cloidMax = 32

	// quoteStep formats a spot market order's quote amount. The instrument has no quote
	// step; 1e-8 is the finest OKX prints.
	quoteStep = 1e-8

	userAgent = "mftiko"

	bucketOrders = "orders"
	bucketRead   = "read"
	bucketCAA    = "caa"
)

func orderCost(n int) []exchange.Cost {
	return []exchange.Cost{{Bucket: bucketOrders, N: n}}
}

func readCost() []exchange.Cost {
	return []exchange.Cost{{Bucket: bucketRead, N: 1}}
}

// signerState holds the once-parsed key. signer writes it; after that it is only read.
type signerState struct {
	once    sync.Once
	sig     *codec.Signer
	signErr error
}

func (s *signerState) signer(c exchange.Credentials) (*codec.Signer, error) {
	s.once.Do(func() {
		if c.Type != exchange.KeyHMAC {
			s.signErr = fmt.Errorf("okx: signs with HMAC-SHA256: %w", exchange.ErrUnsupported)
			return
		}
		s.sig, s.signErr = codec.NewSigner(c)
	})
	return s.sig, s.signErr
}

func (a *account) signer() (*codec.Signer, error) {
	return a.signerState.signer(a.cfg.Credentials)
}

func (a *account) passphrase() error {
	if a.cfg.Credentials.Passphrase == "" {
		return errors.New("okx: passphrase is empty")
	}
	return nil
}

func (a *account) restBase() string { return restHost(a.cfg) }

func restHost(cfg exchange.AccountConfig) string {
	if cfg.RESTEndpoint != "" {
		return strings.TrimRight(cfg.RESTEndpoint, "/")
	}
	return prodREST
}

// privateURL is the private socket. StreamEndpoint replaces it. Simulated, with no
// StreamEndpoint, selects the demo socket; the REST host stays put and the demo flag is the
// header.
func (a *account) privateURL() string {
	if ep := strings.TrimRight(a.cfg.StreamEndpoint, "/"); ep != "" {
		return ep
	}
	if a.cfg.Simulated {
		return demoPrivate
	}
	return prodPrivate
}

// sign builds one signed request. The prehash is timestamp + method + path + query + body.
// timestamp is UTC with milliseconds. The body is the bytes that are sent (D008).
func (a *account) sign(now time.Time, method, path, query string, body []byte, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	return signCfg(&a.signerState, a.cfg, now, method, path, query, body, cost, mutates)
}

// signCfg is the same signature account.sign builds. History signs with it too: the backfill
// job does not start an account worker (D601).
func signCfg(st *signerState, cfg exchange.AccountConfig, now time.Time, method, path, query string, body []byte, cost []exchange.Cost, mutates bool) (exchange.HTTPRequest, error) {
	if cfg.Credentials.Passphrase == "" {
		return exchange.HTTPRequest{}, errors.New("okx: passphrase is empty")
	}
	sig, err := st.signer(cfg.Credentials)
	if err != nil {
		return exchange.HTTPRequest{}, err
	}
	ts := now.UTC().Format("2006-01-02T15:04:05.000Z")
	prehash := make([]byte, 0, len(ts)+len(method)+len(path)+len(query)+len(body)+1)
	prehash = append(prehash, ts...)
	prehash = append(prehash, method...)
	prehash = append(prehash, path...)
	if query != "" {
		prehash = append(prehash, '?')
		prehash = append(prehash, query...)
	}
	prehash = append(prehash, body...)
	header := []exchange.Header{
		{Name: "OK-ACCESS-KEY", Value: cfg.Credentials.Key},
		{Name: "OK-ACCESS-SIGN", Value: string(sig.SignBase64(nil, prehash))},
		{Name: "OK-ACCESS-TIMESTAMP", Value: ts},
		{Name: "OK-ACCESS-PASSPHRASE", Value: cfg.Credentials.Passphrase},
		{Name: "User-Agent", Value: userAgent},
	}
	if cfg.Simulated {
		header = append(header, exchange.Header{Name: "x-simulated-trading", Value: "1"})
	}
	if method == http.MethodPost {
		header = append(header, exchange.Header{Name: "Content-Type", Value: "application/json"})
	}
	return exchange.HTTPRequest{
		Method: method, Base: restHost(cfg), Path: path,
		Query: []byte(query), Header: header, Body: body, Cost: cost, Mutates: mutates,
	}, nil
}

func queryString(pairs []pair) string {
	var q codec.Query
	for _, p := range pairs {
		if p.v == "" {
			continue
		}
		q = q.Add(p.k, p.v)
	}
	return string(q)
}

func (a *account) get(now time.Time, path string, pairs []pair, cost []exchange.Cost) (exchange.HTTPRequest, error) {
	return a.sign(now, http.MethodGet, path, queryString(pairs), nil, cost, false)
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

func (a *account) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }

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

// exchNs reads a millisecond ts, else outTime. outTime is microseconds when it is longer
// than a millisecond stamp.
func exchNs(ts, out string) int64 {
	if n := msString(ts); n != 0 {
		return n
	}
	if out == "" || out == "0" {
		return 0
	}
	n, err := strconv.ParseInt(out, 10, 64)
	if err != nil || n == 0 {
		return 0
	}
	if len(out) > 13 {
		return n * int64(time.Microsecond)
	}
	return n * int64(time.Millisecond)
}

func formatDec(v, step float64) (string, error) {
	if !decimal.Has(v) {
		return "", errors.New("okx: number is absent")
	}
	if !(step > 0) {
		return "", errors.New("okx: step is not set")
	}
	return decimal.Format(v, step), nil
}

func checkCloid(s string) error {
	if s == "" {
		return errors.New("okx: client order id is empty")
	}
	if len(s) > cloidMax {
		return fmt.Errorf("okx: client order id is %d characters, limit %d", len(s), cloidMax)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		default:
			return fmt.Errorf("okx: client order id contains %q", c)
		}
	}
	return nil
}

func instrument(inst *symbol.Info) error {
	if inst == nil || inst.ExchTicker == "" {
		return errors.New("okx: instrument is missing")
	}
	if v := inst.Ticker.Venue; v != "" && v != Name {
		return fmt.Errorf("okx: %s belongs to %s", inst.ExchTicker, v)
	}
	return nil
}

// tdMode is cash for spot and cross for a contract. Params["tdMode"] replaces it.
func tdMode(inst *symbol.Info, params map[string]string) (string, error) {
	if err := instrument(inst); err != nil {
		return "", err
	}
	mode := "cross"
	if inst.Ticker.Category == symbol.Spot {
		mode = "cash"
	}
	if s := params["tdMode"]; s != "" {
		switch s {
		case "cash", "cross", "isolated":
			mode = s
		default:
			return "", fmt.Errorf("okx: tdMode %q", s)
		}
	}
	return mode, nil
}

func sideWire(s exchange.Side) (string, error) {
	switch s {
	case exchange.SideBuy:
		return "buy", nil
	case exchange.SideSell:
		return "sell", nil
	default:
		return "", errors.New("okx: side is missing")
	}
}

func sideOf(s string) exchange.Side {
	switch s {
	case "buy":
		return exchange.SideBuy
	case "sell":
		return exchange.SideSell
	default:
		return exchange.SideUnspecified
	}
}

func ordTypeOf(t exchange.OrderType, tif exchange.TimeInForce) (string, error) {
	switch t {
	case exchange.OrderTypeMarket:
		if tif != exchange.TIFUnspecified && tif != exchange.TIFIOC {
			return "", fmt.Errorf("okx: a market order is IOC: %w", exchange.ErrUnsupported)
		}
		return "market", nil
	case exchange.OrderTypeLimit:
		switch tif {
		case exchange.TIFGTC:
			return "limit", nil
		case exchange.TIFIOC:
			return "ioc", nil
		case exchange.TIFFOK:
			return "fok", nil
		case exchange.TIFPostOnly:
			return "post_only", nil
		default:
			return "", errors.New("okx: time in force is missing")
		}
	default:
		return "", errors.New("okx: order type is missing")
	}
}

func typeOf(s string) exchange.OrderType {
	switch s {
	case "market", "optimal_limit_ioc":
		return exchange.OrderTypeMarket
	case "limit", "post_only", "fok", "ioc":
		return exchange.OrderTypeLimit
	default:
		return exchange.OrderTypeUnspecified
	}
}

func statusOf(s string) exchange.OrderStatus {
	switch s {
	case "live":
		return exchange.OrderStatusNew
	case "partially_filled":
		return exchange.OrderStatusPartiallyFilled
	case "filled":
		return exchange.OrderStatusFilled
	case "canceled", "cancelled", "mmp_canceled":
		return exchange.OrderStatusCancelled
	default:
		return exchange.OrderStatusUnknown
	}
}

// categoryOf reads instType and, where that is not enough, the instId. A USD quote is
// coin-margined (Inverse). MARGIN uses the spot instrument.
func categoryOf(instType, instID string) symbol.Category {
	parts := strings.Split(instID, "-")
	if instType == "OPTION" || optionID(parts) {
		return symbol.Option
	}
	quote := ""
	if len(parts) >= 2 {
		quote = parts[1]
	}
	coin := quote == "USD"
	switch instType {
	case "SWAP":
		if coin {
			return symbol.Inverse
		}
		return symbol.Perp
	case "FUTURES":
		if coin {
			return symbol.Inverse
		}
		return symbol.Future
	case "SPOT", "MARGIN":
		return symbol.Spot
	}
	switch {
	case len(parts) >= 3 && parts[len(parts)-1] == "SWAP":
		if coin {
			return symbol.Inverse
		}
		return symbol.Perp
	case len(parts) == 3 && isDate(parts[2]):
		if coin {
			return symbol.Inverse
		}
		return symbol.Future
	case optionID(parts):
		return symbol.Option
	default:
		return symbol.Spot
	}
}

func optionID(parts []string) bool {
	if len(parts) < 5 {
		return false
	}
	last := parts[len(parts)-1]
	return last == "C" || last == "P"
}

func isDate(s string) bool {
	if len(s) != 6 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func exchKey(instType, instID string) symbol.ExchKey {
	return symbol.ExchKey{Venue: Name, Category: categoryOf(instType, instID), ExchTicker: instID}
}

// wireQty converts a platform quantity into OKX sz. Spot sz is the platform quantity.
// A contract's sz is the contract count: platform qty divided by ContractSize (D203).
func wireQty(inst *symbol.Info, qty float64) (string, error) {
	step := inst.Filters[symbol.QtyStep]
	if inst.Ticker.Category != symbol.Spot {
		if !(inst.ContractSize > 0) {
			return "", fmt.Errorf("okx: %s has no contract size", inst.ExchTicker)
		}
		qty /= inst.ContractSize
		if step > 0 {
			step /= inst.ContractSize
		}
	}
	s, err := formatDec(qty, step)
	if err != nil {
		return "", fmt.Errorf("okx: quantity: %w", err)
	}
	return s, nil
}
