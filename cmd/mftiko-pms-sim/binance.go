package main

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"time"
)

// creds is the PMS's own exchange key, in mftiko-seal's JSON.
type creds struct {
	KeyType string `json:"key_type"`
	Key     string `json:"key"`
	Secret  string `json:"secret"`
}

func (creds) String() string { return "creds{REDACTED}" }

func readCreds(path string) (creds, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return creds{}, err
	}
	var c creds
	if err := json.Unmarshal(b, &c); err != nil {
		return creds{}, errors.New("credentials are not {key_type, key, secret} JSON")
	}
	return c, nil
}

// signer signs a query as Binance does: ed25519 over the payload in base64, or HMAC-SHA256 in hex.
func signer(c creds) (func(string) string, error) {
	switch c.KeyType {
	case "ed25519":
		block, _ := pem.Decode([]byte(c.Secret))
		if block == nil {
			return nil, errors.New("ed25519 secret is not PEM")
		}
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("ed25519 secret does not parse")
		}
		priv, ok := k.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("the PEM key is not ed25519")
		}
		return func(p string) string { return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(p))) }, nil
	case "hmac":
		key := []byte(c.Secret)
		return func(p string) string {
			m := hmac.New(sha256.New, key)
			m.Write([]byte(p))
			return hex.EncodeToString(m.Sum(nil))
		}, nil
	}
	return nil, fmt.Errorf("key type %q", c.KeyType)
}

// binanceUM reads one account's USDⓈ-M order and trade history. It only reads.
type binanceUM struct {
	rest string
	key  string
	sign func(string) string
	hc   *http.Client
}

func newBinanceUM(rest string, c creds) (*binanceUM, error) {
	sign, err := signer(c)
	if err != nil {
		return nil, err
	}
	return &binanceUM{rest: rest, key: c.Key, sign: sign, hc: &http.Client{Timeout: 15 * time.Second}}, nil
}

// exOrder is one order as the venue reports it.
type exOrder struct {
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	Price         string `json:"price"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
}

// exFill is one trade. Binance's userTrades has no clientOrderId; it is joined from the order
// by orderId (E21-D011).
type exFill struct {
	ID       int64  `json:"id"`
	OrderID  int64  `json:"orderId"`
	Symbol   string `json:"symbol"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Qty      string `json:"qty"`
	Maker    bool   `json:"maker"`
	Time     int64  `json:"time"`
	Fee      string `json:"commission"`
	FeeAsset string `json:"commissionAsset"`
}

// window is Binance's longest startTime..endTime span for both endpoints: under 7 days.
const window = 7*24*time.Hour - time.Minute

// history pages allOrders and userTrades for symbol from since to now, a week at a time.
func (b *binanceUM) history(ctx context.Context, symbol string, since, now time.Time) ([]exOrder, []exFill, error) {
	var orders []exOrder
	var fills []exFill
	for from := since; from.Before(now); from = from.Add(window) {
		to := from.Add(window)
		if to.After(now) {
			to = now
		}
		o, err := pageByTime(ctx, b, "/fapi/v1/allOrders", symbol, from, to, func(o exOrder) (int64, int64) { return o.OrderID, o.Time })
		if err != nil {
			return nil, nil, err
		}
		orders = append(orders, o...)
		f, err := pageByTime(ctx, b, "/fapi/v1/userTrades", symbol, from, to, func(f exFill) (int64, int64) { return f.ID, f.Time })
		if err != nil {
			return nil, nil, err
		}
		fills = append(fills, f...)
	}
	return orders, fills, nil
}

// pageByTime reads [from, to] 1000 rows at a time, moving startTime to the last row's time.
// Rows at that same millisecond come again and are dropped by id.
func pageByTime[T any](ctx context.Context, b *binanceUM, path, symbol string, from, to time.Time, key func(T) (id, ms int64)) ([]T, error) {
	const limit = 1000
	seen := map[int64]bool{}
	var out []T
	start := from.UnixMilli()
	for {
		q := url.Values{
			"symbol": {symbol}, "startTime": {strconv.FormatInt(start, 10)},
			"endTime": {strconv.FormatInt(to.UnixMilli(), 10)}, "limit": {strconv.Itoa(limit)},
		}
		var page []T
		if err := b.get(ctx, path, q, &page); err != nil {
			return nil, err
		}
		last := start
		added := 0
		for _, row := range page {
			id, ms := key(row)
			last = max(last, ms)
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, row)
			added++
		}
		if len(page) < limit || added == 0 {
			return out, nil
		}
		start = last
	}
}

func (b *binanceUM) get(ctx context.Context, path string, q url.Values, out any) error {
	q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	q.Set("recvWindow", "10000")
	payload := q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.rest+path+"?"+payload+"&signature="+url.QueryEscape(b.sign(payload)), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", b.key)
	resp, err := b.hc.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %d %s", path, resp.StatusCode, body)
	}
	return json.Unmarshal(body, out)
}

func sortHistory(orders []exOrder, fills []exFill) {
	slices.SortFunc(orders, func(a, b exOrder) int { return cmpInt(a.Time, b.Time, a.OrderID, b.OrderID) })
	slices.SortFunc(fills, func(a, b exFill) int { return cmpInt(a.Time, b.Time, a.ID, b.ID) })
}

func cmpInt(t1, t2, id1, id2 int64) int {
	switch {
	case t1 != t2:
		if t1 < t2 {
			return -1
		}
		return 1
	case id1 < id2:
		return -1
	case id1 > id2:
		return 1
	}
	return 0
}
