package main

import (
	"bytes"
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
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

// mode reads the venue account's position mode, or sets it with -dual: true is hedge mode, which
// the account worker refuses to start on (E06-D007, E06-D016). The venues only switch it with no open
// orders and no positions (Bybit: none on the symbols switched). It talks to the venue directly,
// with the credentials JSON of mftiko-seal, and only to a testnet or demo trading.
//
//   - BinanceUM: GET/POST /fapi/v1/positionSide/dual.
//   - Okx: GET /api/v5/account/config (posMode), POST /api/v5/account/set-position-mode; always
//     with x-simulated-trading: 1. -acct-lv sets the account mode first: a SWAP needs 2 or more,
//     and a new demo account is 1 (spot only).
//   - Bybit: POST /v5/position/switch-mode for linear -coin, or one -symbol. v5 has no read of
//     the mode, and the worker sees it only through an order or position with positionIdx 1 or 2
//     (E06-D017): -hedge-order PRICE places a resting buy on -symbol with positionIdx 1, -cancel-all
//     cancels the linear orders on -symbol. Without -dual it lists the linear orders and positions
//     of -coin with their positionIdx.
func mode() error {
	venue := flag.String("venue", "BinanceUM", "BinanceUM, Bybit or Okx")
	credsFile := flag.String("creds", "", "credentials JSON file (mftiko-seal's format)")
	rest := flag.String("rest", "", "REST host; a testnet only (default: the venue's testnet, or OKX's host with demo trading)")
	dual := flag.String("dual", "", "true sets hedge mode, false one-way; empty only reads")
	coin := flag.String("coin", "USDT", "Bybit: the linear settle coin to switch or list")
	symbol := flag.String("symbol", "", "Bybit: switch only this linear symbol; the symbol of -hedge-order and -cancel-all")
	hedgeOrder := flag.String("hedge-order", "", "Bybit: place a resting buy at this price on -symbol with positionIdx 1")
	qty := flag.String("qty", "", "Bybit: the quantity of -hedge-order")
	cancelAll := flag.Bool("cancel-all", false, "Bybit: cancel every linear order on -symbol")
	acctLv := flag.String("acct-lv", "", "Okx: set the account mode (2 is single-currency margin)")
	flag.Parse()
	f, err := os.Open(*credsFile)
	if err != nil {
		return err
	}
	c, err := readCreds(f)
	f.Close()
	if err != nil {
		return err
	}
	switch *venue {
	case "BinanceUM":
		return binanceMode(c, or(*rest, "https://testnet.binancefuture.com"), *dual)
	case "Okx":
		return okxMode(c, or(*rest, "https://www.okx.com"), *dual, *acctLv)
	case "Bybit":
		v := bybitVenue{c: c, rest: or(*rest, "https://api-testnet.bybit.com")}
		if !strings.Contains(v.rest, "testnet") {
			return fmt.Errorf("%s is not a testnet host", v.rest)
		}
		return v.run(*dual, *coin, *symbol, *hedgeOrder, *qty, *cancelAll)
	}
	return fmt.Errorf("venue %q", *venue)
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// do sends one request and returns its body; a status other than 200 is an error.
func do(req *http.Request) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: %d %s", req.Method, req.URL.Path, resp.StatusCode, body)
	}
	return bytes.TrimSpace(body), nil
}

func binanceMode(c sealbox.Credentials, rest, dual string) error {
	if !strings.Contains(rest, "testnet") {
		return fmt.Errorf("%s is not a testnet host", rest)
	}
	sign, err := signer(c.KeyType, c.Secret)
	if err != nil {
		return err
	}
	call := func(method string, q url.Values) ([]byte, error) {
		q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
		payload := q.Encode()
		req, err := http.NewRequest(method, rest+"/fapi/v1/positionSide/dual?"+payload+"&signature="+url.QueryEscape(sign(payload)), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-MBX-APIKEY", c.Key)
		return do(req)
	}
	if dual != "" {
		v, err := strconv.ParseBool(dual)
		if err != nil {
			return fmt.Errorf("-dual %q", dual)
		}
		body, err := call(http.MethodPost, url.Values{"dualSidePosition": {strconv.FormatBool(v)}})
		if err != nil {
			return err
		}
		fmt.Printf("%s set dualSidePosition=%v: %s\n", stamp(), v, body)
	}
	body, err := call(http.MethodGet, url.Values{})
	if err != nil {
		return err
	}
	fmt.Printf("%s position mode %s\n", stamp(), body)
	return nil
}

// signer signs a query as Binance does: ed25519 over the payload in base64, or HMAC-SHA256 in hex.
func signer(keyType, secret string) (func(string) string, error) {
	switch keyType {
	case "ed25519":
		blk, _ := pem.Decode([]byte(secret))
		if blk == nil {
			return nil, errors.New("ed25519 secret is not PEM")
		}
		k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
		if err != nil {
			return nil, errors.New("ed25519 secret does not parse")
		}
		priv, ok := k.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("the PEM key is not ed25519")
		}
		return func(p string) string { return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(p))) }, nil
	case "hmac":
		return func(p string) string { return hex.EncodeToString(hmacSHA256(secret, p)) }, nil
	}
	return nil, fmt.Errorf("key type %q", keyType)
}

func hmacSHA256(secret, payload string) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(payload))
	return m.Sum(nil)
}

// okxMode always sends x-simulated-trading: 1, so it never touches a live account.
func okxMode(c sealbox.Credentials, rest, dual, acctLv string) error {
	if c.KeyType != "hmac" || c.Passphrase == "" {
		return errors.New("OKX takes an hmac key with a passphrase")
	}
	call := func(method, path string, body []byte) ([]byte, error) {
		ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		req, err := http.NewRequest(method, rest+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("OK-ACCESS-KEY", c.Key)
		req.Header.Set("OK-ACCESS-SIGN", base64.StdEncoding.EncodeToString(hmacSHA256(c.Secret, ts+method+path+string(body))))
		req.Header.Set("OK-ACCESS-TIMESTAMP", ts)
		req.Header.Set("OK-ACCESS-PASSPHRASE", c.Passphrase)
		req.Header.Set("x-simulated-trading", "1")
		req.Header.Set("Content-Type", "application/json")
		out, err := do(req)
		if err != nil {
			return nil, err
		}
		var env struct{ Code, Msg string }
		if err := json.Unmarshal(out, &env); err != nil || env.Code != "0" {
			return nil, fmt.Errorf("%s %s: %s", method, path, out)
		}
		return out, nil
	}
	if acctLv != "" {
		if _, err := strconv.Atoi(acctLv); err != nil {
			return fmt.Errorf("-acct-lv %q", acctLv)
		}
		body, err := call(http.MethodPost, "/api/v5/account/set-account-level", []byte(`{"acctLv":"`+acctLv+`"}`))
		if err != nil {
			return err
		}
		fmt.Printf("%s set acctLv=%s: %s\n", stamp(), acctLv, body)
	}
	if dual != "" {
		v, err := strconv.ParseBool(dual)
		if err != nil {
			return fmt.Errorf("-dual %q", dual)
		}
		pm := "net_mode"
		if v {
			pm = "long_short_mode"
		}
		body, err := call(http.MethodPost, "/api/v5/account/set-position-mode", []byte(`{"posMode":"`+pm+`"}`))
		if err != nil {
			return err
		}
		fmt.Printf("%s set posMode=%s: %s\n", stamp(), pm, body)
	}
	body, err := call(http.MethodGet, "/api/v5/account/config", nil)
	if err != nil {
		return err
	}
	var cfg struct {
		Data []struct {
			PosMode string `json:"posMode"`
			AcctLv  string `json:"acctLv"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &cfg); err != nil || len(cfg.Data) == 0 {
		return fmt.Errorf("account config: %s", body)
	}
	fmt.Printf("%s posMode=%s acctLv=%s\n", stamp(), cfg.Data[0].PosMode, cfg.Data[0].AcctLv)
	for _, path := range []string{"/api/v5/trade/orders-pending?instType=SWAP", "/api/v5/account/positions?instType=SWAP"} {
		body, err := call(http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		var r struct {
			Data []struct {
				InstID  string `json:"instId"`
				OrdID   string `json:"ordId"`
				Side    string `json:"side"`
				Sz      string `json:"sz"`
				Px      string `json:"px"`
				State   string `json:"state"`
				Pos     string `json:"pos"`
				PosSide string `json:"posSide"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			return err
		}
		fmt.Printf("%s %s: %d\n", stamp(), path, len(r.Data))
		for _, x := range r.Data {
			fmt.Printf("  %s ord=%s %s sz=%s px=%s %s pos=%s posSide=%s\n", x.InstID, x.OrdID, x.Side, x.Sz, x.Px, x.State, x.Pos, x.PosSide)
		}
	}
	return nil
}

type bybitVenue struct {
	c    sealbox.Credentials
	rest string
}

// call signs as v5 does: HMAC-SHA256 in hex of timestamp, key, recv window, then the query or the
// JSON body. A retCode other than 0 is an error.
func (v bybitVenue) call(method, path, query string, body []byte) ([]byte, error) {
	if v.c.KeyType != "hmac" {
		return nil, errors.New("Bybit takes an hmac key here")
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	const window = "5000"
	payload := query
	u := v.rest + path
	if method == http.MethodGet {
		u += "?" + query
	} else {
		payload = string(body)
	}
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-BAPI-API-KEY", v.c.Key)
	req.Header.Set("X-BAPI-TIMESTAMP", ts)
	req.Header.Set("X-BAPI-RECV-WINDOW", window)
	req.Header.Set("X-BAPI-SIGN", hex.EncodeToString(hmacSHA256(v.c.Secret, ts+v.c.Key+window+payload)))
	req.Header.Set("Content-Type", "application/json")
	out, err := do(req)
	if err != nil {
		return nil, err
	}
	var env struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
	}
	if err := json.Unmarshal(out, &env); err != nil || env.RetCode != 0 {
		return nil, fmt.Errorf("%s %s: %s", method, path, out)
	}
	return out, nil
}

func (v bybitVenue) run(dual, coin, symbol, hedgeOrder, qty string, cancelAll bool) error {
	if dual != "" {
		on, err := strconv.ParseBool(dual)
		if err != nil {
			return fmt.Errorf("-dual %q", dual)
		}
		m := map[string]any{"category": "linear", "mode": 0}
		if on {
			m["mode"] = 3
		}
		scope := "coin=" + coin
		if symbol != "" {
			m["symbol"], scope = symbol, "symbol="+symbol
		} else {
			m["coin"] = coin
		}
		b, _ := json.Marshal(m)
		body, err := v.call(http.MethodPost, "/v5/position/switch-mode", "", b)
		if err != nil {
			return err
		}
		fmt.Printf("%s switch-mode %s mode=%v: %s\n", stamp(), scope, m["mode"], body)
	}
	if hedgeOrder != "" || cancelAll {
		if symbol == "" {
			return errors.New("-hedge-order and -cancel-all need -symbol")
		}
	}
	if hedgeOrder != "" {
		if qty == "" {
			return errors.New("-hedge-order needs -qty")
		}
		b, _ := json.Marshal(map[string]any{"category": "linear", "symbol": symbol, "side": "Buy", "orderType": "Limit",
			"qty": qty, "price": hedgeOrder, "timeInForce": "GTC", "positionIdx": 1})
		body, err := v.call(http.MethodPost, "/v5/order/create", "", b)
		if err != nil {
			return err
		}
		fmt.Printf("%s hedge order %s buy %s@%s positionIdx=1: %s\n", stamp(), symbol, qty, hedgeOrder, body)
	}
	if cancelAll {
		b, _ := json.Marshal(map[string]any{"category": "linear", "symbol": symbol})
		body, err := v.call(http.MethodPost, "/v5/order/cancel-all", "", b)
		if err != nil {
			return err
		}
		fmt.Printf("%s cancel-all %s: %s\n", stamp(), symbol, body)
	}
	return v.list(coin)
}

// list prints the linear orders and positions of coin with their positionIdx: 1 or 2 is what the
// worker reads as hedge mode.
func (v bybitVenue) list(coin string) error {
	q := "category=linear&settleCoin=" + url.QueryEscape(coin)
	for _, path := range []string{"/v5/order/realtime", "/v5/position/list"} {
		body, err := v.call(http.MethodGet, path, q, nil)
		if err != nil {
			return err
		}
		var r struct {
			Result struct {
				List []struct {
					Symbol      string `json:"symbol"`
					Side        string `json:"side"`
					Size        string `json:"size"`
					Qty         string `json:"qty"`
					Price       string `json:"price"`
					PositionIdx int    `json:"positionIdx"`
				} `json:"list"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			return err
		}
		fmt.Printf("%s %s %s: %d\n", stamp(), path, coin, len(r.Result.List))
		for _, x := range r.Result.List {
			fmt.Printf("  %s %s qty=%s%s price=%s positionIdx=%d\n", x.Symbol, x.Side, x.Qty, x.Size, x.Price, x.PositionIdx)
		}
	}
	return nil
}
