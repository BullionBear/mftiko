package main

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
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
)

// mode reads the BinanceUM account's position mode, or sets it with -dual: true is hedge mode,
// which the account worker refuses to start on (D607, D616). The venue only switches it with no
// open orders and no positions. It talks to the venue directly, with the credentials JSON of
// mftiko-seal, and only to a testnet host.
func mode() error {
	credsFile := flag.String("creds", "", "credentials JSON file (mftiko-seal's format)")
	rest := flag.String("rest", "https://testnet.binancefuture.com", "BinanceUM REST host; a testnet only")
	dual := flag.String("dual", "", "true sets hedge mode, false one-way; empty only reads")
	flag.Parse()
	if !strings.Contains(*rest, "testnet") {
		return fmt.Errorf("%s is not a testnet host", *rest)
	}
	f, err := os.Open(*credsFile)
	if err != nil {
		return err
	}
	c, err := readCreds(f)
	f.Close()
	if err != nil {
		return err
	}
	sign, err := signer(c.KeyType, c.Secret)
	if err != nil {
		return err
	}
	call := func(method string, q url.Values) (string, error) {
		q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
		payload := q.Encode()
		u := *rest + "/fapi/v1/positionSide/dual?" + payload + "&signature=" + url.QueryEscape(sign(payload))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, method, u, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("X-MBX-APIKEY", c.Key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("%s: %d %s", method, resp.StatusCode, body)
		}
		return strings.TrimSpace(string(body)), nil
	}
	if *dual != "" {
		v, err := strconv.ParseBool(*dual)
		if err != nil {
			return fmt.Errorf("-dual %q", *dual)
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
		return func(p string) string {
			m := hmac.New(sha256.New, []byte(secret))
			m.Write([]byte(p))
			return hex.EncodeToString(m.Sum(nil))
		}, nil
	}
	return nil, fmt.Errorf("key type %q", keyType)
}
