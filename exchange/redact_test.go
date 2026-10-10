package exchange

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestMaskListenKey(t *testing.T) {
	for in, want := range map[string]string{
		"listenKey=lk1": "listenKey=REDACTED",
		"symbol=BTCUSDT&listenKey=lk1&recvWindow=5000": "symbol=BTCUSDT&listenKey=REDACTED&recvWindow=5000",
		"LISTENKEY=lk1": "LISTENKEY=REDACTED",
		"/sapi/v1/userDataStream/isolated?listenKey=lk1":   "/sapi/v1/userDataStream/isolated?listenKey=REDACTED",
		`{"listenKey":"lk1"}`:                              `{"listenKey":"REDACTED"}`,
		`{"id":7,"result": {"listenKey" : "l\"k1"}}`:       `{"id":7,"result": {"listenKey" : "REDACTED"}}`,
		`{"e":"listenKeyExpired","E":1,"listenKey":"lk1"}`: `{"e":"listenKeyExpired","E":1,"listenKey":"REDACTED"}`,
		// No listenKey parameter or member: left as it is.
		"mylistenKey=keep&x=listenKey": "mylistenKey=keep&x=listenKey",
		`{"e":"listenKeyExpired"}`:     `{"e":"listenKeyExpired"}`,
		"symbol=BTCUSDT&signature=ab":  "symbol=BTCUSDT&signature=ab",
	} {
		if got := string(MaskListenKey([]byte(in))); got != want {
			t.Errorf("MaskListenKey(%s)\n got %s\nwant %s", in, got, want)
		}
	}
	b := []byte("symbol=BTCUSDT")
	if got := MaskListenKey(b); &got[0] != &b[0] {
		t.Error("copied a value that holds no listenKey")
	}
}

func TestMaskURL(t *testing.T) {
	for in, want := range map[string]string{
		"wss://fstream.binance.com/private/ws/lk1":         "wss://fstream.binance.com/REDACTED",
		"wss://stream.binance.com:9443/stream?streams=lk1": "wss://stream.binance.com:9443/REDACTED",
		"wss://ws.okx.com:8443":                            "wss://ws.okx.com:8443",
		"wss://user:pass@ws.okx.com/":                      "wss://ws.okx.com",
		"lk1":                                              "REDACTED",
		"%zz":                                              "REDACTED",
	} {
		if got := MaskURL(in); got != want {
			t.Errorf("MaskURL(%s) = %s, want %s", in, got, want)
		}
	}
}

// Header values, listenKeys and login frames never show, however a request, response or dial is
// formatted or logged; the rest of the query and body does.
func TestNothingFormatsASecret(t *testing.T) {
	req := HTTPRequest{
		Method: "PUT", Base: "https://api.binance.com", Path: "/api/v3/userDataStream",
		Query:  []byte("symbol=BTCUSDT&listenKey=lk-query"),
		Header: []Header{{Name: "X-MBX-APIKEY", Value: "k-visible"}, {Name: "OK-ACCESS-PASSPHRASE", Value: "pp-secret"}},
		Body:   []byte(`{"listenKey":"lk-body","instId":"BTC-USDT"}`),
	}
	resp := HTTPResponse{Status: 200, Header: []Header{{Name: "X-MBX-USED-WEIGHT-1M", Value: "2"}}, Body: []byte(`{"listenKey":"lk-resp"}`)}
	dial := Dial{
		URL:    "wss://fstream.binance.com/private/ws/lk-path?listenKey=lk-url",
		Frames: [][]byte{[]byte(`{"op":"login","args":[{"apiKey":"k-visible","passphrase":"pp-secret"}]}`)},
	}
	for _, tc := range []struct {
		v    fmt.Stringer
		want string
	}{
		{req, `PUT https://api.binance.com/api/v3/userDataStream?symbol=BTCUSDT&listenKey=REDACTED [X-MBX-APIKEY: REDACTED, OK-ACCESS-PASSPHRASE: REDACTED] {"listenKey":"REDACTED","instId":"BTC-USDT"}`},
		{resp, `200 [X-MBX-USED-WEIGHT-1M: 2] {"listenKey":"REDACTED"}`},
		{dial, `wss://fstream.binance.com/REDACTED, 1 frames`},
	} {
		if got := tc.v.String(); got != tc.want {
			t.Errorf("String\n got %s\nwant %s", got, tc.want)
		}
	}

	var buf bytes.Buffer
	log := zerolog.New(&buf)
	log.Info().Interface("req", req).Interface("reqp", &req).Stringer("reqs", req).Interface("resp", resp).
		Interface("respp", &resp).Interface("dial", dial).Interface("dialp", &dial).Msg("x")
	outs := []string{buf.String()}
	held := struct {
		C Call[string]
		R HTTPResponse
		D Dial
	}{Call[string]{Req: req}, resp, dial}
	for _, v := range []any{req, &req, resp, &resp, dial, &dial, held} {
		outs = append(outs, fmt.Sprintf("%v|%+v|%#v|%s|%q|%x", v, v, v, v, v, v))
	}
	for _, v := range []any{req, &req, resp, &resp, dial, &dial} {
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		outs = append(outs, string(j))
	}
	for _, out := range outs {
		for _, secret := range []string{"k-visible", "pp-secret", "lk-query", "lk-body", "lk-resp", "lk-path", "lk-url"} {
			if strings.Contains(out, secret) {
				t.Errorf("leaked %q in %s", secret, out)
			}
		}
	}
	if s := buf.String(); !strings.Contains(s, "symbol=BTCUSDT") || !strings.Contains(s, "BTC-USDT") {
		t.Errorf("the rest of the query and body is not shown: %s", s)
	}
}
