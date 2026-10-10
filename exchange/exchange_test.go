package exchange

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestCredentialsNeverFormat(t *testing.T) {
	c := Credentials{Type: KeyHMAC, Key: "k-visible", Secret: "s3cret", Passphrase: "pp"}
	wrapped := AccountConfig{APIID: 7, Credentials: c}

	var buf bytes.Buffer
	log := zerolog.New(&buf)
	log.Info().Interface("cfg", wrapped).Interface("c", c).Msg("x")
	j, _ := json.Marshal(wrapped)

	outs := []string{fmt.Sprint(c), fmt.Sprint(&c), string(j), buf.String()}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%t", "%T"} {
		outs = append(outs, fmt.Sprintf(verb, c), fmt.Sprintf(verb, &c), fmt.Sprintf(verb, wrapped))
	}
	for _, out := range outs {
		for _, secret := range []string{"s3cret", "pp", "k-visible"} {
			if strings.Contains(out, secret) {
				t.Errorf("leaked %q in %s", secret, out)
			}
		}
	}
}

func TestHTTPRequestHeadersNeverFormat(t *testing.T) {
	req := HTTPRequest{Method: "GET", Base: "https://api.example", Path: "/api/v5/account/balance", Query: []byte("ccy=BTC"),
		Header: []Header{{Name: "OK-ACCESS-KEY", Value: "k-visible"}, {Name: "OK-ACCESS-PASSPHRASE", Value: "pp-visible"}}}
	call := Call[int]{Req: req}

	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%t", "%10.3v"} {
		outs = append(outs, fmt.Sprintf(verb, req), fmt.Sprintf(verb, &req), fmt.Sprintf(verb, call))
	}
	for _, out := range outs {
		for _, v := range []string{"k-visible", "pp-visible"} {
			for _, secret := range []string{v, hex.EncodeToString([]byte(v)), strings.ToUpper(hex.EncodeToString([]byte(v)))} {
				if strings.Contains(out, secret) {
					t.Errorf("leaked %q in %s", secret, out)
				}
			}
		}
	}

	// Only the values go: the rest of the request stays readable, and the request itself is untouched.
	got := fmt.Sprintf("%+v", req)
	for _, want := range []string{"GET https://api.example/api/v5/account/balance?ccy=BTC", "OK-ACCESS-PASSPHRASE: REDACTED"} {
		if !strings.Contains(got, want) {
			t.Errorf("%s lacks %q", got, want)
		}
	}
	if req.Header[0].Value != "k-visible" {
		t.Errorf("Format changed the request: %s", req.Header[0].Value)
	}
	bare := HTTPRequest{Method: "GET", Path: "/p", Query: []byte("a=1"), Mutates: true}
	for _, verb := range []string{"%v", "%+v", "%s"} {
		if got := fmt.Sprintf(verb, bare); got != "GET /p?a=1" {
			t.Errorf("%s without headers: %s, want GET /p?a=1", verb, got)
		}
	}
}

func TestRegistry(t *testing.T) {
	venue := func(name string) Venue {
		return Venue{Venue: symbol.Venue{Name: name, Categories: []symbol.Category{symbol.Spot}}}
	}
	kind := func(name string, trades ...string) AccountKind {
		return AccountKind{Name: name, Trades: trades}
	}
	bad := map[string][]Declaration{
		"duplicate venue": {
			{Exchange: "a", Venues: []Venue{venue("X")}},
			{Exchange: "b", Venues: []Venue{venue("X")}},
		},
		"venue without categories": {{Exchange: "a", Venues: []Venue{{Venue: symbol.Venue{Name: "Y"}}}}},
		"duplicate account kind": {
			{Exchange: "a", Venues: []Venue{venue("X")}, AccountKinds: []AccountKind{kind("K", "X")}},
			{Exchange: "b", AccountKinds: []AccountKind{kind("K", "X")}},
		},
		"kind trading an unknown venue": {{Exchange: "a", AccountKinds: []AccountKind{kind("K", "Nowhere")}}},
		"kind trading nothing":          {{Exchange: "a", AccountKinds: []AccountKind{kind("K")}}},
	}
	for name, decls := range bad {
		if _, err := NewRegistry(decls...); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// A venue and an account kind may share a name: two enums (D301).
	r, err := NewRegistry(
		Declaration{Exchange: "a", Venues: []Venue{venue("X"), venue("W")}, AccountKinds: []AccountKind{kind("X", "X"), kind("XPM", "X", "W")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.Venues(), ","); got != "W,X" {
		t.Fatalf("Venues() = %s", got)
	}
	if got := strings.Join(r.AccountKinds(), ","); got != "X,XPM" {
		t.Fatalf("AccountKinds() = %s", got)
	}
	if _, ok := r.Lookup("XPM"); ok {
		t.Fatal("an account kind resolved as a venue")
	}
	pm, _ := r.AccountKind("XPM")
	if !pm.CanTrade("W") || pm.CanTrade("XPM") {
		t.Fatal("CanTrade")
	}
}
