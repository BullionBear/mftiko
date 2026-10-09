package codec

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/exchange"
)

func signer(t *testing.T, typ exchange.KeyType, secret string) *Signer {
	t.Helper()
	s, err := NewSigner(exchange.Credentials{Type: typ, Key: "api-key", Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pkcs8PEM(t *testing.T, key any) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

// Binance's HMAC examples: developers.binance.com, "SIGNED Endpoint Examples for POST /api/v3/order" (REST)
// and "SIGNED request example (HMAC)" (WebSocket API). The REST payloads are built with Query, so the
// percent-encoding of a non-ASCII symbol is checked against Binance's too.
func TestBinanceHMACVectors(t *testing.T) {
	s := signer(t, exchange.KeyHMAC, "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j")
	order := func(symbol string) Query {
		return Query(nil).Add("symbol", symbol).Add("side", "BUY").Add("type", "LIMIT").Add("timeInForce", "GTC").
			AddDecimal("quantity", 1, 1).AddDecimal("price", 0.1, 0.1).AddInt("recvWindow", 5000).
			AddInt("timestamp", 1499827319559)
	}
	for _, c := range []struct{ payload, query, sig string }{
		{
			string(order("LTCBTC")),
			"symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559",
			"c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71",
		},
		{
			string(order("１２３４５６")),
			"symbol=%EF%BC%91%EF%BC%92%EF%BC%93%EF%BC%94%EF%BC%95%EF%BC%96&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559",
			"e1353ec6b14d888f1164ae9af8228a3dbd508bc82eb867db8ab6046442f33ef3",
		},
		{
			"apiKey=vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A&price=52000.00&quantity=0.01000000&recvWindow=100&side=SELL&symbol=BTCUSDT&timeInForce=GTC&timestamp=1645423376532&type=LIMIT",
			"",
			"aa1b5712c094bc4e57c05a1a5c1fd8d88dcd628338ea863fec7b88e59fe2db24",
		},
		{
			"apiKey=vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A&price=0.10000000&quantity=1.00000000&recvWindow=5000&side=BUY&symbol=１２３４５６&timeInForce=GTC&timestamp=1645423376532&type=LIMIT",
			"",
			"b33892ae8e687c939f4468c6268ddd4c40ac1af18ad19a064864c47bae0752cd",
		},
	} {
		if c.query != "" && c.payload != c.query {
			t.Errorf("payload\n%s\nwant\n%s", c.payload, c.query)
		}
		if got := string(s.SignHex(nil, []byte(c.payload))); got != c.sig {
			t.Errorf("%s\nsigned %s, want %s", c.payload, got, c.sig)
		}
	}
}

// Binance's REST docs show no ed25519 key, and the WebSocket API docs show signatures without their key. The
// published vector with a key is in Binance's Java connector: clients/spot/src/test (Ed25519ApiTest,
// test-ed25519-prv-key.pem), GET /api/v3/account at timestamp 1736393892000.
func TestBinanceEd25519Vector(t *testing.T) {
	const key = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEILpXm6EtDzSzPJWHTbP904h5R3bzb0QsLjTf1tAt2OlB\n-----END PRIVATE KEY-----\n"
	s := signer(t, exchange.KeyEd25519, key)
	q := Query(nil).AddInt("timestamp", 1736393892000)
	sig := s.SignBase64(nil, q)
	if got, want := string(sig), "1KohSdsEYUHEO2qTbkkjbmFdufaUTF9dKat6oUwE2RHT+zThFX4i1MMeNC5nqH0Eya07jbRN9/Lq1HaTgjD2Aw=="; got != want {
		t.Fatalf("signed %s, want %s", got, want)
	}
	q = q.AddBytes("signature", sig)
	if got, want := string(q), "timestamp=1736393892000&signature=1KohSdsEYUHEO2qTbkkjbmFdufaUTF9dKat6oUwE2RHT%2BzThFX4i1MMeNC5nqH0Eya07jbRN9%2FLq1HaTgjD2Aw%3D%3D"; got != want {
		t.Errorf("query %s, want %s", got, want)
	}
}

// RFC 8032 §7.1, TESTs 1–3: the algorithm's own vectors, through the PKCS #8 PEM path.
func TestEd25519RFC8032(t *testing.T) {
	for _, c := range []struct{ seed, msg, sig string }{
		{"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60", "",
			"e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"},
		{"4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb", "72",
			"92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"},
		{"c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7", "af82",
			"6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"},
	} {
		seed, _ := hex.DecodeString(c.seed)
		msg, _ := hex.DecodeString(c.msg)
		s := signer(t, exchange.KeyEd25519, pkcs8PEM(t, ed25519.NewKeyFromSeed(seed)))
		if got := string(s.SignHex(nil, msg)); got != c.sig {
			t.Errorf("seed %s…: %s, want %s", c.seed[:8], got, c.sig)
		}
		raw, _ := hex.DecodeString(c.sig)
		if got := string(s.SignBase64(nil, msg)); got != base64.StdEncoding.EncodeToString(raw) {
			t.Errorf("seed %s…: base64 %s", c.seed[:8], got)
		}
	}
}

// OKX's docs give the prehash rule, an example secret and example inputs, but no signature made with that
// secret (www.okx.com/docs-v5, "Signature" and "Login"). The expected values here are openssl's on the
// documented inputs:
//
//	printf '%s' "$prehash" | openssl dgst -sha256 -hmac "$secret" -binary | base64
func TestOKXHMAC(t *testing.T) {
	s := signer(t, exchange.KeyHMAC, "22582BD0CFF14C41EDBF1AB98506286D")
	for _, c := range []struct{ prehash, sig string }{
		{"2020-12-08T09:08:57.715Z" + "GET" + "/api/v5/account/balance?ccy=BTC", "HiZhvSfMtWJA3uUIVXV3a/bSXNPCWvYFXoGCVS8V4zY="},
		{"2020-12-08T09:08:57.715Z" + "POST" + "/api/v5/account/set-leverage" + `{"instId":"BTC-USDT","lever":"5","mgnMode":"isolated"}`,
			"eCnnCgWLjlQ9XnpUkrcny3qNq3WW/81KNrDr/XR6Xv8="},
		{"1538054050" + "GET" + "/users/self/verify", "+LdIr8lkkvhr5hoA3g9TMC0+uQJ849ftAcocA/ouu4M="},
	} {
		if got := string(s.SignBase64(nil, []byte(c.prehash))); got != c.sig {
			t.Errorf("%s: %s, want %s", c.prehash, got, c.sig)
		}
	}
}

// Bybit's docs show the payloads and their signatures but mask both the key and the secret as XXXXXXXXXX
// (bybit-exchange.github.io/docs/v5/guide, "Authentication"; v5/ws/connect, "Authentication"). The expected
// values are openssl's on the documented payloads, with that placeholder as the secret:
//
//	printf '%s' "$payload" | openssl dgst -sha256 -hmac XXXXXXXXXX
func TestBybitHMAC(t *testing.T) {
	s := signer(t, exchange.KeyHMAC, "XXXXXXXXXX")
	for _, c := range []struct{ payload, sig string }{
		{"1658384314791" + "XXXXXXXXXX" + "5000" + "category=option&symbol=BTC-29JUL22-25000-C",
			"c00720f96c5934ca7057ac28ae65b823f83b8b67a8fe784e7795ca0fa3c148ec"},
		{"1658385579423" + "XXXXXXXXXX" + "5000" + `{"category": "option"}`,
			"fbb4648b585b3e62de2c4eaecc8f3b6066aa18e98cc300eaff4d87f15b55724a"},
		{"GET/realtime" + "1662350400000", "3d6cbbfa4569aab894f661add824ff3f06090d0edba388f1e47dc15e6a7b5a8b"},
	} {
		if got := string(s.SignHex(nil, []byte(c.payload))); got != c.sig {
			t.Errorf("%s: %s, want %s", c.payload, got, c.sig)
		}
	}
}

func TestSignAppends(t *testing.T) {
	s := signer(t, exchange.KeyHMAC, "k")
	msg := []byte("m")
	raw := s.Sign(nil, msg)
	if len(raw) != 32 {
		t.Fatalf("HMAC-SHA256 is %d bytes", len(raw))
	}
	got := s.SignHex([]byte("sig="), msg)
	if want := "sig=" + hex.EncodeToString(raw); string(got) != want {
		t.Errorf("%s, want %s", got, want)
	}
	if s.Type() != exchange.KeyHMAC {
		t.Errorf("Type %s", s.Type())
	}
}

// Neither a Signer nor an error from NewSigner shows the secret, whatever formats or logs it.
func TestCredentialsNeverLeak(t *testing.T) {
	seed := bytes.Repeat([]byte{0xA7}, ed25519.SeedSize)
	edPEM := pkcs8PEM(t, ed25519.NewKeyFromSeed(seed))
	rsaKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	rsaPEM := pkcs8PEM(t, rsaKey)
	pemBody := func(p string) string { return strings.Split(p, "\n")[1] }

	// The secret, and the forms a key's bytes would take in a message: the PEM body, hex, Go's byte lists.
	secrets := []string{"hmac-s3cret-value", pemBody(edPEM), pemBody(rsaPEM), hex.EncodeToString(seed),
		fmt.Sprint(seed[:4]), "a7a7a7a7", "167 167 167", "pass-phrase-x", "api-key-visible"}
	check := func(what, out string) {
		t.Helper()
		for _, s := range secrets {
			if strings.Contains(out, s) {
				t.Errorf("%s leaked %q:\n%s", what, s, out)
			}
		}
	}

	for name, c := range map[string]exchange.Credentials{
		"empty hmac":     {Type: exchange.KeyHMAC},
		"hmac as ed":     {Type: exchange.KeyEd25519, Secret: "hmac-s3cret-value"},
		"rsa as ed":      {Type: exchange.KeyEd25519, Secret: rsaPEM},
		"truncated":      {Type: exchange.KeyEd25519, Secret: edPEM[:60] + "\n-----END PRIVATE KEY-----\n"},
		"wrong block":    {Type: exchange.KeyEd25519, Secret: strings.ReplaceAll(edPEM, "PRIVATE KEY", "EC PRIVATE KEY")},
		"ed448 oid":      {Type: exchange.KeyEd25519, Secret: strings.Replace(edPEM, "K2Vw", "K2Vx", 1)},
		"unknown type":   {Type: "hmac-s3cret-value", Secret: "hmac-s3cret-value"},
		"with key field": {Type: "rsa", Key: "api-key-visible", Secret: "hmac-s3cret-value", Passphrase: "pass-phrase-x"},
	} {
		s, err := NewSigner(c)
		if err == nil {
			t.Errorf("%s: accepted (%v)", name, s)
			continue
		}
		if !errors.Is(err, ErrKey) {
			t.Errorf("%s: %v is not ErrKey", name, err)
		}
		check(name, fmt.Sprintf("%v %+v %#v %s", err, err, err, err))
	}

	var buf bytes.Buffer
	log := zerolog.New(&buf)
	for _, s := range []*Signer{
		signer(t, exchange.KeyHMAC, "hmac-s3cret-value"),
		signer(t, exchange.KeyEd25519, edPEM),
	} {
		wrapped := struct {
			S    *Signer
			V    Signer
			Name string
		}{s, *s, "x"}
		log.Info().Interface("s", s).Interface("w", wrapped).Stringer("str", s).Msg("signer")
		j, err := json.Marshal(wrapped)
		if err != nil {
			t.Fatal(err)
		}
		check("json", string(j))
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%o", "%b", "%t"} {
			check(verb, fmt.Sprintf(verb, s)+fmt.Sprintf(verb, *s)+fmt.Sprintf(verb, wrapped))
		}
	}
	check("zerolog", buf.String())
}
