package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/internal/sym"
)

func TestVenueDoer(t *testing.T) {
	if _, err := venueDoer("https://proxy.example:3128", "", time.Second); err == nil {
		t.Fatal("an https proxy was accepted")
	}
	d, err := venueDoer("", "", time.Second)
	if err != nil || d == nil {
		t.Fatalf("direct: %v", err)
	}

	dir := t.TempDir()
	missing := filepath.Join(dir, "absent.pem")
	if _, err := venueDoer("", missing, time.Second); err == nil {
		t.Fatal("a missing roots file was accepted")
	}
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := venueDoer("", bad, time.Second); err == nil {
		t.Fatal("a file with no certificates was accepted")
	}
	good := filepath.Join(dir, "roots.pem")
	if err := os.WriteFile(good, testCertPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := venueDoer("http://127.0.0.1:9", good, time.Second); err != nil {
		t.Fatal(err)
	}
}

// phaseOneFeeds is the E2-10 set: the categories whose Listing is implemented
// and refreshed into the store. Inverse and option stay out of this list.
const phaseOneFeeds = "BinanceUM/Perp,BinanceUM/Future,Binance/Spot,Bybit/Spot,Bybit/Perp,Bybit/Future,Okx/Spot,Okx/Perp,Okx/Future"

func TestPhaseOneFeeds(t *testing.T) {
	d, err := venueDoer("", "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	feeds, err := sym.SelectFeeds(all.Registry(), phaseOneFeeds, d)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(feeds); got != 9 {
		t.Fatalf("feeds %d", got)
	}
	var names []string
	for _, f := range feeds {
		names = append(names, f.Venue+"/"+string(f.Category))
	}
	if strings.Join(names, ",") != phaseOneFeeds {
		t.Fatalf("feeds %s", strings.Join(names, ","))
	}
}

func testCertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "mftiko-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestGRPCFlags(t *testing.T) {
	dir := t.TempDir()
	tokens := filepath.Join(dir, "tokens")
	if err := os.WriteFile(tokens, []byte("pms tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cert := filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(cert, testCertPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	log := zerolog.Nop()

	// Without -grpc-addr nothing changes: no gRPC server.
	if cfg, err := (grpcFlags{}).config(log); cfg != nil || err != nil {
		t.Fatalf("off: %v %v", cfg, err)
	}
	cfg, err := grpcFlags{addr: ":0", tokenFile: tokens}.config(log)
	if err != nil || cfg == nil || cfg.Tokens == nil || cfg.TLS != nil {
		t.Fatalf("h2c: %+v %v", cfg, err)
	}
	bad := map[string]grpcFlags{
		"addr without token file": {addr: ":0"},
		"empty token file":        {addr: ":0", tokenFile: empty},
		"missing token file":      {addr: ":0", tokenFile: filepath.Join(dir, "absent")},
		"cert without key":        {addr: ":0", tokenFile: tokens, certFile: cert},
		"key without cert":        {addr: ":0", tokenFile: tokens, keyFile: cert},
		"cert as key":             {addr: ":0", tokenFile: tokens, certFile: cert, keyFile: cert},
		"token file without addr": {tokenFile: tokens},
		"tls without addr":        {certFile: cert, keyFile: cert},
	}
	for name, f := range bad {
		if _, err := f.config(log); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}
