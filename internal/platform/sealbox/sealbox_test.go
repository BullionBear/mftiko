package sealbox

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// creds are made at run time: secret scanners flag literals, and the tests only need values they
// can look for in output.
var creds = Credentials{KeyType: "hmac", Key: fixture("key"), Secret: fixture("sec"), Passphrase: fixture("pp")}

func fixture(tag string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return tag + "-" + hex.EncodeToString(b)
}

func mustKey(t *testing.T) *PrivateKey {
	t.Helper()
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// noSecret fails when any part of the credentials or the key shows in s.
func noSecret(t *testing.T, s string, k *PrivateKey) {
	t.Helper()
	for _, v := range []string{creds.Key, creds.Secret, creds.Passphrase} {
		if strings.Contains(s, v) {
			t.Fatalf("%q shows %q", s, v)
		}
	}
	if k != nil && strings.Contains(s, fmt.Sprint(k.priv)) {
		t.Fatalf("%q shows the private key", s)
	}
}

func TestSealOpenCredentials(t *testing.T) {
	k := mustKey(t)
	sealed, err := SealCredentials(k.Public(), 42, "BinanceUM", creds)
	if err != nil {
		t.Fatal(err)
	}
	if sealed[0] != Version || len(sealed) < headerLen+48 {
		t.Fatalf("header %x", sealed[:headerLen])
	}
	noSecret(t, string(sealed), nil)
	again, err := SealCredentials(k.Public(), 42, "BinanceUM", creds)
	if err != nil || string(again) == string(sealed) {
		t.Fatal("sealing twice gave the same bytes")
	}
	got, err := OpenCredentials(k, sealed, 42, "BinanceUM")
	if err != nil || got != creds {
		t.Fatalf("opened %v", err)
	}
}

func TestOtherInstanceCannotOpen(t *testing.T) {
	a, b := mustKey(t), mustKey(t)
	sealed, err := SealCredentials(a.Public(), 42, "BinanceUM", creds)
	if err != nil {
		t.Fatal(err)
	}
	_, err = OpenCredentials(b, sealed, 42, "BinanceUM")
	if !errors.Is(err, ErrOtherKey) || Reason(err) != "credentials_other_key" {
		t.Fatalf("got %v (%s)", err, Reason(err))
	}
	noSecret(t, err.Error(), b)

	// The fingerprint is only a label: forging it to b's still does not open.
	forged := append([]byte(nil), sealed...)
	fp := b.Public().Fingerprint()
	copy(forged[1:headerLen], fp[:])
	if _, err := OpenCredentials(b, forged, 42, "BinanceUM"); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("forged fingerprint: %v", err)
	}
}

func TestUnreadable(t *testing.T) {
	k := mustKey(t)
	sealed, err := SealCredentials(k.Public(), 42, "BinanceUM", creds)
	if err != nil {
		t.Fatal(err)
	}
	flip := func(i int) []byte {
		b := append([]byte(nil), sealed...)
		b[i] ^= 1
		return b
	}
	for name, b := range map[string][]byte{
		"empty":     nil,
		"version":   flip(0),
		"short":     sealed[:headerLen+10],
		"body":      flip(len(sealed) - 1),
		"ephemeral": flip(headerLen),
		"not proto": mustSeal(t, k.Public(), []byte{0xff, 0xff, 0xff}),
	} {
		_, err := OpenCredentials(k, b, 42, "BinanceUM")
		if !errors.Is(err, ErrUnreadable) || Reason(err) != "credentials_unreadable" {
			t.Errorf("%s: got %v", name, err)
		}
	}
	if Reason(nil) != "" {
		t.Fatal("no reason for nil")
	}
}

func mustSeal(t *testing.T, pub PublicKey, plain []byte) []byte {
	t.Helper()
	b, err := Seal(pub, plain)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOtherAccount(t *testing.T) {
	k := mustKey(t)
	sealed, err := SealCredentials(k.Public(), 42, "BinanceUM", creds)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		id   int64
		kind string
	}{{43, "BinanceUM"}, {42, "Okx"}} {
		_, err := OpenCredentials(k, sealed, c.id, c.kind)
		if !errors.Is(err, ErrOtherAccount) || Reason(err) != "credentials_other_account" {
			t.Errorf("%d %s: got %v", c.id, c.kind, err)
		}
	}
	for _, bad := range []struct {
		id   int64
		kind string
		c    Credentials
	}{{0, "BinanceUM", creds}, {42, "", creds}, {42, "BinanceUM", Credentials{Key: "k"}}} {
		if _, err := SealCredentials(k.Public(), bad.id, bad.kind, bad.c); err == nil {
			t.Errorf("sealed %d %q", bad.id, bad.kind)
		}
	}
}

func TestNothingPrintsSecrets(t *testing.T) {
	k := mustKey(t)
	j, _ := json.Marshal(struct {
		C Credentials
		K *PrivateKey
	}{creds, k})
	for _, s := range []string{
		fmt.Sprintf("%v %+v %#v %s %q %x %d", creds, creds, creds, creds, creds, creds, creds),
		fmt.Sprintf("%v %+v %#v %s %x", k, k, k, k, k),
		string(j),
	} {
		noSecret(t, s, k)
	}
}

func TestKeyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "td.key")
	k := mustKey(t)
	if err := WriteKeyFile(path, k); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v %v", fi.Mode(), err)
	}
	if err := WriteKeyFile(path, mustKey(t)); err == nil {
		t.Fatal("overwrote a key")
	}
	got, err := LoadKeyFile(path)
	if err != nil || got.Public() != k.Public() || got.priv != k.priv {
		t.Fatalf("loaded %v", err)
	}
	sealed, err := SealCredentials(k.Public(), 1, "Okx", creds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCredentials(got, sealed, 1, "Okx"); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKeyFile(path); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("group-readable key: %v", err)
	}

	junk := filepath.Join(dir, "junk.key")
	if err := os.WriteFile(junk, []byte(creds.Secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKeyFile(junk); err == nil || strings.Contains(err.Error(), creds.Secret) {
		t.Fatalf("junk key: %v", err)
	}
	if _, err := LoadKeyFile(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing key file")
	}
}

func TestPublicKeyText(t *testing.T) {
	k := mustKey(t)
	p, err := ParsePublicKey("  " + k.Public().String() + "\n")
	if err != nil || p != k.Public() {
		t.Fatalf("%v", err)
	}
	for _, s := range []string{"", "abc", k.Public().String()[:20]} {
		if _, err := ParsePublicKey(s); err == nil {
			t.Errorf("parsed %q", s)
		}
	}
}
