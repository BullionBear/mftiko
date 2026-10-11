package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

func TestKeygenSealOpen(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "td.key")
	var out bytes.Buffer
	if err := run([]string{"keygen", "-key", key}, nil, &out); err != nil {
		t.Fatal(err)
	}
	pub := strings.TrimSpace(out.String())
	out.Reset()
	if err := run([]string{"pubkey", "-key", key}, nil, &out); err != nil || strings.TrimSpace(out.String()) != pub {
		t.Fatalf("pubkey %q %v", out.String(), err)
	}
	pubFile := filepath.Join(dir, "td.pub")
	if err := os.WriteFile(pubFile, []byte(pub+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	secret := fixture()
	in := `{"key_type":"hmac","key":"k","secret":"` + secret + `"}`
	for _, p := range []string{pub, "@" + pubFile} {
		out.Reset()
		if err := run([]string{"seal", "-pubkey", p, "-api-id", "7", "-kind", "Okx"}, strings.NewReader(in), &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), secret) {
			t.Fatal("printed the secret")
		}
		sealed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out.String()))
		if err != nil {
			t.Fatal(err)
		}
		k, err := sealbox.LoadKeyFile(key)
		if err != nil {
			t.Fatal(err)
		}
		c, err := sealbox.OpenCredentials(k, sealed, 7, "Okx")
		if err != nil || c.Secret != secret || c.KeyType != "hmac" {
			t.Fatalf("opened %v", err)
		}
	}
}

func TestSealRefusesBadInput(t *testing.T) {
	k, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := k.Public().String()
	secret := fixture()
	for _, c := range []struct {
		args []string
		in   string
	}{
		{[]string{"seal", "-pubkey", pub, "-api-id", "7", "-kind", "Okx"}, `{"key":"k","secret":"` + secret + `",`},
		{[]string{"seal", "-pubkey", pub, "-api-id", "7", "-kind", "Okx"}, `{"key":"k","secret":"` + secret + `","extra":1}`},
		{[]string{"seal", "-pubkey", pub, "-kind", "Okx"}, `{"key":"k","secret":"` + secret + `"}`},
		{[]string{"seal", "-api-id", "7", "-kind", "Okx"}, `{"key":"k","secret":"` + secret + `"}`},
		{[]string{"keygen"}, ""},
		{[]string{"nope"}, ""},
		{nil, ""},
	} {
		var out bytes.Buffer
		err := run(c.args, strings.NewReader(c.in), &out)
		if err == nil {
			t.Errorf("%v succeeded", c.args)
			continue
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(out.String(), secret) {
			t.Errorf("%v: the secret shows in %q", c.args, err)
		}
	}
}

// fixture is a value made at run time, so secret scanners have no literal to flag.
func fixture() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "v-" + hex.EncodeToString(b)
}
