package accountkey

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// repeatKey is 32 copies of b, standard base64. Tests use repeated bytes on purpose.
func repeatKey(b byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, keySize))
}

func mustParse(t *testing.T, s string) Box {
	t.Helper()
	box, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func TestParseOrderAndRejects(t *testing.T) {
	cur := mustParse(t, " v2:"+repeatKey(0x02)+", v1:"+repeatKey(0x01)+" ")
	if cur.Current() != 2 || !bytes.Equal(uints(cur.Versions()), []byte{2, 1}) {
		t.Fatalf("current %d versions %v", cur.Current(), cur.Versions())
	}
	one := mustParse(t, "v1:"+repeatKey(0x11))
	if one.Current() != 1 || len(one.Versions()) != 1 {
		t.Fatalf("single %+v", one.Versions())
	}

	marker := repeatKey(0x41)
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", "account keys are required"},
		{"blank", "  ", "account keys are required"},
		{"no version", repeatKey(0x01), "want v<version>"},
		{"zero", "v0:" + repeatKey(0x01), "want v<version>"},
		{"leading zero", "v01:" + repeatKey(0x01), "want v<version>"},
		{"plus", "v+2:" + repeatKey(0x01), "want v<version>"},
		{"huge", "v4294967296:" + repeatKey(0x01), "want v<version>"},
		{"trailing comma", "v1:" + repeatKey(0x01) + ",", "want v<version>"},
		{"raw base64", "v1:" + strings.TrimRight(repeatKey(0x01), "="), "standard base64"},
		{"short", "v1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x02}, 16)), "key is 16 bytes, want 32"},
		{"duplicate", "v1:" + repeatKey(0x01) + ",v1:" + repeatKey(0x02), "duplicate version 1"},
		{"space in key", "v1:" + marker[:8] + " " + marker[8:], "standard base64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want substring %q", err, tc.want)
			}
			if strings.Contains(err.Error(), marker) || strings.Contains(err.Error(), repeatKey(0x01)) || strings.Contains(err.Error(), repeatKey(0x41)) {
				t.Fatalf("error quotes key material: %v", err)
			}
		})
	}
}

func uints(v []uint32) []byte {
	out := make([]byte, len(v))
	for i, n := range v {
		out[i] = byte(n)
	}
	return out
}

func TestSealOpenAndOldVersion(t *testing.T) {
	old := mustParse(t, "v1:"+repeatKey(0x01))
	both := mustParse(t, "v2:"+repeatKey(0x02)+",v1:"+repeatKey(0x01))
	plain := bytes.Repeat([]byte{0x42}, 24)

	sealed, err := old.Seal(7, "BinanceUM", plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) < MinCiphertext {
		t.Fatalf("len %d", len(sealed))
	}
	ver, err := HeaderVersion(sealed)
	if err != nil || ver != 1 {
		t.Fatalf("header %d %v", ver, err)
	}
	if bytes.Contains(sealed, plain) {
		t.Fatal("plaintext is in the ciphertext")
	}
	again, err := old.Seal(7, "BinanceUM", plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(sealed, again) || bytes.Equal(sealed[versionLen:versionLen+nonceLen], again[versionLen:versionLen+nonceLen]) {
		t.Fatal("two seals shared a nonce")
	}
	if bytes.Equal(sealed[versionLen:versionLen+nonceLen], make([]byte, nonceLen)) {
		t.Fatal("nonce is all zero")
	}

	got, err := both.Open(7, "BinanceUM", sealed)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("old version did not open: %v", err)
	}
	fresh, err := both.Seal(7, "BinanceUM", plain)
	if err != nil {
		t.Fatal(err)
	}
	if ver, err := HeaderVersion(fresh); err != nil || ver != 2 {
		t.Fatalf("current header %d %v", ver, err)
	}
	if _, err := old.Open(7, "BinanceUM", fresh); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("old ring opened a v2 blob: %v", err)
	}
	if _, err := both.Open(7, "BinanceUM", fresh); err != nil {
		t.Fatal(err)
	}

	empty, err := both.Seal(maxAPIID, "k", nil)
	if err != nil || len(empty) != MinCiphertext {
		t.Fatalf("empty plaintext %d %v", len(empty), err)
	}
	if got, err := both.Open(maxAPIID, "k", empty); err != nil || len(got) != 0 {
		t.Fatalf("empty open %q %v", got, err)
	}
}

func TestTamperAndAssociatedData(t *testing.T) {
	box := mustParse(t, "v2:"+repeatKey(0x02)+",v1:"+repeatKey(0x01))
	plain := bytes.Repeat([]byte{0x07}, 32)
	sealed, err := box.Seal(7, "BinanceUM", plain)
	if err != nil {
		t.Fatal(err)
	}
	flip := func(i int) []byte {
		b := append([]byte(nil), sealed...)
		b[i] ^= 1
		return b
	}
	for name, b := range map[string][]byte{
		"empty":   nil,
		"short":   sealed[:MinCiphertext-1],
		"version": flip(0),
		"nonce":   flip(versionLen),
		"tag":     flip(len(sealed) - 1),
		"body":    flip(versionLen + nonceLen),
	} {
		_, err := box.Open(7, "BinanceUM", b)
		if name == "version" {
			// Flipping the first byte lands on a version this box does not hold, or on v1,
			// whose key then fails the tag. Either failure is a refusal.
			if !errors.Is(err, ErrUnknownVersion) && !errors.Is(err, ErrUnauthentic) && !errors.Is(err, ErrMalformed) {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if name == "empty" || name == "short" {
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if !errors.Is(err, ErrUnauthentic) {
			t.Errorf("%s: %v", name, err)
		}
	}

	// A version byte rewritten to the other configured key still does not open.
	swapped := append([]byte(nil), sealed...)
	swapped[3] = 1
	if _, err := box.Open(7, "BinanceUM", swapped); !errors.Is(err, ErrUnauthentic) {
		t.Fatalf("swapped version: %v", err)
	}
	unknown := append([]byte(nil), sealed...)
	unknown[3] = 9
	if _, err := box.Open(7, "BinanceUM", unknown); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("unknown version: %v", err)
	}

	for _, c := range []struct {
		id   int64
		kind string
	}{{8, "BinanceUM"}, {7, "Okx"}, {7, "BinanceUMX"}} {
		if _, err := box.Open(c.id, c.kind, sealed); !errors.Is(err, ErrUnauthentic) {
			t.Errorf("ad %d %s: %v", c.id, c.kind, err)
		}
	}
	if _, err := HeaderVersion(nil); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	if _, err := HeaderVersion(sealed); err != nil {
		t.Fatal(err)
	}
}

func TestBindingRejected(t *testing.T) {
	box := mustParse(t, "v1:"+repeatKey(0x11))
	plain := []byte{1}
	for _, c := range []struct {
		id   int64
		kind string
	}{{0, "BinanceUM"}, {-1, "BinanceUM"}, {maxAPIID + 1, "BinanceUM"}, {7, ""}} {
		if _, err := box.Seal(c.id, c.kind, plain); err == nil {
			t.Errorf("sealed %d %q", c.id, c.kind)
		}
		if _, err := box.Open(c.id, c.kind, bytes.Repeat([]byte{1}, MinCiphertext)); err == nil {
			t.Errorf("opened %d %q", c.id, c.kind)
		}
	}
	var zero Box
	if _, err := zero.Seal(7, "BinanceUM", plain); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatal(err)
	}
	if zero.Current() != 0 || len(zero.Versions()) != 0 {
		t.Fatal("zero box has keys")
	}
}

func TestNothingPrintsTheKey(t *testing.T) {
	marker := repeatKey(0x11)
	box := mustParse(t, "v1:"+marker)
	raw, err := json.Marshal(struct {
		B Box
	}{box})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		box.String(),
		fmt.Sprintf("%v", box),
		fmt.Sprintf("%+v", box),
		fmt.Sprintf("%#v", box),
		fmt.Sprintf("%s", box),
		string(raw),
		fmt.Sprintf("%+v", struct{ B Box }{box}),
	} {
		if strings.Contains(s, marker) || strings.Contains(s, "17, 17") || !strings.Contains(s, "REDACTED") {
			t.Fatalf("printed %q", s)
		}
	}
}
