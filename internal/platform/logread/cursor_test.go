//go:build unix

package logread

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"testing"
	"unicode"
)

func TestCursorRoundTrip(t *testing.T) {
	c := encode(0x11, 0x22, 64, 0x33)
	got, err := decode(c)
	if err != nil {
		t.Fatal(err)
	}
	if got.dev != 0x11 || got.ino != 0x22 || got.off != 64 || got.hash != 0x33 {
		t.Fatalf("%+v", got)
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(string(c))
	if err != nil || len(raw) != 32 {
		t.Fatalf("raw %d %v", len(raw), err)
	}
	if binary.LittleEndian.Uint64(raw[0:8]) != 0x11 || binary.LittleEndian.Uint64(raw[16:24]) != 64 {
		t.Fatal("layout")
	}
	if _, err := decode(Cursor(string(c) + "=")); !errors.Is(err, ErrCursor) {
		t.Fatal(err)
	}
	for _, bad := range []Cursor{"", "!!!", "YQ", Cursor(base64.URLEncoding.EncodeToString(make([]byte, 32)))} {
		if _, err := decode(bad); !errors.Is(err, ErrCursor) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
}

func TestCursorAlphabet(t *testing.T) {
	samples := []Cursor{
		encode(0, 0, 0, fnv64(nil)),
		encode(1, 2, 3, 4),
		encode(^uint64(0), ^uint64(0), 1<<62, ^uint64(0)),
	}
	for _, c := range samples {
		if c == "" {
			t.Fatal("empty token")
		}
		for _, r := range string(c) {
			ok := r == '-' || r == '_' || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
			if !ok || r == ';' || r == '=' || unicode.IsSpace(r) {
				t.Fatalf("%q contains %q", c, r)
			}
		}
	}
}

func TestFNV1a64Empty(t *testing.T) {
	h := fnv.New64a()
	if fnv64(nil) != h.Sum64() || fnv64(nil) == 0 {
		t.Fatalf("empty hash %d", fnv64(nil))
	}
	if fnv64([]byte("abcd")) == fnv64([]byte("abce")) {
		t.Fatal("hash ignored a byte")
	}
}
