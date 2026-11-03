// Package accountkey encrypts account credentials for Postgres (E21-D003, E21-D019, E21-D020).
//
// The cipher is XChaCha20-Poly1305. Each Seal draws a random 24-byte nonce. The associated data
// is the api_id as 8 big-endian bytes followed by the kind's bytes, so a ciphertext copied under
// another account or another kind does not open.
//
// A ciphertext is the key version (uint32, big-endian), the nonce, then the AEAD output. The
// version selects the key. A Box holds every configured key: the first one encrypts, and any of
// them decrypts.
//
// Keys arrive as one string, the value of MFTIKO_ACCOUNT_KEYS or -account-keys:
//
//	v2:<standard base64>,v1:<standard base64>
//
// Each key is 32 bytes. Versions are integers from 1 with no leading zero, and they do not repeat.
// The first entry is the current encryption key. The rest only decrypt.
//
// Nothing here logs. Box prints REDACTED. Errors name the check that failed and never include a
// key or plaintext.
package accountkey

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	keySize    = 32
	versionLen = 4
	nonceLen   = chacha20poly1305.NonceSizeX
	tagLen     = 16
	// MinCiphertext is the shortest blob Seal writes: version, nonce, and the Poly1305 tag.
	MinCiphertext = versionLen + nonceLen + tagLen
)

// maxAPIID is 2^53 − 1, the api_id ceiling (E10-D008, E21-D002).
const maxAPIID int64 = 1<<53 - 1

var (
	// ErrMalformed means the blob is too short to be a ciphertext.
	ErrMalformed = errors.New("accountkey: ciphertext is unreadable")
	// ErrUnauthentic means the ciphertext, nonce, or associated data does not match.
	ErrUnauthentic = errors.New("accountkey: ciphertext does not open")
	// ErrUnknownVersion means the ciphertext names a key this box does not have.
	ErrUnknownVersion = errors.New("accountkey: unknown key version")
)

const redacted = "accountkey.Box{REDACTED}"

// Box is the key ring. The first key encrypts. Every key decrypts. The zero Box has no keys.
type Box struct {
	keys []key
	by   map[uint32][keySize]byte
}

type key struct {
	version uint32
	raw     [keySize]byte
}

func (Box) String() string             { return redacted }
func (Box) GoString() string           { return redacted }
func (Box) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redacted) }
func (Box) MarshalJSON() ([]byte, error) {
	return []byte(`"REDACTED"`), nil
}
func (Box) MarshalText() ([]byte, error) { return []byte("REDACTED"), nil }

// Current is the version Seal uses. It is 0 when the box is empty.
func (b Box) Current() uint32 {
	if len(b.keys) == 0 {
		return 0
	}
	return b.keys[0].version
}

// Versions is the configured versions, current first.
func (b Box) Versions() []uint32 {
	out := make([]uint32, len(b.keys))
	for i, k := range b.keys {
		out[i] = k.version
	}
	return out
}

// Parse reads the MFTIKO_ACCOUNT_KEYS form. Surrounding space on the whole string and on each
// comma-separated entry is ignored. An empty string, a repeated version, or a key that is not
// 32 bytes of standard base64 is an error. The error does not quote the entry.
func Parse(s string) (Box, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Box{}, errors.New("account keys are required")
	}
	parts := strings.Split(s, ",")
	keys := make([]key, 0, len(parts))
	seen := make(map[uint32]struct{}, len(parts))
	for i, part := range parts {
		ver, raw, err := parseEntry(strings.TrimSpace(part))
		if err != nil {
			return Box{}, fmt.Errorf("account keys: entry %d: %w", i+1, err)
		}
		if _, ok := seen[ver]; ok {
			return Box{}, fmt.Errorf("account keys: duplicate version %d", ver)
		}
		seen[ver] = struct{}{}
		keys = append(keys, key{version: ver, raw: raw})
	}
	by := make(map[uint32][keySize]byte, len(keys))
	for _, k := range keys {
		by[k.version] = k.raw
	}
	return Box{keys: keys, by: by}, nil
}

func parseEntry(s string) (uint32, [keySize]byte, error) {
	var key [keySize]byte
	label, b64, ok := strings.Cut(s, ":")
	if !ok || !versionLabel(label) {
		return 0, key, errors.New("want v<version>:<base64 of 32 bytes>")
	}
	n, err := strconv.ParseUint(label[1:], 10, 32)
	if err != nil || n == 0 {
		return 0, key, errors.New("want v<version>:<base64 of 32 bytes>")
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != keySize {
		n := len(raw)
		clear(raw)
		if err == nil {
			return 0, key, fmt.Errorf("key is %d bytes, want 32", n)
		}
		return 0, key, errors.New("key must be 32 bytes in standard base64")
	}
	copy(key[:], raw)
	clear(raw)
	return uint32(n), key, nil
}

// versionLabel reports whether s is "v" plus a decimal integer from 1 with no leading zero.
func versionLabel(s string) bool {
	if len(s) < 2 || s[0] != 'v' || s[1] < '1' || s[1] > '9' {
		return false
	}
	for i := 2; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Seal encrypts plaintext for apiID and kind with the current key.
func (b Box) Seal(apiID int64, kind string, plaintext []byte) ([]byte, error) {
	if len(b.keys) == 0 {
		return nil, errors.New("account keys are required")
	}
	if err := checkBinding(apiID, kind); err != nil {
		return nil, err
	}
	k := b.keys[0]
	aead, err := chacha20poly1305.NewX(k.raw[:])
	if err != nil {
		return nil, err
	}
	var nonce [nonceLen]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	out := make([]byte, versionLen, MinCiphertext+len(plaintext))
	binary.BigEndian.PutUint32(out, k.version)
	out = append(out, nonce[:]...)
	return aead.Seal(out, nonce[:], plaintext, associatedData(apiID, kind)), nil
}

// Open decrypts ciphertext sealed for apiID and kind. A wrong account, a wrong kind, and a
// damaged blob all fail. A version this box does not hold is ErrUnknownVersion.
func (b Box) Open(apiID int64, kind string, ciphertext []byte) ([]byte, error) {
	if len(b.keys) == 0 {
		return nil, errors.New("account keys are required")
	}
	if err := checkBinding(apiID, kind); err != nil {
		return nil, err
	}
	ver, nonce, body, err := split(ciphertext)
	if err != nil {
		return nil, err
	}
	raw, ok := b.by[ver]
	if !ok {
		return nil, ErrUnknownVersion
	}
	aead, err := chacha20poly1305.NewX(raw[:])
	if err != nil {
		return nil, err
	}
	plain, err := aead.Open(nil, nonce, body, associatedData(apiID, kind))
	if err != nil {
		return nil, ErrUnauthentic
	}
	return plain, nil
}

// HeaderVersion is the key version carried in ciphertext, without decrypting.
func HeaderVersion(ciphertext []byte) (uint32, error) {
	ver, _, _, err := split(ciphertext)
	if err != nil {
		return 0, err
	}
	return ver, nil
}

func split(b []byte) (ver uint32, nonce, body []byte, err error) {
	if len(b) < MinCiphertext {
		return 0, nil, nil, ErrMalformed
	}
	ver = binary.BigEndian.Uint32(b[:versionLen])
	nonce = b[versionLen : versionLen+nonceLen]
	body = b[versionLen+nonceLen:]
	return ver, nonce, body, nil
}

func checkBinding(apiID int64, kind string) error {
	if apiID < 1 || apiID > maxAPIID {
		return errors.New("accountkey: api_id must be from 1 to 9007199254740991")
	}
	if kind == "" {
		return errors.New("accountkey: kind is empty")
	}
	return nil
}

func associatedData(apiID int64, kind string) []byte {
	ad := make([]byte, 8+len(kind))
	binary.BigEndian.PutUint64(ad[:8], uint64(apiID))
	copy(ad[8:], kind)
	return ad
}
