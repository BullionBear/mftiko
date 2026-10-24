// Package sealbox seals account credentials to a TD instance's public key (E06-D013).
//
// A TD instance has one X25519 key pair. The private key is a file on that instance's host, 0600,
// whose path is deployment configuration; the public key goes to whoever seals: the mftiko-seal
// command now, the API server later (E10). Sealing is NaCl's anonymous sealed box
// (golang.org/x/crypto/nacl/box SealAnonymous: X25519, XSalsa20-Poly1305), behind a small header:
//
//	version (1 byte, 1) | fingerprint of the recipient key (8 bytes) | sealed box
//
// The fingerprint is the first 8 bytes of SHA-256 over the public key. It tells "sealed to another
// instance" apart from "damaged" without trying the key, so the TD agent's REJECTED says which.
//
// What is sealed is a state.v1.AccountCredentials. Its api_id and kind are checked against the
// account after opening, since a sealed box has no associated data: a ciphertext copied under
// another account opens, and is then refused.
//
// Nothing here logs. Credentials prints REDACTED for every verb, and errors name what failed, never
// the bytes.
package sealbox

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
)

// Version is the header's first byte. Another algorithm gets another version.
const Version = 1

const (
	keySize        = 32
	fingerprintLen = 8
	headerLen      = 1 + fingerprintLen
)

var (
	// ErrOtherKey: the credentials were sealed to another instance's key. Reseal them with this
	// instance's public key.
	ErrOtherKey = errors.New("sealbox: sealed to another key")
	// ErrUnreadable: the sealed bytes are empty, of an unknown version, cut short, or damaged.
	ErrUnreadable = errors.New("sealbox: sealed credentials are unreadable")
	// ErrOtherAccount: the credentials opened, but were sealed for another api_id or account kind.
	ErrOtherAccount = errors.New("sealbox: sealed for another account")
)

// Reason is the REJECTED reason the TD agent writes for an account whose credentials do not open
// (E06-D013), or "" for nil.
func Reason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrOtherKey):
		return "credentials_other_key"
	case errors.Is(err, ErrOtherAccount):
		return "credentials_other_account"
	default:
		return "credentials_unreadable"
	}
}

// PublicKey is a TD instance's X25519 public key.
type PublicKey [keySize]byte

// String is the key in standard base64, the form the key files and mftiko-seal use.
func (p PublicKey) String() string { return base64.StdEncoding.EncodeToString(p[:]) }

// Fingerprint is the first 8 bytes of SHA-256 over the key, as the header carries it.
func (p PublicKey) Fingerprint() [fingerprintLen]byte {
	sum := sha256.Sum256(p[:])
	var f [fingerprintLen]byte
	copy(f[:], sum[:])
	return f
}

// ParsePublicKey reads a key in standard base64; surrounding space is ignored.
func ParsePublicKey(s string) (PublicKey, error) {
	var p PublicKey
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != keySize {
		return p, errors.New("sealbox: a public key is 32 bytes in standard base64")
	}
	copy(p[:], b)
	return p, nil
}

// PrivateKey is a TD instance's key pair. It prints REDACTED.
type PrivateKey struct {
	pub  PublicKey
	priv [keySize]byte
}

// Public is the key to seal to.
func (k *PrivateKey) Public() PublicKey { return k.pub }

const redactedKey = "sealbox.PrivateKey{REDACTED}"

func (*PrivateKey) String() string               { return redactedKey }
func (*PrivateKey) GoString() string             { return redactedKey }
func (*PrivateKey) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, redactedKey) }
func (*PrivateKey) MarshalJSON() ([]byte, error) { return []byte(`"REDACTED"`), nil }
func (*PrivateKey) MarshalText() ([]byte, error) { return []byte("REDACTED"), nil }

// GenerateKey makes a new key pair.
func GenerateKey() (*PrivateKey, error) {
	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &PrivateKey{pub: *pub, priv: *priv}, nil
}

// WriteKeyFile writes k's private key to path, mode 0600, in standard base64. It refuses a path that
// exists: a key is replaced only by moving the old one away first, since every account sealed to
// it has to be resealed.
func WriteKeyFile(path string, k *PrivateKey) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("sealbox: %w", err)
	}
	_, err = io.WriteString(f, base64.StdEncoding.EncodeToString(k.priv[:])+"\n")
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("sealbox: write %s: %w", path, err)
	}
	return nil
}

// LoadKeyFile reads the private key at path. It refuses a file that the group or others can read
// or write.
func LoadKeyFile(path string) (*PrivateKey, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("sealbox: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("sealbox: %s is not a regular file", path)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("sealbox: %s is mode %04o; the key must be 0600", path, fi.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("sealbox: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(b)))
	clear(b)
	if err != nil || len(raw) != keySize {
		clear(raw)
		// The decoder's error could quote the file; say only which file.
		return nil, fmt.Errorf("sealbox: %s does not hold a 32-byte key in standard base64", path)
	}
	k := &PrivateKey{}
	copy(k.priv[:], raw)
	clear(raw)
	pub, err := publicOf(&k.priv)
	if err != nil {
		return nil, err
	}
	k.pub = pub
	return k, nil
}

func publicOf(priv *[keySize]byte) (PublicKey, error) {
	var p PublicKey
	b, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	if err != nil {
		return p, errors.New("sealbox: the key file does not hold a usable X25519 key")
	}
	copy(p[:], b)
	return p, nil
}

// Seal seals plaintext to pub.
func Seal(pub PublicKey, plaintext []byte) ([]byte, error) {
	fp := pub.Fingerprint()
	out := make([]byte, 0, headerLen+len(plaintext)+box.AnonymousOverhead)
	out = append(out, Version)
	out = append(out, fp[:]...)
	p := [keySize]byte(pub)
	return box.SealAnonymous(out, plaintext, &p, rand.Reader)
}

// Open opens what Seal made for k.
func Open(k *PrivateKey, sealed []byte) ([]byte, error) {
	if len(sealed) < headerLen+box.AnonymousOverhead || sealed[0] != Version {
		return nil, ErrUnreadable
	}
	fp := k.pub.Fingerprint()
	if !bytes.Equal(sealed[1:headerLen], fp[:]) {
		return nil, ErrOtherKey
	}
	pub := [keySize]byte(k.pub)
	out, ok := box.OpenAnonymous(nil, sealed[headerLen:], &pub, &k.priv)
	if !ok {
		return nil, ErrUnreadable
	}
	return out, nil
}

// Credentials are an account's credentials once opened. They print REDACTED for every verb.
type Credentials struct {
	KeyType    string // hmac | ed25519
	Key        string
	Secret     string
	Passphrase string
}

const redactedCreds = "sealbox.Credentials{REDACTED}"

func (Credentials) String() string               { return redactedCreds }
func (Credentials) GoString() string             { return redactedCreds }
func (Credentials) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, redactedCreds) }
func (Credentials) MarshalJSON() ([]byte, error) { return []byte(`"REDACTED"`), nil }

// SealCredentials seals c for the account apiID of kind to pub: what goes into
// state.v1.Account.sealed_credentials.
func SealCredentials(pub PublicKey, apiID int64, kind string, c Credentials) ([]byte, error) {
	if apiID <= 0 || kind == "" {
		return nil, errors.New("sealbox: credentials need an api_id and an account kind")
	}
	if c.Key == "" || c.Secret == "" {
		return nil, errors.New("sealbox: credentials need a key and a secret")
	}
	plain, err := proto.Marshal(&statev1.AccountCredentials{
		ApiId: apiID, Kind: kind, KeyType: c.KeyType, Key: c.Key, Secret: c.Secret, Passphrase: c.Passphrase,
	})
	if err != nil {
		return nil, errors.New("sealbox: credentials do not marshal")
	}
	defer clear(plain)
	return Seal(pub, plain)
}

// OpenCredentials opens an account's sealed_credentials with this instance's key and checks that
// they were sealed for apiID and kind. Errors are ErrOtherKey, ErrUnreadable or ErrOtherAccount;
// Reason turns them into the REJECTED reason.
func OpenCredentials(k *PrivateKey, sealed []byte, apiID int64, kind string) (Credentials, error) {
	plain, err := Open(k, sealed)
	if err != nil {
		return Credentials{}, err
	}
	defer clear(plain)
	var m statev1.AccountCredentials
	if err := proto.Unmarshal(plain, &m); err != nil {
		return Credentials{}, ErrUnreadable
	}
	if m.ApiId != apiID || m.Kind != kind {
		return Credentials{}, ErrOtherAccount
	}
	return Credentials{KeyType: m.KeyType, Key: m.Key, Secret: m.Secret, Passphrase: m.Passphrase}, nil
}
