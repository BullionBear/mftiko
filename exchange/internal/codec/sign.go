package codec

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"

	"github.com/BullionBear/mftiko/exchange"
)

// ErrKey: the credentials hold no usable signing key. The error never says more than which check failed: the
// key is not in it, and neither is anything a parser said about the key's bytes.
var ErrKey = errors.New("codec: unusable signing key")

// Signer signs with an api_id's secret: HMAC-SHA256 (KeyHMAC) or ed25519 (KeyEd25519; the Binance adapter
// signs with nothing else). The secret is parsed once, in NewSigner. What is signed is the venue's payload, built by appending
// into a buffer; the Sign methods append the signature to dst. Safe for concurrent use.
//
// Like the Credentials it comes from, a Signer formats as REDACTED with every verb and as JSON.
type Signer struct {
	typ exchange.KeyType
	mac []byte
	ed  ed25519.PrivateKey
}

// NewSigner parses c's secret: the HMAC secret as its bytes, the ed25519 key as a PEM-encoded PKCS #8
// private key, which is what Binance's key generator writes.
func NewSigner(c exchange.Credentials) (*Signer, error) {
	switch c.Type {
	case exchange.KeyHMAC:
		if c.Secret == "" {
			return nil, fmt.Errorf("%w: empty HMAC secret", ErrKey)
		}
		return &Signer{typ: c.Type, mac: []byte(c.Secret)}, nil
	case exchange.KeyEd25519:
		k, err := parseEd25519(c.Secret)
		if err != nil {
			return nil, err
		}
		return &Signer{typ: c.Type, ed: k}, nil
	default:
		return nil, fmt.Errorf("%w: unknown key type", ErrKey)
	}
}

// parseEd25519 reads a PEM PKCS #8 ed25519 private key. The parsers' own errors are dropped: they can quote
// the bytes they choked on.
func parseEd25519(s string) (ed25519.PrivateKey, error) {
	blk, _ := pem.Decode([]byte(s))
	if blk == nil || blk.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%w: ed25519 key is not a PEM PRIVATE KEY block", ErrKey)
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: ed25519 key is not PKCS #8", ErrKey)
	}
	ed, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: PKCS #8 key is not ed25519", ErrKey)
	}
	return ed, nil
}

// Type is the kind of key: exchange.KeyHMAC or exchange.KeyEd25519.
func (s *Signer) Type() exchange.KeyType { return s.typ }

// Sign appends the raw signature of msg to dst: 32 bytes of HMAC-SHA256, or 64 of ed25519.
func (s *Signer) Sign(dst, msg []byte) []byte {
	if s.typ == exchange.KeyEd25519 {
		return append(dst, ed25519.Sign(s.ed, msg)...)
	}
	m := hmac.New(sha256.New, s.mac)
	m.Write(msg)
	return m.Sum(dst)
}

// SignHex appends the signature of msg in lowercase hex: Binance HMAC, Bybit.
func (s *Signer) SignHex(dst, msg []byte) []byte {
	var raw [ed25519.SignatureSize]byte
	return hex.AppendEncode(dst, s.Sign(raw[:0], msg))
}

// SignBase64 appends the signature of msg in standard, padded base64: Binance ed25519, OKX. In a query it
// still needs percent-encoding (Query.AddBytes).
func (s *Signer) SignBase64(dst, msg []byte) []byte {
	var raw [ed25519.SignatureSize]byte
	return base64.StdEncoding.AppendEncode(dst, s.Sign(raw[:0], msg))
}

const redactedSigner = "codec.Signer{REDACTED}"

// Format covers every verb: %d or %x would otherwise print the key's bytes.
func (Signer) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redactedSigner) }

func (Signer) String() string   { return redactedSigner }
func (Signer) GoString() string { return redactedSigner }

// MarshalJSON covers zerolog's Interface and any other JSON encoder.
func (Signer) MarshalJSON() ([]byte, error) { return []byte(`"REDACTED"`), nil }
