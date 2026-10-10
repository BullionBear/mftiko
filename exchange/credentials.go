package exchange

import (
	"fmt"
	"io"
)

// KeyType is how the venue signs: tdctl.Credentials.key_type.
type KeyType string

const (
	KeyHMAC    KeyType = "hmac"
	KeyEd25519 KeyType = "ed25519"
)

// Credentials are an api_id's unsealed API key. They reach the account worker only through the
// socketpair in Init, and the TD agent only to build a History (D601); never env, files or etcd.
//
// Every formatting path redacts them, so a stray log field or %v cannot leak a secret.
type Credentials struct {
	Type       KeyType
	Key        string
	Secret     string // HMAC secret, or the PEM-encoded ed25519 private key
	Passphrase string // OKX
}

const redacted = "exchange.Credentials{REDACTED}"

// Format covers every verb: String only serves %v, %s, %q and %x, and %d or %t would print the fields.
func (Credentials) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redacted) }

func (Credentials) String() string   { return redacted }
func (Credentials) GoString() string { return redacted }

// MarshalJSON covers zerolog's Interface and any other JSON encoder.
func (Credentials) MarshalJSON() ([]byte, error) { return []byte(`"REDACTED"`), nil }
