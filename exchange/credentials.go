package exchange

// KeyType is how the venue signs: tdctl.Credentials.key_type.
type KeyType string

const (
	KeyHMAC    KeyType = "hmac"
	KeyEd25519 KeyType = "ed25519"
)

// Credentials are an api_id's unsealed API key. They reach the account worker only through the
// socketpair in Init, and the TD agent only to build a History (D20); never env, files or etcd.
//
// Every formatting path redacts them, so a stray log field or %v cannot leak a secret.
type Credentials struct {
	Type       KeyType
	Key        string
	Secret     string // HMAC secret, or the PEM-encoded ed25519 private key
	Passphrase string // OKX
}

const redacted = "exchange.Credentials{REDACTED}"

func (Credentials) String() string   { return redacted }
func (Credentials) GoString() string { return redacted }

// MarshalJSON covers zerolog's Interface and any other JSON encoder.
func (Credentials) MarshalJSON() ([]byte, error) { return []byte(`"REDACTED"`), nil }
