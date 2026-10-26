package binance

import (
	"errors"
	"slices"
	"testing"

	"github.com/BullionBear/mftiko/exchange"
)

// Every kind advertises the one key type the adapter signs with (E03-D003). An advertised HMAC let
// the API accept a BinanceUM account whose worker then failed every dial (#385).
func TestKindsAdvertiseWhatTheySign(t *testing.T) {
	for _, k := range Declare().AccountKinds {
		if !slices.Equal(k.KeyTypes, []exchange.KeyType{exchange.KeyEd25519}) {
			t.Errorf("%s advertises %v, want only ed25519", k.Name, k.KeyTypes)
		}
	}
	var s signerState
	if _, err := s.get("BinanceUM", exchange.Credentials{Type: exchange.KeyHMAC}); !errors.Is(err, exchange.ErrUnsupported) {
		t.Fatalf("HMAC credentials: %v, want ErrUnsupported", err)
	}
}
