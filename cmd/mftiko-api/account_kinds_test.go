package main

import (
	"testing"

	"github.com/BullionBear/mftiko/exchange/all"
)

func TestAccountKindsFromRegistry(t *testing.T) {
	byName := map[string]struct {
		supported  bool
		trades     int
		passphrase bool
		keyTypes   []string
		exchange   string
	}{}
	for _, k := range accountKinds(all.Registry()) {
		byName[k.Name] = struct {
			supported  bool
			trades     int
			passphrase bool
			keyTypes   []string
			exchange   string
		}{k.Supported(), len(k.Trades), k.Passphrase, k.KeyTypes, k.Exchange}
	}
	pm := byName["BinancePM"]
	if pm.supported || pm.trades != 3 {
		t.Fatalf("BinancePM %+v", pm)
	}
	okx := byName["Okx"]
	if !okx.supported || !okx.passphrase || len(okx.keyTypes) != 1 || okx.keyTypes[0] != "hmac" || okx.exchange != "okx" {
		t.Fatalf("Okx %+v", okx)
	}
	um := byName["BinanceUM"]
	// Binance kinds advertise only ed25519, the type the adapter signs with (E12-D076).
	if !um.supported || len(um.keyTypes) != 1 || um.keyTypes[0] != "ed25519" || um.exchange != "binance" {
		t.Fatalf("BinanceUM %+v", um)
	}
}
