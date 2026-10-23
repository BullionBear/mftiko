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
	}{}
	for _, k := range accountKinds(all.Registry()) {
		byName[k.Name] = struct {
			supported  bool
			trades     int
			passphrase bool
			keyTypes   []string
		}{k.Supported(), len(k.Trades), k.Passphrase, k.KeyTypes}
	}
	pm := byName["BinancePM"]
	if pm.supported || pm.trades != 3 {
		t.Fatalf("BinancePM %+v", pm)
	}
	okx := byName["Okx"]
	if !okx.supported || !okx.passphrase || len(okx.keyTypes) != 1 || okx.keyTypes[0] != "hmac" {
		t.Fatalf("Okx %+v", okx)
	}
	um := byName["BinanceUM"]
	if !um.supported || len(um.keyTypes) != 2 || um.keyTypes[0] != "hmac" || um.keyTypes[1] != "ed25519" {
		t.Fatalf("BinanceUM %+v", um)
	}
}
