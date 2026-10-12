package conn

import (
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/exchange/binance"
)

func TestRESTOverrideMovesDepthHost(t *testing.T) {
	v, ok := all.Registry().Venue(binance.VenueUM)
	if !ok {
		t.Fatal("no BinanceUM")
	}
	enc, err := EncodeRESTOverrides(map[string]string{binance.VenueUM: "https://testnet.binancefuture.com/ignored"})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvRESTOverrides, enc)
	m, err := RESTOverridesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	inst := &symbol.Info{ExchTicker: "BTCUSDT", Ticker: symbol.UniversalTicker{Venue: binance.VenueUM}}
	call, err := WithRESTBase(v.REST, m[binance.VenueUM]).Depth(inst, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if call.Req.Base != "https://testnet.binancefuture.com" || call.Req.Path != "/fapi/v1/depth" {
		t.Fatalf("request %s%s", call.Req.Base, call.Req.Path)
	}
	plain, err := v.REST.Depth(inst, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Req.Base == call.Req.Base {
		t.Fatal("the override changed the venue's default")
	}
}

func TestRESTOverrideRejectsBadURL(t *testing.T) {
	for _, raw := range []string{"", "wss://fstream.binancefuture.com", "testnet.binancefuture.com"} {
		if _, err := EncodeRESTOverrides(map[string]string{"BinanceUM": raw}); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
	t.Setenv(EnvRESTOverrides, `{"BinanceUM":"ftp://x"}`)
	if _, err := RESTOverridesFromEnv(); err == nil {
		t.Error("bad env accepted")
	}
}
