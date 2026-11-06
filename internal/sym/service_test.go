package sym

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestErrorReasons(t *testing.T) {
	err := readErr(context.Canceled, "get ticker Bybit_Spot_BTCUSDT", missed("ticker Bybit_Spot_BTCUSDT", false))
	if err == nil || err.Error() != "unavailable: get ticker Bybit_Spot_BTCUSDT: context canceled" {
		t.Fatalf("canceled: %v", err)
	}
	err = readErr(errors.New("SELECT secret FROM symbol_active"), "list Bybit/Spot", notFound("unused"))
	if err == nil || err.Error() != "unavailable: list Bybit/Spot: database read failed" {
		t.Fatalf("sql: %v", err)
	}
	err = statusUnavailable(symbol.Info{}, errors.New("sym: status"))
	if err == nil || err.Error() != `unavailable: status "" cannot be encoded` {
		t.Fatalf("status: %v", err)
	}
	if _, err := parseCategory(""); err == nil || err.Error() != `invalid: category "" is empty` {
		t.Fatalf("category: %v", err)
	}
	if err := rejectVenue("by-bit"); err == nil || err.Error() != `invalid: venue "by-bit" is not canonical` {
		t.Fatalf("venue: %v", err)
	}
	sv := NewService(&Store{}, nil, zerolog.Nop())
	if _, err := sv.refresh(context.Background(), &symv1.RefreshRequest{Venue: "Bybit"}); err == nil || err.Error() != "unavailable: refresh is not served by this process" {
		t.Fatalf("refresh: %v", err)
	}
}

func TestStatusProto(t *testing.T) {
	cases := []struct {
		in   Status
		want symv1.SymbolStatus
	}{
		{Prelisting, symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING},
		{Tradable, symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE},
		{Halted, symv1.SymbolStatus_SYMBOL_STATUS_HALTED},
		{Settling, symv1.SymbolStatus_SYMBOL_STATUS_SETTLING},
		{Expired, symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED},
		{Delisted, symv1.SymbolStatus_SYMBOL_STATUS_DELISTED},
		{Unknown, symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN},
	}
	for _, tc := range cases {
		got, err := statusProto(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("%s -> %v %v", tc.in, got, err)
		}
	}
	if _, err := statusProto(symbol.Unspecified); err == nil {
		t.Fatal("unspecified was accepted")
	}
}
