package symbol_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestParseRoundTrip(t *testing.T) {
	for _, text := range []string{
		"Gate_Spot_BTCUSDT",
		"GateFutures_Perp_BTCUSDT",
		"Bybit_Spot_ETHUSDT",
		"Bybit_Perp_ETHUSDT",
		"BinanceCM_Inverse_BTCUSD",
		"BinanceCM_Future_BTCUSD-260925",
		"BinanceUM_Future_BTCUSDT-250926",
		"BinanceUM_Option_BTCUSDT-260905-100000-C",
		"Bybit_Option_AVAXUSDC-260905-6D4-C",
		"Bybit_Option_AVAXUSDC-260905-6D4-P",
		"BinanceEO_Option_BTCUSDT-261226-100000-C",
		"Okx_Option_BTCUSD-261225-100000-C",
		"Bybit_Perp_BTCUSDC",
		"Gate_Spot_龙虾USDT",
		"Bybit_Future_BTCUSDT-261301", // six digits, not a calendar check
		"Okx_Perp_BTCUSDT",
		"OKX_Spot_BTCUSDT", // venue case is not folded; the registry spelling is Resolve's job
	} {
		got, err := symbol.Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		if got.String() != text {
			t.Fatalf("String() = %q, want %q", got.String(), text)
		}
		again, err := symbol.Parse(got.String())
		if err != nil || again != got {
			t.Fatalf("Parse(String()) = %+v, %v; want %+v", again, err, got)
		}
	}
}

func TestParseRejectsNonCanonical(t *testing.T) {
	for _, bad := range []string{
		"",
		"Gate_Spot",
		"Gate_Spot_BTC_USDT",
		"Gate__BTCUSDT",
		"Gate_Bogus_BTCUSDT",
		"gate_Spot_BTCUSDT",
		"Gate_spot_BTCUSDT",
		"Gate_Spot_btcusdt",
		"Gate_Spot_BTC-USDT",
		"Gate_Spot_BTCUSDT-250926",
		"BinanceUM_Future_BTCUSDT250926",
		"Bybit_Option_AVAXUSDC-260905-6.4-C",
		"Bybit_Option_AVAXUSDC-260905-6d4-C",
		"Bybit_Option_AVAXUSDC-260905-6D40-C",
		"Bybit_Option_AVAXUSDC-260905-6D0-C",
		"Bybit_Option_BTCUSDT-260905-10_000-C",
		"Bybit_Option_BTCUSDT-260905-6400.0-C",
		"Bybit_Option_BTCUSDT-260905-10 000-C",
		" Bybit_Perp_BTCUSDT",
		"Bybit_Perp_BTC/USDT",
		"Bybit_Perp_BTCUSDT_",
		"Gate_Spot_龙虾_USDT",
		"Gate_Spot_龙虾/USDT",
	} {
		if _, err := symbol.Parse(bad); !errors.Is(err, symbol.ErrInvalidTicker) {
			t.Errorf("Parse(%q) err = %v, want ErrInvalidTicker", bad, err)
		}
	}
}

func TestParseDotNamesTheStrikeRule(t *testing.T) {
	_, err := symbol.Parse("Bybit_Option_AVAXUSDC-260905-6.4-C")
	if err == nil || !strings.Contains(err.Error(), "6D4") {
		t.Fatalf("err = %v, want the StrikeDecimal spelling", err)
	}
}

func TestSpotAndPerpAreDifferent(t *testing.T) {
	spot, err := symbol.Parse("Bybit_Spot_BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	perp, err := symbol.Parse("Bybit_Perp_BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	if spot == perp || spot.Venue != perp.Venue || spot.Symbol != perp.Symbol {
		t.Fatalf("spot %+v perp %+v", spot, perp)
	}
}

type venueReg map[string]symbol.Venue

func (r venueReg) Lookup(name string) (symbol.Venue, bool) {
	v, ok := r[name]
	return v, ok
}

func (r venueReg) Names() []string {
	out := make([]string, 0, len(r))
	for name := range r {
		out = append(out, name)
	}
	return out
}

func testVenues() venueReg {
	return venueReg{
		"Gate":      {Name: "Gate", Categories: []symbol.Category{symbol.Spot}},
		"Binance":   {Name: "Binance", Categories: []symbol.Category{symbol.Spot}},
		"BinanceUM": {Name: "BinanceUM", Categories: []symbol.Category{symbol.Perp, symbol.Future}},
		"BinanceCM": {Name: "BinanceCM", Categories: []symbol.Category{symbol.Inverse, symbol.Future}},
		"BinanceEO": {Name: "BinanceEO", Categories: []symbol.Category{symbol.Option}},
		"Bybit": {Name: "Bybit", Categories: []symbol.Category{
			symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option,
		}},
		"Okx": {Name: "Okx", Categories: []symbol.Category{
			symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option,
		}},
	}
}

func TestResolve(t *testing.T) {
	venues := testVenues()
	for _, tc := range []struct {
		in, want string
	}{
		{"Bybit_Perp_BTCUSDT", "Bybit_Perp_BTCUSDT"},
		{"bybit_perp_btc/usdt", "Bybit_Perp_BTCUSDT"},
		{"BYBIT_PERP_BTC-USDT", "Bybit_Perp_BTCUSDT"},
		{" bybit_perp_btc usdt ", "Bybit_Perp_BTCUSDT"},
		{"GATE_SPOT_BTC/USDT", "Gate_Spot_BTCUSDT"},
		{" Gate_spot_btc-usdt ", "Gate_Spot_BTCUSDT"},
		{"binanceum_future_btcusdt250926", "BinanceUM_Future_BTCUSDT-250926"},
		{"binanceum_future_btcusdt-250926", "BinanceUM_Future_BTCUSDT-250926"},
		{"binanceum_future_btc-usdt-250926", "BinanceUM_Future_BTCUSDT-250926"},
		{"okx_option_btc-usd-261225-6.4-c", "Okx_Option_BTCUSD-261225-6D4-C"},
		{"OKX_OPTION_AVAXUSDC-260905-6d4-C", "Okx_Option_AVAXUSDC-260905-6D4-C"},
		{"bybit_option_btcusdt-260905-6400.0-c", "Bybit_Option_BTCUSDT-260905-6400-C"},
		{"bybit_option_btcusdt-260905-7e4-p", "Bybit_Option_BTCUSDT-260905-70000-P"},
		{"gate_spot_龙虾/usdt", "Gate_Spot_龙虾USDT"},
	} {
		got, err := symbol.Resolve(tc.in, venues)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", tc.in, err)
		}
		if got.String() != tc.want {
			t.Fatalf("Resolve(%q) = %q, want %q", tc.in, got.String(), tc.want)
		}
		if _, err := symbol.Parse(got.String()); err != nil {
			t.Fatalf("Parse(Resolve(%q)): %v", tc.in, err)
		}
	}
}

func TestResolveRejects(t *testing.T) {
	venues := testVenues()
	if _, err := symbol.Resolve("Kraken_Spot_BTCUSDT", venues); !errors.Is(err, symbol.ErrUnknownVenue) {
		t.Fatalf("unknown venue: %v", err)
	}
	if _, err := symbol.Resolve("Gate_Perp_BTCUSDT", venues); !errors.Is(err, symbol.ErrCategory) {
		t.Fatalf("category: %v", err)
	}
	if _, err := symbol.Resolve("Binance_Perp_BTCUSDT", venues); !errors.Is(err, symbol.ErrCategory) {
		t.Fatalf("binance perp: %v", err)
	}
	if _, err := symbol.Resolve("bybit_perp_btc_usdt", venues); !errors.Is(err, symbol.ErrInvalidTicker) {
		t.Fatalf("extra separator: %v", err)
	}
	// '_' is the ticker separator, so a thousands separator in the strike never
	// reaches SpellStrike through Resolve. The strike field on a Listing does.
	if _, err := symbol.Resolve("bybit_option_btcusdt-260905-10_000-c", venues); !errors.Is(err, symbol.ErrInvalidTicker) {
		t.Fatalf("underscore in strike: %v", err)
	}
	if _, err := symbol.Resolve("bybit_futures_btcusdt", venues); !errors.Is(err, symbol.ErrInvalidTicker) {
		t.Fatalf("category: %v", err)
	}
	if _, err := symbol.Resolve("bybit_option_btcusdt-260905-10 000-c", venues); !errors.Is(err, symbol.ErrInvalidTicker) {
		t.Fatalf("unspellable strike: %v", err)
	}
	if _, err := symbol.Resolve("", nil); !errors.Is(err, symbol.ErrInvalidTicker) {
		t.Fatalf("empty: %v", err)
	}
}

func FuzzParseRoundTrip(f *testing.F) {
	for _, s := range []string{
		"Gate_Spot_BTCUSDT",
		"Bybit_Perp_BTCUSDT",
		"BinanceCM_Inverse_BTCUSD",
		"BinanceUM_Future_BTCUSDT-260925",
		"Bybit_Option_AVAXUSDC-260905-6D4-C",
		"Okx_Option_BTCUSD-261225-100000-P",
		"Gate_Spot_龙虾USDT",
		"Bybit_Future_BTCUSDT-261301",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, err := symbol.Parse(s)
		if err != nil {
			return
		}
		if got.String() != s {
			t.Fatalf("Parse accepted non-canonical %q as %q", s, got.String())
		}
		again, err := symbol.Parse(got.String())
		if err != nil || again != got {
			t.Fatalf("Parse(String()) = %+v, %v; want %+v", again, err, got)
		}
	})
}

func FuzzConstructedRoundTrip(f *testing.F) {
	f.Add("Bybit", "Option", "AVAXUSDC-260905-6D4-C")
	f.Add("Gate", "Spot", "龙虾USDT")
	f.Add("BinanceUM", "Future", "BTCUSDT-260925")
	f.Fuzz(func(t *testing.T, venue, cat, sym string) {
		in := symbol.UniversalTicker{Venue: venue, Category: symbol.Category(cat), Symbol: sym}
		out, err := symbol.Parse(in.String())
		if err != nil {
			return
		}
		if out != in {
			t.Fatalf("Parse(%q) = %+v, constructed %+v", in.String(), out, in)
		}
	})
}
