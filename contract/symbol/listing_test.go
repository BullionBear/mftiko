package symbol_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestListingTicker(t *testing.T) {
	for _, tc := range []struct {
		name string
		l    symbol.Listing
		want string
	}{
		{
			name: "spot",
			l: symbol.Listing{
				Venue: "Binance", Category: symbol.Spot, Base: "btc", Quote: "usdt",
				ExchTicker: "BTC-USDT",
			},
			want: "Binance_Spot_BTCUSDT",
		},
		{
			name: "perp",
			l: symbol.Listing{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
				ExchTicker: "NOT-A-SPLIT",
			},
			want: "Bybit_Perp_BTCUSDT",
		},
		{
			name: "inverse alias",
			l: symbol.Listing{
				Venue: "BinanceCM", Category: symbol.Inverse, Base: "XBT", Quote: "USD",
				ExchTicker: "BTCUSD_PERP",
			},
			want: "BinanceCM_Inverse_BTCUSD",
		},
		{
			name: "dated",
			l: symbol.Listing{
				Venue: "BinanceUM", Category: symbol.Future, Base: "BTC", Quote: "USDT",
				ExpiryCode: "260925", ExchTicker: "BTCUSDT_260925",
			},
			want: "BinanceUM_Future_BTCUSDT-260925",
		},
		{
			name: "option decimal strike",
			l: symbol.Listing{
				Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD",
				ExpiryCode: "261225", Strike: 6.4, OptionType: "c",
				ExchTicker: "BTC-USD-261225-6.4-C",
			},
			want: "Okx_Option_BTCUSD-261225-6D4-C",
		},
		{
			name: "option integer trailing zeros",
			l: symbol.Listing{
				Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD",
				ExpiryCode: "261225", Strike: 6400.0, OptionType: "P",
			},
			want: "Okx_Option_BTCUSD-261225-6400-P",
		},
		{
			name: "option fractional strike below one",
			l: symbol.Listing{
				Venue: "Bybit", Category: symbol.Option, Base: "DOGE", Quote: "USDT",
				ExpiryCode: "260905", Strike: 0.125, OptionType: "C",
			},
			want: "Bybit_Option_DOGEUSDT-260905-0D125-C",
		},
		{
			name: "cjk spot",
			l:    symbol.Listing{Venue: "Gate", Category: symbol.Spot, Base: "龙虾", Quote: "usdt"},
			want: "Gate_Spot_龙虾USDT",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.l.Ticker()
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.want {
				t.Fatalf("got %s, want %s", got.String(), tc.want)
			}
			again, err := symbol.Parse(got.String())
			if err != nil || again != got {
				t.Fatalf("Parse = %+v, %v", again, err)
			}
		})
	}
}

func TestListingTickerRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		l    symbol.Listing
	}{
		{name: "perp with expiry", l: symbol.Listing{Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExpiryCode: "260925"}},
		{name: "future without date", l: symbol.Listing{Venue: "BinanceUM", Category: symbol.Future, Base: "BTC", Quote: "USDT"}},
		{name: "bad date", l: symbol.Listing{Venue: "BinanceUM", Category: symbol.Future, Base: "BTC", Quote: "USDT", ExpiryCode: "2609"}},
		{name: "option without strike", l: symbol.Listing{Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD", ExpiryCode: "261225", OptionType: "C"}},
		{name: "bad option type", l: symbol.Listing{Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD", ExpiryCode: "261225", Strike: 1, OptionType: "call"}},
		{name: "lowercase venue", l: symbol.Listing{Venue: "bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT"}},
		{name: "empty asset", l: symbol.Listing{Venue: "Bybit", Category: symbol.Perp, Base: "", Quote: "USDT"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.l.Ticker(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// E02-D003's comparison table. Every row is a Validate case, and the stored rows
// also derive linear / inverse / quanto and 幣本位 / U 本位.
func TestValidateE02D003(t *testing.T) {
	contract := func(l symbol.Listing) symbol.Listing {
		if l.ContractSize == 0 {
			l.ContractSize = 1
		}
		if l.Status == "" {
			l.Status = symbol.Tradable
		}
		return l
	}
	for _, tc := range []struct {
		name                 string
		l                    symbol.Listing
		wantErr              error
		wantTicker           string
		linear, inverse      bool
		quanto, coin, uMargn bool
	}{
		{
			name: "linear BTCUSDT",
			l: contract(symbol.Listing{
				Venue: "BinanceUM", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
				ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT",
			}),
			wantTicker: "BinanceUM_Perp_BTCUSDT",
			linear:     true, uMargn: true,
		},
		{
			name: "linear BTCUSDC",
			l: contract(symbol.Listing{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDC",
				ContractValueCcy: "BTC", PriceCcy: "USDC", Settle: "USDC",
			}),
			wantTicker: "Bybit_Perp_BTCUSDC",
			linear:     true, uMargn: true,
		},
		{
			name: "inverse",
			l: contract(symbol.Listing{
				Venue: "BinanceCM", Category: symbol.Inverse, Base: "BTC", Quote: "USD",
				ContractValueCcy: "USD", PriceCcy: "USD", Settle: "BTC",
			}),
			wantTicker: "BinanceCM_Inverse_BTCUSD",
			inverse:    true, coin: true,
		},
		{
			name: "U margined option",
			l: contract(symbol.Listing{
				Venue: "BinanceEO", Category: symbol.Option, Base: "BTC", Quote: "USDT",
				ExpiryCode: "261226", Strike: 100000, OptionType: "C",
				ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT",
			}),
			wantTicker: "BinanceEO_Option_BTCUSDT-261226-100000-C",
			linear:     true, uMargn: true,
		},
		{
			name: "coin margined option",
			l: contract(symbol.Listing{
				Venue: "Okx", Category: symbol.Option, Base: "BTC", Quote: "USD",
				ExpiryCode: "261225", Strike: 100000, OptionType: "C",
				ContractValueCcy: "BTC", PriceCcy: "BTC", Settle: "BTC",
			}),
			wantTicker: "Okx_Option_BTCUSD-261225-100000-C",
			coin:       true,
		},
		{
			name: "quanto",
			l: contract(symbol.Listing{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USD",
				ContractValueCcy: "BTC", PriceCcy: "USD", Settle: "USDC",
			}),
			wantErr:    symbol.ErrQuanto,
			wantTicker: "Bybit_Perp_BTCUSD",
			quanto:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.l.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate err = %v, want %v", err, tc.wantErr)
			}
			got, err := tc.l.Ticker()
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.wantTicker {
				t.Fatalf("ticker %s, want %s", got.String(), tc.wantTicker)
			}
			info := symbol.Info{
				Base: tc.l.Base, Quote: tc.l.Quote,
				ContractValueCcy: tc.l.ContractValueCcy, PriceCcy: tc.l.PriceCcy, Settle: tc.l.Settle,
			}
			if info.Linear() != tc.linear || info.Inverse() != tc.inverse || info.Quanto() != tc.quanto ||
				info.CoinMargined() != tc.coin || info.UMargined() != tc.uMargn {
				t.Fatalf("linear=%v inverse=%v quanto=%v coin=%v u=%v",
					info.Linear(), info.Inverse(), info.Quanto(), info.CoinMargined(), info.UMargined())
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	ok := symbol.Listing{
		Venue: "BinanceUM", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
		ContractSize: 0.001, ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT",
		Status: symbol.Tradable,
	}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		l          symbol.Listing
		keepStatus bool
		wantErr    error
	}{
		{
			name:    "missing contract size",
			l:       symbol.Listing{Category: symbol.Perp, Base: "BTC", Quote: "USDT", ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "negative contract size",
			l:       symbol.Listing{Category: symbol.Perp, Base: "BTC", Quote: "USDT", ContractSize: -1, ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "missing price ccy",
			l:       symbol.Listing{Category: symbol.Future, Base: "BTC", Quote: "USDT", ContractSize: 1, ContractValueCcy: "BTC", Settle: "USDT"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "missing settlement",
			l:       symbol.Listing{Category: symbol.Inverse, Base: "BTC", Quote: "USD", ContractSize: 1, ContractValueCcy: "USD", PriceCcy: "USD"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "missing value ccy",
			l:       symbol.Listing{Category: symbol.Option, Base: "BTC", Quote: "USDT", ContractSize: 1, PriceCcy: "USDT", Settle: "USDT"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "lowercase asset",
			l:       symbol.Listing{Category: symbol.Perp, Base: "btc", Quote: "USDT", ContractSize: 1, ContractValueCcy: "BTC", PriceCcy: "USDT", Settle: "USDT"},
			wantErr: symbol.ErrAsset,
		},
		{
			name:    "alias not applied",
			l:       symbol.Listing{Category: symbol.Inverse, Base: "XBT", Quote: "USD", ContractSize: 1, ContractValueCcy: "USD", PriceCcy: "USD", Settle: "BTC"},
			wantErr: symbol.ErrAsset,
		},
		{
			name:    "spot with settlement",
			l:       symbol.Listing{Category: symbol.Spot, Base: "BTC", Quote: "USDT", Settle: "USDT"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "unknown category",
			l:       symbol.Listing{Category: "Swap", Base: "BTC", Quote: "USDT", Status: symbol.Tradable},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:       "unspecified status",
			l:          symbol.Listing{Category: symbol.Spot, Base: "BTC", Quote: "USDT"},
			keepStatus: true,
			wantErr:    symbol.ErrInvalidListing,
		},
		{
			name:    "status string unspecified",
			l:       symbol.Listing{Category: symbol.Spot, Base: "BTC", Quote: "USDT", Status: "UNSPECIFIED"},
			wantErr: symbol.ErrInvalidListing,
		},
		{
			name:    "unknown status",
			l:       symbol.Listing{Category: symbol.Spot, Base: "BTC", Quote: "USDT", Status: "NOPE"},
			wantErr: symbol.ErrInvalidListing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := tc.l
			if !tc.keepStatus && l.Status == "" {
				l.Status = symbol.Tradable
			}
			if err := l.Validate(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
	for _, st := range []symbol.Status{symbol.Expired, symbol.Delisted, symbol.Halted, symbol.Unknown, symbol.Prelisting, symbol.Settling} {
		l := symbol.Listing{Category: symbol.Spot, Base: "BTC", Quote: "USDT", Status: st}
		if err := l.Validate(); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if l.IsTradable() {
			t.Fatalf("%s is tradable", st)
		}
	}
	spot := symbol.Listing{Category: symbol.Spot, Base: "BTC", Quote: "USDT", Status: symbol.Tradable}
	if err := spot.Validate(); err != nil {
		t.Fatal(err)
	}
	if !spot.IsTradable() || !(symbol.Info{Status: symbol.Tradable}).IsTradable() {
		t.Fatal("TRADABLE is not tradable")
	}
	if (*symbol.Listing)(nil).IsTradable() {
		t.Fatal("nil listing is tradable")
	}
	cjk := symbol.Listing{Category: symbol.Spot, Base: "龙虾", Quote: "USDT", Status: symbol.Tradable}
	if err := cjk.Validate(); err != nil {
		t.Fatal(err)
	}
	if info := (symbol.Info{Base: "BTC", Quote: "USDT"}); info.Linear() || info.Inverse() || info.Quanto() || info.CoinMargined() || info.UMargined() {
		t.Fatal("spot grouped as a contract")
	}
}

func TestStrikeSpelling(t *testing.T) {
	for v, want := range map[float64]string{6400.0: "6400", 6.4: "6D4", 0.125: "0D125", 1e5: "100000", 0.00001: "0D00001"} {
		got, err := symbol.StrikeSpelling(v)
		if err != nil || got != want {
			t.Errorf("StrikeSpelling(%v) = %q, %v; want %q", v, got, err, want)
		}
	}
	for _, v := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if got, err := symbol.StrikeSpelling(v); !errors.Is(err, symbol.ErrInvalidTicker) {
			t.Errorf("StrikeSpelling(%v) = %q, %v; want ErrInvalidTicker", v, got, err)
		}
	}
}

// TestFiltersJSON: an absent bound is null in JSON and NaN in Go, and Equal takes two of them as equal.
func TestFiltersJSON(t *testing.T) {
	f := symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.00001, symbol.MaxQty: math.NaN()}
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"max_qty":null,"price_tick":0.1,"qty_step":1e-05}`; string(b) != want {
		t.Fatalf("Marshal = %s", b)
	}
	var g symbol.Filters
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatal(err)
	}
	if !f.Equal(g) || !g.Equal(f) {
		t.Fatalf("round trip: %v != %v", g, f)
	}
	if f.Equal(symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.00001, symbol.MaxQty: 1}) {
		t.Fatal("an absent bound equals 1")
	}
	if _, err := json.Marshal(symbol.Filters{"x": math.Inf(1)}); err == nil {
		t.Fatal("Inf marshalled")
	}
}
