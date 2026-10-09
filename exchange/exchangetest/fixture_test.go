package exchangetest_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

func TestLoadFixture(t *testing.T) {
	btc := inst(symbol.Perp, "BTC", "BTCUSDT")
	spot := inst(symbol.Spot, "BTC", "BTCUSDT")

	for _, tc := range []struct {
		name    string
		prior   []symbol.Listing
		json    string
		path    string
		nilRead bool
		wantErr error
		substr  string
		want    []symbol.Listing
		spot    []symbol.Listing
	}{
		{
			name: "two categories",
			json: `[
				{"Venue":"Bybit","Category":"Perp","Base":"BTC","Quote":"USDT","ExchTicker":"BTCUSDT","ContractSize":"1","Settle":"USDT","Active":true,"Filters":{"price_tick":"0.1","qty_step":"0.001"}},
				{"Venue":"Bybit","Category":"Spot","Base":"BTC","Quote":"USDT","ExchTicker":"BTCUSDT","ContractSize":"1","Settle":"USDT","Active":true,"Filters":{"price_tick":"0.1","qty_step":"0.001"}}
			]`,
			want: []symbol.Listing{btc},
			spot: []symbol.Listing{spot},
		},
		{
			name: "omitted venue is the source venue",
			json: `[{"Category":"Perp","Base":"ETH","Quote":"USDT","ExchTicker":"ETHUSDT","ContractSize":"1","Settle":"USDT","Active":true,"Filters":{"price_tick":"0.1","qty_step":"0.001"}}]`,
			want: []symbol.Listing{inst(symbol.Perp, "ETH", "ETHUSDT")},
		},
		{
			name:  "empty array adds nothing",
			json:  `[]`,
			prior: []symbol.Listing{btc},
			want:  []symbol.Listing{btc},
		},
		{
			name: "omitted active stays false and omitted filters stay nil",
			json: `[{"Venue":"Bybit","Category":"Perp","Base":"BTC","Quote":"USDT","ExchTicker":"BTCUSDT"}]`,
			want: []symbol.Listing{{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT",
			}},
		},
		{
			name: "empty filters stay empty",
			json: `[{"Venue":"Bybit","Category":"Perp","Base":"BTC","Quote":"USDT","ExchTicker":"BTCUSDT","Filters":{}}]`,
			want: []symbol.Listing{{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT",
				Filters: map[string]string{},
			}},
		},
		{
			name:   "unknown field",
			json:   `[{"Venue":"Bybit","Category":"Perp","ExchTicker":"BTCUSDT","NoSuch":1}]`,
			substr: "unknown field",
			prior:  []symbol.Listing{btc},
			want:   []symbol.Listing{btc},
		},
		{
			name: "different case matches the exported field",
			json: `[{"Venue":"Bybit","Category":"Perp","Base":"BTC","Quote":"USDT","ExchTicker":"BTCUSDT","active":true}]`,
			want: []symbol.Listing{{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT", Active: true,
			}},
		},
		{
			name:   "null is not an array",
			json:   `null`,
			substr: "JSON array",
			prior:  []symbol.Listing{btc},
			want:   []symbol.Listing{btc},
		},
		{
			name:   "object is not an array",
			json:   `{"Venue":"Bybit"}`,
			substr: "cannot unmarshal",
			prior:  []symbol.Listing{btc},
			want:   []symbol.Listing{btc},
		},
		{
			name:   "trailing value",
			json:   `[]{}`,
			substr: "trailing data",
			prior:  []symbol.Listing{btc},
			want:   []symbol.Listing{btc},
		},
		{
			name:   "broken json",
			json:   `[`,
			substr: "EOF",
			prior:  []symbol.Listing{btc},
			want:   []symbol.Listing{btc},
		},
		{
			name:    "nil reader",
			nilRead: true,
			substr:  "nil",
			prior:   []symbol.Listing{btc},
			want:    []symbol.Listing{btc},
		},
		{
			name:    "wrong venue",
			json:    `[{"Venue":"Okx","Category":"Perp","ExchTicker":"BTCUSDT"}]`,
			wantErr: exchangetest.ErrVenueMismatch,
			substr:  "row 0",
			prior:   []symbol.Listing{btc},
			want:    []symbol.Listing{btc},
		},
		{
			name:    "duplicate inside the file applies nothing",
			json:    `[{"Venue":"Bybit","Category":"Perp","Base":"ETH","Quote":"USDT","ExchTicker":"ETHUSDT","Active":true},{"Venue":"Bybit","Category":"Perp","ExchTicker":"ETHUSDT"}]`,
			wantErr: exchangetest.ErrDuplicate,
			substr:  "row 1",
			prior:   []symbol.Listing{btc},
			want:    []symbol.Listing{btc},
		},
		{
			name:    "duplicate of an instrument already stored",
			json:    `[{"Venue":"Bybit","Category":"Perp","Base":"ETH","Quote":"USDT","ExchTicker":"ETHUSDT","Active":true},{"Venue":"Bybit","Category":"Perp","ExchTicker":"BTCUSDT"}]`,
			wantErr: exchangetest.ErrDuplicate,
			substr:  "row 1",
			prior:   []symbol.Listing{btc},
			want:    []symbol.Listing{btc},
		},
		{
			name:   "row with no ticker",
			json:   `[{"Venue":"Bybit","Category":"Perp","ExchTicker":""}]`,
			substr: "row 0",
			prior:  []symbol.Listing{btc},
			want:   []symbol.Listing{btc},
		},
		{
			name: "testdata file",
			path: filepath.Join("testdata", "listing.json"),
			want: []symbol.Listing{{
				Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT",
				ExchTicker: "BTCUSDT", ContractSize: "1", Settle: "USDT", Active: true,
				Filters: map[string]string{symbol.PriceTick: "0.1", symbol.QtyStep: "0.001"},
			}},
			spot: []symbol.Listing{{
				Venue: "Bybit", Category: symbol.Spot, Base: "BTC", Quote: "USDT",
				ExchTicker: "BTCUSDT", ContractSize: "1", Settle: "USDT", Active: true,
				Filters: map[string]string{symbol.PriceTick: "0.01", symbol.QtyStep: "0.00001"},
			}},
		},
		{
			name:    "missing file",
			path:    filepath.Join("testdata", "no-such.json"),
			wantErr: os.ErrNotExist,
			want:    []symbol.Listing{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSource(t)
			mustAdd(t, s, tc.prior...)
			var err error
			switch {
			case tc.nilRead:
				err = s.LoadFixture(nil)
			case tc.path != "":
				err = s.LoadFixtureFile(tc.path)
			default:
				err = s.LoadFixture(strings.NewReader(tc.json))
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
			if tc.substr != "" && (err == nil || !strings.Contains(err.Error(), tc.substr)) {
				t.Fatalf("got %v, want substring %q", err, tc.substr)
			}
			if tc.wantErr == nil && tc.substr == "" && err != nil {
				t.Fatal(err)
			}
			equalList(t, listed(t, s, symbol.Perp), tc.want)
			if tc.spot != nil {
				equalList(t, listed(t, s, symbol.Spot), tc.spot)
			}
		})
	}
}

func TestLoadFixtureDoesNotSeeAPartialApply(t *testing.T) {
	// A duplicate on row 1 must not leave row 0 behind.
	s := newSource(t)
	body := `[
		{"Venue":"Bybit","Category":"Perp","Base":"ETH","Quote":"USDT","ExchTicker":"ETHUSDT","Active":true,"Filters":{"price_tick":"9"}},
		{"Venue":"Bybit","Category":"Perp","ExchTicker":"ETHUSDT","Filters":{"price_tick":"1"}}
	]`
	if err := s.LoadFixture(strings.NewReader(body)); !errors.Is(err, exchangetest.ErrDuplicate) {
		t.Fatal(err)
	}
	got, err := s.Listing(context.Background(), symbol.Perp)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || got == nil {
		t.Fatalf("partial apply: %#v", got)
	}
}

// Every exported field of symbol.Listing has to survive a fixture. The value is filled by kind, not
// by name, so a field added to Listing is covered without a change to the loader or to this list.
func TestFixtureRoundTripAllFields(t *testing.T) {
	original := nonzeroListing(t)
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	s, err := exchangetest.New(original.Venue)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.LoadFixture(strings.NewReader("[" + string(raw) + "]")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Listing(context.Background(), original.Category)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %#v", got)
	}
	if !reflect.DeepEqual(got[0], original) {
		t.Fatalf("got %#v\nwant %#v", got[0], original)
	}
}

func nonzeroListing(t *testing.T) symbol.Listing {
	t.Helper()
	var l symbol.Listing
	v := reflect.ValueOf(&l).Elem()
	typ := v.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := v.Field(i)
		name := typ.Field(i).Name
		if !f.CanSet() {
			t.Fatalf("Listing.%s is not settable; the round trip cannot fill it", name)
		}
		switch f.Kind() {
		case reflect.String:
			f.SetString("v" + name)
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(42)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(7)
		case reflect.Float32, reflect.Float64:
			f.SetFloat(1.5)
		case reflect.Map:
			if f.Type().Key().Kind() != reflect.String || f.Type().Elem().Kind() != reflect.String {
				t.Fatalf("Listing.%s is %s; teach the fixture round trip about this map", name, f.Type())
			}
			m := reflect.MakeMap(f.Type())
			m.SetMapIndex(reflect.ValueOf(name), reflect.ValueOf("bound"))
			f.Set(m)
		default:
			t.Fatalf("Listing.%s has kind %s; teach the fixture round trip about it", name, f.Kind())
		}
	}
	return l
}
