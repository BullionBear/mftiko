package topicdoc

import (
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestProbeShape(t *testing.T) {
	spot := probe("Binance", symbol.Spot)
	if spot.ExchTicker != probeSymbol || spot.Ticker.Symbol != probeSymbol || spot.Ticker.Category != symbol.Spot ||
		spot.ContractSize != 0 || spot.Status != symbol.Tradable {
		t.Fatalf("spot probe %+v", spot)
	}
	perp := probe("BinanceUM", symbol.Perp)
	if perp.ContractSize != 1 || perp.Ticker.Venue != "BinanceUM" || perp.Status != symbol.Tradable {
		t.Fatalf("perp probe %+v", perp)
	}
}

func TestReplaceMarkers(t *testing.T) {
	src := "head\n" + Begin + "\nold\n" + End + "\ntail\n"
	got, err := replace(src, "new\n")
	if err != nil {
		t.Fatal(err)
	}
	want := "head\n" + Begin + "\nnew\n" + End + "\ntail\n"
	if got != want {
		t.Fatalf("got %q", got)
	}
	if _, err := replace("no markers\n", "new\n"); err == nil {
		t.Fatal("missing markers returned nil")
	}
}

func TestSectionRoundTrip(t *testing.T) {
	table, err := Markdown()
	if err != nil {
		t.Fatal(err)
	}
	src := "head\n" + Begin + "\n" + table + End + "\n"
	got, err := Section(src)
	if err != nil {
		t.Fatal(err)
	}
	if got != table {
		t.Fatalf("section %q", got)
	}
	if strings.Contains(table, "kline_") {
		t.Fatal("kline intervals are separate rows")
	}
	if strings.Count(table, "| `kline` |") != 1 {
		t.Fatal("kline is not one row")
	}
}
