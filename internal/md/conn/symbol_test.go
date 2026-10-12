package conn

import (
	"context"
	"errors"
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestSymbolTableReplacesAssignment(t *testing.T) {
	ticker := symbol.UniversalTicker{Venue: "Binance", Category: symbol.Spot, Symbol: "BTCUSDT"}
	c := New(Config{
		Venue: "Binance", Route: "public",
		Symbols: stubSymbols{info: &symbol.Info{
			Ticker: ticker, ExchTicker: "BTCUSDT", Status: symbol.Tradable,
			Filters: symbol.Filters{symbol.PriceTick: 0.1},
		}},
	})
	c.SetDesired(symbolDesired(1, "STALE", 1))
	if c.exch["btcusdt@depth"] != "BTCUSDT" || c.tick["btcusdt@depth"] != 0.1 {
		t.Fatalf("table %+v %+v", c.exch, c.tick)
	}

	c.symbols = stubSymbols{miss: true}
	c.SetDesired(symbolDesired(2, "STALE", 1))
	if c.exch["btcusdt@depth"] != "" || c.tick["btcusdt@depth"] != 0 {
		t.Fatalf("missing row kept the assignment %+v %+v", c.exch, c.tick)
	}

	c.symbols = stubSymbols{err: errors.New("sym down")}
	c.SetDesired(symbolDesired(3, "KEEP", 0.5))
	if c.exch["btcusdt@depth"] != "KEEP" || c.tick["btcusdt@depth"] != 0.5 {
		t.Fatalf("transient lookup dropped the assignment %+v %+v", c.exch, c.tick)
	}
}

func symbolDesired(seq uint64, exch string, tick float64) *mdctlv1.Desired {
	return &mdctlv1.Desired{
		Version: &commonv1.Version{Seq: seq},
		Atoms: []*mdctlv1.AtomAssignment{{
			AtomId:     "Binance:public:btcusdt@depth",
			Feeds:      []string{"orderbook_delta.Binance_Spot_BTCUSDT"},
			ExchTicker: exch, PriceTick: tick,
		}},
	}
}

type stubSymbols struct {
	info *symbol.Info
	miss bool
	err  error
}

func (s stubSymbols) Get(_ context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error) {
	if includeInactive {
		return nil, errors.New("worker reads the active table")
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.miss || s.info == nil || s.info.Ticker != t {
		return nil, symbol.ErrNotFound
	}
	return s.info, nil
}
