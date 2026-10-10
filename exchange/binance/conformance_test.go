package binance

import (
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestConformance(t *testing.T) { conformance.Run(t, spec()) }

func TestSpotConformance(t *testing.T) { conformance.Run(t, spotSpec()) }

func FuzzDecodeSpotTrade(f *testing.F) { conformance.FuzzDecodeTrade(f, spotSpec()) }

func FuzzDecodePublic(f *testing.F) { conformance.FuzzDecode(f, spec(), "public") }

func FuzzDecodeMarket(f *testing.F) { conformance.FuzzDecode(f, spec(), "market") }

func FuzzDecodePrivate(f *testing.F) { conformance.FuzzDecodePrivate(f, spec()) }

func FuzzDecodeTrade(f *testing.F) { conformance.FuzzDecodeTrade(f, spec()) }

func FuzzParseSubmit(f *testing.F) { conformance.FuzzParse(f, spec(), "submit_limit") }

func spec() conformance.Spec {
	return conformance.Spec{
		Venue:   VenueUM,
		Dir:     "testdata/conformance/BinanceUM",
		Market:  &market{p: um},
		REST:    &rest{p: um},
		Account: accountFactory(kindUM),
		History: historyFactory(kindUM),
		KeyType: exchange.KeyEd25519,
		Cases: []conformance.Case{
			conformance.Call("submit_limit", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_reject", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_rate", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_auth", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("cancel", func(e conformance.Env) (exchange.Call[struct{}], error) {
				return e.Account.Cancel(e.Now, exchange.CancelRef{Instrument: btcInst(), ClientOrderID: cloidGTC})
			}),
			conformance.Call("cancel_batch", func(e conformance.Env) (exchange.Call[[]error], error) {
				return e.Account.CancelBatch(e.Now, []exchange.CancelRef{
					{Instrument: btcInst(), ClientOrderID: cloidGTC},
					{Instrument: btcInst(), ClientOrderID: cloidIOC},
				})
			}),
			conformance.Call("open_orders", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				calls, err := e.Account.Snapshot(e.Now)
				if err != nil {
					return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
				}
				return calls[0], nil
			}),
			conformance.Call("positions", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				calls, err := e.Account.Snapshot(e.Now)
				if err != nil {
					return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
				}
				return calls[1], nil
			}),
			conformance.Call("balances", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				calls, err := e.Account.Snapshot(e.Now)
				if err != nil {
					return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
				}
				return calls[2], nil
			}),
			conformance.Call("leverage", func(e conformance.Env) (exchange.Call[float64], error) {
				return e.Account.Leverage(e.Now, btcInst())
			}),
			conformance.Call("countdown", func(e conformance.Env) (exchange.Call[struct{}], error) {
				return e.Account.(exchange.CancelOnDisconnect).ArmCancelOnDisconnect(e.Now, 120*time.Second, []*symbol.Info{btcInst()})
			}),
			conformance.Call("user_trades", func(e conformance.Env) (exchange.Call[*exchange.FillPage], error) {
				return e.History.Fills(e.Now, exchange.FillQuery{Instrument: btcInst(), FromID: "100", Limit: 1})
			}),
			conformance.Call("order_lookup", func(e conformance.Env) (exchange.Call[*exchange.OrderUpdate], error) {
				return e.History.Order(e.Now, btcInst(), "1001")
			}),
			// The same testnet fill as ws/testnet.jsonl: stream trade id 545553575.
			conformance.Call("user_trades_testnet", func(e conformance.Env) (exchange.Call[*exchange.FillPage], error) {
				return e.History.Fills(e.Now, exchange.FillQuery{Instrument: btcInst(), FromID: "545553575", Limit: 1})
			}),
			conformance.Call("order_testnet", func(e conformance.Env) (exchange.Call[*exchange.OrderUpdate], error) {
				return e.History.Order(e.Now, btcInst(), "28625103204")
			}),
			conformance.Call("listing_perp", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Perp)
			}),
			conformance.Call("listing_future", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Future)
			}),
			conformance.Call("depth", func(e conformance.Env) (exchange.Call[*exchange.BookUpdate], error) {
				return e.REST.Depth(btcInst(), 1000)
			}),
			conformance.Call("klines", func(e conformance.Env) (exchange.Call[[]exchange.Kline], error) {
				return e.REST.Klines(btcInst(), "1m", 0, 5)
			}),
			conformance.Call("open_interest", func(e conformance.Env) (exchange.Call[*exchange.OpenInterest], error) {
				return e.REST.OpenInterest(btcInst())
			}),
		},
		FillPairs: []conformance.FillPair{
			{Session: "private", Case: "user_trades"},
			{Session: "testnet", Case: "user_trades_testnet"},
		},
		TradeSamples: []conformance.TradeSample{
			{Name: "limit GTC", Submit: limitOrder(exchange.TIFGTC, cloidGTC)},
			{Name: "limit IOC", Submit: limitOrder(exchange.TIFIOC, cloidIOC)},
			{Name: "limit FOK", Submit: limitOrder(exchange.TIFFOK, cloidFOK)},
			{Name: "limit GTX", Submit: limitOrder(exchange.TIFPostOnly, cloidGTX)},
			{Name: "market", Submit: marketOrder(cloidMKT)},
			{Name: "cancel by client id", Cancel: &exchange.CancelRef{Instrument: btcInst(), ClientOrderID: cloidGTC}},
		},
	}
}

// D602 ids are decimal and at most 19 digits. These are 19.
const (
	cloidGTC = "1767225600000000001"
	cloidIOC = "1767225600000000002"
	cloidFOK = "1767225600000000003"
	cloidGTX = "1767225600000000004"
	cloidMKT = "1767225600000000005"
)

func btcInst() *symbol.Info {
	return &symbol.Info{
		ExchTicker: "BTCUSDT",
		Filters:    symbol.Filters{symbol.PriceTick: 0.1, symbol.QtyStep: 0.001},
	}
}

func limitOrder(tif exchange.TimeInForce, cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: btcInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: tif,
		Qty: 0.001, QuoteQty: decimal.Absent(), Price: 65000, ClientOrderID: cloid,
	}
}

func marketOrder(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: btcInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: 0.001, QuoteQty: decimal.Absent(), Price: decimal.Absent(), ClientOrderID: cloid,
	}
}

func spotSpec() conformance.Spec {
	return conformance.Spec{
		Venue:   VenueSpot,
		Dir:     "testdata/conformance/Binance",
		Account: accountFactory(kindSpot),
		History: historyFactory(kindSpot),
		KeyType: exchange.KeyEd25519,
		Cases: []conformance.Case{
			conformance.Call("submit_limit", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, spotLimit(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_market", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, spotMarket(cloidMKT))
			}),
			conformance.Call("cancel", func(e conformance.Env) (exchange.Call[struct{}], error) {
				return e.Account.Cancel(e.Now, exchange.CancelRef{Instrument: spotInst(), ClientOrderID: cloidGTC})
			}),
			conformance.Call("open_orders", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				calls, err := e.Account.Snapshot(e.Now)
				if err != nil || len(calls) < 1 {
					return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
				}
				return calls[0], nil
			}),
			conformance.Call("balances", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				calls, err := e.Account.Snapshot(e.Now)
				if err != nil || len(calls) < 2 {
					return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
				}
				return calls[1], nil
			}),
			conformance.Call("my_trades", func(e conformance.Env) (exchange.Call[*exchange.FillPage], error) {
				return e.History.Fills(e.Now, exchange.FillQuery{Instrument: spotInst(), FromID: "100", Limit: 1})
			}),
			conformance.Call("order_lookup", func(e conformance.Env) (exchange.Call[*exchange.OrderUpdate], error) {
				return e.History.Order(e.Now, spotInst(), "1001")
			}),
			// The same testnet fill as ws/testnet.jsonl: stream trade id 207624.
			conformance.Call("my_trades_testnet", func(e conformance.Env) (exchange.Call[*exchange.FillPage], error) {
				return e.History.Fills(e.Now, exchange.FillQuery{Instrument: spotInst(), FromID: "207624", Limit: 1})
			}),
			conformance.Call("order_testnet", func(e conformance.Env) (exchange.Call[*exchange.OrderUpdate], error) {
				return e.History.Order(e.Now, spotInst(), "1106510")
			}),
		},
		FillPairs: []conformance.FillPair{
			{Session: "testnet", Case: "my_trades_testnet"},
		},
		TradeSamples: []conformance.TradeSample{
			{Name: "limit GTC", Submit: spotLimit(exchange.TIFGTC, cloidGTC)},
			{Name: "limit IOC", Submit: spotLimit(exchange.TIFIOC, cloidIOC)},
			{Name: "limit FOK", Submit: spotLimit(exchange.TIFFOK, cloidFOK)},
			{Name: "limit post-only", Submit: spotLimit(exchange.TIFPostOnly, cloidGTX)},
			{Name: "market", Submit: spotMarket(cloidMKT)},
			{Name: "cancel by client id", Cancel: &exchange.CancelRef{Instrument: spotInst(), ClientOrderID: cloidGTC}},
		},
	}
}

func spotInst() *symbol.Info {
	return &symbol.Info{
		ExchTicker: "BTCUSDT",
		Filters:    symbol.Filters{symbol.PriceTick: 0.01, symbol.QtyStep: 0.00001},
	}
}

func spotLimit(tif exchange.TimeInForce, cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: tif,
		Qty: 0.001, QuoteQty: decimal.Absent(), Price: 65000, ClientOrderID: cloid,
	}
}

func spotMarket(cloid string) *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: spotInst(), Side: exchange.SideBuy, Type: exchange.OrderTypeMarket,
		Qty: 0.001, QuoteQty: decimal.Absent(), Price: decimal.Absent(), ClientOrderID: cloid,
	}
}
