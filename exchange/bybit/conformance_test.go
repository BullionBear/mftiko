package bybit

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestConformance(t *testing.T) { conformance.Run(t, spec()) }

func FuzzDecodePrivate(f *testing.F) { conformance.FuzzDecodePrivate(f, spec()) }

func FuzzDecodeSpot(f *testing.F) { conformance.FuzzDecode(f, spec(), "spot") }

func FuzzDecodeLinear(f *testing.F) { conformance.FuzzDecode(f, spec(), "linear") }

func FuzzDecodeTrade(f *testing.F) { conformance.FuzzDecodeTrade(f, spec()) }

func spec() conformance.Spec {
	return conformance.Spec{
		Venue:  Name,
		Dir:    "testdata/conformance/Bybit",
		Market: &market{},
		REST:   &rest{},
		Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
			return &account{cfg: cfg}, nil
		},
		History: func(cfg exchange.AccountConfig) (exchange.History, error) {
			return &history{cfg: cfg}, nil
		},
		Instruments: []*symbol.Info{btcInst()},
		Cases: []conformance.Case{
			conformance.Call("submit_limit", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_market", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, marketOrder(cloidMKT))
			}),
			conformance.Call("submit_postonly", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFPostOnly, cloidGTX))
			}),
			conformance.Call("submit_spot", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, spotQuote(cloidGTC))
			}),
			conformance.Call("submit_reject", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_auth", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, limitOrder(exchange.TIFGTC, cloidGTC))
			}),
			conformance.Call("submit_rate", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
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
				return snapCall(e, "/v5/order/realtime", "settleCoin=USDT")
			}),
			conformance.Call("positions", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return snapCall(e, "/v5/position/list", "settleCoin=USDT")
			}),
			conformance.Call("balances", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return snapCall(e, "/v5/account/wallet-balance", "accountType=UNIFIED")
			}),
			conformance.Call("leverage", func(e conformance.Env) (exchange.Call[float64], error) {
				return e.Account.Leverage(e.Now, btcInst())
			}),
			conformance.Call("dcp", func(e conformance.Env) (exchange.Call[struct{}], error) {
				return e.Account.(exchange.CancelOnDisconnect).ArmCancelOnDisconnect(e.Now, 10*time.Second, nil)
			}),
			conformance.Call("listing_spot", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Spot)
			}),
			conformance.Call("listing_perp", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Perp)
			}),
			conformance.Call("listing_future", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Future)
			}),
			conformance.Call("klines", func(e conformance.Env) (exchange.Call[[]exchange.Kline], error) {
				return e.REST.Klines(btcInst(), "1m", 0, 5)
			}),
			conformance.Call("executions", func(e conformance.Env) (exchange.Call[*exchange.FillPage], error) {
				return e.History.Fills(e.Now, exchange.FillQuery{
					Instrument: btcInst(),
					StartNs:    e.Now.UnixNano(),
					EndNs:      e.Now.Add(time.Hour).UnixNano(),
					Limit:      50,
				})
			}),
			conformance.Call("order_lookup", func(e conformance.Env) (exchange.Call[*exchange.OrderUpdate], error) {
				return e.History.Order(e.Now, btcInst(), "90f08fa4-4426-474b-8dea-f68e2b0fbd31")
			}),
		},
		FillPairs: []conformance.FillPair{
			{Session: "fills", Case: "executions"},
		},
		TradeSamples: []conformance.TradeSample{
			{Name: "limit GTC", Submit: limitOrder(exchange.TIFGTC, cloidGTC)},
			{Name: "market", Submit: marketOrder(cloidMKT)},
			{Name: "spot quote", Submit: spotQuote(cloidGTC)},
			{Name: "cancel by client id", Cancel: &exchange.CancelRef{Instrument: btcInst(), ClientOrderID: cloidGTC}},
		},
	}
}

// D602 ids are decimal and at most 19 digits. These are 19.
const (
	cloidGTC = "1767225600000000001"
	cloidIOC = "1767225600000000002"
	cloidGTX = "1767225600000000004"
	cloidMKT = "1767225600000000005"
)

func snapCall(e conformance.Env, path, query string) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	calls, err := e.Account.Snapshot(e.Now)
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	for _, c := range calls {
		if c.Req.Path == path && strings.Contains(string(c.Req.Query), query) {
			return c, nil
		}
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, fmt.Errorf("bybit: snapshot has no %s %s", path, query)
}
