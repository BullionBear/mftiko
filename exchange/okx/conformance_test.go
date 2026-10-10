package okx

import (
	"fmt"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

func TestConformance(t *testing.T) { conformance.Run(t, spec()) }

func FuzzDecodePrivate(f *testing.F) { conformance.FuzzDecodePrivate(f, spec()) }

func FuzzDecodePublic(f *testing.F) { conformance.FuzzDecode(f, spec(), "public") }

func FuzzDecodeBusiness(f *testing.F) { conformance.FuzzDecode(f, spec(), "business") }

func spec() conformance.Spec {
	return conformance.Spec{
		Venue:      Name,
		Dir:        "testdata/conformance/Okx",
		Passphrase: true,
		Market:     &market{},
		REST:       &rest{},
		Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
			return &account{cfg: cfg}, nil
		},
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
			conformance.Call("submit_quote", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
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
				return e.Account.Cancel(e.Now, exchange.CancelRef{Instrument: spotInst(), ClientOrderID: cloidGTC})
			}),
			conformance.Call("cancel_batch", func(e conformance.Env) (exchange.Call[[]error], error) {
				return e.Account.CancelBatch(e.Now, []exchange.CancelRef{
					{Instrument: spotInst(), ClientOrderID: cloidGTC},
					{Instrument: spotInst(), ClientOrderID: cloidIOC},
				})
			}),
			conformance.Call("open_orders", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return snapCall(e, "/api/v5/trade/orders-pending")
			}),
			conformance.Call("positions", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return snapCall(e, "/api/v5/account/positions")
			}),
			conformance.Call("balances", func(e conformance.Env) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
				return snapCall(e, "/api/v5/account/balance")
			}),
			conformance.Call("leverage", func(e conformance.Env) (exchange.Call[float64], error) {
				return e.Account.Leverage(e.Now, swapInst())
			}),
			conformance.Call("caa", func(e conformance.Env) (exchange.Call[struct{}], error) {
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
			conformance.Call("listing_inverse", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Inverse)
			}),
			conformance.Call("klines", func(e conformance.Env) (exchange.Call[[]exchange.Kline], error) {
				return e.REST.Klines(spotInst(), "1m", 0, 5)
			}),
		},
	}
}

func snapCall(e conformance.Env, path string) (exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	calls, err := e.Account.Snapshot(e.Now)
	if err != nil {
		return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, err
	}
	for _, c := range calls {
		if c.Req.Path == path {
			return c, nil
		}
	}
	return exchange.Call[exchange.Page[exchange.AccountSnapshot]]{}, fmt.Errorf("okx: snapshot has no %s", path)
}
