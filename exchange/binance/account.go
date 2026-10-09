package binance

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

func accountFactory(k kind) exchange.AccountFactory {
	return func(cfg exchange.AccountConfig) (exchange.Account, error) {
		return &account{k: k, cfg: cfg}, nil
	}
}

// account serves every Binance account kind. The kind picks the API family: api (spot), fapi (UM),
// dapi (CM), eapi (EO); BinancePM uses papi and picks /papi/v1/margin, /um or /cm by the ticker's
// venue.
type account struct {
	k   kind
	cfg exchange.AccountConfig
}

var (
	_ exchange.Account            = (*account)(nil)
	_ exchange.CancelOnDisconnect = (*account)(nil)
)

func (a *account) Limits() exchange.AccountLimits { return exchange.AccountLimits{} }

func (a *account) Submit(time.Time, *exchange.OrderRequest) (exchange.Call[*exchange.OrderResult], error) {
	return exchange.Call[*exchange.OrderResult]{}, exchange.ErrNotImplemented
}

func (a *account) Cancel(time.Time, exchange.CancelRef) (exchange.Call[struct{}], error) {
	return exchange.Call[struct{}]{}, exchange.ErrNotImplemented
}

// CancelBatch: futures batchOrders; spot has no batch cancel by id (Limits().CancelBatch = 1).
func (a *account) CancelBatch(time.Time, []exchange.CancelRef) (exchange.Call[[]error], error) {
	return exchange.Call[[]error]{}, exchange.ErrNotImplemented
}

// Snapshot: openOrders, positionRisk (futures), account balances.
func (a *account) Snapshot(time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	return nil, exchange.ErrNotImplemented
}

func (a *account) Leverage(time.Time, *symbol.Info) (exchange.Call[float64], error) {
	return exchange.Call[float64]{}, exchange.ErrNotImplemented
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	return nil, exchange.ErrNotImplemented
}

// NewTradeStream: the WebSocket API with an ed25519 session.logon (E3-07, E3-10).
func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	return nil, exchange.ErrNotImplemented
}

// ArmCancelOnDisconnect: countdownCancelAll, per symbol (futures only).
func (a *account) ArmCancelOnDisconnect(time.Time, time.Duration, []*symbol.Info) (exchange.Call[struct{}], error) {
	return exchange.Call[struct{}]{}, exchange.ErrNotImplemented
}

func (a *account) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
