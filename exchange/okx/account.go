package okx

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

type account struct{ cfg exchange.AccountConfig }

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

// CancelBatch: /api/v5/trade/cancel-batch-orders.
func (a *account) CancelBatch(time.Time, []exchange.CancelRef) (exchange.Call[[]error], error) {
	return exchange.Call[[]error]{}, exchange.ErrNotImplemented
}

func (a *account) Snapshot(time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	return nil, exchange.ErrNotImplemented
}

func (a *account) Leverage(time.Time, *symbol.Info) (exchange.Call[float64], error) {
	return exchange.Call[float64]{}, exchange.ErrNotImplemented
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	return nil, exchange.ErrNotImplemented
}

// ArmCancelOnDisconnect: cancel-all-after, account-wide; syms is ignored.
func (a *account) ArmCancelOnDisconnect(time.Time, time.Duration, []*symbol.Info) (exchange.Call[struct{}], error) {
	return exchange.Call[struct{}]{}, exchange.ErrNotImplemented
}

func (a *account) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }
