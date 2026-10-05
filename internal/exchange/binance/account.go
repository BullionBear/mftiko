package binance

import (
	"context"
	"time"

	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

func accountFactory(p product) exchange.AccountFactory {
	return func(cfg exchange.AccountConfig) (exchange.Account, error) {
		return &account{p: p, cfg: cfg}, nil
	}
}

type account struct {
	p   product
	cfg exchange.AccountConfig
}

var (
	_ exchange.Account            = (*account)(nil)
	_ exchange.CancelOnDisconnect = (*account)(nil)
)

func (a *account) Limits() exchange.AccountLimits { return exchange.AccountLimits{} }

func (a *account) Submit(context.Context, *exchange.OrderRequest) (*exchange.OrderResult, error) {
	return nil, exchange.ErrNotImplemented
}

func (a *account) Cancel(context.Context, exchange.CancelRef) error {
	return exchange.ErrNotImplemented
}

// CancelBatch: futures batchOrders; spot has no batch cancel by id (Limits().CancelBatch = 1).
func (a *account) CancelBatch(context.Context, []exchange.CancelRef) ([]error, error) {
	return nil, exchange.ErrNotImplemented
}

// Snapshot: openOrders, positionRisk (futures), account balances.
func (a *account) Snapshot(context.Context) (*exchange.AccountSnapshot, error) {
	return nil, exchange.ErrNotImplemented
}

func (a *account) Leverage(context.Context, *symbol.Info) (string, error) {
	return "", exchange.ErrNotImplemented
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	return nil, exchange.ErrNotImplemented
}

// ArmCancelOnDisconnect: countdownCancelAll, per symbol (futures only).
func (a *account) ArmCancelOnDisconnect(context.Context, time.Duration, []*symbol.Info) error {
	return exchange.ErrNotImplemented
}
