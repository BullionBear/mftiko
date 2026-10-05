// Package all builds the registry of every exchange adapter. Binaries that need adapters (md-agent,
// md-conn, md-fetch, sym, td-agent, td-account, api) call Registry once at startup.
package all

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/exchange/binance"
	"github.com/BullionBear/mftiko/internal/exchange/bybit"
	"github.com/BullionBear/mftiko/internal/exchange/okx"
)

// Registry returns the registry of every exchange in this build. It panics on an inconsistent
// declaration, a programming error that all_test.go catches first.
func Registry() *exchange.Registry {
	r, err := exchange.NewRegistry(
		binance.Declare(),
		okx.Declare(),
		bybit.Declare(),
	)
	if err != nil {
		panic(err)
	}
	return r
}
