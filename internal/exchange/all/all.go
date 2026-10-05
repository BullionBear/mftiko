// Package all builds the registry of every exchange adapter. Binaries that need adapters (md-agent,
// md-conn, md-fetch, sym, td-agent, td-account) call Registry once at startup.
package all

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/exchange/binance"
	"github.com/BullionBear/mftiko/internal/exchange/bybit"
	"github.com/BullionBear/mftiko/internal/exchange/okx"
)

// Registry returns the registry of every exchange in this build. It panics on a duplicate venue
// name, a programming error that all_test.go catches first.
func Registry() *exchange.Registry {
	r, err := exchange.NewRegistry(
		binance.Venues(),
		okx.Venues(),
		bybit.Venues(),
	)
	if err != nil {
		panic(err)
	}
	return r
}
