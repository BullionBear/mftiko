// Package md is the market-data plane: lease intake, feed and selector derivation into atoms, the option-chain orchestrator, connection placement (D26), and the provider that writes acks.
//
// Each feed's UniversalTicker is resolved through SYM first (symbol.Resolver), then
// internal/exchange Market.Atom turns it into an atom and Budget/Cost place it (D26, D31).
//
// Shape only: not implemented yet.
package md
