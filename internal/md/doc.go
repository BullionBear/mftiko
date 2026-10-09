// Package md is the market-data plane: lease intake, feed and selector derivation into atoms, the option-chain orchestrator, connection placement (D26), and the provider that writes acks.
//
// Each feed is resolved through SYM first (symbol.Resolver). Specs name it with a UniversalTicker;
// that string becomes a UID at spec load (D38). exchange Market.Atom turns the Info into an atom
// and Budget/Cost place it (D26, D31). md.f subjects still end in the ticker; whether they switch
// to the UID is not decided.
//
// Shape only: not implemented yet.
package md
