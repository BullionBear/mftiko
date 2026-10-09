// Package md is the market-data plane: lease intake, feed and selector derivation into atoms, the option-chain orchestrator, connection placement (D26), and the provider that writes acks.
//
// Each feed is resolved through SYM first (symbol.Resolver). Specs name it with a UniversalTicker;
// that string becomes a UID at spec load (D38). exchange Market.Atom turns the Info into an atom
// and Budget/Cost place it (D26, D31). NATS subjects stay human-readable, not UID. The exact
// spelling is not final (PROTOCOL §10 P14); md.f is still md.f.{inst}.{topic}.{ticker}.
//
// Shape only: not implemented yet.
package md
