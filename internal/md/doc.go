// Package md is the market-data plane: lease intake, feed and selector derivation into atoms, the option-chain orchestrator, connection placement (D403), and the provider that writes acks.
//
// Each feed is resolved through SYM first (symbol.Resolver). Specs name it with a UniversalTicker;
// that string becomes a UID at spec load (D203). exchange Market.Atom turns the Info into an atom
// and Budget/Cost place it (D403, D202). NATS subjects stay human-readable, not UID. The exact
// spelling is not final (PROTOCOL §10 P14); md.f is still md.f.{inst}.{topic}.{ticker}.
//
// The agent is internal/md/agent. Selectors are a later epic.
package md
