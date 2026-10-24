// Package md is the market-data plane: lease intake, feed and selector derivation into atoms, the option-chain orchestrator, connection placement (E04-D003), and the provider that writes acks.
//
// A feed key is topic.UniversalTicker. The MD agent reads the venue from that ticker and the route
// from the topic, places the feed on md/<venue>/<route>/<n>, and sends the feed list (E04-D003). It
// does not query SYM. The conn worker loads that venue's active table at Init, follows
// sym.v1.changed.{venue}, and calls Market.Atom to subscribe and unsubscribe (E04-D026, E04-D033, E02-D002).
// NATS subjects stay human-readable, not UID. A feed is md.v1.f.{inst}.{topic}.{ticker} (E04-D055).
//
// The agent is internal/md/agent. Selectors are a later epic.
package md
