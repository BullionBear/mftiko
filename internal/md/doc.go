// Package md is the market-data plane: lease intake, feed and selector derivation into atoms, the option-chain orchestrator, connection placement (D403), and the provider that writes acks.
//
// A feed key is topic.UniversalTicker. The MD agent reads the venue from that ticker and the route
// from the topic, places the feed on md/<venue>/<route>/<n>, and sends the feed list (D403). It
// does not query SYM. The conn worker loads that venue's active table at Init, follows
// sym.changed, and calls Market.Atom to subscribe and unsubscribe (D426, D433, D202). NATS
// subjects stay human-readable, not UID. The exact spelling is not final (PROTOCOL §10 P14);
// md.f is still md.f.{inst}.{topic}.{ticker}.
//
// The agent is internal/md/agent. Selectors are a later epic.
package md
