// Package agent is the market-data agent (D901, D401, D403). It turns feed demand into atoms,
// places each atom on a conn worker, and writes the ack. It does not dial an exchange, evaluate a
// selector, or read a session spec: the book stays in the conn worker that holds the atom (D402),
// and a selector is a later epic.
//
// A feed key is topic.UniversalTicker. Resolver.Get resolves the ticker to a UID; after that,
// placement and the ack use the UID, and fields come from GetByUID. The atom id stays
// venue:route:channel, so orderbook and orderbook_delta share one subscribe. The owner of an atom
// is the set of (session, feed) demands, not a counter.
//
// A new atom goes on the (venue, route) connection that has room and the smallest n. n+1 opens
// only when every lower n is full. The placement is written to etcd before the plan is pushed
// (D110) and reused after a restart. Nothing is consolidated back onto a lower n.
//
// When the owner set hits zero the atom stays subscribed for linger (default 5s, D905), then the
// worker is stopped. Linger is in memory: a restart rebuilds owners from demand and drops a
// placement that no longer has one. The last reported conn_epoch is copied into the next Init
// (D409).
package agent
