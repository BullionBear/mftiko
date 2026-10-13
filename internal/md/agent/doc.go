// Package agent is the market-data agent (D901, D401, D403). It places feed demand on conn
// workers and writes the ack. It does not dial an exchange, evaluate a selector, read a session
// spec, or talk to SYM. The book and the subscription stay in the conn worker (D402). A selector
// is a later epic.
//
// A feed key is topic.UniversalTicker. Venue is in the ticker. The route comes from the topic
// and that ticker, which is enough to name md/<venue>/<route>/<n>. The agent sends the feed
// list. The conn worker loads its venue's active table at Init, follows sym.v1.changed, and turns
// those feeds into atoms and reports each one as FeedStatus. SUBSCRIBED becomes READY while
// the worker is running, and REJECTED is copied onto the ack. The owner of a feed is the set
// of (session, feed) demands, not a counter.
//
// Feeds that share a channel take one budget slot and stay on one connection. A new channel
// goes on the (venue, route) connection that has room and the smallest n. n+1 opens only when
// every lower n is full. The placement is written to etcd before the plan is pushed
// (D110) and reused after a restart. Nothing is consolidated back onto a lower n.
//
// When the owner set hits zero the feed stays on the worker for linger (default 5s, D905), then
// it leaves the list and the worker unsubscribes whatever atom no longer has a feed. Linger is
// in memory: a restart rebuilds owners from demand and drops a placement that no longer has one.
// The last reported conn_epoch is copied into the next Init (D409).
package agent
