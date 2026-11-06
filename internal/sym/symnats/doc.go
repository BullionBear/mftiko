// Package symnats is SYM on NATS (PROTOCOL §7.3, E22-D002): the server for the sym subject, the
// change notifications, and the NATS client Resolver. It depends on sym.API, the contract and
// natsx. The business rules are sym.Service's; this package owns only what is NATS.
//
// Serve answers sym in a queue group of the same name, so any replica answers. A request is a
// symv1.Request whose oneof names the call. A success is a symv1.Reply and nothing else. A
// *sym.Error is a natsx error reply (option A): natsx.CodeHandler and the text
// "<code>: <reason>"; Reply.error and Reply.reason stay empty. A payload that is not a Request
// is natsx.CodeBadRequest. A Reply at or over NATS's 1 MiB payload is unavailable, on top of
// the page cap sym.Service applies on every protocol (E22-D010).
//
// Change notifications: NATSChanged is the sym.ChangedPublisher cmd/mftiko-sym gives the Store.
// After a revision commits it publishes Changed{venue, category, revision} on
// sym.v1.changed.{venue}.{category} (ChangedSubject, E04-D057). Venue and category are the
// human-readable names. Session, account worker and md.fetch subscribe ChangedWildcard; a conn
// worker subscribes ChangedVenueWildcard of its venue. Only the replica that committed
// publishes. SYM's own NATS user does not subscribe sym.v1.changed.> (E12-D079).
//
// OpenRemote is the symbol.Resolver other processes run (E2-09, E02-D003). It pages List into a
// sym.Snapshot of the active table, reloads a table on its notification, on a periodic revision
// comparison and on reconnect, and asks SYM for include_inactive without caching the answer.
package symnats
