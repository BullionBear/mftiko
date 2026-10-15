// Package fetch is the stateless md.fetch service (D402, D418–D423).
//
// One process per region joins the queue group of each venue it can reach,
// md.v1.fetch.{venue}. Any replica answers. There is no agent, no lease, and no
// database. The program does not route across regions: NATS delivers a request
// to a local member of the queue group when there is one.
//
// A request is a public REST read. Klines pages backward with endNs when the
// caller asks for more than one exchange page, at most MaxKlines. OpenInterest
// is the query-only value venues do not push. Nothing is cached.
//
// The instrument is a UID, resolved through a symbol.Resolver loaded from the
// active table (D422, D461). cmd/mftiko-mdfetch wires that resolver to SYM over NATS.
package fetch
