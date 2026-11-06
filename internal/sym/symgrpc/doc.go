// Package symgrpc is SYM over gRPC, contract S (PROTOCOL §7.3, E22-D005–E22-D008): the
// mftiko.sym.v1.SymService handler for services outside mftiko. It depends on sym.API, the
// contract and connect, and not on the other adapters (E22-D002).
//
// Get, GetByUid, List, Reverse and Revisions call sym.API and wrap the result in its
// …Response. A *sym.Error becomes a status (E22-D004): invalid is InvalidArgument, not_found is
// NotFound, unavailable is Unavailable, or Canceled and DeadlineExceeded when the cause is the
// context's. The message is the same "<code>: <reason>" the sym subject sends. WatchChanges is
// Unimplemented until E22-07. There is no Refresh (E22-D008).
//
// Every SymService and reflection call carries "authorization: Bearer <token>" from the token
// file. A missing or unknown token is Unauthenticated. Each call writes one access log line with
// the token's name. grpc.health.v1 needs no token (E22-D014).
//
// The listener speaks HTTP/2 over TLS when a certificate is set, h2c otherwise, and HTTP/1.1 for
// the Connect protocol (E22-D007).
package symgrpc
