// Package natsx is the core-NATS client for the data plane.
//
// NATS carries strategy data only: market data, orders, account events, symbol data (D7, D14). It does
// not orchestrate processes. This package never enables JetStream, knows no business subject, and has no
// streaming RPC: Request and Serve are one request and one reply.
//
// Publish and Subscribe pass bytes through unchanged. The md.f envelope lives in contract/wire/envelope
// and is not framed here.
//
// A session worker's reply inbox is not the shared _INBOX. Pass Options.InboxPrefix _INBOX_{session_id}
// (D30). The JWT and seed Options carries are produced elsewhere; signing them needs the session's
// subjects, which this package does not know.
package natsx
