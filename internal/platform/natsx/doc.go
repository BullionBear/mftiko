// Package natsx is the core-NATS client for the data plane.
//
// NATS carries strategy data only: market data, orders, account events, symbol data (D002, D004). It does
// not orchestrate processes. This package never enables JetStream, knows no business subject, and has no
// streaming RPC: Request and Serve are one request and one reply.
//
// Publish and Subscribe pass bytes through unchanged. The md.f envelope lives in contract/wire/envelope
// and is not framed here. Disconnects, reconnects, closes, and asynchronous errors are logged to
// Options.Logger. OnReconnect is an additional callback for a process that must
// reload after a gap; it runs on the client's reconnect goroutine and must not block.
//
// Credentials are not set here (D906). The inbox prefix is Options.InboxPrefix:
// sessions use _INBOX_{session_id} (D906), platform processes use _INBOX_{plane}_{inst} (D416).
package natsx
