// Package natsx is the core-NATS client for the data plane.
//
// NATS carries strategy data only: market data, orders, account events, symbol data (E00-D002, E00-D004). It does
// not orchestrate processes. This package never enables JetStream, knows no business subject, and has no
// streaming RPC: Request and Serve are one request and one reply.
//
// Publish and Subscribe pass bytes through unchanged. The md.f envelope lives in contract/wire/envelope
// and is not framed here. Disconnects, reconnects, closes, and asynchronous errors are logged to
// Options.Logger. OnDisconnect and OnReconnect are additional callbacks for a
// process that must notice a gap; they run on the client's goroutine and must not block.
//
// MD and md.fetch set Options.CredsFile from MFTIKO_NATS_CREDS (E04-D015). A session
// does not: its JWT arrives in Init (E09-D006). The inbox prefix is
// Options.InboxPrefix: sessions use _INBOX_{session_id} (E09-D006), platform
// processes use _INBOX_{plane} (E04-D016).
package natsx
