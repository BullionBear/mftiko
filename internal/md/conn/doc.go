// Package conn is one MD conn worker: a single websocket for a (venue, route, n), the diff
// from the latest desired subscription, and publication of decoded events on md.f (D402, D403, D409).
//
// What a frame means is exchange.Session. The socket, its pings, its idle timeout and its
// MaxLifetime are exchange/transport/ws. That package never reconnects. This one does. The dial
// right after a drop is immediate. A dial that fails waits, then twice as long, up to a cap.
// conn_epoch increases only after a dial succeeds, and each feed's seq starts at 1 on that
// connection. Both ride in the envelope. Before the dial, Serve writes the number it is about
// to use into a C4 Report, so the shim holds it before any md.f byte of that connection exists.
// A failed dial does not consume the number. The agent copies the shim's report into the next
// Init; this package does not.
//
// Depth updates are folded into a sdk/book ring on this connection (D424–D434). orderbook and
// orderbook_delta are two feeds of that book. orderbook_delta forwards each normalized increment
// unchanged. orderbook is the top-N, rebuilt only when an increment touches it. A reset and the
// book md.view reads are the levels inside the ring. SnapshotREST takes one snapshot and then
// only increments; it refetches only on a sequence gap or a checksum failure (D426).
// SnapshotStream does not: a gap or a checksum failure unsubscribes and subscribes that one
// channel after a per-book backoff, and the venue's next snapshot is the reset (D434). The book
// dies with the connection: reconnect starts again, and conn_epoch changes only when the dial
// succeeds. A resync keeps the epoch and takes the next seq of each feed. SnapshotREST depth
// runs in this process, off the read goroutine, so a ping still gets an answer. Tape for
// trade, aggtrade and liquidation is a per-atom ring of the bytes published on md.f (D435–D440).
// md.view is served on another goroutine: tape pages, or the current in-window book (D441–D446).
// Serve is what cmd/mftiko-md-conn runs.
package conn
