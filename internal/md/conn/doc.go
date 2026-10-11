// Package conn is one MD conn worker: a single websocket for a (venue, route, n), the diff
// from the latest desired subscription, and publication of decoded events on md.f (D402, D403, D409).
//
// What a frame means is exchange.Session. The socket, its pings, its idle timeout and its
// MaxLifetime are exchange/transport/ws. That package never reconnects. This one does. The dial
// right after a drop is immediate. A dial that fails waits, then twice as long, up to a cap.
// conn_epoch increases only after a dial succeeds, and each feed's seq starts at 1 on that
// connection. Both ride in the envelope. The latest epoch is kept in memory; handing it to the
// next process belongs to a later ticket.
//
// A depth update is not published and does not take a sequence number. Folding the local book,
// tape, and md.view are later tickets. cmd/mftiko-md-conn does not call Run yet.
package conn
