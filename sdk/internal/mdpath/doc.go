// Package mdpath is the session's path to market data (DECISIONS D846–D849, PROTOCOL §7, §9.4).
//
// Apply takes a StartPayload or Command bindings snapshot. A snapshot whose selectors are
// not empty is rejected and not applied (D822, D846). Otherwise only a Version newer than the
// one already applied is taken; the first snapshot is applied even when its Version is zero.
// The subscribed set is the feed subjects in that snapshot: subjects that were not held are
// subscribed, subjects that left the snapshot are unsubscribed. The subject string is the one
// on the binding. Nothing in this package builds an md.f subject (D106).
//
// A live md.f message is delivered as soon as it arrives. This package does not ask for tape
// and does not hold live messages (D847). Delivery goes through the dispatcher, so the prehook
// still drops a message older than recv_window_ms. A strategy that wants history calls Tape.
// The reply's TapePage.head is the feed's high water; the strategy treats a live
// (conn_epoch, seq) at or below that cursor as a duplicate.
//
// md.view and md.fetch each wait InitPayload.recv_window_ms. The reply, including a timeout
// or a missing responder, is handed to the callback on the TD buffer (D848). md.view is the
// feed subject with the md.v1.f. prefix replaced by md.v1.view. md.fetch is
// md.v1.fetch.{venue}, where venue is the instrument's venue name.
//
// A message whose envelope is short, whose version is not 1, or whose reserved bytes are not
// zero is dropped. The dispatcher counts it in QueueStat.dropped and logs it. The session
// does not end (D849). A topic whose hook is nil is skipped before the payload is decoded
// (D805). orderbook_delta is not one of those hooks: it is given to Config.Book as the raw
// message, and sdk/internal/bookpath applies it (D857–D861). A payload that does not decode
// is logged and not delivered; it is not an envelope drop.
//
// The session worker's Run opens a Path, applies Start and Live snapshots, and
// reads bindings_applied from Applied.
package mdpath
