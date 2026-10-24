// Package bookpath is the session's local order book (DECISIONS E08-D057–E08-D061, PROTOCOL §7, §9.5).
//
// A snapshot is always md.view's BookSnapshot: the conn worker's own book. This package does
// not ask the exchange. Deltas that arrive while a snapshot is outstanding are buffered inside
// the book and are not hook events. After the snapshot, a buffered delta is applied only when
// its (conn_epoch, seq) is greater, in lexicographic order, and continuous with the cursor.
// A gap, a changed conn_epoch, or a reset marks the book unsynced and asks for another snapshot.
// not_synced and any other failed read wait on the E04-D024 backoff: one second, then double, capped
// at thirty seconds. A retry already waiting, or a snapshot already in flight, is not started again.
//
// The ring length is sdk/book.DefaultN, the window md.view is built with (E04-D030, E08-D059). The tick
// size is the resolved instrument's price_tick. An unresolved UID, or a tick that is missing or
// not positive, is logged and left unsynced. This package does not call SYM.
//
// OnMessage, View, and the scheduled callbacks run on the session dispatcher. View returns
// (nil, false) while that instrument is unsynced (E08-D060). The returned book is valid only for
// the hook that is running (E08-D003).
package bookpath
