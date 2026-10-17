// Package symclient is the session worker's SYM client (DECISIONS D804, D823–D826, PROTOCOL §9.4).
//
// A strategy resolves the tickers in its own config during OnInit and OnStart. Resolve calls
// sym GetRequest and may block. After that, hooks read fields with Info, which only reads
// memory. The client tracks those resolved instruments and no others: it does not look up a UID
// that arrived on an event, and it does not hold the event back (D823). Phase 1 has no selector
// (D822).
//
// For each resolved (venue, category) it subscribes to sym.v1.changed.{venue}.{category}. A
// notification, a revision check about every five minutes, and a NATS reconnect (the session
// calls Reconnect) refetch those rows with GetByUidRequest. When a resolved instrument has moved
// to the terminal table, that refetch uses include_inactive, keeps the last row, and Info still
// returns it (D826). The terminal status is the inactive mark.
//
// Info of a UID this session has not resolved returns ErrUnknownUID. The client does not retry
// that and does not end the session (D824). Run maps ErrUnknownUID and ErrNotInInit onto the
// sdk sentinels of the same name.
package symclient
