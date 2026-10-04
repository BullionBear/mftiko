// Package book is an opt-in helper that keeps a local book from orderbook_delta: snapshot from
// md.view, then deltas with seq > S, refetching on a gap (DECISIONS D21, PROTOCOL §7.1).
package book
