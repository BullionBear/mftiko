// Package cloidgen is the session worker's client_order_id generator (DECISIONS E08-D011–E08-D014,
// PROTOCOL §9.4).
//
// One counter per process, shared by every algo and every account. A millisecond has 1024
// ids, seq 0..1023. When that millisecond is used up, ts_ms borrows the next one. There is
// no cap on borrowing, so ts_ms can run ahead of the wall clock passed as nowMs. That takes
// more than a million ids a second, which does not happen in practice (E08-D012). A clock step
// backwards does not move ts_ms back: the id keeps the counter's ts_ms.
//
// The counter is not persisted. A new process starts at seq 0 again. A client_order_id that
// collides with one from an earlier incarnation is the account worker's idempotency problem
// (E08-D013, E06-D019), not something this package prevents.
package cloidgen
