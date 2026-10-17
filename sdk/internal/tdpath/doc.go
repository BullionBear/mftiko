// Package tdpath is the session's path to TD (DECISIONS D840–D845, PROTOCOL §9.4, §9.6).
//
// It keeps the orders of this session's own algos: enough for Submit to record a local
// pending_new, and for Cancel, BatchCancel, Order and OpenOrders. Positions, balances and the
// full order state machine are a later change (#238).
//
// Listen subscribes to td.evt before any ReconRequest. Until that reply has been given to
// OnRecon, events stay held. The hold uses the TD buffer cap (65536); one more event ends the
// session FAILED (D841, D815). After the reply, an event is delivered only when its
// source.incarnation is the reply's and its seq is the next one. A later gap, or an event
// whose incarnation changed, holds again and asks for a new Recon (D844).
//
// One attempt waits InitPayload.recv_window_ms. A Submit that times out becomes UNKNOWN and
// is not queried or sent again (D843, D605). A cancel that times out is left as it was, and
// the strategy is not told (D845). A Recon that times out, or finds no responder, is asked
// again after 1s, then 2s, doubling up to 30s; events stay held (D842).
//
// Run does not wire this package yet.
package tdpath
