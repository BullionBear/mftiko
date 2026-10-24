// Package tdpath is the session's path to TD (DECISIONS E08-D040–E08-D045, E08-D053–E08-D056, PROTOCOL §9.4, §9.6).
//
// Orders of this session's own algos live in the OMS, keyed by (api_id, client_order_id).
// Submit records PENDING_NEW before it returns. An OrderUpdate replaces that order wholesale.
// FILLED, CANCELLED, REJECTED and EXPIRED leave the OMS after OnOrder. A Recon replaces the
// account's orders. A PENDING_NEW submitted after the ReconRequest this reply answers, and
// absent from the snapshot, stays (E08-D054). A td.evt Recon is not that reply, so an order the
// snapshot omits is gone.
//
// The ledger is one set of positions (by UID) and balances (by asset) per account.
// PositionUpdate and BalanceUpdate store those absolute values. Recon replaces both sets.
// Fill does not change the ledger (E08-D055). Balance free, locked and prelock are stored as TD
// sent them (E08-D053).
//
// OnOrder, OnFill, OnPosition, OnBalance and OnRecon run after that write. Order, OpenOrders,
// Position and Balance return protobuf copies and do not block on the network (E08-D056).
//
// Listen subscribes to td.evt before any ReconRequest. Until that reply has been given to
// OnRecon, events stay held. The hold uses the TD buffer cap (65536); one more event ends the
// session FAILED (E08-D041, E08-D015). After the reply, an event is delivered only when its
// source.incarnation is the reply's and its seq is the next one. A held event from an
// older incarnation is dropped. A held event from a newer incarnation stays held and
// asks again; an ask that does not move the watermark waits on the Recon backoff
// (E08-D044, E08-D042). Once the baseline is live, a later gap or a different incarnation
// holds again and asks for a new Recon (E08-D044).
//
// One attempt waits InitPayload.recv_window_ms. A Submit that times out becomes UNKNOWN and
// is not queried or sent again (E08-D043, E06-D005). A cancel that times out is left as it was, and
// the strategy is not told (E08-D045). A Recon that times out, or finds no responder, is asked
// again after 1s, then 2s, doubling up to 30s; events stay held (E08-D042).
//
// The session worker's Run opens a Path, listens before OnStart, and asks for the
// first Recon after OnStart returns.
package tdpath
