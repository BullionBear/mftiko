// Package sdk is the Go strategy SDK and the reference implementation of the session worker
// contract (DECISIONS E08-D002, PROTOCOL §9). A strategy is a Go program whose main calls Run.
//
// A strategy implements Strategy and the optional hooks it needs (strategy.go), and acts through
// Context (context.go). Instruments are UIDs throughout (DECISIONS E08-D004, E08-D023–E08-D026). Resolve and
// Info are implemented by sdk/internal/symclient. The TD path (Submit, Cancel, BatchCancel, the
// own-algo order book, and the first Recon) is sdk/internal/tdpath (DECISIONS E08-D040–E08-D045). The MD
// path (bindings, live delivery, Tape, BookSnapshot, and md.fetch) is sdk/internal/mdpath
// (DECISIONS E08-D046–E08-D049). The local order book, fed by md.view and orderbook_delta, is
// sdk/internal/bookpath (DECISIONS E08-D057–E08-D061). Run wires those clients, reports
// bindings_applied from the MD path, and leaves volume reads to the strategy
// (DECISIONS E08-D050–E08-D052, E09-D004).
//
// # Logging
//
// Log and Context.Log are the strategy logger, the same zerolog line the platform writes
// (DECISIONS E15-D017–E15-D022). component is strategy; version and commit come from this binary;
// Context.Log also carries session_id. Both write stderr. Stdout stays the strategy's own data.
//
// Log what concerns the session with Context.Log, inside a hook. Log is the process logger and has
// no session_id: use it only outside a hook (E15-D019).
//
// Log alerts match fields, not text (E16-D005). Rejected orders, orders whose outcome is unknown,
// and the strategy's PnL carry an event field, at this level and with these fields (E16-D006):
//
//	event               level  fields
//	order_rejected      warn   code, reason, client_order_id
//	order_unknown       error  client_order_id
//	pnl                 info   pnl, ccy
//	pnl_floor_breached  error  pnl, floor, ccy
//
// From OnOrder, write order_rejected when the order is REJECTED, with the event's Code and Reason,
// and order_unknown when it turns UNKNOWN: the order may have reached the exchange, which makes it
// worse than a reject (E08-D005, E06-D005). From OnReject, a Cancel or BatchCancel that TD refused is
// order_rejected too, one line per client_order_id. pnl is what the strategy computes, not TD's
// account state, and ccy is its unit, chosen by the strategy. The floor is in the session's params:
// the strategy compares, and writes pnl_floor_breached when pnl is below it (E16-D007).
//
//	log := ctx.Log()
//	log.Warn().Str("event", "order_rejected").Int32("code", ev.Code).Str("reason", ev.Reason).
//		Str("client_order_id", ev.Order.GetClientOrderId()).Msg("order rejected")
//
// The SDK itself writes the messages "hook exceeded 100ms" and "td buffer full", at info, with
// session_id. Stderr is collected centrally (E16-D011): do not log secrets, such as the seed in
// InitPayload.nats.
//
// The event field is a recommendation (PROTOCOL §9.1 item 7), not SDK API: conformance does not
// check it. examples/strategies/logevents writes each line the alerts match.
package sdk
