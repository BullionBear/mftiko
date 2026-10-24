// Package account is the TD account worker: one api_id's private connections and HTTP pool, recon,
// and the account state it reports (E06-D003). It runs under a shim and speaks C4 through
// contract/wire/c4 (PROTOCOL §5, §6.3).
//
// Venue specifics come from exchange: the api_id's AccountKind builds the Account, and through it
// the PrivateStream and TradeStream codecs. Connections go through exchange/transport/ws, REST
// through the Doer the binary gives.
//
// Connections have roles. The events connection carries the account's private events: the trade
// stream when it carries them (TradeLimits.Events, Binance spot), the PrivateStream otherwise. The
// orders connection is a trade stream that does not (OKX, Bybit, Binance UM). Only the events
// connection decides the account state: when it drops the account is DEGRADED until it is back and a
// recon has run; an orders connection that drops leaves orders to REST (E06-D014, E06-D024). A query that
// still has no result five minutes after its first failure is the same DEGRADED state, reason
// query_failed, and it clears once those queries conclude and the events connection is up (E06-D022). A
// connection about to reach its MaxLifetime is replaced before it closes, so the role never goes
// down (E06-D014). The events connection then runs one recon and stays READY; the orders connection
// does not recon (E06-D035, E06-D036).
//
// The account's state is the book's (book.go): the OMS holds live orders only, the ledger positions,
// balances and leverage. It publishes every change on td.evt and answers td.view from memory (E06-D006,
// E06-D015). After Start it also answers td.order: local rejects stay on the reply, and every later
// change is one OrderUpdate (E06-D010, E06-D018–E06-D024).
//
// Batch cancel is one explicit client-order-id list, forwarded as one venue request (E06-D025–E06-D028).
// The worker looks each live order up by that id and sends the order's order id. It does not choose
// orders, split the list, or check which algo an order belongs to. Cancel-on-disconnect is not wired:
// AccountSettings.cancel_on_disconnect_s and CancelOnDisconnect stay unused (E06-D025).
package account
