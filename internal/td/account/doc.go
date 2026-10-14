// Package account is the TD account worker: one api_id's private connections and HTTP pool, recon,
// and the account state it reports (D603). It runs under a shim and speaks C4 through
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
// recon has run; an orders connection that drops leaves orders to REST (D614, D624). A query that
// still has no result five minutes after its first failure is the same DEGRADED state, reason
// query_failed, and it clears once those queries conclude and the events connection is up (D622). A
// connection about to reach its MaxLifetime is replaced before it closes, so the replacement is not
// a reconnect (D614).
//
// The account's state is the book's (book.go): the OMS holds live orders only, the ledger positions,
// balances and leverage. It publishes every change on td.evt and answers td.view from memory (D606,
// D615). After Start it also answers td.order: local rejects stay on the reply, and every later
// change is one OrderUpdate (D610, D618–D624).
//
// Not yet: batch cancel and cancel-on-disconnect (E6-09).
package account
