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
// recon has run; an orders connection that drops leaves orders to REST (D614). A connection about to
// reach its MaxLifetime is replaced before it closes, so the replacement is not a reconnect (D614).
//
// Not yet: the OMS, ledger, td.evt and td.view (E6-07, behind Ledger), orders (E6-08), batch cancel
// and cancel-on-disconnect (E6-09).
package account
