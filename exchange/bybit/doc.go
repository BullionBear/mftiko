// Package bybit is the Bybit adapter: one venue and one account kind, both named Bybit (two enums,
// D301), a unified account (D403). The category
// (spot, linear, inverse, option) is part of every request and of every public websocket path.
//
// Facts the implementation must honor. Checked 2026-10-05 against bybit-exchange.github.io/docs/v5;
// re-check before implementing.
//
//   - Routes: "spot", "linear", "inverse", "option" (/v5/public/<category>). Spread trading has its
//     own path too and is not modeled yet. Private data is on /v5/private, after auth.
//   - The same channel string exists on several routes: "publicTrade.BTCUSDT" is both a spot and a
//     linear atom. The route comes from the resolved instrument's category, and it is part of the
//     atom id <venue>:<route>:<channel> (D202).
//   - Capacity is characters, not streams: the args of one public connection may total at most
//     21,000 characters. Cost is len(channel), Budget is 21000.
//   - Spot takes at most 10 args per subscribe request (SessionLimits.PerRequest); options at most
//     2000 args per connection, which the character budget reaches first.
//   - Keepalive: the client sends {"op":"ping"} every 20s. At most 500 new connections per 5 minutes
//     per domain.
//   - Books are SnapshotStream: type "snapshot" replaces the book, "delta" chains on u.
//   - Auth signs "GET/realtime" + expires with HMAC-SHA256.
//   - Dead-man switch: Disconnected Cancel Protection (DCP), account-wide.
//   - History: execution list, cursor paginated; execId is the fill id on both REST and the
//     execution stream. Order, window size and retention: TODO(verify).
//
// Shape only: every method that would talk to Bybit returns exchange.ErrNotImplemented.
package bybit
