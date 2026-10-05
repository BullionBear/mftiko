// Package binance is the Binance adapter. It declares five venues (D26): Binance (spot), BinanceUM,
// BinanceCM, BinanceEO and BinancePM. One market codec serves the first four (same combined-stream
// JSON); BinancePM is an account only, its market data comes from the other venues.
//
// Facts the implementation must honor. Checked 2026-10-05 against developers.binance.com; re-check
// before implementing, Binance moved a lot in 2026.
//
//   - BinanceUM: since the 2026-03-06 notice the websocket is split by path on fstream.binance.com:
//     /public (bookTicker, depth), /market (aggTrade, markPrice, kline, tickers, forceOrder),
//     /private (user data). The legacy URLs ended 2026-04-23. These are the routes "public" and
//     "market"; the private path belongs to the account stream.
//   - BinanceUM, Binance spot: 1024 streams per connection (D26), so cost 1 and budget 1024.
//   - BinanceCM: COIN-M was integrated into the USDⓈ-M architecture on 2026-06-30 (REST, streams and
//     account behaviour). Hosts, routes and limits: TODO(verify). Whether CM stays its own venue is
//     DECISIONS Q20.
//   - BinanceEO: the options system was rebuilt in 2026; market streams are documented under
//     fstream.binance.com/market. Routes and limits: TODO(verify).
//   - Books are SnapshotREST: buffer the diff stream, GET depth, drop updates with u < lastUpdateId,
//     then chain on U/u (spot) or pu (futures). A break means BookResync.
//   - Connections live at most 24h; the server pings and expects a pong.
//   - Private stream: listenKey over REST, extended periodically (PrivateStream.Refresh), or the
//     websocket API's logon with an ed25519 key. Pick one at implementation time.
//   - Futures have a per-symbol dead-man switch (countdownCancelAll): CancelOnDisconnect.
//   - userTrades is limited to a 7-day window per request: HistoryTraits.MaxWindow.
//
// Shape only: every method that would talk to Binance returns exchange.ErrNotImplemented.
package binance
