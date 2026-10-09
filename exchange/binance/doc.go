// Package binance is the Binance adapter. It declares two enums (D301):
//
//   - Venues (markets): Binance (spot), BinanceUM, BinanceCM, BinanceEO. One market codec serves all
//     four (same combined-stream JSON).
//   - Account kinds: Binance, BinanceUM, BinanceCM, BinanceEO, each trading the venue of the same
//     name, and BinancePM, which trades Binance, BinanceUM and BinanceCM through papi and is no
//     venue: there is no BinancePM_* ticker.
//
// Facts the implementation must honor. Checked 2026-10-05 against developers.binance.com; re-check
// before implementing.
//
//   - BinanceUM: since the 2026-03-06 notice the websocket is split by path on fstream.binance.com:
//     /public (bookTicker, depth), /market (aggTrade, markPrice, kline, tickers, forceOrder),
//     /private (user data). The legacy URLs ended 2026-04-23. These are the routes "public" and
//     "market"; the private path belongs to the account stream.
//   - BinanceUM, Binance spot: 1024 streams per connection (D403), so cost 1 and budget 1024.
//   - BinanceCM: the CM-UM integration (rolled out 2026-06-24 to 06-30) kept dapi and
//     dstream.binance.com, and the CM wallet stays separate, so CM stays its own venue (D301).
//     dstream has no path split: one route, "default". Streams per connection: TODO(verify).
//   - Since that integration the all-market arrays (!bookTicker, !ticker@arr, !miniTicker@arr,
//     !forceOrder@arr, !contractInfo) carry UM and CM symbols on both fstream and dstream, tagged
//     st = 1 (UM) or 2 (CM). The decoder must drop the other venue's rows, or a CM symbol would be
//     stamped as a BinanceUM ticker. Some single-symbol streams can also be subscribed across hosts;
//     the platform does not use that (D301: UM and CM never share a connection).
//   - Also since then, not modelled in the adapter contract (D301): UM and CM share the IP weight
//     budget (2400/min) and the order-count limits (1200/min, 300/10s), share dualSidePosition
//     (setting it on either flips both), and CM follows UM's self-trade-prevention setting.
//   - BinanceEO: the options system was rebuilt in 2026; index, kline, mark price and open
//     interest streams are documented under fstream.binance.com/market. Depth, trades and limits:
//     TODO(verify).
//   - Books are SnapshotREST: buffer the diff stream, GET depth, drop updates with u < lastUpdateId,
//     then chain on U/u (spot) or pu (futures). A break means BookResync.
//   - Connections live at most 24h; the server pings and expects a pong.
//   - Private stream: listenKey over REST, extended periodically (PrivateStream.Refresh), or the
//     websocket API's logon with an ed25519 key. Pick one at implementation time. A BinancePM
//     stream carries margin, UM and CM events; each event's ExchKey names its venue.
//   - Futures have a per-symbol dead-man switch (countdownCancelAll): CancelOnDisconnect.
//   - userTrades is limited to a 7-day window per request: HistoryTraits.MaxWindow.
//
// Shape only: every method that would talk to Binance returns exchange.ErrNotImplemented.
package binance
