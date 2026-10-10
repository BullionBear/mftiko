// Package binance is the Binance adapter. It declares two enums (D301):
//
//   - Venues (markets): Binance (spot), BinanceUM, BinanceCM, BinanceEO. One market codec serves all
//     four (same combined-stream JSON).
//   - Account kinds: Binance, BinanceUM, BinanceCM, BinanceEO, each trading the venue of the same
//     name, and BinancePM, which trades Binance, BinanceUM and BinanceCM through papi and is no
//     venue: there is no BinancePM_* ticker.
//
// Facts the implementation must honor. Checked 2026-10-05 against developers.binance.com, and the
// UM public wire again on 2026-10-10 against fapi.binance.com and fstream.binance.com.
//
//   - BinanceUM: since the 2026-03-06 notice the websocket is split by path on fstream.binance.com:
//     /public/ws (bookTicker, depth), /market/ws (aggTrade, markPrice, kline, tickers, forceOrder),
//     /private (user data). The legacy URLs ended 2026-04-23. These are the routes "public" and
//     "market"; the private path belongs to the account stream. Both sockets take a JSON
//     SUBSCRIBE and answer with raw events, not the combined-stream wrapper. The wrapper is
//     still accepted. A client may send 10 messages a second. The server pings every 3 minutes
//     and drops a connection that sends no pong for 10. btcusdt@trade was acked and then silent
//     for 20s, so aggTrade is the trade feed.
//   - BinanceUM, Binance spot: 1024 streams per connection (D403), so cost 1 and budget 1024.
//   - BinanceCM: the CM-UM integration (rolled out 2026-06-24 to 06-30) kept dapi and
//     dstream.binance.com, and the CM wallet stays separate, so CM stays its own venue (D301).
//     dstream has no path split: one route, "default". Streams per connection: TODO(verify).
//   - Since that integration, st = 1 is UM and st = 2 is CM, on single-symbol events as well as
//     the all-market arrays (!miniTicker@arr, !ticker@arr, !markPrice@arr). !bookTicker on
//     /public/ws is one bookTicker per message, not an array. A row whose st is present and not 1
//     is dropped, or a CM symbol would be stamped as a BinanceUM ticker. Some single-symbol
//     streams can also be subscribed across hosts; the platform does not use that (D301: UM and
//     CM never share a connection).
//   - Also since then, not modelled in the adapter contract (D301): UM and CM share the IP weight
//     budget (2400/min) and the order-count limits (1200/min, 300/10s), share dualSidePosition
//     (setting it on either flips both), and CM follows UM's self-trade-prevention setting.
//   - BinanceEO: the options system was rebuilt in 2026; index, kline, mark price and open
//     interest streams are documented under fstream.binance.com/market. Depth, trades and limits:
//     TODO(verify).
//   - Books are SnapshotREST: buffer the diff stream, GET depth, drop updates with u < lastUpdateId,
//     then chain on U/u (spot) or pu (futures). A break means BookResync.
//   - Connections live at most 24h; the server pings and expects a pong.
//   - Private stream: UM orders go over the WebSocket API (ws-fapi, session.logon, ed25519 only).
//     That socket does not carry private events (D303), so the account opens a second connection.
//     POST /fapi/v1/listenKey, then wss://fstream.binance.com/private/ws?listenKey=&events=
//     (testnet: wss://stream.binancefuture.com/private/ws). PUT the listenKey every 30 minutes;
//     it dies at 60. The 2026-03 path split retired /ws/<listenKey>. A BinancePM stream carries
//     margin, UM and CM events; each event's ExchKey names its venue. PM is not implemented here.
//   - Futures have a per-symbol dead-man switch (countdownCancelAll): CancelOnDisconnect.
//   - userTrades (GET /fapi/v1/userTrades) is the fill history. One request spans at most 7
//     days (HistoryTraits.MaxWindow). fromId cannot be combined with startTime or endTime.
//     The venue keeps three months (HistoryTraits.Retention). The trade id is the same
//     value ORDER_TRADE_UPDATE reports as t (D601).
//
// UM private wire, checked against the USD-M testnet on 2026-10-10. UM public wire, checked the
// same day against production fapi and fstream. Other products still return exchange.ErrNotImplemented.
//
// # Public fields
//
// Venue JSON stays in this package. Wire times are milliseconds; events store nanoseconds.
//
// Listing is GET /fapi/v1/exchangeInfo, weight 1, one page. exchangeInfo has no contractSize:
// the order quantity is already the base amount, so contract_size is 1.
//
//	symbol            exch_ticker
//	baseAsset         base, and contract_value_ccy (qty is base)
//	quoteAsset        quote, and price_ccy
//	marginAsset       settle. Realized PnL is credited here. Neither base nor quote means quanto,
//	                  and the row is not stored (D203)
//	contractType      PERPETUAL and TRADIFI_PERPETUAL are Perp. CURRENT_QUARTER and NEXT_QUARTER
//	                  are Future. Any other type is left out
//	symbol suffix     a future's expiry code: the six digits after '_', not computed from deliveryDate
//	deliveryDate      ExpiryNs of a future. A perp does not expire, so ExpiryNs stays 0
//	status            PENDING_TRADING PRELISTING, TRADING TRADABLE, PRE_DELIVERING DELIVERING
//	                  PRE_SETTLE SETTLING all SETTLING, DELIVERED EXPIRED, CLOSE DELISTED.
//	                  Anything else is UNKNOWN
//	PRICE_FILTER      price_tick, min_price, max_price
//	LOT_SIZE          qty_step, min_qty, max_qty
//	MIN_NOTIONAL      min_notional
//
// The channel is the subscribe parameter. Raw events name no speed, so depth is reported as
// @depth@100ms and mark price as @markPrice@1s, which is what Atom subscribes. A wrapped frame
// keeps the stream name it arrived with.
//
//	bookTicker                 bestquote. Bid b, bid qty B, ask a, ask qty A, time T else E
//	depth / depth@100ms        orderbook and orderbook_delta. U first, u last, pu the previous u.
//	                           REST lastUpdateId is the snapshot. Updates with u below it are
//	                           already in the snapshot; the first one that still overlaps it
//	                           bridges; after that pu must equal the previous u
//	aggTrade                   aggtrade. Price p, qty q, id a, first f, last l. m means the buyer
//	                           is the maker, so the taker side is the other one
//	kline_<interval>           kline. Interval i, with 1M written 1mo. Closed is x. Quote volume q.
//	                           REST klines are oldest first, at most 1500. The newest candle of a
//	                           latest request is still forming and is left out. endTime drops a
//	                           candle that closes after it. Weight is 1, 2, 5 or 10 as the limit
//	                           crosses 100, 500 and 1000
//	ticker                     24hrTicker. Last c, open o, high h, low l, base volume v, quote
//	                           volume q, change percent P as the venue sends it (0.253 is 0.253%)
//	24hrMiniTicker             the same ticker event, on <symbol>@miniTicker. It has no P
//	markPrice@1s               mark_price and funding_rate. Mark p, index i, funding r, next
//	                           funding T. The interval between fundings is not in the event
//	forceOrder                 liquidation. S is the forced order; the position being closed is
//	                           the other side. Price is ap, else p. Qty is q
//
// Open interest is not pushed (D401). GET /fapi/v1/openInterest, weight 1, has no quote value.
// Depth is GET /fapi/v1/depth. Limits 5, 10, 20 and 50 cost 2, 100 costs 5, 500 costs 10,
// 1000 costs 20. The book snapshot asks for 1000. Usage reads X-MBX-USED-WEIGHT-1M.
//
// # Order status
//
//	NEW                OrderStatusNew
//	PARTIALLY_FILLED   OrderStatusPartiallyFilled
//	FILLED             OrderStatusFilled
//	PENDING_CANCEL     OrderStatusPendingCancel
//	CANCELED           OrderStatusCancelled
//	REJECTED           OrderStatusRejected
//	EXPIRED            OrderStatusExpired
//	EXPIRED_IN_MATCH   OrderStatusExpired   (an IOC that did not fill)
//
// Anything else is OrderStatusUnknown. The synchronous answer (order.place result, REST order)
// and ORDER_TRADE_UPDATE.o.X use this table. ORDER_TRADE_UPDATE.o.x is the execution type: a
// fill is x=TRADE with a non-zero trade id t, which is the same id userTrades returns as id (D601).
//
// # ORDER_TRADE_UPDATE
//
//	o.s symbol, o.i order id, o.c client order id, o.S side, o.o type, o.X status
//	o.q orig qty, o.p price, o.z filled qty, o.ap avg price, o.T transaction time (ms)
//	o.l last qty, o.L last price, o.n fee, o.N fee asset, o.m maker, o.t trade id
//	o.ps position side: BOTH keeps the signed amount; SHORT is stored as a negative size
//
// # ACCOUNT_UPDATE
//
//	a.B[] balances: a asset, wb wallet, cw cross wallet.
//	  Free is cw. Locked is wb-cw when that is positive (isolated margin).
//	a.P[] positions: s symbol, pa amount, ep entry, up unrealized, ps position side.
//
// listenKeyExpired ends the stream (AccountSink.Expired). Other event names are ignored:
// a new event type must not drop the connection.
//
// # Refusals
//
// PROTOCOL §7.2: 200–299 are the platform's names for a refusal, and Native keeps Binance's
// code. -2010 is 201, which is what the conformance fake uses for insufficient balance.
// Unlisted codes stay Binance's own number, which is outside 200–299.
//
//	200  no such order            -2011 -2013
//	201  insufficient balance     -2010 -2018 -2019
//	202  quantity or notional     -1013 -1100 -4003 -4004 -4005 -4164
//	203  price or precision       -1111 -4001 -4002 -4014 -4131
//	204  post-only or would take  -2021 -5022
//	205  reduce-only or position  -2022 -2024 -4118
//	206  position side            -4061
//	207  too many orders          -1015 -2025 -2027
//	208  client order id          -4015
//
// A FOK that cannot fill immediately is refused with -5021 and is not stored as an
// order. An IOC in that position comes back status EXPIRED. -5021 is not in the table
// above, so Code stays -5021.
//
// -1003, HTTP 429 and 418 are RateLimited. -1022, -2014, -2015 and HTTP 401/403 are ErrAuth.
// A refused session.logon is ErrAuth from Decode; an order refusal is the answer's error.
//
// # Signing
//
// Ed25519 only, base64 over the query string that is sent (codec.Query, then signature
// appended). REST sends that query with X-MBX-APIKEY. The WebSocket API signs session.logon
// the same way, parameters sorted by name. After logon, order.place and order.cancel must
// not carry a signature: the testnet answers -1106 if they do. The request id is "o"+n for
// an order and "c"+n for a cancel, so Decode needs no memory of what was sent (D303).
// The codec's own logon id is "logon", which cannot collide with those.
//
// Hosts: REST defaults to https://fapi.binance.com, the trade socket to
// wss://ws-fapi.binance.com/ws-fapi/v1, the private socket to
// wss://fstream.binance.com/private/ws. AccountConfig.RESTEndpoint replaces the REST host.
// When that host is testnet.binancefuture.com, the sockets follow it:
// wss://testnet.binancefuture.com/ws-fapi/v1 and wss://stream.binancefuture.com/private/ws.
// AccountConfig.StreamEndpoint, when set, replaces the private base URL.
package binance
