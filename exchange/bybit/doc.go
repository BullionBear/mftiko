// Package bybit is the Bybit adapter: one venue and one account kind, both named Bybit (two enums,
// D301), a unified account (D403). The category
// (spot, linear, inverse, option) is part of every request and of every public websocket path.
//
// Public facts below were checked 2026-10-05 against bybit-exchange.github.io/docs/v5.
// Private REST and the private stream were checked on the testnet on 2026-10-10.
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
//     execution stream. The private stream below uses that id. Order lookup, window size and
//     retention are still exchange.ErrNotImplemented (E3-13).
//
// Private REST and the private stream were checked against the testnet on 2026-10-10
// (api-testnet.bybit.com, stream-testnet.bybit.com/v5/private). Public market data is still
// exchange.ErrNotImplemented. Phase 1 sends orders over REST; NewTradeStream is
// exchange.ErrUnsupported (D303).
//
// # Orders
//
// POST /v5/order/create, /v5/order/cancel and /v5/order/cancel-batch. Category comes from the
// instrument: spot, linear (Perp and Future), inverse, option. A market order is IOC and carries
// no price. Spot market orders set marketUnit to baseCoin or quoteCoin; a quote amount is
// formatted at 1e-8. orderLinkId is at most 36 characters. The create acknowledgement has an
// order id and no status, so a retCode 0 is OrderStatusPendingNew; the private stream carries
// the status. Cancel-batch is one category, at most 5 option orders, 10 spot or 20 linear or
// inverse. The account limit is 5, so one worker chunk fits every category. Each order costs
// one count on the 10/s order budget (spot's own create limit is 20/s).
//
// # Order status
//
//	Created, Untriggered          OrderStatusPendingNew
//	New, Triggered                OrderStatusNew
//	PartiallyFilled               OrderStatusPartiallyFilled
//	Filled                        OrderStatusFilled
//	PendingCancel                 OrderStatusPendingCancel
//	Cancelled,                    OrderStatusCancelled
//	  PartiallyFilledCanceled,
//	  Deactivated
//	Rejected                      OrderStatusRejected
//
// Anything else is OrderStatusUnknown. PartiallyFilledCanceled is terminal: the filled quantity
// stays on the order, and the fill itself arrives on the execution topic. rejectReason is kept
// only when the status is Rejected and the text is not EC_NoError.
//
// # Recon
//
// Snapshot is ten calls. Open orders are GET /v5/order/realtime with openOnly=0, limit 50:
// spot, linear settleCoin USDT, linear settleCoin USDC, inverse, option. Positions are
// GET /v5/position/list, limit 200, for linear USDT, linear USDC, inverse and option; a row
// with size 0 is left out. Buy is a positive size, Sell is negative. Balances are
// GET /v5/account/wallet-balance?accountType=UNIFIED. Free is walletBalance minus locked minus
// totalOrderIM, and locked plus totalOrderIM is Locked. Position margin stays in the wallet.
// A non-empty nextPageCursor, different from the cursor just sent, is the next page.
// Leverage is GET /v5/position/list for that symbol. Spot has none.
//
// # Private stream
//
// No Prepare and no Refresh. Dial is wss://stream.bybit.com/v5/private, or
// wss://stream-testnet.bybit.com/v5/private when the REST host is api-testnet.bybit.com.
// AccountConfig.StreamEndpoint replaces that URL. The first frame is
// {"op":"auth","args":[apiKey, expires, signature]} with expires 10s ahead and signature
// HMAC-SHA256 of "GET/realtime" + expires. The second subscribes to order, execution,
// position, wallet and dcp. Ready is the subscribe acknowledgement. A refused auth is ErrAuth.
// The client sends {"op":"ping"} every 20s. A topic this codec does not read is ignored.
//
//	order        one Order per data row. category and symbol are the ExchKey. A linear
//	             symbol containing '-' is a Future (BTCUSDT-16OCT26); other linear symbols
//	             are Perp. qty is the order quantity, unless marketUnit is quoteCoin, in
//	             which case that quantity is QuoteQty and Qty is the filled base quantity.
//	             cumExecQty is FilledQty, avgPrice is AvgPrice. updatedTime, else
//	             createdTime, else the frame's creationTime, in milliseconds.
//	execution    a Fill when execType is Trade, AdlTrade, BustTrade or BlockTrade, execId is
//	             set and execQty is not 0. execId is the fill id (D601). isMaker chooses
//	             maker or taker. feeCurrency is the fee asset. Funding, Settle and Delivery
//	             are not fills.
//	position     one Position per row with a non-zero size. The entry is avgPrice on the
//	             REST list and entryPrice on this topic; a zero avgPrice falls through,
//	             because a field the topic leaves out decodes as 0. markPrice,
//	             unrealisedPnl and leverage are optional.
//	wallet       one Balance per coin row, with the same free and locked split as the REST
//	             wallet. A zero wallet with nothing locked is left out.
//	dcp          ignored. The subscription is what lets Disconnected Cancel Protection fire.
//
// # Dead-man switch
//
// POST /v5/order/disconnected-cancel-all. syms is ignored. timeWindow is whole seconds from
// 3 to 300; the body does not set product, so the window is the venue's default (OPTIONS).
// The private stream's dcp subscription is required for the switch to run. Checked on the
// testnet on 2026-10-10: this UID is refused with 3500716 "DCP of this UID is inactive".
// That code is not in the 200–299 table, so Code stays 3500716.
//
// # Refusals
//
// PROTOCOL §7.2: 200–299 are the platform's names for a refusal, and Native keeps Bybit's
// code. Unlisted codes stay Bybit's own number, which is outside 200–299.
//
//	200  no such order            110001 110008 110010 170139 170142 170143 170213
//	201  insufficient balance     110004 110006 110007 110012 110044 110045
//	                              110051 110052 110053 170033 170131
//	202  quantity or notional     110017 110094 170124 170136 170140 170148
//	                              170341 170381 170382
//	203  price or precision       110003 110120 110121 30208 30209
//	                              170132 170133 170134 170137 170192 170193 170194
//	                              170371 170372
//	204  post-only or would take  170218
//	205  reduce-only or position  110023 110042
//	207  too many orders          110009 110020 170005 170810
//	208  client order id          110072 170141
//
// 10006, 10018, 170222, retCode 429 and HTTP 429 are RateLimited. HTTP 403 with no retCode is
// RateLimited (Bybit's "access too frequent"). 10003, 10004, 10005, 10007, 10010, 33004,
// -2015 and HTTP 401 are ErrAuth. 10000, a server timeout, is ErrUnknownOutcome: the order
// may have been accepted. Usage reads X-Bapi-Limit minus X-Bapi-Limit-Status into bucket
// "endpoint". That window belongs to the endpoint that answered, so the numbers are not one
// shared counter.
//
// # Signing
//
// HMAC-SHA256 hex over timestamp + api key + recv window (5000) + the query string (GET) or
// the raw JSON body (POST). Headers are X-BAPI-API-KEY, X-BAPI-TIMESTAMP, X-BAPI-RECV-WINDOW
// and X-BAPI-SIGN. REST defaults to https://api.bybit.com. AccountConfig.RESTEndpoint replaces
// it; a host of api-testnet.bybit.com selects the testnet private socket.
package bybit
