// Package okx is the OKX adapter: one venue and one account kind, both named Okx (the CamelCase
// token of D201; two enums, D301), a unified account (D403). Spot, swap, futures and options share it; the instId tells them apart (BTC-USDT,
// BTC-USDT-SWAP).
//
// Public market data is still exchange.ErrNotImplemented (E3-14). Private REST and the private
// stream below were checked on demo trading on 2026-10-10. Submit and Cancel go over REST (D303);
// NewTradeStream is exchange.ErrUnsupported. History is still exchange.ErrNotImplemented (E3-13).
//
// Facts the implementation must honor. Checked 2026-10-05 against okx.com/docs-v5 for the public
// socket, and 2026-10-10 on demo trading for the private calls.
//
//   - Routes: "public" (/ws/v5/public) and "business" (/ws/v5/business, candles among others). The
//     topic decides the route (D403). Private data is on /ws/v5/private, after login.
//   - No published cap on channels per connection: Budget is 0. What is limited: 480
//     subscribe/unsubscribe/login requests per connection per hour (SessionLimits.Rate), 64 KB of
//     channels per request (PerRequestBytes), 3 new connections per second per IP.
//   - Keepalive: send the text "ping" when nothing arrived for under 30s; expect "pong". The
//     private client sends "ping" every 20s.
//   - Books are SnapshotStream: the first push after subscribe is the snapshot, then seqId/prevSeqId
//     chain, plus a crc32 checksum of the top levels (BookChecksum).
//   - Login signs timestamp + "GET" + "/users/self/verify" with HMAC-SHA256; the key needs a
//     passphrase (Credentials.Passphrase).
//   - Dead-man switch: cancel-all-after, account-wide.
//   - Demo trading is AccountConfig.Simulated. It adds the header x-simulated-trading: 1 on REST
//     and, unless StreamEndpoint is set, dials wss://wspap.okx.com:8443/ws/v5/private. The REST
//     host stays https://www.okx.com. RESTEndpoint replaces that host and does not by itself turn
//     demo trading on.
//
// # Orders
//
// POST /api/v5/trade/order, /api/v5/trade/cancel-order and /api/v5/trade/cancel-batch-orders.
// instId is the instrument's ExchTicker. tdMode is cash for spot and cross for a contract;
// Params["tdMode"] may set cash, cross or isolated, and Params["posSide"] may set net, long or
// short. A limit order's ordType is limit, post_only, ioc or fok from the time in force. A market
// order is IOC and carries no price. Spot market orders set tgtCcy to base_ccy or quote_ccy; a
// quote amount is formatted at 1e-8. A contract's sz is the contract count: platform qty divided
// by ContractSize (D203). Spot sz is the platform quantity. clOrdId is 1 to 32 letters or digits.
// reduceOnly is the string "true"; spot has none. Cancel-batch is at most 20 orders and may mix
// instruments. Each order costs one count on the 60-per-2s order budget.
//
// The create acknowledgement has an order id and no state, so sCode 0 is OrderStatusPendingNew.
// The private stream carries the state. inTime and outTime are on the envelope; ts on the row is
// milliseconds, and outTime is microseconds when it is longer than a millisecond stamp.
//
// # Order status
//
//	live                 OrderStatusNew
//	partially_filled     OrderStatusPartiallyFilled
//	filled               OrderStatusFilled
//	canceled,            OrderStatusCancelled
//	  cancelled,
//	  mmp_canceled
//
// Anything else is OrderStatusUnknown. The orders channel does not carry a rejected state: a
// refusal arrives as the REST sCode.
//
// On the way back, sz, accFillSz and fillSz are the venue's sz. Spot sz is base, or quote when
// tgtCcy is quote_ccy (then that amount is QuoteQty and Qty is the filled base). A contract's sz
// is the contract count. The frame does not carry ctVal, so this codec does not multiply by
// ContractSize; the account worker does that when it resolves the ExchKey (D203).
//
// # Recon
//
// Snapshot is three calls. Open orders are GET /api/v5/trade/orders-pending with limit 100, every
// instType in one list. Positions are GET /api/v5/account/positions, limit 100. A row with pos 0
// is left out. posSide short makes the size negative; long stays positive; net keeps the sign
// OKX sent. Balances are GET /api/v5/account/balance. Free is availBal and Locked is frozenBal. A
// coin with neither is left out. A full page of 100, with a new cursor, is the next page: after
// is the last ordId, or the last posId for positions.
//
// Leverage is GET /api/v5/account/leverage-info for that instId and mgnMode cross. Spot has none.
// A net row is the answer. Long and short rows that disagree are an error.
//
// # Private stream
//
// No Prepare and no Refresh. Dial is wss://ws.okx.com:8443/ws/v5/private, or
// wss://wspap.okx.com:8443/ws/v5/private when AccountConfig.Simulated is set.
// AccountConfig.StreamEndpoint replaces that URL. The worker sends the login frame and
// waits for its answer before the subscribe frame: a subscribe that arrives first is
// refused with 60011. The first frame is
// {"op":"login","args":[{"apiKey","passphrase","timestamp","sign"}]} with timestamp in unix
// seconds and sign the base64 HMAC-SHA256 of timestamp + "GET" + "/users/self/verify". The second
// subscribes to orders and positions with instType ANY, and to account. Ready is login code 0
// plus a subscribe acknowledgement for each of those three. A refused login, and error codes
// 60004, 60005, 60006, 60007, 60009 and 60011, are ErrAuth. The client sends the text "ping"
// every 20s and the venue answers "pong". A channel this codec does not read is ignored.
//
//	orders       one Order per data row. instType and instId are the ExchKey: SPOT and MARGIN are
//	             Spot, SWAP with a USD quote is Inverse and any other SWAP is Perp, FUTURES with a
//	             USD quote is Inverse and any other FUTURES is Future, OPTION is Option. The same
//	             split is read off the instId when instType is absent. An instId of five parts
//	             ending in C or P is an option. uTime, else cTime, in milliseconds. When the row
//	             carries a tradeId and a non-zero fillSz, it is also a Fill. tradeId is the fill
//	             id (D601), the same field fills-history will use. execType T is taker and M is
//	             maker. fillFee is the fee and fillFeeCcy is the fee asset; OKX's sign is kept
//	             (a charge is negative). fillTime, else uTime.
//	positions    one Position per row with a non-zero pos, with the same sign rule as the REST
//	             list. avgPx is the entry, markPx the mark, upl the unrealized pnl, lever the
//	             leverage.
//	account      one Balance per details row, with the same availBal and frozenBal split as the
//	             REST balance. A zero row is left out.
//
// # Dead-man switch
//
// POST /api/v5/trade/cancel-all-after. syms is ignored. timeOut is whole seconds: 0 turns the
// timer off, and 10 to 120 sets it. Checked on demo trading on 2026-10-10: 10 is accepted, 0
// clears triggerTime, and 2 and 9 are 51000.
//
// # Refusals
//
// PROTOCOL §7.2: 200–299 are the platform's names for a refusal, and Native keeps OKX's code.
// Unlisted codes stay OKX's own number, which is outside 200–299. sCode is the per-order code;
// the envelope code is the whole request.
//
//	200  no such order            51400 51401 51402
//	201  insufficient balance     51008
//	202  quantity or notional     51020 51121
//	203  price or precision       51122 51137 51138
//	205  reduce-only or position  51169
//	208  client order id          51016
//
// 51008, 51016, 51020, 51137 and 51400 were returned by demo trading on 2026-10-10. 51401,
// 51402, 51121, 51122, 51138 and 51169 are the same table's neighbours and were not provoked.
// 50011 and HTTP 429 are RateLimited. 50101, 50102, 50103, 50104, 50105, 50106, 50107, 50110,
// 50111, 50112, 50113, 50114 and HTTP 401 are ErrAuth. 50101, 50105 and 50113 were returned on
// demo trading. 50004 (endpoint timeout; the order may have been accepted) is ErrUnknownOutcome.
// OKX does not report a remaining budget in the response, so Usage is empty.
//
// # Signing
//
// REST signs timestamp + method + requestPath + body with HMAC-SHA256 and sends the signature in
// standard base64. timestamp is UTC to the millisecond, for example 2026-01-01T00:00:00.000Z.
// requestPath includes the query string. Headers are OK-ACCESS-KEY, OK-ACCESS-SIGN,
// OK-ACCESS-TIMESTAMP and OK-ACCESS-PASSPHRASE. Demo trading adds x-simulated-trading: 1.
// REST defaults to https://www.okx.com. AccountConfig.RESTEndpoint replaces it.
package okx
