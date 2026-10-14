// Package okx is the OKX adapter: one venue and one account kind, both named Okx (the CamelCase
// token of D201; two enums, D301), a unified account (D403). Spot, swap, futures and options share it; the instId tells them apart (BTC-USDT,
// BTC-USDT-SWAP).
//
// Public market data covers spot, linear swaps (USDT and USDC), linear futures and coin-margined
// perpetual swaps. Checked on production on 2026-10-10. Private REST and the private stream below
// were checked on demo trading the same day. Submit and Cancel go over REST (D303); NewTradeStream
// is exchange.ErrUnsupported. History is fills-history (E3-16). Options are not listed.
//
// Facts the implementation must honor. Checked 2026-10-10 on production for the public socket and
// the public REST calls, and the same day on demo trading for the private calls.
//
//   - Routes: "public" (wss://ws.okx.com:8443/ws/v5/public) and "business"
//     (wss://ws.okx.com:8443/ws/v5/business, candles). The topic decides the route (D403). Private
//     data is on /ws/v5/private, after login.
//   - No published cap on channels per connection: Budget is 0 and Cost is 1. What is limited: 480
//     subscribe, unsubscribe and login requests per connection per hour (SessionLimits.Rate), and
//     64 KB of channel names per request. The codec chunks at 32 KB of "<channel>:<instId>" so the
//     encoded args stay inside that 64 KB. Three new connections per second per IP.
//   - Keepalive: send the text "ping" when nothing arrived for under 30s; expect "pong". The public
//     and private clients both send "ping" every 20s.
//   - Books are SnapshotStream on books:<instId>. The first push is the snapshot (action snapshot,
//     prevSeqId -1). An update applies when its prevSeqId equals the seqId already held. seqId is
//     not dense. Since 2026-06-23 the checksum field is still present and always 0, so this codec
//     does not implement BookChecksum. A broken prevSeqId chain is BookResync.
//   - Login signs timestamp + "GET" + "/users/self/verify" with HMAC-SHA256; the key needs a
//     passphrase (Credentials.Passphrase).
//   - Dead-man switch: cancel-all-after, account-wide.
//   - History: GET /api/v5/trade/fills-history, newest first. tradeId is the fill id on both REST
//     and the orders channel (D601). GET /api/v5/trade/order looks one order up by ordId.
//   - Demo trading is AccountConfig.Simulated. It adds the header x-simulated-trading: 1 on REST
//     and, unless StreamEndpoint is set, dials wss://wspap.okx.com:8443/ws/v5/private. The REST
//     host stays https://www.okx.com. RESTEndpoint replaces that host and does not by itself turn
//     demo trading on.
//
// # Public market
//
// GET /api/v5/public/instruments?instType=SPOT|SWAP|FUTURES. The venue ignores limit and after, so
// one response is the whole instType and a full page is not a next page. Perp is the linear rows of
// SWAP (ctType linear, settle equal to the quote). Inverse is the inverse rows of that same SWAP
// page (settle equal to the base). Future is the linear rows of FUTURES. A coin-margined dated
// future is not listed: an Inverse ticker cannot carry an expiry, and the linear USD_UM future
// already uses Okx_Future_<pair>-<date>. Option listing is exchange.ErrUnsupported. A row whose
// settle is neither base nor quote is quanto and is left out (D203). A row that fails Validate,
// Ticker or the 15-digit check is left out.
//
// Spot base and quote are baseCcy and quoteCcy. A contract's pair is uly (else instFamily), split
// on the hyphen: BTC-USD_UM-261030 has uly BTC-USD. ContractSize is ctVal. contract_value_ccy is
// ctValCcy, which is the base on a linear contract and the quote on an inverse one. price_ccy is
// the quote. settle is settleCcy. ExpiryCode is the six-digit date at the end of a future's instId,
// and ExpiryNs is expTime in nanoseconds. Status: live is Tradable, suspend is Halted, preopen is
// Prelisting, expired is Expired, and anything else, including test, is Unknown.
//
// qty_step, min_qty and max_qty are in contract_value_ccy (D203). Spot uses lotSz, minSz and
// maxLmtSz as they are. A contract multiplies each by ctVal, so a swap with lotSz 0.01 and ctVal
// 0.01 has qty_step 0.0001. price_tick is tickSz. max_notional is maxLmtAmt when OKX sends one.
// Spot BTC-USDT publishes lotSz 1e-8 and maxLmtSz 9999999999, which is 18 significant digits.
// float64 holds 15 (D008), so max_qty is stored as the largest value that still fits. When the
// venue publishes no max and ten million fits at the step, max is left unset.
//
// GET /api/v5/market/candles. bar is the venue interval (1m, 1H, 1D, 1M for 1mo). The page is 300.
// The venue returns the newest candle first, and confirm "0" is the one still forming. Those are
// dropped; the rest come back oldest first. after, when endNs is set, asks for candles earlier than
// that millisecond. GET /api/v5/market/books is a snapshot for a caller that wants one without the
// stream. Open interest has no REST call: the open-interest channel pushes it (D401).
//
// The channel string is "<channel>:<instId>". A subscribe frame is
// {"id","op":"subscribe","args":[{"channel","instId"}]} and the ack echoes id, once per arg.
//
//	bbo-tbt         BestQuote. One bid and one ask.
//	trades          one Trade per row. side is the taker. tradeId is the id. sz on a derivative is
//	                the contract count. The frame has no ctVal, so this codec does not multiply by
//	                ContractSize; the conn worker does that when it resolves the instrument (D203).
//	                Spot sz is already base.
//	books           orderbook_delta. A level is [price, sz, deprecated, numOrders];
//	                sz "0" deletes the level. Derivative sz is the contract count, same as trades.
//	                books5 and bbo-tbt are not this book.
//	tickers         Ticker. Spot vol24h is base and volCcy24h is quote. A contract's vol24h is
//	                contracts and volCcy24h is the coin amount, which is platform qty on a linear
//	                contract, so Volume24h is volCcy24h and QuoteVolume24h is absent. ChangePct24h
//	                is never sent.
//	funding-rate    contracts only. fundingRate is the rate. fundingTime is NextFundingNs.
//	                IntervalS is nextFundingTime minus fundingTime, in seconds. OKX sends no interval
//	                field, and nextFundingRate is often empty.
//	open-interest   contracts only. oi is contracts and oiCcy is the coin amount, so OpenInterest is
//	                oiCcy (platform qty on a linear contract). OpenInterestValue is oiUsd. On an
//	                inverse contract oiCcy is still coin, not the quote amount.
//	mark-price      contracts only. markPx is the mark. The index is a different channel
//	                (index-tickers) with a different instId, so IndexPrice is absent.
//	candle<bar>     business route. The row is ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm.
//	                confirm "1" is closed. Spot volume is vol (base) and the quote volume is
//	                volCcyQuote. A derivative's vol is contracts, so Volume is volCcy, the coin amount.
//	liquidation-orders
//	                not an atom. The channel takes instType, not instId, so one feed covers every
//	                swap. The event has no instrument to route it by.
//
// A channel this codec does not read is ignored. "ping" and "pong" are ignored.
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
// instruments. Each order costs one count on the 60-per-2s order budget. The three carry the
// expTime header: the signing time plus AccountConfig.Validity (5 seconds by default), Unix ms.
// The venue refuses a request that arrives after it (D605). expTime is not part of the prehash.
//
// The create acknowledgement has an order id and no state, so sCode 0 is OrderStatusNew: the venue
// has the request and did not refuse it, and the order is not on the book yet.
// The private stream carries the state. inTime and outTime are on the envelope; ts on the row is
// milliseconds, and outTime is microseconds when it is longer than a millisecond stamp.
//
// # Order status
//
//	live                 OrderStatusAccepted
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
// Snapshot is four calls. Open orders are GET /api/v5/trade/orders-pending with limit 100, every
// instType in one list. Positions are GET /api/v5/account/positions, limit 100. A row with pos 0
// is left out. posSide short makes the size negative; long stays positive; net keeps the sign
// OKX sent. Balances are GET /api/v5/account/balance. Free is availBal and Locked is frozenBal. A
// coin with neither is left out. A full page of 100, with a new cursor, is the next page: after
// is the last ordId, or the last posId for positions. GET /api/v5/account/config gives posMode:
// long_short_mode is AccountSnapshot.HedgeMode, net_mode is not (D607).
//
// QueryOrder is GET /api/v5/trade/order by instId and clOrdId, or ordId when the ref has none.
// It finds live and ended orders. 51603 (Order does not exist) is exchange.ErrNotFound (D605).
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
//	             id (D601), the same field fills-history returns. execType T is taker and M is
//	             maker. fillFee is the fee and fillFeeCcy is the fee asset; OKX's sign is kept
//	             (a charge is negative). fillTime, else uTime.
//	positions    one Position per row with a non-zero pos, with the same sign rule as the REST
//	             list. avgPx is the entry, markPx the mark, upl the unrealized pnl, lever the
//	             leverage.
//	account      one Balance per details row, with the same availBal and frozenBal split as the
//	             REST balance. A zero row is left out.
//
// # History
//
// GET /api/v5/trade/fills-history for one instType and one instId. instType comes from the
// instrument: Spot is SPOT, Perp is SWAP, Future is FUTURES, Option is OPTION, and Inverse is
// SWAP unless the contract expires, in which case it is FUTURES. Rows arrive newest first
// (HistoryTraits.Descending). tradeId is the fill id, the same id the orders channel reports.
// A row with no tradeId, or a zero fillSz, is left out; the cursor still advances past it. A
// liquidation or ADL row keeps its tradeId, which OKX sends as a negative id, because the orders
// channel uses that same id. billId of the oldest row, when the page is full and that id differs
// from the cursor just sent, is FillPage.Next and goes back as after. There is no from-id. A page
// is at most 100. The venue keeps 3 months (HistoryTraits.Retention, taken as 90 days). One
// request has no published cap on the span of begin and end, so MaxWindow is 0. begin and end are
// millisecond timestamps and filter ts. fee and feeCcy are that fill's fee, the number the orders
// channel calls fillFee and fillFeeCcy; OKX's sign is kept. fillTime, else ts.
//
// GET /api/v5/trade/order looks up one order by instId and ordId. An empty list is an error, so a
// fill whose order is not visible yet is asked again on the next round (D601).
//
// Checked on demo trading on 2026-10-10: the tradeId on the orders channel is the tradeId in
// fills-history, and the fee, price and quantity match.
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
