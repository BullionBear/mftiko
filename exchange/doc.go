// Package exchange defines the exchange adapters. Everything venue-specific sits behind these
// interfaces; nothing outside package exchange parses a venue's wire format or signs a venue request.
//
// # Exchange, venue, account kind
//
// An exchange package (binance, okx, bybit) is a unit of code. It declares two independent enums
// (D301), whose names may coincide:
//
//   - Venues are markets, the <Venue> token of UniversalTicker (D201): one connection's worth of
//     public data and one set of instruments (D403). Binance declares Binance, BinanceUM, BinanceCM
//     and BinanceEO.
//   - Account kinds are what an api_id is: the credential it takes and the venues it can trade.
//     Binance declares Binance, BinanceUM, BinanceCM, BinanceEO and BinancePM. The account kind
//     BinancePM trades Binance_Spot_*, BinanceUM_* and BinanceCM_* tickers and routes each request
//     by the ticker's venue; there is no BinancePM venue and no BinancePM_* ticker.
//
// Registry holds both; package all builds the registry of every exchange.
//
// # Who calls what
//
//	caller                       interface                           decisions
//	MD agent (planning, no I/O)  Market: Atom, Budget, Cost          D401, D403, D202
//	MD conn worker               Market.NewSession, BookSync,        D402, D803
//	                             PublicREST.Depth (in process)
//	md.fetch                     PublicREST: Klines, OpenInterest    D402
//	SYM                          PublicREST.Listing (ListingOver)    D201
//	in-process Resolver (dev)    InProcessUIDSource.Listing          E2-05
//	TD account worker            AccountKind.Account, PrivateStream  D602, D603, D301
//	backfill read                AccountKind.History                 D601, D301
//
// Any of a venue's parts may be absent.
//
// # Sans-I/O (D302)
//
// An adapter is a codec. It turns intents into bytes (a subscribe frame, a signed HTTPRequest) and
// bytes into events (Session.Decode, PrivateStream.Decode, Call.Parse), and does nothing else: it
// does not dial, read a clock, sleep, start a goroutine or retry. The worker that owns the connection
// sends what the adapter built and hands back what came in; a REST Call runs with Run or Collect over a
// Doer. The time a request is signed at comes in as now.
//
// So the transport stays out of this package: websockets are github.com/coder/websocket and REST is
// github.com/valyala/fasthttp in the workers and in exchange/transport, and neither type appears in an
// interface here. JSON is github.com/bytedance/sonic, in the two frozen configurations every adapter
// shares; package exchange/internal/codec holds them, with signing and query encoding.
//
// Buffers are the worker's and are lent (D302). A frame passed to Decode, and a response passed to
// Parse, are valid until the call returns; anything decoded from them may point into them (sonic does
// not copy strings), so what a Sink or a caller keeps it copies. A REST Parse decodes with copied
// strings, because the transport reuses the response.
//
// Orders over a websocket (D303) go through a TradeStream, the same kind of codec: the worker owns
// the request ids and the deadlines (Inflight), and the codec writes into the venue's request id what
// it needs to read the answer, so Decode keeps no memory of what was sent.
//
// Errors: Parse classifies the venue's answer as a *Reject, a *RateLimited or ErrAuth; the transport
// classifies what has no answer, ErrUnknownOutcome when the request may have gone out. Rate limits:
// each HTTPRequest names its Cost, Usage reads what the venue reports, and the worker paces.
//
// # Rules every adapter follows
//
//   - Numbers are float64 (DECISIONS D008), read with decimal.Parse and written to a venue with
//     decimal.Append at the instrument's step: plain notation, the step's decimals, never an exponent. An
//     optional number the venue does not send is decimal.Absent. Nothing compares numbers with ==.
//   - A request body that is signed is marshalled once; the signature covers those bytes, and those bytes
//     are what is sent.
//   - Times are int64 unix nanoseconds, fields end in Ns. The adapter fills only the venue's own
//     time (ExchTsNs); the worker stamps when it received a frame.
//   - The enums (Side, OrderType, TimeInForce, OrderStatus) have the numbers of their common.v1 and
//     td.v1 namesakes, so the worker converts them by a cast (enums_test.go holds them together).
//   - Instruments go in resolved and come out venue-named (D202, D203). Specs write a UniversalTicker;
//     the caller resolves that to *symbol.Info at load (symbol.Resolver.Get) and hands the adapter
//     the Info, so adapters never call SYM. Inside the system the instrument is Info.UID. What the
//     venue sends back carries a symbol.ExchKey, which the caller turns into that UID with
//     symbol.Resolver.Reverse.
//   - Fill.FillID is the venue's trade id and must be identical on the private stream and in
//     History (D601): the database deduplicates on (api_id, fill_id).
//   - Event pointers passed to a Sink are valid only for the duration of the call.
//   - Credentials never reach a log line (see Credentials).
//   - An adapter does not retry, back off or reconnect on its own. The worker that owns the
//     connection decides; the adapter reports what happened.
//
// # Why the adapter has its own types
//
// The adapter does not decode into the generated protobuf (gen/mftiko/md/v1, td/v1). A book update
// carries venue sequence fields (Binance U/u/pu, OKX seqId/prevSeqId, checksum) that the wire model
// does not, and adapters stay testable without `make proto`. The conn worker and the account worker
// map these types to md.v1 and td.v1; the cost is one struct copy per event, small next to the JSON
// decode.
//
// Shape only: the interfaces are the contract; the exchange packages return ErrNotImplemented.
package exchange
