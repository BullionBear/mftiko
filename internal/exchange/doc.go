// Package exchange defines the exchange adapters. Everything venue-specific sits behind these
// interfaces; nothing outside internal/exchange parses a venue's wire format or signs a venue request.
//
// # Exchange and venue
//
// An exchange package (binance, okx, bybit) is a unit of code. It declares one or more venues.
// A venue is one connection plus one credential (DECISIONS D26): Binance declares Binance, BinanceUM,
// BinanceCM, BinanceEO and BinancePM. The venue name is the <Venue> token of UniversalTicker (D11).
// Registry maps venue names to adapters; package all builds the registry of every exchange.
//
// # Who calls what
//
//	caller                       interface                         decisions
//	MD agent (planning, no I/O)  Market: Atom, Budget, Cost        D12, D26
//	MD conn worker               Market.Session, BookSync,         D21, D24
//	                             PublicREST.Depth (in process)
//	md.fetch                     PublicREST: Klines, OpenInterest  D21
//	SYM                          PublicREST.Instruments            D11
//	TD account worker            Account, PrivateStream            D25, D27
//	TD agent (backfill)          History                           D20
//
// Any of a venue's parts may be absent: BinancePM has no public market data, only an account.
//
// # Rules every adapter follows
//
//   - Decimals are strings exactly as the venue sent them. No float64 anywhere.
//   - Times are int64 unix nanoseconds, fields end in Ns.
//   - Symbols in and out are venue-native (Symbol). Translating to and from UniversalTicker is a
//     table lookup in the SymbolResolver (D11), done by the caller, never string surgery here.
//   - Fill.FillID is the venue's trade id and must be identical on the private stream and in
//     History (D20): the database deduplicates on (api_id, fill_id).
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
