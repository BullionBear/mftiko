// Package sym is the symbol plane: the service that keeps the golden record of every venue's
// instruments and answers the sym subject (D11, D31). It follows mftik's symbol plane.
//
// It is a plain service, not an agent: no lease, no fencing. Every region runs it as a NATS queue
// group; any replica answers. Its lifetime is not tied to any session, because "what is the tick
// size" must have an answer whether or not anything is trading.
//
// Refresh:
//
//   - One source per (venue, category), from the venue's adapter (exchange PublicREST.Listing). That
//     is the unit a refresh may delist within: a spot refresh never deactivates perp rows.
//   - Each source is refreshed on its own; one venue being down does not stop the others. Failures
//     are reported, not raised.
//   - Every listed instrument is upserted. Rows of that (venue, category) that are missing from the
//     response become DELISTED. EXPIRED and DELISTED move to the terminal table and are not refreshed
//     again (D38). The UniversalTicker string stays the same, so orders and sessions do not re-key.
//   - Hourly by default, plus on request.
//
// Storage is PostgreSQL. The decided shape (D38) is two listing tables: an active table
// (PRELISTING, TRADABLE, HALTED, SETTLING, UNKNOWN), which the Resolver and hot paths read, and a
// terminal table (EXPIRED, DELISTED) for historical lookup of old fills and positions and for
// reverse lookup by ticker. Terminal rows can be archived. The status enum is still provisional.
// migrations/0002_symbol is still one symbol_ticker plus symbol_filter; this service does not
// implement the split yet. Only this service writes them.
//
// It is also the only place that uppercases asset codes and applies the alias table (XBT to BTC,
// D38): normalize first, then judge quanto, then derive the ticker. Adapters pass native codes.
// Strikes go through spell_strike before the ticker, so 6400.0 and 6400 are one ticker. USD, USDC
// and USDT stay distinct. A USD quote settled in USDC is quanto: it is not stored in phase 1, and
// the demand is REJECTED. Adapters fill contract_value_ccy, price_ccy and the PnL settlement
// asset from native venue fields; this service does not derive them.
//
// Reads (PROTOCOL §7.3): the Resolver and hot paths read the active table only. One instrument by
// exact UniversalTicker (a single row, never a venue table); a (venue, category) page of
// symbol.ListPage; the reverse lookup from a venue's spelling. Historical lookup (old fills and
// positions, reverse lookup of a terminal ticker) reads the terminal table and is not part of that
// cache. Clients cache through symbol.Resolver.
//
// Shape only: not implemented yet.
package sym
