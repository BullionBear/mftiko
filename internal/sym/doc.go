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
//   - Every listed instrument is upserted; active rows of that (venue, category) missing from the
//     response are marked inactive. Rows are never deleted: orders and sessions keep referring to
//     delisted instruments.
//   - Hourly by default, plus on request.
//
// Storage is PostgreSQL, symbol_ticker and symbol_filter (migrations/0002_symbol). Only this
// service writes them.
//
// Reads (PROTOCOL §7.3): one instrument by exact UniversalTicker (a single row, never a venue
// table); a (venue, category) table in pages of symbol.ListPage; the reverse lookup from a
// venue's spelling. Clients cache through symbol.Resolver.
//
// Shape only: not implemented yet.
package sym
