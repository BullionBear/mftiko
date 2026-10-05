// Package symbol is the platform's instrument identity and the client side of the symbol plane,
// copied from mftik (DECISIONS D11, D31).
//
// UniversalTicker is the placeholder for an instrument everywhere in the platform: session specs,
// lease items, md.f subjects, td.order, the database. A venue's own spelling (ExchTicker) appears in
// two places only: in the exchange adapters and in SYM's tables.
//
// Getting from one to the other is a lookup, never string surgery. "BTCUSDT" does not carry its
// base/quote split, and venues publish tickers that are not base + separator + quote at all. So:
//
//	UniversalTicker ──Resolver.Get──▶ Info (ExchTicker, filters, expiry) ──▶ exchange adapter
//	exchange adapter ──ExchKey{Venue, Category, ExchTicker}──Resolver.Reverse──▶ UniversalTicker
//
// Callers resolve before they call an adapter, and adapters never call SYM: adapter functions stay
// pure (D31). The market-data hot path does no lookup at all: a conn worker stamps each event with
// the UniversalTicker of the feed it serves (mdctl.AtomAssignment).
//
// The plane itself (refresh, tables, the sym subject) is internal/sym.
//
// Shape only: types and contracts; the functions return ErrNotImplemented.
package symbol
