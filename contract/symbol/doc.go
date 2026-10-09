// Package symbol is the platform's instrument identity and the client side of the symbol plane,
// copied from mftik (DECISIONS D11, D31).
//
// UniversalTicker is the human-facing name, used only when writing configs and specs (D38).
// Inside the system the instrument is a UID: STS, TD and MD pass that integer, and each plane
// loads the fields it needs with GetByUID. Lease items and td.order still carry the ticker
// string; switching those fields to UID is later work. NATS subjects stay human-readable, not
// UID; the exact spelling is not final (PROTOCOL §10 P14), so md.f is still
// md.f.{inst}.{topic}.{ticker}. A venue's own spelling (ExchTicker) appears in two places only: in the
// exchange adapters and in SYM's tables.
//
// Getting from one to the other is a lookup, never string surgery. "BTCUSDT" does not carry its
// base/quote split, and venues publish tickers that are not base + separator + quote at all. So:
//
//	spec ticker ──Resolver.Get──▶ Info.UID          (config/spec load boundary, D38)
//	UID ──Resolver.GetByUID──▶ Info                 (every plane, after that)
//	exchange adapter ──ExchKey──Resolver.Reverse──▶ Info.UID
//
// Callers resolve before they call an adapter, and adapters never call SYM: adapter functions stay
// pure (D31). The market-data hot path does no lookup at all: a conn worker stamps each event with
// the instrument of the feed it serves (mdctl.AtomAssignment). That stamp is still the ticker.
// Subjects stay human-readable; their exact spelling is PROTOCOL §10 P14.
//
// The plane itself (refresh, tables, the sym subject) is internal/sym.
//
// Asset codes are uppercased there, and the alias table (XBT to BTC) is applied only there,
// before quanto is judged and the ticker is derived (D38). Adapters pass native codes and do not
// rename. Strikes are normalized with spell_strike first, so 6400.0 and 6400 are one ticker.
// USD, USDC and USDT are three assets. Quanto (a settlement asset that is neither base nor quote)
// is not stored in phase 1, and the demand is REJECTED. qty is in contract_value_ccy (contracts
// times contract_size): base on linear and options, quote on inverse, not a contract count.
//
// Listings will be stored in two tables, split by whether the status can still change (D38): the
// active table (PRELISTING, TRADABLE, HALTED, SETTLING, UNKNOWN), which is all the Resolver and the
// hot paths read, and the terminal table (EXPIRED, DELISTED), kept for historical lookup and never
// refreshed. A listing moves when it becomes terminal; the UniversalTicker string and the UID stay
// the same. The same ticker listed again after delisting gets a new UID. UID is the primary key of
// both tables and is unique across them. The enum is still provisional. UID width is provisional
// (PROTOCOL §10 P13); this package does not implement the split or the allocator.
//
// Shape only: types and contracts; the functions return ErrNotImplemented.
package symbol
