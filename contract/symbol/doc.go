// Package symbol is the platform's instrument identity and the client side of the symbol plane,
// copied from mftik (DECISIONS D201, D202).
//
// UniversalTicker is the human-facing name, used only when writing configs and specs (D203).
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
//	spec ticker ──Resolver.Get──▶ Info.UID          (config/spec load boundary, D203)
//	UID ──Resolver.GetByUID──▶ Info                 (every plane, after that)
//	exchange adapter ──ExchKey──Resolver.Reverse──▶ Info.UID
//
// Callers resolve before they call an adapter, and adapters never call SYM: adapter functions stay
// pure (D202). The market-data hot path does no lookup at all: a conn worker stamps each event with
// the instrument of the feed it serves (mdctl.AtomAssignment). That stamp is still the ticker.
// Subjects stay human-readable; their exact spelling is PROTOCOL §10 P14.
//
// The plane itself (refresh, tables, the sym subject) is internal/sym.
//
// CanonicalAsset and AssetAliases live here so every adapter shares one table (D203).
// SYM applies them before it judges quanto and derives the ticker. Adapters pass native
// codes and do not keep a table of their own. A strike is a float64 (D008), spelled with
// StrikeSpelling, so 6400.0 and 6400 are one ticker.
// USD, USDC and USDT are three assets. Quanto (a settlement asset that is neither base nor quote)
// is not stored in phase 1, and the demand is REJECTED. qty is in contract_value_ccy (contracts
// times contract_size): base on linear and options, quote on inverse, not a contract count.
//
// Listings are stored in two tables, split by whether the status can still change (D203): the
// active table (PRELISTING, TRADABLE, HALTED, SETTLING, UNKNOWN), which is all the Resolver and the
// hot paths read, and the terminal table (EXPIRED, DELISTED), kept for historical lookup and never
// refreshed. A listing moves when it becomes terminal; the UniversalTicker string and the UID stay
// the same. The same ticker listed again after delisting gets a new UID. UID is the primary key of
// both tables and is unique across them. SYM allocates it at random, never reuses it, and
// persists it on the row, so a restart or reload keeps it; a collision is retried. It is not a
// monotonic sequence. symbol_filter splits the same way and moves with the row. Selectors have
// no UID; Universe.current is a selector and is still TBD, outside D203. The enum is still
// provisional. The contract type is uint32, range MinUID..MaxUID (1..4294967295,
// PROTOCOL §10 P13). Zero is invalid. Postgres stores the column as bigint with
// CHECK (uid BETWEEN 1 AND 4294967295). ValidUID applies that range. The tables
// and the allocator are internal/sym; this package does not allocate UIDs.
// Status is the provisional enum (D203): Unspecified is invalid, and only
// Tradable may trade. The store tables are already split on that enum.
package symbol
