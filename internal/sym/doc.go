// Package sym is the symbol plane: the service that keeps the golden record of every venue's
// instruments and answers the sym subject (D201, D202). It follows mftik's symbol plane.
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
//     again (D203). The UniversalTicker string and the UID stay the same, so historical rows do
//     not re-key. The same ticker listed again after delisting gets a new UID.
//   - Hourly by default, plus on request.
//
// Storage is PostgreSQL (migrations/0002_symbol). symbol_active holds PRELISTING, TRADABLE,
// HALTED, SETTLING and UNKNOWN, which the Resolver and hot paths read. symbol_terminal holds
// EXPIRED and DELISTED, for historical lookup of old fills and positions and for reverse lookup
// by UID. UID is the primary key of both. Terminal rows can be archived. The status enum is
// still provisional; the contract field stays bool.
//
// Insert allocates a UID by drawing from Source (RandomSource in production). It inserts that
// uid into symbol_uid first and retries when the insert reports a unique violation on the uid.
// symbol_uid is never deleted, so archiving a listing does not free the uid. The candidate is
// also stored on the listing row, so a restart does not change it. Move copies the row and its
// symbol_filter rows onto the terminal tables in one transaction; the UID and the UniversalTicker
// stay the same, and the active row is deleted in that transaction. symbol_reject_shared_uid
// still rejects a uid that is on the other listing table, which the registry alone does not.
// A later Insert of the same ticker draws a new UID. The old terminal row stays. List pages
// with offset and limit; inconsistency across pages is accepted. Selectors have no UID.
// Universe.current is a selector and is still TBD, outside D203.
// The UID is uint32, the same type as Info.UID. The interval is symbol.MinUID..symbol.MaxUID
// and 0 is invalid. symbol.ValidUID is that check. The BIGINT columns use the same bounds.
// The sym subject and GetByUID are not served yet.
// Only this service writes the tables.
//
// It is also the only place that uppercases asset codes and applies the alias table (XBT to BTC,
// D203): normalize first, then judge quanto, then derive the ticker. Adapters pass native codes.
// Strikes go through spell_strike before the ticker, so 6400.0 and 6400 are one ticker. USD, USDC
// and USDT stay distinct. A USD quote settled in USDC is quanto: it is not stored in phase 1, and
// the demand is REJECTED. Adapters fill contract_value_ccy, price_ccy and the PnL settlement
// asset from native venue fields; this service does not derive them.
//
// Reads (PROTOCOL §7.3): the Resolver and hot paths read the active table only. One instrument by
// exact UniversalTicker at spec load (a single row, never a venue table), then by UID; a
// (venue, category) page of symbol.ListPage; the reverse lookup from a venue's spelling, which
// yields the UID. Historical lookup (old fills and positions, reverse lookup of a terminal UID)
// reads the terminal table and is not part of that cache. Clients cache through symbol.Resolver.
//
// The sym subject is not served yet.
package sym
