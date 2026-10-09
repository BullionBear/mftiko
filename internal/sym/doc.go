// Package sym is the symbol plane: the service that keeps the golden record of every venue's
// instruments and answers the sym subject (D201, D202). It follows mftik's symbol plane.
//
// It is a plain service, not an agent: no lease, no fencing. Every region runs it as a NATS queue
// group; any replica answers. Its lifetime is not tied to any session, because "what is the tick
// size" must have an answer whether or not anything is trading.
//
// Refresh (Store.Refresh, Runner):
//
//   - One feed per (venue, category), from the venue's adapter (exchange PublicREST.Listing, run
//     over an HTTP transport by exchange.ListingOver). That is the unit a refresh may delist
//     within: a spot refresh never deactivates perp rows.
//   - Each feed is refreshed on its own. Runner waits jitter before the first pass and then
//     DefaultInterval plus a fresh jitter draw. Trigger runs one venue, or every feed when the
//     venue is empty. One feed's error does not stop the others. Failures are reported, not raised.
//   - A listing read that fails, including a page that fails halfway, is discarded before the
//     transaction starts. No row is delisted and symbol_revision does not change.
//   - Listings that fail Listing.Validate, or whose ticker or decimals cannot be derived, are not
//     written. They are logged and counted. When the payload can be tied to an active row of this
//     same unit, that row is left as it was: a bad payload is not absence.
//   - The writes are one transaction: upsert every listing that passed the gate, move active rows
//     this feed did not return, and increment symbol_revision by one when any of that changed.
//     An unchanged feed does not advance the revision. Insert and Move, the single-row primitives,
//     each bump on their own; Refresh does not use those bumps.
//   - A missing active row becomes DELISTED. A Future or Option whose expiry is already past becomes
//     EXPIRED, including one the venue still returns. EXPIRED and DELISTED move to the terminal
//     table with their filters and are not updated again (D203). The UniversalTicker string and the
//     UID stay the same. The same ticker listed again after delisting gets a new UID. A contract
//     that is still expired is not inserted again.
//   - The listing's status is stored as given. PRELISTING, TRADABLE, HALTED, SETTLING and UNKNOWN
//     stay on the active table. EXPIRED and DELISTED move. Changing from one non-terminal status
//     to another is a content change. Unspecified is invalid and is not written.
//   - After that transaction commits, and only when the revision advanced, SYM publishes
//     Changed{venue, category, revision} on sym.changed.{venue}.{category} (PROTOCOL §7.3).
//     Venue and category are the human-readable names. PROTOCOL §10 P14 may rename the subject.
//     A failed publish is logged and the commit stands. A later revision check recovers a missed one.
//
// Storage is PostgreSQL (migrations/0002_symbol). symbol_active holds PRELISTING, TRADABLE,
// HALTED, SETTLING and UNKNOWN, which the Resolver and hot paths read. symbol_terminal holds
// EXPIRED and DELISTED, for historical lookup of old fills and positions and for reverse lookup
// by UID. UID is the primary key of both. Terminal rows can be archived. The status enum is
// still provisional; the contract uses it, and Unspecified is not stored.
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
// Service answers the sym subject over natsx: Get, GetByUid, List, Reverse,
// Refresh and Revisions. A success is the Reply protobuf. invalid, not_found and unavailable
// are a natsx error reply (option A): the text is that word, then ": " and a
// reason that names the field, the value, and why. include_inactive reads the
// terminal table as well as the active one; the default is the active table
// only, including Reverse. When both tables match, the active row wins. Several
// terminal rows and no active row yield the greatest updated_at_ns, then the
// greatest uid. Reverse of a terminal row, with include_inactive set, returns
// the uid stored on that row. List pages with offset and limit. A limit of 0
// means symbol.ListPage, and a limit above that is invalid. A reply that
// reaches 1 MiB is unavailable rather than a shorter page.
// Only this service writes the tables.
//
// It is also the only place that uppercases asset codes and applies the alias table (XBT to BTC,
// D203): normalize first, then Validate (quanto), then derive the ticker. Adapters pass native
// codes. The exchange spelling is stored unchanged, so Bitmex XBTUSDT is Bitmex_Spot_BTCUSDT
// with exch_ticker XBTUSDT.
// Strikes go through spell_strike before the ticker, so 6400.0 and 6400 are one ticker. USD, USDC
// and USDT stay distinct. A USD quote settled in USDC is quanto: it is not stored in phase 1, and
// the demand is REJECTED. Adapters fill contract_value_ccy, price_ccy and the PnL settlement
// asset from native venue fields. Spot may leave the two currency columns empty and still pass
// Listing.Validate; those columns are NOT NULL, so an empty spot value is stored as base and an
// empty spot price as quote, which is the spot row of the D203 table. A contract that omits them
// fails Validate and is not stored. Settle stays NULL on spot.
//
// Reads (PROTOCOL §7.3): the Resolver and hot paths read the active table only. One instrument by
// exact UniversalTicker at spec load (a single row, never a venue table), then by UID; a
// (venue, category) page of symbol.ListPage; the reverse lookup from a venue's spelling, which
// yields the UID. Historical lookup (old fills and positions, and a reverse with
// include_inactive) reads the terminal table and is not part of that cache.
// Clients cache through symbol.Resolver.
//
// In-process Resolver (E2-05), for tests and local development only:
//
// Resolver implements symbol.Resolver without the SYM service and without PostgreSQL.
// At startup it calls Listing for each configured (venue, category).
// It canonicalizes asset codes with normalizeAssets, the same helper SYM refresh
// uses, then keeps rows that pass Listing.Validate and whose status is on the
// active table: PRELISTING, TRADABLE, HALTED, SETTLING, UNKNOWN. EXPIRED and
// DELISTED are not cached. UNSPECIFIED is invalid. Only TRADABLE may trade.
// A row that passes Validate but fails Ticker is skipped: the failure is logged
// and counted, and the rest of that venue still loads. The indexes are
// UID to Info, ticker to UID, and ExchKey to UID. A reload builds the next snapshot
// completely and publishes it once, so a reader sees the previous snapshot or the next
// one, never a mix of the two. A ticker that is still listed keeps its UID. A ticker
// that drops out is retired, and that UID is not issued again; the same ticker listed
// again gets a new one.
//
// This process allocates those UIDs. Tests pass a Source with a fixed sequence; otherwise
// the draw is RandomSource. Either way the value is not inserted into symbol_uid and must
// not be sent to another process or plane. Real venues wait for the UIDs SYM assigns.
// NewResolver rejects a listing that does not implement exchange.InProcessUIDSource.
// exchangetest.Source is the implementation that does. OpenRemote is the NATS Resolver
// (E2-09): it loads only the active table through SYM List, keeps these same indexes,
// and asks SYM for include_inactive without caching that answer. cmd/mftiko-sym publishes
// with NATSChanged. Only this in-process Resolver allocates UIDs.
//
// This process keeps no terminal table, so includeInactive does not widen a lookup.
// A row whose status is EXPIRED or DELISTED is omitted even when the listing still
// returns it. Only TRADABLE may trade.
//
// This process does not answer the sym subject. Service does, from PostgreSQL.
package sym
