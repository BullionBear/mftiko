package symbol

// Info is one instrument as SYM serves it: the golden record every plane reads.
type Info struct {
	// UID is the primary key of both SYM tables, unique across them (D203). SYM allocates
	// a random integer, never reuses it, and stores it on the row, so a restart or reload
	// does not change it. A delist followed by a relist gets a new one. Zero is never
	// assigned. The contract type is uint64. Width and range are still open
	// (PROTOCOL §10 P13); the suggested range is 1..2^63-1.
	UID        uint64
	Ticker     UniversalTicker
	ExchTicker string
	Base       string
	Quote      string
	// ContractSize and Settle use the D203 units. See Listing. Settle is the PnL
	// settlement asset, not the margin asset. Quanto is not stored in phase 1.
	ContractSize string
	Settle       string
	ExpiryNs     int64
	Strike       string // SYM applies spell_strike before the ticker (D203)
	OptionType   string
	// Active is tentative (D203): only TRADABLE trades. The enum is PRELISTING,
	// TRADABLE, HALTED, SETTLING, EXPIRED, DELISTED, UNKNOWN. Non-terminal statuses
	// stay on the active table the Resolver reads; EXPIRED and DELISTED move to the
	// terminal table without changing the ticker or the UID. This stays bool, and the store stays
	// one table, until that design is implemented.
	Active      bool
	Filters     map[string]string
	UpdatedAtNs int64
}

// Exch is the venue-side key of the instrument.
func (i *Info) Exch() ExchKey {
	return ExchKey{Venue: i.Ticker.Venue, Category: i.Ticker.Category, ExchTicker: i.ExchTicker}
}

// ExchKey names an instrument the way a venue's messages do. Adapters emit it on the way out
// (fills, orders, positions); the caller turns it into a UID with Resolver.Reverse (D203).
//
//   - Venue is part of it because one account kind can trade several venues: a BinancePM private
//     stream carries UM, CM and spot-margin events (D301).
//   - Category is part of it because on a unified-account venue the spelling alone is ambiguous:
//     Bybit's spot and linear books both list "BTCUSDT".
type ExchKey struct {
	Venue      string
	Category   Category
	ExchTicker string
}
