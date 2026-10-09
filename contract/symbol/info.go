package symbol

// MinUID and MaxUID are the inclusive bounds of a symbol UID (D203, PROTOCOL §10 P13).
// Zero is invalid. The proto field is uint32, so the upper bound is 2^32-1.
// Postgres stores the column as bigint with CHECK (uid BETWEEN 1 AND 4294967295).
const (
	MinUID uint32 = 1
	MaxUID uint32 = 4294967295
)

// Info is one instrument as SYM serves it: the golden record every plane reads.
type Info struct {
	// UID is the primary key of both SYM tables, unique across them (D203). SYM allocates
	// a random integer, never reuses it, and stores it on the row, so a restart or reload
	// does not change it. A delist followed by a relist gets a new one. The contract type
	// is uint32, range MinUID..MaxUID (1..4294967295). Zero is invalid (PROTOCOL §10 P13).
	// Postgres stores the column as bigint with CHECK (uid BETWEEN 1 AND 4294967295).
	UID        uint32
	Ticker     UniversalTicker
	ExchTicker string
	Base       string
	Quote      string
	// ContractSize, ContractValueCcy, PriceCcy and Settle use the D203 units.
	// See Listing. Settle is the PnL settlement asset, not the margin asset.
	// Quanto is not stored in phase 1.
	ContractSize     string
	ContractValueCcy string
	PriceCcy         string
	Settle           string
	ExpiryNs         int64
	Strike           string // SYM applies spell_strike before the ticker (D203)
	OptionType       string
	// Active is tentative (D203): only TRADABLE trades. The enum is PRELISTING,
	// TRADABLE, HALTED, SETTLING, EXPIRED, DELISTED, UNKNOWN. Non-terminal statuses
	// stay on the active table the Resolver reads; EXPIRED and DELISTED move to the
	// terminal table without changing the ticker or the UID. This stays bool until
	// that enum is final. The store tables are already split (migrations/0002_symbol).
	Active      bool
	Filters     map[string]string
	UpdatedAtNs int64
}

// ValidUID reports whether uid may identify an instrument.
func ValidUID(uid uint32) bool {
	return uid >= MinUID && uid <= MaxUID
}

// Linear, Inverse and Quanto group an instrument for display from the three
// currency fields (D203). They do not read Category. See classify.
func (i Info) Linear() bool  { return i.margin().linear }
func (i Info) Inverse() bool { return i.margin().inverse }
func (i Info) Quanto() bool  { return i.margin().quanto }

// CoinMargined is 幣本位: settlement equals base (D203). Display and grouping
// only. Spot, which has no settlement, is false.
func (i Info) CoinMargined() bool { return i.margin().coin }

// UMargined is U 本位: a stored instrument whose settlement is not base (D203).
// Quanto is not stored and is not U-margined. Spot is false.
func (i Info) UMargined() bool { return i.margin().u }

func (i Info) margin() margin {
	return classify(i.Base, i.Quote, i.ContractValueCcy, i.PriceCcy, i.Settle)
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
