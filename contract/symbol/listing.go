package symbol

// Filter names SYM stores. Not exhaustive: a venue may publish a restriction no other venue has,
// and it is stored under its own name.
//
// PriceTick is in price_ccy. QtyStep, MinQty and MaxQty are in contract_value_ccy, the same unit
// as platform qty (D203).
const (
	PriceTick   = "price_tick"
	QtyStep     = "qty_step"
	MinQty      = "min_qty"
	MaxQty      = "max_qty"
	MinNotional = "min_notional"
	MaxNotional = "max_notional"
	MinPrice    = "min_price"
	MaxPrice    = "max_price"
)

// Listing is one instrument from a venue's listing, canonicalized by the adapter
// (exchange.PublicREST.Listing). SYM turns it into a row and serves it as Info.
type Listing struct {
	Venue      string
	Category   Category
	Base       string
	Quote      string
	ExchTicker string // the venue's own spelling, what goes on the wire
	// ExpiryCode is YYMMDD taken from the venue's own dated ticker, not computed from ExpiryNs.
	ExpiryCode string
	ExpiryNs   int64 // 0 = does not expire
	// Strike is as the venue published it. SYM normalizes it with spell_strike before
	// the ticker is derived (D203): 6400.0 and 6400 are one ticker. "" unless an option.
	Strike     string
	OptionType string // C | P; "" unless an option
	// ContractSize is one contract's face value (D203). Platform qty is the contract
	// count times this value, in contract_value_ccy: base on linear and options, quote
	// on inverse. Inverse qty is that quote amount, not a contract count.
	ContractSize string
	// Settle is the PnL settlement asset (settlement_asset, D203), not the margin asset.
	// Empty on spot. Adapters fill it from the venue's native field; nothing downstream
	// derives it. Quanto (neither Base nor Quote) is not stored in phase 1.
	// Coin-margined versus U-margined is display only: Settle equal to Base means
	// coin-margined. The trading path does not use that label.
	Settle string
	// Active is tentative (D203). It becomes a status enum: PRELISTING, TRADABLE, HALTED,
	// SETTLING, EXPIRED, DELISTED, UNKNOWN. Only TRADABLE trades. A native status that
	// does not map is UNKNOWN and is not tradable. Adding an enum value later does not
	// make a non-tradable instrument tradable. PRELISTING, TRADABLE, HALTED, SETTLING and
	// UNKNOWN stay on the active table; EXPIRED and DELISTED move to the terminal table
	// and the UniversalTicker string does not change. The contract stays bool, and the
	// store stays one table, until that enum and the split are implemented.
	Active bool
	// Filters: name -> bound. A present key with "" means the venue publishes the restriction but
	// sets no limit, which is not the same as leaving it out.
	Filters map[string]string
}

// Ticker derives the instrument's identity from the venue's own fields, never by splitting
// ExchTicker: Symbol is upper(Base)+upper(Quote), then -ExpiryCode for a dated contract, then
// -Strike-OptionType for an option. SYM normalizes the strike with spell_strike before this
// spelling (D203): 6400.0 and 6400 are one ticker, and a fractional strike uses StrikeDecimal.
func (l *Listing) Ticker() (UniversalTicker, error) {
	return UniversalTicker{}, ErrNotImplemented
}
