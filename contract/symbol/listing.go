package symbol

// Filter names SYM stores. Not exhaustive: a venue may publish a restriction no other venue has,
// and it is stored under its own name.
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
	ExpiryNs   int64  // 0 = does not expire
	Strike     string // as the venue published it; "" unless an option
	OptionType string // C | P; "" unless an option
	// ContractSize is one contract's face value (D38). The asset is base on linear and
	// options, quote (USD per contract) on inverse. Empty means 1 of that asset.
	ContractSize string
	// Settle is the settlement asset (D38). Empty on spot. With Base and Quote it
	// picks the class: equal to Quote is linear or a USD-margined option, equal to
	// Base is inverse or a coin-margined option, anything else is quanto.
	Settle string
	// Active is tentative (D38): it becomes a status enum. The trading path accepts
	// only what will be TRADABLE. A native status that does not map is not tradable.
	Active bool
	// Filters: name -> bound. A present key with "" means the venue publishes the restriction but
	// sets no limit, which is not the same as leaving it out.
	Filters map[string]string
}

// Ticker derives the instrument's identity from the venue's own fields, never by splitting
// ExchTicker: Symbol is upper(Base)+upper(Quote), then -ExpiryCode for a dated contract, then
// -Strike-OptionType for an option, with the strike's decimal point spelled StrikeDecimal.
func (l *Listing) Ticker() (UniversalTicker, error) {
	return UniversalTicker{}, ErrNotImplemented
}
