package symbol

// Info is one instrument as SYM serves it: the golden record every plane reads.
type Info struct {
	Ticker       UniversalTicker
	ExchTicker   string
	Base         string
	Quote        string
	ContractSize string
	Settle       string
	ExpiryNs     int64
	Strike       string
	OptionType   string
	Active       bool
	Filters      map[string]string
	UpdatedAtNs  int64
}

// Exch is the venue-side key of the instrument.
func (i *Info) Exch() ExchKey { return ExchKey{Category: i.Ticker.Category, ExchTicker: i.ExchTicker} }

// ExchKey names an instrument the way a venue's messages do. The category is part of it because on
// a unified-account venue the spelling alone is ambiguous: Bybit's spot and linear BTCUSDT are
// both "BTCUSDT". Adapters emit it on the way out (fills, orders, positions); the caller turns it
// back into a UniversalTicker with Resolver.Reverse.
type ExchKey struct {
	Category   Category
	ExchTicker string
}
