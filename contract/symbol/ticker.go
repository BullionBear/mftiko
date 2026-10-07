package symbol

import "strings"

// Separator joins the three parts of a UniversalTicker, so none of them may contain it.
const Separator = "_"

// StrikeDecimal is how a fractional option strike writes its decimal point: 6.4 is "6D4". A '.' cannot
// appear in a ticker because it separates a feed key's topic from its ticker, and a ticker is one
// NATS subject token.
const StrikeDecimal = "D"

// Category is the market an instrument trades on within a venue. The spelling is load-bearing: it
// is rendered into the ticker as is.
type Category string

const (
	Spot    Category = "Spot"
	Perp    Category = "Perp"
	Inverse Category = "Inverse" // coin-margined, a product of its own: BinanceCM_Inverse_BTCUSD
	Future  Category = "Future"
	Option  Category = "Option"
)

// UniversalTicker is one instrument anywhere: <Venue>_<Category>_<SYMBOL>.
//
//	Binance_Spot_BTCUSDT
//	Bybit_Perp_BTCUSDT          Bybit_Spot_BTCUSDT is a different instrument
//	BinanceCM_Inverse_BTCUSD
//	BinanceUM_Future_BTCUSDT-260925
//	Okx_Option_BTCUSD-261225-100000-C
//
// Venue is CamelCase ASCII with no separator. SYMBOL is base+quote with no separator; a dated
// contract adds -YYMMDD, an option also -STRIKE-C|P. Comparable, so it keys maps directly.
type UniversalTicker struct {
	Venue    string
	Category Category
	Symbol   string
}

// String is the canonical rendering, the only form that goes on the wire or into a column.
func (t UniversalTicker) String() string {
	return strings.Join([]string{t.Venue, string(t.Category), t.Symbol}, Separator)
}

// Parse is strict and consults no registry: for anything already inside the platform (wire
// payloads, columns, feed keys). It rejects any spelling it would have had to change, because two
// spellings of one instrument is the bug this type exists to prevent.
func Parse(s string) (UniversalTicker, error) {
	return UniversalTicker{}, ErrNotImplemented
}

// Resolve is lenient, for user input only (a spec's YAML, an API query): it folds case and pair
// punctuation ("bybit_perp_btc/usdt"), and checks the venue and that the venue trades the category.
// Everything past that boundary passes the parsed value around.
func Resolve(s string, venues Venues) (UniversalTicker, error) {
	return UniversalTicker{}, ErrNotImplemented
}
