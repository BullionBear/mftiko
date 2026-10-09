package symbol

import (
	"fmt"
	"strings"
)

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
// contract adds -YYMMDD, an option also -STRIKE-C|P. A symbol is not ASCII-only: a venue may
// list a CJK pair, and rejecting it would drop the whole book. Comparable, so it keys maps directly.
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
	venue, cat, symbol, err := splitTicker(s)
	if err != nil {
		return UniversalTicker{}, err
	}
	if !canonicalVenue(venue) {
		return UniversalTicker{}, fmt.Errorf("%w: venue %q", ErrInvalidTicker, venue)
	}
	c, ok := exactCategory(cat)
	if !ok {
		return UniversalTicker{}, fmt.Errorf("%w: category %q", ErrInvalidTicker, cat)
	}
	if err := checkSymbol(symbol, c); err != nil {
		return UniversalTicker{}, err
	}
	return UniversalTicker{Venue: venue, Category: c, Symbol: symbol}, nil
}

// Resolve is lenient, for user input only (a spec's YAML, an API query): it folds case and pair
// punctuation ("bybit_perp_btc/usdt"), and checks the venue and that the venue trades the category.
// A glued future (btcusdt250926) becomes the hyphenated form. Everything past that boundary
// passes the parsed value around.
func Resolve(s string, venues Venues) (UniversalTicker, error) {
	venue, catName, symbol, err := splitTicker(strings.TrimSpace(s))
	if err != nil {
		return UniversalTicker{}, err
	}
	v, err := lookupVenue(venue, venues)
	if err != nil {
		return UniversalTicker{}, err
	}
	cat, err := foldCategory(catName)
	if err != nil {
		return UniversalTicker{}, err
	}
	if !v.trades(cat) {
		return UniversalTicker{}, fmt.Errorf("%w: %s does not trade %s", ErrCategory, v.Name, cat)
	}
	norm := normalizeSymbol(symbol, cat)
	t := UniversalTicker{Venue: v.Name, Category: cat, Symbol: norm}
	if err := checkSymbol(t.Symbol, t.Category); err != nil {
		return UniversalTicker{}, err
	}
	if !canonicalVenue(t.Venue) {
		return UniversalTicker{}, fmt.Errorf("%w: venue %q", ErrInvalidTicker, t.Venue)
	}
	return t, nil
}

// splitTicker cuts a ticker into three non-empty parts. '_' inside a part is
// not pair punctuation: it is the separator, so more or fewer than three parts
// is rejected.
func splitTicker(s string) (venue, cat, symbol string, err error) {
	parts := strings.Split(s, Separator)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("%w: %q", ErrInvalidTicker, s)
	}
	return parts[0], parts[1], parts[2], nil
}
