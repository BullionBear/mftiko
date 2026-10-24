package symbol

import (
	"fmt"
	"strings"
	"unicode"
)

// canonicalVenue is the venue half of a ticker: CamelCase ASCII, no separator (E02-D001).
// OKX matches this and is still not the registry spelling Okx; Parse does not consult
// the registry, Resolve does.
func canonicalVenue(name string) bool {
	if name == "" || name[0] < 'A' || name[0] > 'Z' {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func (c Category) ok() bool {
	switch c {
	case Spot, Perp, Inverse, Future, Option:
		return true
	default:
		return false
	}
}

func (c Category) structured() bool {
	return c == Future || c == Option
}

// foldCategory matches a category name, ignoring case and surrounding space.
func foldCategory(name string) (Category, error) {
	name = strings.TrimSpace(name)
	for _, c := range []Category{Spot, Perp, Inverse, Future, Option} {
		if strings.EqualFold(name, string(c)) {
			return c, nil
		}
	}
	return "", fmt.Errorf("%w: category %q", ErrInvalidTicker, name)
}

func exactCategory(name string) (Category, bool) {
	c := Category(name)
	if c.ok() && string(c) == name {
		return c, true
	}
	return "", false
}

// dateCode is YYMMDD as a venue writes it: six digits, not a calendar check.
// The venue already named the contract; the field only has to be recognizable
// so it is not glued to the pair.
func dateCode(s string) bool {
	if len(s) != 6 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// canonicalPair folds pair punctuation. BTC_USDT, BTC-USDT, btc/usdt and
// BTCUSDT are one spelling. It does not split base from quote.
func canonicalPair(symbol string) string {
	var b strings.Builder
	b.Grow(len(symbol))
	for _, r := range strings.TrimSpace(symbol) {
		if r == '/' || r == '-' || r == '_' || unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

// forbiddenInSymbol is the first character a platform symbol may not contain.
// '-' separates fields of a dated or option symbol, so it is allowed.
// '.' is refused because a fractional strike spells StrikeDecimal, and '.'
// separates a feed key's topic from its ticker.
func forbiddenInSymbol(symbol string) string {
	for _, r := range symbol {
		switch r {
		case '/', '_', '.':
			return string(r)
		default:
			if unicode.IsSpace(r) {
				return string(r)
			}
		}
	}
	return ""
}

func forbidError(symbol, bad string) error {
	why := fmt.Sprintf("%q separates the ticker's three parts, and '-' separates a dated or option symbol's fields", Separator)
	if bad == "." {
		why = fmt.Sprintf("a fractional strike spells its decimal point %q (6.4 is %q), because '.' separates a feed key's topic from its ticker", StrikeDecimal, "6"+StrikeDecimal+"4")
	}
	return fmt.Errorf("%w: %q cannot appear inside symbol %q; %s", ErrInvalidTicker, bad, symbol, why)
}

// checkSymbol accepts a symbol that is already in platform form.
func checkSymbol(symbol string, cat Category) error {
	if symbol == "" {
		return fmt.Errorf("%w: empty symbol", ErrInvalidTicker)
	}
	if bad := forbiddenInSymbol(symbol); bad != "" {
		return forbidError(symbol, bad)
	}
	if normalizeSymbol(symbol, cat) != symbol {
		return fmt.Errorf("%w: symbol %q is not canonical", ErrInvalidTicker, symbol)
	}
	return nil
}

// normalizeSymbol is the platform spelling of a symbol on a given book.
//
// Spot, perp and inverse are a pair: separators fall out. Future and option
// keep '-' between fields, so BTC-USDT-250926 becomes BTCUSDT-250926. The
// glued future BTCUSDT250926 is accepted on those books so typed input does
// not fork a second instrument; Parse still rejects it, because storing it
// would change the spelling. A fractional strike is folded with SpellStrike.
func normalizeSymbol(symbol string, cat Category) string {
	raw := strings.ToUpper(strings.TrimSpace(symbol))
	if raw == "" {
		return ""
	}
	if !cat.structured() {
		return canonicalPair(raw)
	}
	parts := strings.Split(raw, "-")
	if optionParts(parts) {
		pair := canonicalPair(strings.Join(parts[:len(parts)-3], "-"))
		strike, err := SpellStrike(parts[len(parts)-2])
		date := parts[len(parts)-3]
		opt := parts[len(parts)-1]
		if pair != "" && err == nil {
			return pair + "-" + date + "-" + strike + "-" + opt
		}
		// A strike SpellStrike cannot fold stays verbatim. Gluing it onto
		// the pair would build a ticker Parse cannot read back.
		if pair != "" {
			return pair + "-" + date + "-" + parts[len(parts)-2] + "-" + opt
		}
	}
	if len(parts) >= 2 && dateCode(parts[len(parts)-1]) {
		pair := canonicalPair(strings.Join(parts[:len(parts)-1], "-"))
		if pair != "" {
			return pair + "-" + parts[len(parts)-1]
		}
	}
	compact := canonicalPair(raw)
	if len(compact) > 6 && dateCode(compact[len(compact)-6:]) {
		pair := compact[:len(compact)-6]
		if pair != "" {
			return pair + "-" + compact[len(compact)-6:]
		}
	}
	return compact
}

func optionParts(parts []string) bool {
	if len(parts) < 4 {
		return false
	}
	last := parts[len(parts)-1]
	if last != "C" && last != "P" {
		return false
	}
	if !dateCode(parts[len(parts)-3]) {
		return false
	}
	return parts[len(parts)-2] != ""
}

func (v Venue) trades(c Category) bool {
	for _, got := range v.Categories {
		if got == c {
			return true
		}
	}
	return false
}

func lookupVenue(name string, venues Venues) (Venue, error) {
	if venues == nil {
		return Venue{}, fmt.Errorf("%w: nil registry", ErrUnknownVenue)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return Venue{}, fmt.Errorf("%w: empty", ErrUnknownVenue)
	}
	if v, ok := venues.Lookup(name); ok {
		return v, nil
	}
	for _, n := range venues.Names() {
		if strings.EqualFold(n, name) {
			if v, ok := venues.Lookup(n); ok {
				return v, nil
			}
		}
	}
	return Venue{}, fmt.Errorf("%w: %q", ErrUnknownVenue, name)
}
