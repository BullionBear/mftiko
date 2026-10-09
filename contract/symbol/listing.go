package symbol

import (
	"fmt"
	"strings"
)

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
	// Empty on spot, which has no contract count. Required on a contract.
	ContractSize string
	// ContractValueCcy is the unit of qty (D203). Adapters fill it from the venue's
	// native field; nothing here derives it. Empty is allowed on spot. A contract
	// must set it: base on linear and options, quote on inverse.
	ContractValueCcy string
	// PriceCcy is the unit of price, price_tick and option premium (D203).
	// Adapters fill it. Empty is allowed on spot. A contract must set it.
	PriceCcy string
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
// ExchTicker. The symbol is the canonical base plus the canonical quote (uppercase, alias
// table), then -ExpiryCode for a dated contract, then -Strike-OptionType for an option.
// The strike goes through SpellStrike first (D203): 6400.0 and 6400 are one ticker, and a
// fractional strike uses StrikeDecimal. The result is always a string Parse accepts.
func (l *Listing) Ticker() (UniversalTicker, error) {
	if l == nil {
		return UniversalTicker{}, fmt.Errorf("%w: nil listing", ErrInvalidTicker)
	}
	if !canonicalVenue(l.Venue) {
		return UniversalTicker{}, fmt.Errorf("%w: venue %q", ErrInvalidTicker, l.Venue)
	}
	if !l.Category.ok() {
		return UniversalTicker{}, fmt.Errorf("%w: category %q", ErrInvalidTicker, l.Category)
	}
	base, err := CanonicalAsset(l.Base)
	if err != nil {
		return UniversalTicker{}, err
	}
	quote, err := CanonicalAsset(l.Quote)
	if err != nil {
		return UniversalTicker{}, err
	}
	pair := base + quote
	var symbol string
	switch l.Category {
	case Spot, Perp, Inverse:
		if l.ExpiryCode != "" || l.Strike != "" || l.OptionType != "" {
			return UniversalTicker{}, fmt.Errorf("%w: %s has no expiry or strike", ErrInvalidTicker, l.Category)
		}
		symbol = pair
	case Future:
		if !dateCode(l.ExpiryCode) {
			return UniversalTicker{}, fmt.Errorf("%w: expiry %q", ErrInvalidTicker, l.ExpiryCode)
		}
		if l.Strike != "" || l.OptionType != "" {
			return UniversalTicker{}, fmt.Errorf("%w: future has no strike", ErrInvalidTicker)
		}
		symbol = pair + "-" + l.ExpiryCode
	case Option:
		if !dateCode(l.ExpiryCode) {
			return UniversalTicker{}, fmt.Errorf("%w: expiry %q", ErrInvalidTicker, l.ExpiryCode)
		}
		opt := strings.ToUpper(strings.TrimSpace(l.OptionType))
		if opt != "C" && opt != "P" {
			return UniversalTicker{}, fmt.Errorf("%w: option type %q", ErrInvalidTicker, l.OptionType)
		}
		strike, err := SpellStrike(l.Strike)
		if err != nil {
			return UniversalTicker{}, err
		}
		if strings.ContainsAny(strike, "./_ \t") || strings.HasPrefix(strike, "-") {
			return UniversalTicker{}, fmt.Errorf("%w: strike %q", ErrInvalidTicker, strike)
		}
		symbol = pair + "-" + l.ExpiryCode + "-" + strike + "-" + opt
	default:
		return UniversalTicker{}, fmt.Errorf("%w: category %q", ErrInvalidTicker, l.Category)
	}
	t := UniversalTicker{Venue: l.Venue, Category: l.Category, Symbol: symbol}
	if _, err := Parse(t.String()); err != nil {
		return UniversalTicker{}, err
	}
	return t, nil
}

// Validate is the gate E2-05 and E2-06 share before a row is stored.
//
// A contract (anything but spot) needs contract_size and the three currency
// fields. Asset codes must already be canonical: uppercase, alias applied.
// Quanto — settlement neither base nor quote — is an error and is not stored.
// Spot has no settlement asset.
func (l *Listing) Validate() error {
	if l == nil {
		return fmt.Errorf("%w: nil", ErrInvalidListing)
	}
	if !l.Category.ok() {
		return fmt.Errorf("%w: category %q", ErrInvalidListing, l.Category)
	}
	for _, code := range []string{l.Base, l.Quote} {
		if !IsCanonicalAsset(code) {
			return fmt.Errorf("%w: %q", ErrAsset, code)
		}
	}
	for _, code := range []string{l.ContractValueCcy, l.PriceCcy, l.Settle} {
		if code == "" {
			continue
		}
		if !IsCanonicalAsset(code) {
			return fmt.Errorf("%w: %q", ErrAsset, code)
		}
	}
	if l.Category == Spot {
		if l.Settle != "" {
			return fmt.Errorf("%w: spot has no settlement asset", ErrInvalidListing)
		}
		return nil
	}
	if strings.TrimSpace(l.ContractSize) == "" {
		return fmt.Errorf("%w: contract_size", ErrInvalidListing)
	}
	if l.ContractValueCcy == "" || l.PriceCcy == "" || l.Settle == "" {
		return fmt.Errorf("%w: currency", ErrInvalidListing)
	}
	if classify(l.Base, l.Quote, l.ContractValueCcy, l.PriceCcy, l.Settle).quanto {
		return ErrQuanto
	}
	return nil
}
