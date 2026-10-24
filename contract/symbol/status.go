package symbol

// Status is a listing's trading state (E02-D003). The set is still provisional.
// The spellings are the TEXT values the symbol tables check.
//
// The zero value, Unspecified, matches the proto zero SYMBOL_STATUS_UNSPECIFIED.
// It is invalid and not tradable, and it is not stored. A native venue state
// that does not map to one of these values is Unknown, which is stored and
// is not tradable. Only Tradable may trade: a value added later is not
// tradable unless this rule is changed.
type Status string

const (
	Unspecified Status = ""
	Prelisting  Status = "PRELISTING"
	Tradable    Status = "TRADABLE"
	Halted      Status = "HALTED"
	Settling    Status = "SETTLING"
	Expired     Status = "EXPIRED"
	Delisted    Status = "DELISTED"
	Unknown     Status = "UNKNOWN"
)

// Valid reports whether s may be stored. Unspecified and any other spelling
// are not. Expired and Delisted are valid: a refresh moves those rows.
func (s Status) Valid() bool {
	switch s {
	case Prelisting, Tradable, Halted, Settling, Unknown, Expired, Delisted:
		return true
	default:
		return false
	}
}

// IsTradable reports whether an order in this state may be sent.
// Only Tradable is tradable.
func (s Status) IsTradable() bool { return s == Tradable }

// Active reports whether the status stays on symbol_active.
func (s Status) Active() bool {
	switch s {
	case Prelisting, Tradable, Halted, Settling, Unknown:
		return true
	default:
		return false
	}
}

// Terminal reports whether the status belongs on symbol_terminal.
func (s Status) Terminal() bool { return s == Expired || s == Delisted }
