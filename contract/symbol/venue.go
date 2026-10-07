package symbol

// Venue is one entry of the venue registry (D11, D26): a market, the first part of a
// UniversalTicker. Each exchange package declares its venues in code; adding one goes through
// review, not runtime configuration.
//
// A venue is not an account. Who may trade a venue's instruments, and with what credential, is the
// account kind (exchange AccountKind, D32): a separate enum whose names may coincide with
// venue names. Never look one up in the other's table.
type Venue struct {
	Name  string // CamelCase, the first part of a UniversalTicker: BinanceUM, Okx, Bybit
	Label string // for the console
	// Categories the venue lists. A classic venue has one or two; a unified-account venue (Bybit,
	// Okx) has several, and a caller must always name which one it means.
	Categories []Category
}

// Venues is the venue registry as Resolve sees it. exchange's Registry implements it.
type Venues interface {
	Lookup(name string) (Venue, bool)
}
