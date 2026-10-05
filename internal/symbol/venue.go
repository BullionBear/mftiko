package symbol

// Venue is one entry of the venue registry (D11, D26): one connection plus one credential. Each
// exchange package declares its venues in code; adding a venue goes through review, not runtime
// configuration.
type Venue struct {
	Name  string // CamelCase, the first part of a UniversalTicker: BinanceUM, Okx, Bybit
	Label string // for the console
	// Categories the venue trades. A classic-account venue has one; a unified account (Bybit, Okx)
	// has several behind one credential, and a caller must always name which one it means.
	Categories []Category
	KeyTypes   []string // credential algorithms the adapter can sign with: hmac, ed25519
	Passphrase bool     // the credential includes a passphrase (Okx)
}

// Venues is the registry as Resolve sees it. internal/exchange's Registry implements it.
type Venues interface {
	Lookup(name string) (Venue, bool)
}
