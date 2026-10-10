// Package exchangetest is a test helper for callers of an exchange adapter, in the sense of
// net/http/httptest. It is not a venue: it is not declared to the registry and it has no wire format.
//
// Source is an in-memory listing bound to one venue: an exchange.Lister, which is what SYM reads
// through exchange.ListingOver on a real venue. It runs in process, with no HTTPRequest behind it. A
// test adds, removes and modifies instruments, optionally loads them from a fixture, and then calls
// Listing the way SYM does.
// Listing reads one category in pages. The page size is configurable, and a chosen page can be made
// to fail. Either way Listing returns every instrument of that category, or an error and no rows: a
// short read is not a successful listing, which is the rule a refresh relies on.
//
// Source implements exchange.InProcessUIDSource. The in-process Resolver may allocate
// UIDs for these listings. Those UIDs are not SYM's: they stay in that process, are not
// stored, and must not be sent to another process or plane. A real adapter does not
// implement the marker.
//
// TradeVenue is a fake venue for code that drives an exchange.TradeStream (D303): the test chooses
// the order of the answers, drops some, refuses some, or refuses the logon.
//
// A fixture is a JSON array of symbol.Listing values. Object keys are that struct's exported field
// names (encoding/json also accepts a different case of the same name), and the loader decodes into
// the struct itself, so a field added to Listing is picked up here without a new list of fields.
// Replaying a venue's raw traffic, REST and websocket, is the conformance subpackage (E3-04).
package exchangetest
