// Package exchangetest is a test helper for callers of an exchange adapter, in the sense of
// net/http/httptest. It is not a venue: it is not declared to the registry and it has no wire format.
//
// Source is an in-memory PublicREST bound to one venue. A test adds, removes and modifies
// instruments, optionally loads them from a fixture, and then calls Listing the way SYM does.
// Listing reads one category in pages. The page size is configurable, and a chosen page can be made
// to fail. Either way Listing returns every instrument of that category, or an error and no rows: a
// short read is not a successful listing, which is the rule a refresh relies on.
//
// Depth, klines and open interest return exchange.ErrNotImplemented.
//
// A fixture is a JSON array of symbol.Listing values. Object keys are that struct's exported field
// names (encoding/json also accepts a different case of the same name), and the loader decodes into
// the struct itself, so a field added to Listing is picked up here without a new list of fields.
// Replaying a venue's raw REST payload is left to the E3 adapters.
package exchangetest
