// Package conformance checks an exchange adapter against recorded traffic, with no network (E3-04).
// A venue registers once and gets every check its parts allow:
//
//	func spec() conformance.Spec {
//		return conformance.Spec{
//			Venue:   "BinanceUM",
//			Dir:     "testdata/conformance/BinanceUM",
//			Market:  ..., REST: ..., Account: ..., History: ...,
//			KeyType: exchange.KeyEd25519,
//			Cases: []conformance.Case{
//				conformance.Call("listing_perp", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
//					return e.REST.Listing(symbol.Perp)
//				}),
//			},
//		}
//	}
//
//	func TestConformance(t *testing.T)  { conformance.Run(t, spec()) }
//	func FuzzDecodePublic(f *testing.F) { conformance.FuzzDecode(f, spec(), "public") }
//
// # Fixtures
//
// The directory holds two kinds, each beside its golden file:
//
//	ws/<name>.jsonl            a recorded websocket session, one Record per line; the first is the
//	                           meta record (side market with its route, or side private)
//	ws/<name>.golden.jsonl     every Sink or AccountSink call the codec made, one per line, copied
//	                           during the call; "frame" is the record's index after the meta line
//	ws/private_dial.golden.json  Prepare, Dial and Refresh of the private stream
//	http/<case>.json           a recorded REST response for the case of that name
//	http/<case>.golden.json    the request the case builds, and what Parse made of the response
//
// A session may hold http records: a depth snapshot of a book channel (SnapshotREST venues), and, with
// check set, the snapshot the folded book must equal at the end.
//
// Golden files are written, never edited by hand: run the venue's tests with MFTIKO_UPDATE_GOLDEN=1,
// then review the diff. A fixture with no golden file fails.
//
// Requests are built at Spec.Now with fake test credentials (TestCredentials), so a signed request is
// the same bytes on every run and its golden file pins the signing. Recorded traffic is redacted as it
// is written (Writer), so testnet credentials never reach a fixture.
//
// # Checks
//
//   - cases: each case's request and parse match the golden file; the parse records the error kind
//     (Kind), so recorded refusals pin the classification as *Reject, *RateLimited or ErrAuth (D302).
//   - listing: every listing passes Listing.Validate and Ticker after asset canonicalization (D203),
//     and its tick and step fit float64 at the instrument's magnitude (D008).
//   - sessions: each session decodes to its golden events, and decodes the same when every frame goes
//     through one buffer overwritten after each Decode (D302: the codec keeps nothing pointing into a
//     frame).
//   - unknown_frames: Decode refuses a frame it does not know, with an error and no events.
//   - capabilities: the events match Topics (D401). A topic emitted is listed; a field declared absent
//     is never filled; an optional field not declared absent is filled somewhere in the fixtures; a
//     required field is never absent.
//   - books: each book channel replays through its BookSync with no resync and ends equal to the
//     closing snapshot; dropping an update is noticed; with BookChecksum, so is a corrupted book.
//   - planning: Routes, Endpoint (and its override), Atom, Cost and Budget agree (D401, D403); a topic
//     Topics does not list is ErrUnsupported.
//   - subscribe: Subscribe and Unsubscribe cover each channel once, within SessionLimits, with
//     distinct request ids per connection.
//   - private_dial: the private stream's Prepare, Dial and Refresh match their golden file.
//   - fills: each fill of a private session is in the paired History case with the same fill_id,
//     order, side, price and quantity (D601).
//   - credentials: no secret part of the test credentials appears in anything the adapter produced.
//
// The instruments the planning and book checks use are Spec.Instruments plus every valid listing the
// listing cases return.
//
// # Recording
//
// RecordMarket and RecordPrivate record a session over a FrameConn and a Doer; SaveExchange writes
// one REST case. They are transport-agnostic: the transports of E3-02 plug in, and a command that
// records against a venue is wired once they exist.
//
// Not covered yet: the TradeStream of D303 (E3-05) gets its checks with that ticket.
package conformance
