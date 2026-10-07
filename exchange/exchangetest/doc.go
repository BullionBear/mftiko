// Package exchangetest is the conformance harness every adapter runs in its own tests.
//
// Planned checks, from recorded venue traffic in testdata/<venue>/ (no network in tests):
//
//   - Decode: each recorded frame produces exactly the expected Sink calls; unknown frames are an
//     error, not a silent drop.
//   - Books: replaying a recorded session through BookSync yields the venue's own snapshot at the
//     end; an injected gap yields BookResync.
//   - Planning: Atom is a pure function, every channel it returns has Cost <= Budget of its route,
//     and Routes covers every route Atom can return.
//   - fill_id parity (D20): a fill recorded from the private stream and the same fill from History
//     carry the same FillID.
//   - Credentials: no request, error or log line built by the adapter contains the secret.
//
// Shape only: not implemented yet.
package exchangetest
