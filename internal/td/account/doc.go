// Package account is the TD account worker: private websocket and HTTP pool, OMS and ledger, recon and Recon events, degraded-mode order rejection, and the first_submit_ns report (D20, D25, D27).
//
// Venue specifics come from exchange: the api_id's AccountKind builds the Account and
// PrivateStream. Submit is rejected 1xx unless the kind can trade the ticker's venue (D32). Tickers
// are resolved through SYM once per instrument; venue spellings coming back are reversed through
// it (D31).
//
// Shape only: not implemented yet.
package account
