// Package tdaccount is the TD account worker: private websocket and HTTP pool, OMS and ledger, recon and Recon events, degraded-mode order rejection, and the first_submit_ns report (D20, D25, D27).
//
// Venue specifics come from internal/exchange: Account and PrivateStream. Tickers are resolved
// through SYM once per instrument; venue spellings coming back are reversed through it (D31).
//
// Shape only: not implemented yet.
package tdaccount
