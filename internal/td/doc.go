// Package td is the execution plane: account refcount and linger (D901, D905), the provider that writes acks and checks algo_id conflicts (D602), the backfill scheduler, and the history queue and drain (D601).
//
// Backfill reads through exchange History, with each scope's ticker resolved through SYM
// (D202).
//
// Shape only: not implemented yet.
package td
