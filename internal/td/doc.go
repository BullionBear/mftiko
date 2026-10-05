// Package td is the execution plane: account refcount and linger (D9, D28), the provider that writes acks and checks algo_id conflicts (D25), the backfill scheduler, and the history queue and drain (D20).
//
// Backfill reads through internal/exchange History, with each scope's ticker resolved through SYM
// (D31).
//
// Shape only: not implemented yet.
package td
