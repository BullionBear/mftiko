// Package natsdev generates the dev NATS operator, accounts, and user credentials (D412–D417):
// one user per MD, md.fetch and TD instance (D415, D611).
//
// The output directory is gitignored. Nothing in this package is a production secret store:
// STS still signs each session user in memory at spawn (D906), the TD agent signs each account
// worker's user the same way (D611), and this package writes neither. The dev server is one
// process, with no TLS.
package natsdev
