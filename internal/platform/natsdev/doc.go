// Package natsdev generates the dev NATS operator, accounts, and user credentials (D412–D417).
//
// The output directory is gitignored. Nothing in this package is a production secret store:
// STS still signs each session user in memory at spawn (D906) and this package does not write
// session credentials. The dev server is one process, with no TLS.
package natsdev
