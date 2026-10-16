// Package natsdev generates the dev NATS operator, accounts, and user credentials (D412–D417):
// one user per MD and md.fetch instance (D415). The TD agent has no NATS user (D639).
//
// The output directory is gitignored. Nothing in this package is a production secret store:
// STS still signs each session user in memory at spawn (D906), the TD agent signs each account
// worker's user the same way (D611), and this package writes neither. The dev server is one
// process, with no TLS. The generated server config sets max_control_line to 262144 (D827):
// a session JWT lists each feed, and the nats-server default of 4096 rejects that CONNECT line.
package natsdev
