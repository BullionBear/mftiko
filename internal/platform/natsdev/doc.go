// Package natsdev generates the dev NATS operator, accounts, and user credentials (E04-D012–E04-D017):
// one user per MD and md.fetch instance (E04-D015). The TD agent has no NATS user (E06-D039).
//
// The output directory is gitignored. Nothing in this package is a production secret store:
// STS still signs each session user in memory at spawn (E09-D006), the TD agent signs each account
// worker's user the same way (E06-D011), and this package writes neither. The dev server is one
// process, with no TLS. The generated server config sets max_control_line to 262144 (E08-D027):
// a session JWT lists each feed, and the nats-server default of 4096 rejects that CONNECT line.
package natsdev
