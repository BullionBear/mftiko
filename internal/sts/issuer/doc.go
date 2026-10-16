// Package issuer signs the session user JWT (D906, D827).
//
// The size check lives here, at the signing site, not in SessionPermissions
// and not in the SDK. SessionPermissions only lists subjects, so it never
// sees a JWT. nats-server applies max_control_line to the CONNECT JSON
// argument (the verb and the trailing CRLF are not counted). SignSession
// measures the argument nats.go sends for a session connection that leaves
// the client name empty, and rejects the spec when that argument is longer
// than the limit. The error says there are too many feeds.
package issuer
