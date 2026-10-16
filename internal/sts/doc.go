// Package sts is the strategy plane: the session planner, the dependency gate
// on MD and TD acks, the strategy artifact host, session volumes and inputs
// (D904), and the per-session NATS credential issuer (D906).
//
// SessionPermissions is the allow list that issuer signs (D818–D822). It does
// not dial etcd or NATS. The planner, the gate, and the issuer are not here yet.
package sts
