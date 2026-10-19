// Package sts is the strategy plane: the session planner, the dependency gate
// on MD and TD acks, the strategy artifact host, session volumes and inputs
// (D904), and the per-session NATS credential issuer (D906).
//
// SessionPermissions is the allow list that issuer signs (D818–D822). It does
// not dial etcd or NATS, and it does not check the JWT size. That check is at
// the signing site, internal/sts/issuer.SignSession (D827): it measures the
// CONNECT argument and rejects a spec with too many feeds. The agent's wiring
// is internal/sts/agent (E9-05). The dependency gate and the artifact host
// are not here yet.
package sts
