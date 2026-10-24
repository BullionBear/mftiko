// Package sts is the strategy plane: the session planner, the dependency gate
// on MD and TD acks, the strategy artifact host, session volumes and inputs
// (E09-D004), and the per-session NATS credential issuer (E09-D006).
//
// SessionPermissions is the allow list that issuer signs (E08-D018–E08-D022). It does
// not dial etcd or NATS, and it does not check the JWT size. That check is at
// the signing site, internal/sts/issuer.SignSession (E08-D027): it measures the
// CONNECT argument and rejects a spec with too many feeds. The agent's wiring
// is internal/sts/agent (E9-05). The dependency gate and the artifact host
// are not here yet.
package sts
