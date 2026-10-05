// Package state is the etcd control plane: key layout under /mftiko/v1, clusterFor routing across regional clusters, writer-key fencing, and leases (D14, D17, PROTOCOL §3).
//
// # Implemented
//
//   - Keys (keys.go): a builder for every row of PROTOCOL §3.1, prefixes for the listed and watched ones,
//     WorkerHash (h in PROTOCOL §2), and Owner, the instance a key belongs to.
//   - Routing (topology.go, clusters.go): Topology maps instances to regions and regions to clusters; Clusters
//     dials one client per region lazily and hands out read-only Readers routed by key (D17). AgentRegions is
//     the set of clusters an agent may connect to.
//   - Terms (term.go): Campaign wins the instance's election in its home region, bumps the persisted epoch and
//     takes the home writer key; Fence takes the writer key in another cluster on first use and records its
//     rev R (PROTOCOL §3.2 W2 steps 1-3). A term ends, once and for good, when its session is lost, a writer
//     key shows a newer epoch, or it is closed.
//   - Fenced writes (write.go): Term.Put, Delete and Txn route by key, fence the target cluster on first use,
//     and run as one txn conditioned on that cluster's writer key still having rev R (W2 step 4); a caller's
//     Txn is nested inside it. A failed condition ends the term with ErrSuperseded, without retry. These are
//     the only write path: Clusters exposes readers only.
//
// Not yet: incarnations (E1-02), lease handling, auth.
package state
