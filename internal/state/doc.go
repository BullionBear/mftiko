// Package state is the etcd control plane: key layout under /mftiko/v1, clusterFor routing across regional clusters, writer-key fencing, and leases (D14, D17, PROTOCOL §3).
//
// # Implemented
//
//   - Keys (keys.go): a builder for every row of PROTOCOL §3.1, prefixes for the listed and watched ones,
//     WorkerHash (h in PROTOCOL §2), and Owner, the instance a key belongs to.
//   - Routing (topology.go, clusters.go): Topology maps instances to regions and regions to clusters; Clusters
//     dials one client per region lazily and hands out read-only Readers routed by key (D17). AgentRegions is
//     the set of clusters an agent may connect to.
//
// Not yet: election, epoch and writer-key fencing (E1-04, E1-05), incarnations (E1-02), lease handling, auth.
package state
