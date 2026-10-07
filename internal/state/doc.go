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
//     key shows a newer epoch, or it is closed. Fencing is per cluster: a superseded term that has not yet
//     noticed can still write to a cluster the new term has not fenced. An agent therefore fences every cluster
//     it will write before its first reconcile (the agent skeleton, E1-15), not on first use.
//   - Fenced writes (write.go): Term.Put, Delete and Txn route by key, fence the target cluster on first use,
//     and run as one txn conditioned on that cluster's writer key still having rev R (W2 step 4); a caller's
//     Txn is nested inside it. A failed condition ends the term with ErrSuperseded, without retry. They also
//     enforce W1: an agent puts and deletes only the keys whose writer it is in §3.1 (ErrNotWriter otherwise),
//     so no write can reach the election, epoch or writer keys, incarnations outside Incarnations, another
//     agent's keys, or the API's. These are the only write path: Clusters exposes readers only.
//   - Incarnations (incarnation.go): LoadIncarnations reads the agent's counters once per term; Next takes a
//     worker key's next number in one fenced txn before the spawn (W6, D8), so a superseded agent gets none;
//     Forget deletes an STS session's counter once it is terminal.
//   - Mirrors (mirror.go): Mirror keeps an in-memory copy of one prefix of one cluster (W4): a paged list at one
//     revision, then a watch that requires a leader. A compacted watch relists (dropping keys whose deletes were
//     compacted away); a broken one is reopened from the last applied event. A probe relists when the cluster was
//     replaced or rebuilt, and reopens a watch that went silent. Readers act level-triggered on Notify, Snapshot
//     and Take; LastContact says how fresh the copy is, WaitRev waits for a revision.
//
// Not yet: the etcd leases that lease entries and acks are bound to (PROTOCOL §3.3), auth and RBAC (E1-08).
package state
