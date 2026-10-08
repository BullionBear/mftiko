// Package state is the etcd control plane: key layout under /mftiko/v1, clusterFor routing across regional clusters, writer-key fencing, and leases (D14, D17, PROTOCOL §3).
//
// # Implemented
//
//   - Keys (keys.go): a builder for every row of PROTOCOL §3.1, prefixes for the listed and watched ones,
//     WorkerHash (h in PROTOCOL §2), and Owner, the instance a key belongs to.
//
//   - Routing (topology.go, clusters.go): Topology maps instances to regions and regions to clusters; Clusters
//     dials one client per region lazily and hands out read-only Readers routed by key (D17). AgentRegions is
//     the set of clusters an agent (its own region) or the controller (every region) may connect to.
//
//   - Terms (term.go): an agent and the controller (PlaneCtl, D33) campaign the same way. Campaign wins the
//     instance's election in its home region, bumps the persisted epoch and takes the home writer key; Fence takes
//     the writer key in another cluster on first use and records its rev R (PROTOCOL §3.2 W2 steps 1-3). A term
//     ends, once and for good, when its session is lost, a writer key shows a newer epoch, or it is closed.
//     PutRecord writes the AgentRecord on the session's lease, the only lease there is (W7). Once the term ends the
//     session is no longer renewed, and Close revokes it, so the record goes with the agent: readers tell a live
//     agent from the last state a gone one left behind.
//     Fencing is per cluster: a superseded term that has not yet noticed can still write to a cluster the new term
//     has not fenced. A term therefore fences every cluster it will write before its first reconcile (the agent
//     skeleton, E1-15), not on first use. Only the controller writes to more than one cluster.
//
//   - Fenced writes (write.go): Term.Put, Delete and Txn route by key, fence the target cluster on first use,
//     and run as one txn conditioned on that cluster's writer key still having rev R (W2 step 4); a caller's
//     Txn is nested inside it. A failed condition ends the term with ErrSuperseded, without retry. They also
//     enforce W1: a term puts and deletes only the keys whose writer it is in §3.1 (ErrNotWriter otherwise). An
//     agent writes its own status, ack and bookkeeping keys, bound to no lease; the controller writes every agent
//     instance's demands. No write reaches the election, epoch or writer keys, incarnations outside Incarnations,
//     the AgentRecord outside PutRecord, another process's keys, or the API server's. These are the only write
//     path: Clusters exposes readers only.
//
//   - Incarnations (incarnation.go): LoadIncarnations reads the agent's counters once per term; Next takes a
//     worker key's next number in one fenced txn before the spawn (W6, D8), so a superseded agent gets none;
//     Forget deletes an STS session's counter once it is terminal.
//
//   - Mirrors (mirror.go): Mirror keeps an in-memory copy of one prefix of one cluster (W4): a paged list at one
//     revision, then a watch that requires a leader. A compacted watch relists (dropping keys whose deletes were
//     compacted away); a broken one is reopened from the last applied event. A probe relists when the cluster was
//     replaced or rebuilt, and reopens a watch that went silent. Readers act level-triggered on Notify, Snapshot
//     and Take; LastContact says how fresh the copy is, WaitRev waits for a revision.
//
//   - Auth (clusters.go, rbac.go): AuthDial connects as the process's own user; the client authenticates again when
//     its token expires or the cluster's users and roles change. AuthTables derives every region's users and roles
//     from the instances, so that etcd enforces W1 as Term does: a write the write scope would refuse, etcd refuses
//     too. Clusters dials a region on first use, on a goroutine of its own; callers wait no longer than their ctx.
//
// Only the AgentRecord is bound to a lease; demands, acks and WorkerStatus are not (PROTOCOL §3.3, §3.2 W7).
package state
