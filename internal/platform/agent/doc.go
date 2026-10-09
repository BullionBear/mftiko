// Package agent is the skeleton shared by the three agents (DECISIONS D2): Intake, Planner, Fleet, Reconciler,
// Policy, and Provider/Gate. Each plane supplies its own Planner, Policy and Provider or Gate.
//
// This package holds the decisions and does no I/O (D40): the types a plane plans with (Plan, Demand, Observed),
// Policy, and the Reconciler, which diffs plans against observations and keeps the restarts and their backoff.
// While the demand is stale it changes nothing by it (D39). The I/O is in the subpackages: runner runs a term's
// loop around the Reconciler, against etcd and a Fleet; c3 is the client of a worker's shim (PROTOCOL §4).
package agent
