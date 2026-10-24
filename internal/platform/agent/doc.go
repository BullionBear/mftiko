// Package agent is the skeleton shared by the three agents (DECISIONS E01-D001): Intake, Planner, Fleet, Reconciler,
// Policy, and Provider/Gate. Each plane supplies its own Planner, Policy and Provider or Gate.
//
// This package holds the decisions and does no I/O (E01-D010): the types a plane plans with (Plan, Demand, Observed),
// Policy, and the Reconciler, which diffs plans against observations and keeps the restarts and their backoff.
// While the demand is stale it changes nothing by it (E01-D009). The I/O is in the subpackages: runner runs a term's
// loop around the Reconciler, against etcd and a Fleet; fleet is the Fleet, a shim per worker; c3 is the client of
// a worker's shim (PROTOCOL §4); testplane runs the whole skeleton with fake workers (E1-19).
package agent
