// Package archtest checks the dependency rules between the blocks of the repository (DECISIONS D35). It has no
// code of its own: the rules run as a test, so `make check` and CI enforce them.
//
//   - contract depends on nothing else in the repository, and outside it only on protobuf and the standard library.
//   - exchange depends only on contract.
//   - sdk depends only on contract, and nothing it links may be etcd, a PostgreSQL driver, exchange or internal.
//   - examples use only sdk and contract, as an outside author would.
//   - internal/platform imports no plane and no exchange.
//   - The planes (internal/api, sts, md, td, sym) do not import each other. They use exchange's interfaces only;
//     cmd registers the exchanges. The API imports no exchange at all and does not use sym.v1.
//   - The agent planes' root packages (internal/sts, md, td) and the skeleton's decisions (internal/platform/agent)
//     link no etcd, NATS, state, natsx, shim, or the skeleton's I/O (agent/c3, agent/runner): they hold the
//     Planner, the Policy and the domain types, testable without a cluster. The wiring is in internal/{plane}/daemon
//     (DECISIONS D40).
//
// cmd, migrations and this package are free.
package archtest
