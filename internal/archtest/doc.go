// Package archtest checks the dependency rules between the blocks of the repository (DECISIONS D006). It has no
// code of its own: the rules run as a test, so `make check` and CI enforce them.
//
//   - contract depends on nothing else in the repository, and outside it only on protobuf and the standard library.
//     contract/gen/api also imports github.com/oapi-codegen/runtime (D1045). contract/symbol depends only on the standard library.
//   - exchange depends only on contract. The exchange package itself, the adapter interfaces, links none of
//     fasthttp, coder/websocket or sonic: they belong to the workers and exchange/transport, and sonic also to
//     exchange/internal/codec (DECISIONS D302).
//   - sdk depends only on contract, and nothing it links may be etcd, a PostgreSQL driver, exchange or internal.
//   - examples use only sdk and contract, as an outside author would.
//   - internal/platform imports no plane and no exchange.
//   - The planes (internal/api, sts, md, td, sym, controller) do not import each other. They use exchange's
//     interfaces and exchange/transport only; cmd registers the exchanges. The API and the controller import no
//     exchange at all and do not use sym.v1 (DECISIONS D1301, D907).
//   - The agent planes' root packages (internal/sts, md, td), the controller's decisions (internal/controller)
//     and the skeleton's decisions (internal/platform/agent) link no etcd, NATS, state, natsx, shim, or the
//     skeleton's I/O (agent/c3, fleet, runner, testplane): they hold the decisions, testable without a cluster.
//     The wiring is in internal/{plane}/daemon (DECISIONS D110, D1301).
//   - internal/td links no contract/wire/cloid. The account worker treats client_order_id as an opaque string;
//     the SDK and conformance own the layout (DECISIONS D633).
//
// cmd, migrations and this package are free.
package archtest
