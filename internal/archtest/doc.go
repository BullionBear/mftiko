// Package archtest checks the dependency rules between the blocks of the repository (DECISIONS E00-D006). It has no
// code of its own: the rules run as a test, so `make check` and CI enforce them.
//
//   - contract depends on nothing else in the repository, and outside it only on protobuf and the standard library.
//     contract/gen/api also imports github.com/oapi-codegen/runtime (E10-D045), and contract/gen/mftiko/sym/v1/symv1connect
//     imports connectrpc.com/connect (E22-D006). contract/symbol depends only on the standard library.
//   - exchange depends only on contract. The exchange package itself, the adapter interfaces, links none of
//     fasthttp, coder/websocket or sonic: they belong to the workers and exchange/transport, and sonic also to
//     exchange/internal/codec (DECISIONS E03-D002).
//   - sdk depends only on contract and pkg/logger. pkg/logger depends only on zerolog and the standard library.
//     Nothing sdk links may be etcd, a PostgreSQL driver, exchange or internal (E15-D018), nor connect, grpc or
//     symv1connect (E22-D006).
//   - examples use only sdk and contract, as an outside author would.
//   - internal/platform imports no plane and no exchange.
//   - The planes (internal/api, sts, md, td, sym, controller) do not import each other. They use exchange's
//     interfaces and exchange/transport only; cmd registers the exchanges. The API and the controller import no
//     exchange at all and do not use sym.v1 or its symv1connect (DECISIONS E13-D001, E09-D007, E22-D006).
//   - The agent planes' root packages (internal/sts, md, td), the controller's decisions (internal/controller)
//     and the skeleton's decisions (internal/platform/agent) link no etcd, NATS, state, natsx, shim, or the
//     skeleton's I/O (agent/c3, fleet, runner, testplane): they hold the decisions, testable without a cluster.
//     The wiring is in internal/{plane}/daemon (DECISIONS E01-D010, E13-D001).
//   - SYM's logic layer, the internal/sym root package, links no protocol: natsx, nats.go, connect, grpc or net/http.
//     Each protocol is an adapter in internal/sym/sym* (symnats, symgrpc, symhttp), and adapters do not import each
//     other. internal/sym/symtest, the conformance suite that drives every adapter, is not an adapter (DECISIONS E22-D002).
//   - internal/td links no contract/wire/cloid. The account worker treats client_order_id as an opaque string;
//     the SDK and conformance own the layout (DECISIONS E06-D033).
//
// cmd, migrations and this package are free.
package archtest
