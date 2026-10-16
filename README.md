# mftiko

Go trading framework with three independent process-control planes — strategy (STS), market data (MD) and execution (TD) — each run by its own agent. Workers sit behind a shim so an agent can rotate without stopping them. Control state lives in etcd (one cluster per region); data moves over NATS.

Design: [docs/DECISIONS.md](docs/DECISIONS.md). Protocols and contracts: [docs/PROTOCOL.md](docs/PROTOCOL.md).

> Status: layout only. Every command and package is a stub.

## Layout

Three blocks plus the exchange package (DECISIONS D006); `internal/archtest` checks the dependency rules between them.

```
internal/                 ① the planes, private
  api/                    API server: declarations into etcd, serves the web console
  platform/               trading-agnostic runtime: agent skeleton, shim, state (etcd), natsx, artifact, sealbox, logger, buildinfo
  sts/                    strategy plane; conformance/ (SDK conformance suite)
  md/                     market-data plane; conn/ (conn worker), fetch/ (md.fetch)
  td/                     execution plane; account/ (account worker), history/ (PostgreSQL)
  sym/                    symbol service
sdk/                      ② Go strategy SDK, the reference implementation of PROTOCOL §9; depends on contract only
contract/                 ③ what both ends compile against; source of truth is docs/PROTOCOL.md
  proto/mftiko/<pkg>/v1/  protobuf contracts
  gen/                    generated Go (make proto; committed, do not edit)
  wire/                   encodings protobuf cannot express: frame (C3, C4), envelope (md.f), cloid (client_order_id); c4, the worker side of C4
  symbol/                 UniversalTicker, venue registry, the Resolver interface
exchange/                 exchange adapters: interfaces by plane, one package per exchange (binance, okx, bybit)
  transport/              the workers' network: proxy, TLS and DNS shared by fasthttp/ (REST Doer) and ws/ (websocket conn)
cmd/                      one directory per binary (bin/<name> after `make build`); the only place that wires them together
  mftiko-api              API server
  mftiko-sts-agent        strategy-plane agent
  mftiko-md-agent         market-data-plane agent
  mftiko-td-agent         execution-plane agent (also backfill and history drain)
  mftiko-shim             per-worker shim
  mftiko-md-conn          MD conn worker: one websocket per (venue, route, n)
  mftiko-td-account       TD account worker: one per api_id (connections, recon, account state)
  mftiko-mdfetch          stateless md.fetch service
  mftiko-sym              symbol service
  mftiko-migrate          applies the PostgreSQL migrations (a deploy step)
  mftiko-pack             builds a strategy artifact (OCI image + mftiko manifest)
  mftiko-etcd-roles       prints each region's etcd users and roles, derived from the instance list
  mftiko-nats-dev         generates dev NATS operator, accounts and user creds, and checks them
  mftiko-seal             makes a TD instance's key pair and seals account credentials to it
  mftiko-conformance      SDK conformance suite
  mftiko-md-accept        drives the MD feed-path acceptance (deploy/accept)
examples/strategies/      hello, tape-keeper; import only sdk and contract, as an outside author would
migrations/               PostgreSQL schema: NNNN_name.up.sql / .down.sql, embedded into mftiko-migrate
deploy/dev/               local development services (PostgreSQL, etcd, NATS)
deploy/accept/            MD feed-path acceptance against a venue's testnet: compose stack, accept.sh, runbook
web/                      SvelteKit console (single-page app served by mftiko-api)
docs/                     DECISIONS.md, PROTOCOL.md
```

## Make

| target | does |
|---|---|
| `make` | `proto`, `build`, `examples`, `web` |
| `make tools` | installs protoc-gen-go into `bin/tools`; buf and protoc-gen-es come from `web/node_modules` |
| `make proto` | regenerates `contract/gen/` and `web/src/lib/gen/` from `contract/proto/`; commit the result with the `.proto` change |
| `make proto-lint` / `make proto-breaking` | buf lint / buf breaking against `main` (`BUF_AGAINST=` another buf input); drafts listed in `buf.yaml` are exempt (DECISIONS D007) |
| `make gen-check` | fails if `contract/gen/`, `web/src/lib/gen/`, `go.mod` or `go.sum` are not what `make proto` produces |
| `make build` | every command into `bin/` |
| `make examples` | example strategies into `bin/examples/` |
| `make test` / `make test-race` / `make vet` | Go tests / with the race detector, uncached / vet |
| `make check` | everything CI runs: `proto-lint`, `proto-breaking`, `gen-check`, `vet`, `build`, `test-race`, `web-check` |
| `make web` / `make web-dev` / `make web-check` | build, dev server, svelte-check |
| `make dev-up` / `make dev-down` | generate dev NATS credentials, then start / stop a local PostgreSQL, etcd and NATS (Docker); `dev-up` waits until all are healthy |
| `make accept-up` / `make accept-down` | start / stop the MD acceptance stack; the checks themselves are in `deploy/accept/README.md` |
| `make dev-nats-smoke` | run the dev NATS permission checks (`deploy/dev/nats-smoke.sh`) against the compose server |
| `make migrate-up` / `make migrate-down` / `make migrate-status` | apply / roll back one / list migrations against `$DATABASE_URL` |
| `make migrate-new NAME=...` | creates the next `NNNN_<name>.up.sql` / `.down.sql` pair |
| `make clean` | removes `bin/` and web build output |
| `make help` | lists targets |

Building and testing the Go code need only Go 1.26+: the generated code is committed. `make proto`, `make web` and `make check` also need Node 22+ and npm; Docker only for `make dev-up`. No other tools need to be installed by hand. `DATABASE_URL` defaults to the local PostgreSQL from `deploy/dev/compose.yaml`; its etcd is at `http://127.0.0.1:2379`, one member with no auth. Its NATS is at `nats://127.0.0.1:4222`, one server, core NATS only, no TLS. The generated server config sets `max_control_line` to 262144 (D827): a session JWT lists each feed, and the server default of 4096 rejects that CONNECT line. `make dev-up` generates the operator, account and user credentials into `deploy/dev/data/nats`, which is gitignored. MD and md.fetch use those creds.

## Tests

The tests of `internal/platform/state` run a real etcd (v3.7.2). They find it through `MFTIKO_ETCD_BIN`, else `etcd` on `PATH`, and are skipped when there is neither. Set `MFTIKO_REQUIRE_ETCD=1` to make a missing etcd a failure instead; CI does, so a run that skipped them cannot pass.

The tests of `internal/platform/natsx` start an embedded core NATS server (`natsxtest`, nats-server v2.15.0, the same release as the dev image). They do not use the compose service and need no extra binary. The tests of `internal/platform/natsdev` start an embedded server with the same operator, account and full-resolver config `make dev-up` writes, and generated credentials in a temporary directory.

The tests of `migrations` and `internal/sym` run a real PostgreSQL (`pgtest`). They find the server through `MFTIKO_PG_BIN` (the `postgres` binary; `initdb` is the one beside it), else `postgres` on `PATH`, and are skipped when there is neither. Set `MFTIKO_REQUIRE_PG=1` to make a missing binary a failure instead; CI does, so a run that skipped them cannot pass.

CI (`.github/workflows/ci.yml`) runs `make check` on every pull request and on `main`, with etcd downloaded from its release and checked against the release's `SHA256SUMS`, PostgreSQL installed from the runner image's packages, and `buf breaking` compared with the pull request's base.

## Logging and build identity

Every binary logs through `internal/platform/logger` (zerolog): JSON, one object per line, on stderr. Each line carries `component`, `version` and `commit`.

- `MFTIKO_LOG_LEVEL`: `trace`, `debug`, `info` (default), `warn`, `error`.
- `MFTIKO_LOG_FORMAT=console`: human-readable output for local development.

`make build` stamps `internal/platform/buildinfo` with the version (`git describe`, matching `vX.Y.Z` tags), the short commit hash and the UTC build time. Every binary prints them with `-version`.

## Other languages

The platform ships only the Go SDK. A strategy in another language works if its SDK follows PROTOCOL §9 and passes `mftiko-conformance` (DECISIONS D802).
