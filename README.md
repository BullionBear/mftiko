# mftiko

Go trading framework with three independent process-control planes — strategy (STS), market data (MD) and execution (TD) — each run by its own agent. Workers sit behind a shim so an agent can rotate without stopping them. Control state lives in etcd (one cluster per region); data moves over NATS.

Design: [docs/DECISIONS.md](docs/DECISIONS.md). Protocols and contracts: [docs/PROTOCOL.md](docs/PROTOCOL.md).

> Status: layout only. Every command and package is a stub.

## Layout

```
cmd/                      one directory per binary (bin/<name> after `make build`)
  mftiko-api              API server: declarations into etcd, serves the web console
  mftiko-sts-agent        strategy-plane agent
  mftiko-md-agent         market-data-plane agent
  mftiko-td-agent         execution-plane agent (also backfill and history drain)
  mftiko-shim             per-worker shim
  mftiko-md-conn          MD conn worker: one websocket per (venue, route, n)
  mftiko-td-account       TD account worker: one per api_id
  mftiko-md-fetch         stateless md.fetch service
  mftiko-sym              symbol service
  mftiko-migrate          applies the PostgreSQL migrations (a deploy step)
  mftiko-pack             builds a strategy artifact (OCI image + mftiko manifest)
  mftiko-conformance      SDK conformance suite
internal/                 platform packages (agent skeleton, planes, shim, workers, state, ...)
  exchange/               exchange adapters: interfaces, registry; one package per exchange (binance, okx, bybit)
  symbol/                 UniversalTicker, venue registry, SYM client (resolve before calling an adapter)
sdk/                      Go strategy SDK, the reference implementation of PROTOCOL §9
examples/strategies/      hello, tape-keeper
proto/mftiko/<pkg>/v1/    protobuf contracts; source of truth is docs/PROTOCOL.md
gen/                      generated Go (make proto, not committed)
migrations/               PostgreSQL schema: NNNN_name.up.sql / .down.sql, embedded into mftiko-migrate
deploy/dev/               local development services (PostgreSQL)
web/                      SvelteKit console (single-page app served by mftiko-api)
docs/                     DECISIONS.md, PROTOCOL.md
```

## Make

| target | does |
|---|---|
| `make` | `proto`, `build`, `examples`, `web` |
| `make tools` | installs protoc-gen-go into `bin/tools`; buf and protoc-gen-es come from `web/node_modules` |
| `make proto` | generates `gen/` and `web/src/lib/gen/` from `proto/` |
| `make proto-lint` / `make proto-breaking` | buf lint / buf breaking against `main` |
| `make build` | every command into `bin/` |
| `make examples` | example strategies into `bin/examples/` |
| `make test` / `make vet` / `make check` | Go tests / vet / everything CI runs |
| `make web` / `make web-dev` / `make web-check` | build, dev server, svelte-check |
| `make db-up` / `make db-down` | start / stop a local PostgreSQL (Docker) |
| `make migrate-up` / `make migrate-down` / `make migrate-status` | apply / roll back one / list migrations against `$DATABASE_URL` |
| `make migrate-new NAME=...` | creates the next `NNNN_<name>.up.sql` / `.down.sql` pair |
| `make clean` | removes `bin/`, `gen/`, web build output and generated TS |
| `make help` | lists targets |

Requires Go 1.26+, Node 22+ and npm; Docker only for `make db-up`. No other tools need to be installed by hand. `DATABASE_URL` defaults to the local PostgreSQL from `deploy/dev/compose.yaml`.

## Logging and build identity

Every binary logs through `internal/logger` (zerolog): JSON, one object per line, on stderr. Each line carries `component`, `version` and `commit`.

- `MFTIKO_LOG_LEVEL`: `trace`, `debug`, `info` (default), `warn`, `error`.
- `MFTIKO_LOG_FORMAT=console`: human-readable output for local development.

`make build` stamps `internal/buildinfo` with the version (`git describe`, matching `vX.Y.Z` tags), the short commit hash and the UTC build time. Every binary prints them with `-version`.

## Other languages

The platform ships only the Go SDK. A strategy in another language works if its SDK follows PROTOCOL §9 and passes `mftiko-conformance` (DECISIONS D19).
