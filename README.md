# mftiko

Go trading framework with three independent process-control planes — strategy (STS), market data (MD) and execution (TD) — each run by its own agent. Workers sit behind a shim so an agent can rotate without stopping them. Control state lives in etcd (one cluster per region); data moves over NATS.

Design: [docs/DECISIONS.md](docs/DECISIONS.md). Protocols and contracts: [docs/PROTOCOL.md](docs/PROTOCOL.md).

> Status: layout only. Every command and package is a stub.

## Layout

Three blocks plus the exchange package (DECISIONS E00-D006); `internal/archtest` checks the dependency rules between them.

```
internal/                 ① the planes, private
  api/                    API server: declarations into etcd, serves the web console
  platform/               trading-agnostic runtime: agent skeleton, shim, state (etcd), natsx, artifact, sealbox, logger, buildinfo
  sts/                    strategy plane; conformance/ (SDK conformance suite)
  md/                     market-data plane; conn/ (conn worker), fetch/ (md.fetch)
  td/                     execution plane; account/ (account worker), history/ (PostgreSQL)
  sym/                    symbol service
  controller/             controller: pure decisions, daemon/ campaigns and writes
sdk/                      ② Go strategy SDK, the reference implementation of PROTOCOL §9; depends on contract only
contract/                 ③ what both ends compile against; source of truth is docs/PROTOCOL.md
  proto/mftiko/<pkg>/v1/  protobuf contracts
  gen/                    generated Go (make proto, make openapi; committed, do not edit)
  openapi/api/v1.yaml     contract A, the API server's OpenAPI document (make openapi)
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
  mftiko-pack             packs a strategy binary into an OCI artifact and pushes it
  mftiko-controller       controller: one leader, derives demands, fences per region
  mftiko-etcd-roles       prints each region's etcd users and roles, derived from the instance list; -apply applies them
  mftiko-retire-clean     deletes one retired instance's own etcd keys
  mftiko-nats-dev         generates dev NATS operator, accounts and user creds, and checks them
  mftiko-seal             makes a TD instance's key pair and seals account credentials to it
  mftiko-conformance      SDK conformance suite
  mftiko-md-accept        drives the MD feed-path acceptance (deploy/accept)
  mftiko-td-accept        drives the TD trading-path acceptance (deploy/accept)
  mftiko-api-accept       drives the end-to-end acceptance through mftiko-api (deploy/accept)
examples/strategies/      hello, tape-keeper; import only sdk and contract, as an outside author would
migrations/               PostgreSQL schema: NNNN_name.up.sql / .down.sql, embedded into mftiko-migrate
deploy/dev/               local development services (PostgreSQL, etcd, NATS, registry) and mftiko-api serving the console
deploy/accept/            MD, TD and end-to-end acceptance against a venue's testnet: compose stack, accept.sh, runbook
web/                      SvelteKit console (single-page app served by mftiko-api); e2e/ is the Playwright smoke
docs/                     DECISIONS.md, PROTOCOL.md, operations.md (logs and retention), release.md, cli.md
```

## Make

| target | does |
|---|---|
| `make` | `proto`, `openapi`, `build`, `examples`, `web` |
| `make tools` | `go install`s protoc-gen-go, buf, oapi-codegen and oasdiff into `bin/tools`, versions pinned (E10-D043, E10-D044) |
| `make proto` | regenerates `contract/gen/mftiko` from `contract/proto/`; commit the result with the `.proto` change |
| `make openapi` | Redocly lint, then regenerates `contract/gen/api/v1` and `web/src/lib/gen/api/v1.ts` (E10-D047) |
| `make proto-lint` / `make proto-breaking` | buf lint / buf breaking against `main` (`BUF_AGAINST=` another buf input); drafts listed in `buf.yaml` are exempt (DECISIONS E00-D007) |
| `make openapi-lint` / `make openapi-breaking` | Redocly lint / `oasdiff breaking` against `main` (`OAS_BASE=` another commit); a breaking change fails the build (E10-D116) |
| `make gen-check` | fails if `contract/gen/`, `web/src/lib/gen/`, `go.mod` or `go.sum` are not what `make proto` and `make openapi` produce |
| `make build` | every command into `bin/` |
| `make cli-dist` | CLI release tarballs and `SHA256SUMS` in `bin/dist/` (E11-D035) |
| `make server-dist` | host server tarball and `SHA256SUMS` in `bin/dist/`; needs `web/build` from `make web` (E12-D026) |
| `make examples` | example strategies into `bin/examples/` |
| `make test` / `make test-race` / `make vet` | Go tests / with the race detector, uncached / vet |
| `make check` | everything CI runs: `proto-lint`, `proto-breaking`, `openapi-lint`, `openapi-breaking`, `gen-check`, `vet`, `build`, `cli-dist`, `test-race`, `sdk-conformance`, `web-check`, `web`, `server-dist` |
| `make web` / `make web-dev` / `make web-check` | build, dev server, svelte-check |
| `make dev-up` / `make dev-down` | generate dev NATS credentials and API config (`deploy/dev/data/api`, token in `token`), build a Linux `mftiko-api` and the console, then start / stop a local PostgreSQL, etcd, NATS, registry and the API with the console on `127.0.0.1:8080` (Docker); `dev-up` waits until all are healthy |
| `make accept-up` / `make accept-down` | start / stop the acceptance stack; the checks themselves, including the end-to-end ones (`accept.sh e2e-up`, `e2e-all`, `console-smoke`), are in `deploy/accept/README.md` |
| `make dev-nats-smoke` | run the dev NATS permission checks (`deploy/dev/nats-smoke.sh`) against the compose server |
| `make migrate-up` / `make migrate-status` | apply pending migrations / list them against `$DATABASE_URL` |
| `make migrate-new NAME=...` | creates the next `NNNN_<name>.up.sql` / `.down.sql` pair |
| `make clean` | removes `bin/` and web build output |
| `make help` | lists targets |

Building and testing the Go code need only Go 1.26+: the generated code is committed. `make openapi`, `make web`, `make check` and `make dev-up` also need Node 22+ and npm; `make proto` needs only the Go tools from `make tools`. Docker only for `make dev-up` and `deploy/accept`. No other tools need to be installed by hand. `DATABASE_URL` defaults to the local PostgreSQL from `deploy/dev/compose.yaml`; its etcd is at `http://127.0.0.1:2379`, one member with no auth. Its NATS is at `nats://127.0.0.1:4222`, one server, core NATS only, no TLS. The generated server config sets `max_control_line` to 262144 (E08-D027): a session JWT lists each feed, and the server default of 4096 rejects that CONNECT line. `make dev-up` generates the operator, account and user credentials into `deploy/dev/data/nats`, which is gitignored. MD and md.fetch use those creds. The dev compose registry is `registry:2` at `127.0.0.1:5000`, with no authentication (E09-D010).

## CLI

`mftiko` is the command-line client for the API. Each `v*` tag's GitHub Release has binaries for darwin/arm64, darwin/amd64, linux/amd64 and linux/arm64. You can also run `go install github.com/BullionBear/mftiko/cmd/mftiko@<tag>`, or `make build`. [docs/cli.md](docs/cli.md) covers installation, the config file and tokens, a first session with `hello`, and the exit codes. `mftiko version` prints the CLI release and the `api.v1` version it was built against.

The same Release carries `mftiko-server_<tag>_linux_amd64.tar.gz`: the host binaries, the console, and `bin/mftiko-release`, which installs and switches versions on a deploy machine. [docs/release.md](docs/release.md) covers the tarball, the switch order and the checksum.

## Packing a strategy

`mftiko-pack` packs a binary that is already built and pushes the image (E09-D002, E09-D036).

```
make examples
make build
bin/mftiko-pack -bin bin/examples/hello -name hello -repo 127.0.0.1:5000/hello -plain-http
```

`make dev-up` starts the registry. The line printed on success is the image manifest digest, which is `StrategyRef.code`. `-tag` is optional and only names the image for a person to look up; the same binary and parameters pack to the same digest either way (E09-D038). A registry that asks for a login is read from `~/.docker/config.json` (E09-D040). `-plain-http` is for the dev registry.

## API authentication

Each person has one static token. The API JSON stores `users[].name` and `users[].token_sha256`, the hex SHA-256 of the token, and never the token itself (E10-D009, E10-D055). The API cannot check the length, so generate at least 32 random bytes and keep the token somewhere the config file does not:

```sh
token=$(openssl rand -base64 32 | tr -d '\n')
printf '%s' "$token" | sha256sum
```

Put the hex digest in `users[].token_sha256`. Clients send the token string itself. The CLI and other automation send `Authorization: Bearer <token>`. The console posts it to `POST /api/v1/auth/login`, which sets an `HttpOnly`, `Secure`, `SameSite=Strict` cookie named `mftiko_session`. `Secure` is set on plaintext HTTP as well (E10-D050). `POST /api/v1/auth/logout` clears that cookie. There is no login rate limit in this phase (E10-D055).

The cookie HMAC key (`MFTIKO_COOKIE_KEY` or `-cookie-key`) is shared by every replica. The Postgres DSN (`MFTIKO_POSTGRES_DSN` or `-postgres-dsn`) is the audit database. The process opens a pool at startup and does not wait for it to connect; `/readyz` still reports etcd only (E10-D048). `GET /metrics` is Prometheus text for this process's own gauges (E12-D038). Apply `migrations/0003_audit` before expecting audit rows. A failed audit write does not change the HTTP response.

## Tests

The tests of `internal/platform/state` run a real etcd (v3.7.2). They find it through `MFTIKO_ETCD_BIN`, else `etcd` on `PATH`, and are skipped when there is neither. Set `MFTIKO_REQUIRE_ETCD=1` to make a missing etcd a failure instead; CI does, so a run that skipped them cannot pass.

The tests of `internal/platform/natsx` start an embedded core NATS server (`natsxtest`, nats-server v2.15.0, the same release as the dev image). They do not use the compose service and need no extra binary. The tests of `internal/platform/natsdev` start an embedded server with the same operator, account and full-resolver config `make dev-up` writes, and generated credentials in a temporary directory.

The tests of `migrations` and `internal/sym` run a real PostgreSQL (`pgtest`). They find the server through `MFTIKO_PG_BIN` (the `postgres` binary; `initdb` is the one beside it), else `postgres` on `PATH`, and are skipped when there is neither. Set `MFTIKO_REQUIRE_PG=1` to make a missing binary a failure instead; CI does, so a run that skipped them cannot pass.

CI (`.github/workflows/ci.yml`) runs `make check` on every pull request and on `main`, with etcd downloaded from its release and checked against the release's `SHA256SUMS`, PostgreSQL installed from the runner image's packages, and `buf breaking` and `oasdiff breaking` compared with the pull request's base. Until E10-20, `oasdiff` prints its report and does not fail the job (E10-D046).

`internal/sts/conformance` is the SDK conformance harness (PROTOCOL §9.8, E08-D028–E08-D039). `go test -race` runs it against `fakeworker`: one compliant pass, and one deliberate failure for each check (E08-D034). Those tests poll observable conditions. Every pull request also runs the full Go SDK suite: `make sdk-conformance` builds `examples/strategies/conform` and runs it through `mftiko-conformance` (E08-D064). Try a worker locally with `mftiko-conformance -- <worker argv>`.

## Logging and build identity

Every binary logs through `internal/platform/logger` (zerolog): JSON, one object per line, on stderr. Each line carries `component`, `version` and `commit`.

- `MFTIKO_LOG_LEVEL`: `trace`, `debug`, `info` (default), `warn`, `error`.
- `MFTIKO_LOG_FORMAT=console`: human-readable output for local development.

`make build` stamps `internal/platform/buildinfo` with the version (`git describe`, matching `vX.Y.Z` tags), the short commit hash and the UTC build time. Every binary prints them with `-version`.

## Other languages

The platform ships only the Go SDK. A strategy in another language works if its SDK follows PROTOCOL §9 and passes `mftiko-conformance` (DECISIONS E08-D002). The Go standard strategy is `examples/strategies/conform`, and CI runs that binary through the harness on every pull request (E08-D064). Claiming a contract in a manifest after the suite passes is a release rule; the STS agent does not read the report at spawn (E08-D032).
