// Package api is the API server: declarative start and end of sessions, accounts, the strategy catalog, and the
// read side for the web console and the CLI (E10-D001).
//
// The contract is the OpenAPI document contract/openapi/api/v1.yaml (E10-D002), not the platform's protos: requests are
// converted to session.v1 SessionSpec and state.v1 Account here, and nothing outside this package sees those types.
// Writes go to etcd under the shared "api" user, guarded by create-only and mod_revision conditions instead of an
// election (PROTOCOL §3.2 W10, E10-D006). They go through state.APIWriter, whose scope lets the API put specs and accounts
// and delete statuses, never put them (E10-D042). A lost condition is 409 conflict; etcd not reached is 503
// region_unavailable. session_id and api_id are drawn at random and checked against the mirrors of the available
// regions, then created create-only; the region written to must be available (E10-D007, E10-D008, E10-D042). Reads are answered
// from one Mirror per cluster (E10-D014). Session and instance reads, session records, and SSE are E10-D092–E10-D097.
// Each SSE connection watches etcd from Last-Event-ID and does not read the mirror's history (E10-D094).
// Every /api request is authenticated, except POST /api/v1/auth/login
// (E10-D009, E10-D051). Cookie writes and login need Content-Type: application/json; bearer writes do not (E10-D110). On reply the audit row is queued and written to Postgres; /healthz, /readyz, and /metrics are not
// (E10-D010, E10-D049, E12-D038). The strategy catalog is served from the registry (E10-D056–E10-D061). Load checks the DSN and the
// registry settings and does not dial. Run creates a Postgres pool and does not wait for it to connect;
// readiness still watches etcd only (E10-D048, modifies E10-D024). A down Postgres makes session create return
// 503 and skip etcd (E10-D031); audit writes fail the same way. Create and end of a session, the
// session_records row, and reconcile are E10-D068–E10-D073. W5 deletion runs in that same reconcile
// and on DELETE /api/v1/sessions/{id} (E10-D080–E10-D085): Postgres is closed first, and a down
// Postgres deletes nothing. A catalog
// request dials the registry when one is configured (E10-D057, E10-D060).
// Either of the command's file flags may be "-" to read that file from stdin; only one of them may.
//
// Settings are three kinds (E10-D023), the same split cmd/mftiko-api parses. The deployment JSON is the file
// mftiko-etcd-roles reads (E10-D012, E13-D012). The API JSON is:
//
//	users[].name, users[].token_sha256   one static token per user, stored as SHA-256 (E10-D009, E10-D055)
//	repositories                          strategy repositories, path without the registry host (E10-D013, E10-D060)
//	registry                              registry host[:port]; empty leaves the catalog unconfigured (E10-D060)
//	registry_creds                        Docker config.json path; empty is anonymous (E09-D033, E10-D060)
//	plain_http                            http for a dev registry:2 (E09-D033, E10-D060)
//	td_keys                               TD instance name → public key, the mftiko-seal pubkey form (E10-D011)
//	retention                             W5 retention, Go duration, default 168h (E10-D017)
//	unavailable_after                     region unavailable threshold, Go duration, default 15s (E10-D014)
//	listen                                listen address, default :8080 (E10-D029)
//	shutdown_timeout                      drain limit after SIGTERM, Go duration, default 10s (E10-D027)
//
// Account kinds are not in either JSON file. The command sets Config.Kinds from the exchange registry
// (E10-D034). This package does not import exchange. Creating an account seals credentials to that TD
// instance's public key (E06-D013, E10-D062). Reads return no credential material (E10-D063).
//
// A zero or omitted duration takes the default. Secrets are not in either JSON file. They are the etcd user "api"
// password (MFTIKO_ETCD_PASSWORD, flag -etcd-password), the cookie HMAC key (MFTIKO_COOKIE_KEY, flag -cookie-key)
// and the Postgres DSN (MFTIKO_POSTGRES_DSN, flag -postgres-dsn). The flag overrides the environment variable, as
// it does for the controller.
//
// Load validates all of that, including the registry address and credentials file, and does not dial Postgres or a
// registry (E10-D024, E10-D060). Run opens a Postgres pool without waiting for a connection (E10-D048). Readiness watches
// etcd only. Session create returns 503 and does not write etcd when Postgres is down (E10-D031). Catalog requests are what dial the registry (E10-D057). A token should be at least 32 random bytes; the
// file stores only the SHA-256, so a shorter value that was hashed in is still accepted (E10-D055). The README has
// the generation command.
// Run dials every region and keeps one Mirror per cluster on the whole /mftiko/v1/ keyspace, not one mirror per
// prefix (E10-D025). /healthz is 200 while the process is up. /readyz is 200 when any region is available; the body
// lists every region and unavailable_regions. It is 503 only when every region is unavailable, and during shutdown
// (E10-D026, E10-D027). GET and HEAD /metrics are Prometheus text for this process, matched before static files,
// unauthenticated, and not in the OpenAPI document (E12-D038). SIGTERM marks readiness 503, stops accepting, waits for in-flight requests up to
// shutdown_timeout and returns nil so the process exits 0. SIGINT takes that same path.
//
// The console files are read from disk when a web directory is set (web/build), not embedded. The directory is
// opened once (os.Root), so replacing it needs a restart (E10-D114). A path that is not
// under /api/ and does not name a file returns index.html. With no web directory the server has only the API
// routes (E10-D028). A certificate and key together serve HTTPS with HTTP/2; without them the listener is plaintext
// HTTP for development (E10-D029).
package api
