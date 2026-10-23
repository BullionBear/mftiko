// Package api is the API server: declarative start and end of sessions, accounts, the strategy catalog, and the
// read side for the web console and the CLI (D1001).
//
// The contract is the OpenAPI document contract/openapi/api/v1.yaml (D1002), not the platform's protos: requests are
// converted to session.v1 SessionSpec and state.v1 Account here, and nothing outside this package sees those types.
// Writes go to etcd under the shared "api" user, guarded by create-only and mod_revision conditions instead of an
// election (PROTOCOL §3.2 W10, D1006). They go through state.APIWriter, whose scope lets the API put specs and accounts
// and delete statuses, never put them (D1042). A lost condition is 409 conflict; etcd not reached is 503
// region_unavailable. session_id and api_id are drawn at random and checked against the mirrors of the available
// regions, then created create-only; the region written to must be available (D1007, D1008, D1042). Reads are answered
// from one Mirror per cluster (D1014). Every /api request is authenticated, except POST /api/v1/auth/login
// (D1009, D1051). On reply the audit row is queued and written to Postgres; /healthz and /readyz are not
// (D1010, D1049). The strategy catalog is served from the registry (D1056–D1061). Load checks the DSN and the
// registry settings and does not dial. Run creates a Postgres pool and does not wait for it to connect;
// readiness still watches etcd only (D1048, modifies D1024). A down Postgres makes session create return
// 503 and skip etcd (D1031); audit writes fail the same way. Create and end of a session, the
// session_records row, and reconcile are D1068–D1073. A catalog
// request dials the registry when one is configured (D1057, D1060). W5 deletion is a later ticket.
// Either of the command's file flags may be "-" to read that file from stdin; only one of them may.
//
// Settings are three kinds (D1023), the same split cmd/mftiko-api parses. The deployment JSON is the file
// mftiko-etcd-roles reads (D1012, D1312). The API JSON is:
//
//	users[].name, users[].token_sha256   one static token per user, stored as SHA-256 (D1009, D1055)
//	repositories                          strategy repositories, path without the registry host (D1013, D1060)
//	registry                              registry host[:port]; empty leaves the catalog unconfigured (D1060)
//	registry_creds                        Docker config.json path; empty is anonymous (D933, D1060)
//	plain_http                            http for a dev registry:2 (D933, D1060)
//	td_keys                               TD instance name → public key, the mftiko-seal pubkey form (D1011)
//	retention                             W5 retention, Go duration, default 168h (D1017)
//	unavailable_after                     region unavailable threshold, Go duration, default 15s (D1014)
//	listen                                listen address, default :8080 (D1029)
//	shutdown_timeout                      drain limit after SIGTERM, Go duration, default 10s (D1027)
//
// Account kinds are not in either JSON file. The command sets Config.Kinds from the exchange registry
// (D1034). This package does not import exchange. Creating an account seals credentials to that TD
// instance's public key (D613, D1062). Reads return no credential material (D1063).
//
// A zero or omitted duration takes the default. Secrets are not in either JSON file. They are the etcd user "api"
// password (MFTIKO_ETCD_PASSWORD, flag -etcd-password), the cookie HMAC key (MFTIKO_COOKIE_KEY, flag -cookie-key)
// and the Postgres DSN (MFTIKO_POSTGRES_DSN, flag -postgres-dsn). The flag overrides the environment variable, as
// it does for the controller.
//
// Load validates all of that, including the registry address and credentials file, and does not dial Postgres or a
// registry (D1024, D1060). Run opens a Postgres pool without waiting for a connection (D1048). Readiness watches
// etcd only. Session create returns 503 and does not write etcd when Postgres is down (D1031). Catalog requests are what dial the registry (D1057). A token should be at least 32 random bytes; the
// file stores only the SHA-256, so a shorter value that was hashed in is still accepted (D1055). The README has
// the generation command.
// Run dials every region and keeps one Mirror per cluster on the whole /mftiko/v1/ keyspace, not one mirror per
// prefix (D1025). /healthz is 200 while the process is up. /readyz is 200 when any region is available; the body
// lists every region and unavailable_regions. It is 503 only when every region is unavailable, and during shutdown
// (D1026, D1027). SIGTERM marks readiness 503, stops accepting, waits for in-flight requests up to
// shutdown_timeout and returns nil so the process exits 0. SIGINT takes that same path.
//
// The console files are read from disk when a web directory is set (web/build), not embedded. A path that is not
// under /api/ and does not name a file returns index.html. With no web directory the server has only the API
// routes (D1028). A certificate and key together serve HTTPS with HTTP/2; without them the listener is plaintext
// HTTP for development (D1029).
package api
