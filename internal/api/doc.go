// Package api is the API server: declarative start and end of sessions, accounts, the strategy catalog, and the
// read side for the web console and the CLI (D1001).
//
// The contract is the OpenAPI document contract/openapi/api/v1.yaml (D1002), not the platform's protos: requests are
// converted to session.v1 SessionSpec and state.v1 Account here, and nothing outside this package sees those types.
// Writes go to etcd under the shared "api" user, guarded by create-only and mod_revision conditions instead of an
// election (PROTOCOL §3.2 W10, D1006). Reads are answered from one Mirror per cluster (D1014). Every request is
// written to the Postgres audit table on reply (D1010). Endpoints, authentication, audit and W5 deletion are later
// tickets (D1024): this process reads and checks the settings for them and does not connect to Postgres or a registry.
// Either of the command's file flags may be "-" to read that file from stdin; only one of them may.
//
// Settings are three kinds (D1023), the same split cmd/mftiko-api parses. The deployment JSON is the file
// mftiko-etcd-roles reads (D1012, D1312). The API JSON is:
//
//	users[].name, users[].token_sha256   one static token per user, stored as SHA-256 (D1009)
//	repositories                          strategy repositories (D1013)
//	td_keys                               TD instance name → public key, the mftiko-seal pubkey form (D1011)
//	retention                             W5 retention, Go duration, default 168h (D1017)
//	unavailable_after                     region unavailable threshold, Go duration, default 15s (D1014)
//	listen                                listen address, default :8080 (D1029)
//	shutdown_timeout                      drain limit after SIGTERM, Go duration, default 10s (D1027)
//
// A zero or omitted duration takes the default. Secrets are not in either JSON file. They are the etcd user "api"
// password (MFTIKO_ETCD_PASSWORD, flag -etcd-password), the cookie HMAC key (MFTIKO_COOKIE_KEY, flag -cookie-key)
// and the Postgres DSN (MFTIKO_POSTGRES_DSN, flag -postgres-dsn). The flag overrides the environment variable, as
// it does for the controller.
//
// Load validates all of that and does not connect to Postgres or a registry (D1024). Readiness watches etcd only.
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
