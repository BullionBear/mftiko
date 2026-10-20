// Package api is the API server: declarative start and end of sessions, accounts, the strategy catalog, and the
// read side for the web console and the CLI (D1001).
//
// The contract is the OpenAPI document contract/openapi/api/v1.yaml (D1002), not the platform's protos: requests are
// converted to session.v1 SessionSpec and state.v1 Account here, and nothing outside this package sees those types.
// Writes go to etcd under the shared "api" user, guarded by create-only and mod_revision conditions instead of an
// election (PROTOCOL §3.2 W10, D1006). Reads are answered from one Mirror per cluster (D1014). Every request is
// written to the Postgres audit table on reply (D1010).
//
// Shape only: not implemented yet.
package api
