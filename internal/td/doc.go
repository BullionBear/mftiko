// Package td is the execution plane. The agent is in internal/td/agent: it starts one account worker
// per api_id that sessions need and writes their acks (D901, D905, D616). The account worker is in
// internal/td/account (D603, D606). The PostgreSQL schema is internal/td/history (D601, E7).
// Who writes fills is open and is not the TD agent (D638).
package td
