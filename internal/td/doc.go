// Package td is the execution plane. The agent is in internal/td/agent: it starts one account worker
// per api_id that sessions need and writes their acks (E09-D001, E09-D005, E06-D016). The account worker is in
// internal/td/account (E06-D003, E06-D006). Trade history is not stored here (E21-D001).
package td
