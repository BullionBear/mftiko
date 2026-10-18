// Package daemon runs one controller term (DECISIONS D1301, D1312–D1321).
// It campaigns in the home region, writes the AgentRecord once, and queues
// later writes by region. Each region is fenced on its first write (D1305).
// One mirror per instance feeds the global view; a region that has not
// finished its first list is not written (D1317, D1318).
//
// Put and Delete are the write entry tests use until E13-04 connects
// derivation (D1313). The mftiko-controller process does not call them.
package daemon
