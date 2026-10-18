// Package controller is the controller's pure logic (DECISIONS D1301). It names
// values the daemon writes, derives demands from session specs (N1), and plans
// the deletes of a session that has ended and of demands whose session is gone
// (N4, N5). The gate is a later ticket. Input is the global view (D1320);
// output is the keys to write and to delete. Nothing here dials a cluster, so
// the package tests without etcd. The wiring is internal/controller/daemon.
package controller
