// Package controller is the controller's pure logic (DECISIONS D1301). It names
// values the daemon writes and derives demands from session specs (N1). Later
// tickets add the gate and the lifecycle diff. Input is the global view
// (D1320); output is the keys to write. Nothing here dials a cluster, so the
// package tests without etcd. The wiring is internal/controller/daemon.
package controller
