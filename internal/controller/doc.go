// Package controller is the controller's pure logic (DECISIONS D1301). It names
// values the daemon writes and, in later tickets, the derivation, the gate and
// the lifecycle diff. Input will be the global view; output will be the keys to
// write and delete. Nothing here dials a cluster, so the package tests without
// etcd. The wiring is internal/controller/daemon.
package controller
