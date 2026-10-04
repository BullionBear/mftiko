// Package buildinfo carries the release identity, stamped in by the Makefile via -ldflags -X.
package buildinfo

// Version is the platform release. A worker and its shim keep the release of the
// agent that spawned them (DECISIONS D4).
var Version = "dev"
