// Package buildinfo carries the build identity, stamped in by the Makefile via -ldflags -X.
package buildinfo

// Set at build time. A worker and its shim keep the release of the agent that spawned them (DECISIONS D103).
var (
	// Version is `git describe --tags --always --dirty --match='v[0-9]*.[0-9]*.[0-9]*'`.
	Version = "dev"
	// CommitHash is the short commit hash.
	CommitHash = "unknown"
	// BuildTime is the UTC build time, RFC 3339.
	BuildTime = "unknown"
)

// String is the one-line form printed by -version.
func String() string {
	return Version + " (commit " + CommitHash + ", built " + BuildTime + ")"
}
