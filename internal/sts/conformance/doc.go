// Package conformance is the SDK conformance harness (D828–D839, PROTOCOL §9.8).
//
// It launches a worker command, speaks C4 on that process's fd 3, and runs an
// embedded nats-server with fake MD and TD peers. Each check is pass, fail, or
// skip. This phase does not require sdk.Run to pass; that is E8-13 (D834).
package conformance
