// Package conformance is the SDK conformance harness (D828–D839, PROTOCOL §9.8).
//
// It launches a worker command, speaks C4 on that process's fd 3, and runs an
// embedded nats-server with fake MD, TD, and SYM peers. Each check is pass,
// fail, or skip. go test runs the fakeworker violations (D834). Every pull
// request also runs examples/strategies/conform through this harness (D864).
package conformance
