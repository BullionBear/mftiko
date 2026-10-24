// Package conformance is the SDK conformance harness (E08-D028–E08-D039, PROTOCOL §9.8).
//
// It launches a worker command, speaks C4 on that process's fd 3, and runs an
// embedded nats-server with fake MD, TD, and SYM peers. Each check is pass,
// fail, or skip. go test runs the fakeworker violations (E08-D034). Every pull
// request also runs examples/strategies/conform through this harness (E08-D064).
package conformance
