// Package sdk is the Go strategy SDK and the reference implementation of the session worker
// contract (DECISIONS D802, PROTOCOL §9). A strategy is a Go program whose main calls Run.
//
// A strategy implements Strategy and the optional hooks it needs (strategy.go), and acts through
// Context (context.go). Instruments are UIDs throughout (DECISIONS D804, D823–D826). Resolve and
// Info are implemented by sdk/internal/symclient. Run does not wire that client yet.
//
// The API is defined; Run is not implemented yet.
package sdk
