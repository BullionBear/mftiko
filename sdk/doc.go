// Package sdk is the Go strategy SDK and the reference implementation of the session worker
// contract (DECISIONS D802, PROTOCOL §9). A strategy is a Go program whose main calls Run.
//
// A strategy implements Strategy and the optional hooks it needs (strategy.go), and acts through
// Context (context.go). Instruments are UIDs throughout (DECISIONS D804, D823–D826). Resolve and
// Info are implemented by sdk/internal/symclient. The TD path (Submit, Cancel, BatchCancel, the
// own-algo order book, and the first Recon) is sdk/internal/tdpath (DECISIONS D840–D845). The MD
// path (bindings, live delivery, Tape, BookSnapshot, and md.fetch) is sdk/internal/mdpath
// (DECISIONS D846–D849). Run wires those clients, reports bindings_applied from the MD
// path, and leaves volume reads to the strategy (DECISIONS D850–D852, D904).
package sdk
