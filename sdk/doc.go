// Package sdk is the Go strategy SDK and the reference implementation of the session worker
// contract (DECISIONS E08-D002, PROTOCOL §9). A strategy is a Go program whose main calls Run.
//
// A strategy implements Strategy and the optional hooks it needs (strategy.go), and acts through
// Context (context.go). Instruments are UIDs throughout (DECISIONS E08-D004, E08-D023–E08-D026). Resolve and
// Info are implemented by sdk/internal/symclient. The TD path (Submit, Cancel, BatchCancel, the
// own-algo order book, and the first Recon) is sdk/internal/tdpath (DECISIONS E08-D040–E08-D045). The MD
// path (bindings, live delivery, Tape, BookSnapshot, and md.fetch) is sdk/internal/mdpath
// (DECISIONS E08-D046–E08-D049). The local order book, fed by md.view and orderbook_delta, is
// sdk/internal/bookpath (DECISIONS E08-D057–E08-D061). Run wires those clients, reports
// bindings_applied from the MD path, and leaves volume reads to the strategy
// (DECISIONS E08-D050–E08-D052, E09-D004).
package sdk
