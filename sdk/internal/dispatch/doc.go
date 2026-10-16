// Package dispatch is the session worker's single dispatcher (DECISIONS D803, D815–D817).
//
// One goroutine runs every hook, so a strategy needs no locks. Two buffers feed it. The TD buffer
// holds orders, timers, Post, and read replies: FIFO, never dropped, 65536 entries, and a full
// buffer ends the session FAILED. The MD buffer is one preallocated 32 MiB byte ring of
// variable-length messages: when a message does not fit, the oldest messages are dropped until it
// does, except the message a hook is reading. A message larger than the ring is dropped and counted.
//
// After each hook the TD buffer is taken first. An MD message is taken only when TD is empty. The
// prehook then reads the envelope timestamp and drops a message older than recv_window_ms without
// decoding it. orderbook_delta does not take that path (E8-15 applies it).
//
// A hook longer than 100ms counts one hook_slow. hook_blocked_ms is how long the current hook has
// been running. A hook longer than hook_timeout_s makes Run return *c4.Exit with outcome HUNG;
// this package does not send Exiting. OnStop is c4.Stopper: c4 calls it after Exiting, and the
// TD drain and the strategy callback run on the dispatcher goroutine.
package dispatch
